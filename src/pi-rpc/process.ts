import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process'
import crossSpawn from 'cross-spawn'
import { StringDecoder } from 'node:string_decoder'
import { getPiCommand, shouldUseShellForPiCommand } from './command.js'

export class PiRpcSpawnError extends Error {
  /** Spawn or startup error code, e.g. ENOENT, PI_RPC_STARTUP_TIMEOUT */
  code?: string

  constructor(message: string, opts?: { code?: string; cause?: unknown }) {
    super(message)
    this.name = 'PiRpcSpawnError'
    this.code = opts?.code
    ;(this as any).cause = opts?.cause
  }
}

const ESC = String.fromCharCode(0x1b)
const CSI = String.fromCharCode(0x9b)

const ANSI_ESCAPE_REGEX = new RegExp(
  `[${ESC}${CSI}][[\\]()#;?]*(?:[0-9]{1,4}(?:;[0-9]{0,4})*)?[0-9A-ORZcf-nqry=><]`,
  'g'
)

function stripAnsi(s: string): string {
  // Basic ANSI escape stripping (colors, cursor movement, etc.)
  return s.replace(ANSI_ESCAPE_REGEX, '')
}

type PiRpcCommand =
  | { type: 'prompt'; id?: string; message: string; images?: unknown[] }
  | { type: 'abort'; id?: string }
  | { type: 'get_state'; id?: string }
  // Model
  | { type: 'get_available_models'; id?: string }
  | { type: 'set_model'; id?: string; provider: string; modelId: string }
  // Thinking
  | { type: 'get_available_thinking_levels'; id?: string }
  | { type: 'set_thinking_level'; id?: string; level: string }
  // Modes
  | { type: 'set_follow_up_mode'; id?: string; mode: 'all' | 'one-at-a-time' }
  | { type: 'set_steering_mode'; id?: string; mode: 'all' | 'one-at-a-time' }
  // Compaction
  | { type: 'compact'; id?: string; customInstructions?: string }
  | { type: 'set_auto_compaction'; id?: string; enabled: boolean }
  // Session
  | { type: 'get_session_stats'; id?: string }
  | { type: 'set_session_name'; id?: string; name: string }
  | { type: 'export_html'; id?: string; outputPath?: string }
  | { type: 'switch_session'; id?: string; sessionPath: string }
  // Messages
  | { type: 'get_messages'; id?: string }
  // Commands
  | { type: 'get_commands'; id?: string }

type PiRpcResponse = {
  type: 'response'
  id?: string
  command: string
  success: boolean
  data?: unknown
  error?: string
}

type PiExtensionUiResponse =
  | { id: string; value: string }
  | { id: string; confirmed: boolean }
  | { id: string; cancelled: true }

export type PiRpcEvent = Record<string, unknown>

/** Maximum wait for an auxiliary context-usage update. */
export const SESSION_STATS_TIMEOUT_MS = 1_000

/**
 * Shape of `stats.contextUsage` in pi's `get_session_stats` response.
 * `tokens` is null while pi has no trustworthy token count (e.g. right after compaction).
 */
export type PiContextUsage = {
  tokens?: number | null
  contextWindow?: number | null
}

export type PiSessionStats = {
  sessionId?: string
  sessionFile?: string
  totalMessages?: number
  cost?: number
  tokens?: {
    input?: number
    output?: number
    cacheRead?: number
    cacheWrite?: number
    total?: number
  }
  contextUsage?: PiContextUsage | null
}

const DEFAULT_STARTUP_TIMEOUT_MS = 30_000
const MAX_EARLY_EVENTS = 256
const MAX_EARLY_EVENT_BYTES = 1024 * 1024
const SHUTDOWN_GRACE_MS = 1000
const STARTUP_INTERACTIVE_METHODS = new Set(['confirm', 'select', 'input', 'editor'])

type SpawnParams = {
  cwd: string
  /** Optional override for `pi` executable name/path */
  piCommand?: string
  /** If set, pi will persist the session to this exact file (via `--session <path>`). */
  sessionPath?: string
  /** Readiness deadline; defaults to PI_ACP_STARTUP_TIMEOUT_MS or 30 seconds. */
  startupTimeoutMs?: number
}

export class PiRpcProcess {
  private static readonly activeProcesses = new Set<PiRpcProcess>()
  private readonly child: ChildProcessWithoutNullStreams
  private readonly pending = new Map<string, { resolve: (v: PiRpcResponse) => void; reject: (e: unknown) => void }>()
  private eventHandlers: Array<(ev: PiRpcEvent) => void> = []
  private readonly preludeLines: string[] = []
  private exited = false
  private exitError: Error | null = null
  private readonly closed: Promise<void>
  private starting = true
  private terminalError: Error | null = null
  private killTimer: ReturnType<typeof setTimeout> | undefined
  private bufferingEvents = true
  private readonly earlyEvents: PiRpcEvent[] = []
  private earlyEventBytes = 0
  private startupState: unknown

  /** The state returned by the readiness handshake, before any session commands. */
  get initialState(): unknown {
    return this.startupState
  }

  private constructor(child: ChildProcessWithoutNullStreams) {
    this.child = child
    PiRpcProcess.activeProcesses.add(this)
    this.closed = new Promise(resolve => {
      child.once('close', (code, signal) => {
        this.exited = true
        clearTimeout(this.killTimer)
        this.fail(this.exitError ?? new Error(`pi process closed (code=${code}, signal=${signal})`))
        PiRpcProcess.activeProcesses.delete(this)
        resolve()
      })
    })

    // Pi RPC uses strict LF-only JSONL framing. Node readline also splits on
    // U+2028/U+2029 which are valid inside JSON strings, corrupting the stream.
    // Use a manual buffer that splits only on \n.
    const decoder = new StringDecoder('utf8')
    let buf = ''

    const onLine = (line: string) => {
      if (line.endsWith('\r')) line = line.slice(0, -1)
      if (!line.trim()) return
      let msg: any
      try {
        msg = JSON.parse(line)
      } catch {
        // pi may emit a human-readable prelude on stdout before NDJSON starts.
        // Capture it so the ACP adapter can surface it on session start.
        const cleaned = stripAnsi(String(line)).trimEnd()
        if (cleaned) this.preludeLines.push(cleaned)
        return
      }

      if (!msg || typeof msg !== 'object' || this.terminalError) return

      if (msg.type === 'response') {
        const id = typeof msg.id === 'string' ? msg.id : undefined
        // A response is never a pi event, including unknown or already timed-out ids.
        const pending = id !== undefined ? this.pending.get(id) : undefined
        if (pending) {
          if (this.starting && msg.command === 'get_state' && msg.success === true) this.starting = false
          pending.resolve(msg as PiRpcResponse)
        }
        return
      }

      if (this.starting && msg.type === 'extension_ui_request' && STARTUP_INTERACTIVE_METHODS.has(msg.method)) {
        this.failStartup(
          new PiRpcSpawnError(
            `Could not start pi: an extension requested interactive ${String(msg.method)} UI before RPC was ready. ` +
              'Startup UI cannot be safely answered before RPC readiness. Review the extension or project trust prompt in an interactive pi terminal in the same working directory, then retry ACP. No approval was sent.',
            { code: 'PI_RPC_STARTUP_UI_UNSUPPORTED' }
          )
        )
        return
      }

      if (this.bufferingEvents) {
        this.earlyEventBytes += Buffer.byteLength(line, 'utf8')
        if (this.earlyEvents.length >= MAX_EARLY_EVENTS || this.earlyEventBytes > MAX_EARLY_EVENT_BYTES) {
          this.failStartup(
            new PiRpcSpawnError('Could not start pi: too many events arrived before the session was ready.', {
              code: 'PI_RPC_STARTUP_EVENT_OVERFLOW'
            })
          )
          return
        }
        this.earlyEvents.push(msg as PiRpcEvent)
        return
      }

      for (const h of this.eventHandlers) h(msg as PiRpcEvent)
    }

    child.stdout.on('data', (chunk: Buffer) => {
      buf += decoder.write(chunk)
      let nl: number
      while ((nl = buf.indexOf('\n')) !== -1) {
        onLine(buf.slice(0, nl))
        buf = buf.slice(nl + 1)
      }
    })

    child.stdout.on('end', () => {
      buf += decoder.end()
      if (buf.length > 0) onLine(buf)
      this.fail(this.exitError ?? new Error('pi stdout closed'))
      this.dispose()
    })

    child.stderr.on('data', () => {
      // Drain stderr so a noisy extension cannot block startup. Never mix it into ACP stdout.
    })
    const onStreamError = (err: Error) => {
      this.fail(err)
      this.dispose()
    }
    child.stdin.on('error', onStreamError)
    child.stdout.on('error', onStreamError)
    child.stderr.on('error', onStreamError)

    child.on('exit', (code, signal) => {
      this.exited = true
      clearTimeout(this.killTimer)
      // stdout may still contain final responses/events; drain it before failing pending requests.
      this.exitError = new Error(`pi process exited (code=${code}, signal=${signal})`)
    })

    child.on('error', onStreamError)
  }

  /** Includes subprocesses still waiting for their startup handshake. */
  static async disposeAll(): Promise<void> {
    const processes = [...PiRpcProcess.activeProcesses]
    for (const proc of processes) proc.dispose()
    await Promise.all(processes.map(proc => proc.closed))
  }

  isAlive(): boolean {
    return !this.exited && !this.terminalError
  }

  private fail(error: Error): void {
    this.terminalError ??= error
    for (const [, p] of this.pending) p.reject(this.terminalError)
    this.pending.clear()
  }

  private failStartup(error: PiRpcSpawnError): void {
    this.fail(error)
    this.dispose()
  }

  static async spawn(params: SpawnParams): Promise<PiRpcProcess> {
    // On Windows, npm commonly creates pi.cmd / pi.bat launcher scripts.
    const cmd = getPiCommand(params.piCommand)
    const timeoutMs =
      params.startupTimeoutMs ?? Number(process.env.PI_ACP_STARTUP_TIMEOUT_MS ?? DEFAULT_STARTUP_TIMEOUT_MS)
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs > 600_000) {
      throw new PiRpcSpawnError('PI_ACP_STARTUP_TIMEOUT_MS must be an integer between 1 and 600000 milliseconds.', {
        code: 'PI_RPC_INVALID_STARTUP_TIMEOUT'
      })
    }

    // Speed/robustness for ACP:
    // - themes are irrelevant in rpc mode and can be noisy/slow to load.
    // Keep extensions + prompt templates enabled because ACP users may rely on them
    // (e.g. MCP extensions, prompt templates for workflows).
    const args = ['--mode', 'rpc', '--no-themes']
    if (params.sessionPath) args.push('--session', params.sessionPath)

    // Windows cmd launchers need shell escaping; direct executables use native argv.
    const start = shouldUseShellForPiCommand(cmd) ? crossSpawn : spawn
    const child = start(cmd, args, {
      cwd: params.cwd,
      stdio: 'pipe',
      env: process.env
    }) as ChildProcessWithoutNullStreams

    // Subscribe before awaiting spawn so no startup events or exits are lost.
    const proc = new PiRpcProcess(child)
    proc.costHistoryVerifiable = !params.sessionPath

    // Ensure spawn failures (e.g. ENOENT when pi isn't installed) are surfaced as a
    // deterministic error instead of later EPIPE/internal-error noise.
    try {
      await new Promise<void>((resolve, reject) => {
        const onSpawn = () => {
          cleanup()
          resolve()
        }
        const onError = (err: any) => {
          cleanup()
          reject(err)
        }
        const cleanup = () => {
          child.off('spawn', onSpawn)
          child.off('error', onError)
        }

        child.once('spawn', onSpawn)
        child.once('error', onError)
      })
    } catch (e: any) {
      proc.dispose()
      const code = typeof e?.code === 'string' ? e.code : undefined
      if (code === 'ENOENT') {
        throw new PiRpcSpawnError(
          `Could not start pi: executable not found (command: ${cmd}). Pi needs to be installed before it can run in ACP clients. Install it via \`npm install -g @earendil-works/pi-coding-agent\` or ensure \`pi\` is on your PATH. Then try again.`,
          { code, cause: e }
        )
      }

      if (code === 'EACCES') {
        throw new PiRpcSpawnError(`Could not start pi: permission denied (command: ${cmd}).`, { code, cause: e })
      }

      throw new PiRpcSpawnError(`Could not start pi (command: ${cmd}).`, { code, cause: e })
    }

    const timeout = setTimeout(() => {
      proc.failStartup(
        new PiRpcSpawnError(
          `Could not start pi: RPC readiness timed out after ${timeoutMs}ms. Check pi and extension startup in an interactive terminal in the same working directory.`,
          { code: 'PI_RPC_STARTUP_TIMEOUT' }
        )
      )
    }, timeoutMs)

    try {
      proc.startupState = await proc.getState()
      if (!proc.isAlive()) throw proc.terminalError ?? proc.exitError ?? new Error('pi exited during startup')
    } catch (error) {
      proc.dispose()
      if (error instanceof PiRpcSpawnError) throw error
      throw new PiRpcSpawnError(`Could not start pi: ${error instanceof Error ? error.message : String(error)}`, {
        code: 'PI_RPC_STARTUP_FAILED',
        cause: error
      })
    } finally {
      clearTimeout(timeout)
    }

    // pi creates session directories lazily; preserve the existing best-effort setup.
    const state = proc.startupState as { sessionFile?: unknown } | null | undefined
    if (typeof state?.sessionFile === 'string') {
      try {
        const { mkdirSync } = await import('node:fs')
        const { dirname } = await import('node:path')
        mkdirSync(dirname(state.sessionFile), { recursive: true })
      } catch {
        // Directory creation is not part of RPC readiness.
      }
    }

    return proc
  }

  onEvent(handler: (ev: PiRpcEvent) => void): () => void {
    this.eventHandlers.push(handler)
    if (this.bufferingEvents) {
      this.bufferingEvents = false
      const buffered = this.earlyEvents.splice(0)
      this.earlyEventBytes = 0
      for (const event of buffered) handler(event)
    }
    return () => {
      this.eventHandlers = this.eventHandlers.filter(h => h !== handler)
    }
  }

  dispose(signal: NodeJS.Signals | number = 'SIGTERM'): void {
    this.fail(new Error('pi process disposed'))
    this.earlyEvents.length = 0
    this.earlyEventBytes = 0
    if (this.exited || this.killTimer) return
    try {
      this.child.kill(signal)
    } catch {
      // Continue cleanup even if the child has already gone away.
    }
    this.killTimer = setTimeout(() => {
      if (!this.exited) {
        try {
          this.child.kill('SIGKILL')
        } catch {
          // The child may have exited between the check and kill.
        }
      }
    }, SHUTDOWN_GRACE_MS)
    this.killTimer.unref()
  }

  /**
   * Human-readable stdout lines emitted before RPC NDJSON begins (e.g. Context/Skills/Extensions info).
   * Themes are typically noisy/less useful for ACP, so callers can filter as needed.
   */
  consumePreludeLines(): string[] {
    const lines = this.preludeLines.splice(0, this.preludeLines.length)
    return lines
  }

  async prompt(message: string, images: unknown[] = []): Promise<void> {
    const res = await this.request({ type: 'prompt', message, images })
    if (!res.success) throw new Error(`pi prompt failed: ${res.error ?? JSON.stringify(res.data)}`)
  }

  async abort(): Promise<void> {
    const res = await this.request({ type: 'abort' })
    if (!res.success) throw new Error(`pi abort failed: ${res.error ?? JSON.stringify(res.data)}`)
  }

  private usageState: unknown = {}
  private usageModelKey: string | undefined
  private costHistoryVerifiable = false

  private recordUsageModel(model: unknown): void {
    const value = model as { provider?: string; id?: string } | undefined
    const key = value?.provider && value.id ? `${value.provider}/${value.id}` : undefined
    if (this.usageModelKey && key !== this.usageModelKey) this.costHistoryVerifiable = false
    this.usageModelKey = key
  }

  getUsageState(): unknown {
    return { ...(this.usageState as object), costHistoryVerifiable: this.costHistoryVerifiable }
  }

  async getState(): Promise<unknown> {
    const res = await this.request({ type: 'get_state' })
    if (!res.success) throw new Error(`pi get_state failed: ${res.error ?? JSON.stringify(res.data)}`)
    this.usageState = res.data
    this.recordUsageModel((res.data as { model?: unknown } | undefined)?.model)
    return res.data
  }

  async getAvailableModels(): Promise<unknown> {
    const res = await this.request({ type: 'get_available_models' })
    if (!res.success) throw new Error(`pi get_available_models failed: ${res.error ?? JSON.stringify(res.data)}`)
    return res.data
  }

  async setModel(provider: string, modelId: string): Promise<unknown> {
    const res = await this.request({ type: 'set_model', provider, modelId })
    if (!res.success) throw new Error(`pi set_model failed: ${res.error ?? JSON.stringify(res.data)}`)
    this.usageState = { model: res.data }
    this.recordUsageModel(res.data)
    return res.data
  }

  async getAvailableThinkingLevels(): Promise<string[]> {
    const res = await this.request({ type: 'get_available_thinking_levels' })
    if (!res.success)
      throw new Error(`pi get_available_thinking_levels failed: ${res.error ?? JSON.stringify(res.data)}`)
    const data = res.data
    const levels = data && typeof data === 'object' && 'levels' in data ? data.levels : undefined
    if (
      !Array.isArray(levels) ||
      levels.length === 0 ||
      !levels.every(level => typeof level === 'string' && level.length > 0)
    ) {
      throw new Error('pi get_available_thinking_levels returned invalid levels')
    }
    return levels
  }

  async setThinkingLevel(level: string): Promise<void> {
    const res = await this.request({ type: 'set_thinking_level', level })
    if (!res.success) throw new Error(`pi set_thinking_level failed: ${res.error ?? JSON.stringify(res.data)}`)
  }

  async setFollowUpMode(mode: 'all' | 'one-at-a-time'): Promise<void> {
    const res = await this.request({ type: 'set_follow_up_mode', mode })
    if (!res.success) throw new Error(`pi set_follow_up_mode failed: ${res.error ?? JSON.stringify(res.data)}`)
  }

  async setSteeringMode(mode: 'all' | 'one-at-a-time'): Promise<void> {
    const res = await this.request({ type: 'set_steering_mode', mode })
    if (!res.success) throw new Error(`pi set_steering_mode failed: ${res.error ?? JSON.stringify(res.data)}`)
  }

  async compact(customInstructions?: string): Promise<unknown> {
    const res = await this.request({ type: 'compact', customInstructions })
    if (!res.success) throw new Error(`pi compact failed: ${res.error ?? JSON.stringify(res.data)}`)
    return res.data
  }

  async setAutoCompaction(enabled: boolean): Promise<void> {
    const res = await this.request({ type: 'set_auto_compaction', enabled })
    if (!res.success) throw new Error(`pi set_auto_compaction failed: ${res.error ?? JSON.stringify(res.data)}`)
  }

  async getSessionStats(timeoutMs?: number): Promise<PiSessionStats> {
    const res = await this.request({ type: 'get_session_stats' }, { timeoutMs })
    if (!res.success) throw new Error(`pi get_session_stats failed: ${res.error ?? JSON.stringify(res.data)}`)
    return (res.data ?? {}) as PiSessionStats
  }

  async setSessionName(name: string): Promise<void> {
    const res = await this.request({ type: 'set_session_name', name })
    if (!res.success) throw new Error(`pi set_session_name failed: ${res.error ?? JSON.stringify(res.data)}`)
  }

  async exportHtml(outputPath?: string): Promise<{ path: string }> {
    const res = await this.request({ type: 'export_html', outputPath })
    if (!res.success) throw new Error(`pi export_html failed: ${res.error ?? JSON.stringify(res.data)}`)
    const data: any = res.data
    return { path: String(data?.path ?? '') }
  }

  async switchSession(sessionPath: string): Promise<void> {
    const res = await this.request({ type: 'switch_session', sessionPath })
    if (!res.success) throw new Error(`pi switch_session failed: ${res.error ?? JSON.stringify(res.data)}`)
    this.costHistoryVerifiable = false
  }

  async getMessages(): Promise<unknown> {
    const res = await this.request({ type: 'get_messages' })
    if (!res.success) throw new Error(`pi get_messages failed: ${res.error ?? JSON.stringify(res.data)}`)
    return res.data
  }

  async getCommands(): Promise<unknown> {
    const res = await this.request({ type: 'get_commands' })
    if (!res.success) throw new Error(`pi get_commands failed: ${res.error ?? JSON.stringify(res.data)}`)
    return res.data
  }

  async sendExtensionUiResponse(response: PiExtensionUiResponse): Promise<void> {
    await this.writeLine(`${JSON.stringify({ type: 'extension_ui_response', ...response })}\n`)
  }

  private request(cmd: PiRpcCommand, opts?: { timeoutMs?: number }): Promise<PiRpcResponse> {
    if (this.terminalError) return Promise.reject(this.terminalError)
    const id = crypto.randomUUID()
    const withId = { ...cmd, id }
    const timeoutMs = opts?.timeoutMs

    const line = `${JSON.stringify(withId)}\n`

    return new Promise<PiRpcResponse>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined

      // Returns false when the id was already dropped (e.g. by the timeout), so the
      // caller can avoid settling the promise twice.
      const drop = (): boolean => {
        if (timer !== undefined) {
          clearTimeout(timer)
          timer = undefined
        }
        return this.pending.delete(id)
      }

      this.pending.set(id, {
        resolve: res => {
          drop()
          resolve(res)
        },
        reject: error => {
          drop()
          reject(error)
        }
      })

      if (timeoutMs !== undefined) {
        timer = setTimeout(() => {
          timer = undefined
          if (!this.pending.delete(id)) return
          reject(new Error(`pi ${cmd.type} timed out after ${timeoutMs}ms`))
        }, timeoutMs)
        // Never let an auxiliary request keep the event loop alive.
        timer.unref?.()
      }

      void this.writeLine(line).catch(error => {
        if (!drop()) return
        reject(error)
      })
    })
  }

  private writeLine(line: string): Promise<void> {
    if (this.terminalError) return Promise.reject(this.terminalError)
    if (this.child.stdin.destroyed || !this.child.stdin.writable) {
      return Promise.reject(new Error('pi stdin is not writable'))
    }
    return new Promise<void>((resolve, reject) => {
      try {
        this.child.stdin.write(line, error => {
          if (error) {
            reject(error)
            return
          }

          resolve()
        })
      } catch (error: unknown) {
        reject(error)
      }
    })
  }
}
