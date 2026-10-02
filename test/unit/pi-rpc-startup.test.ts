import test, { type TestContext } from 'node:test'
import assert from 'node:assert/strict'
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { setTimeout as delay } from 'node:timers/promises'
import { PiRpcProcess, PiRpcSpawnError, type PiRpcEvent } from '../../src/pi-rpc/process.js'
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process'
import { EventEmitter, once } from 'node:events'
import { PassThrough } from 'node:stream'
import { PiAcpSession, SessionManager } from '../../src/acp/session.js'
import { asAgentConn, FakeAgentSideConnection } from '../helpers/fakes.js'

const fixtureUrl = new URL('../helpers/pi-rpc-child.mjs', import.meta.url)

function alive(pid: number): boolean {
  try {
    process.kill(pid, 0)
    return true
  } catch {
    return false
  }
}

async function waitFor(predicate: () => boolean, message: string): Promise<void> {
  const deadline = Date.now() + 4000
  while (!predicate() && Date.now() < deadline) await delay(10)
  assert.ok(predicate(), message)
}

function fixture(t: TestContext, config: Record<string, unknown> = {}) {
  const cwd = mkdtempSync(join(tmpdir(), 'pi-acp-rpc-'))
  const piCommand = join(cwd, process.platform === 'win32' ? 'pi.cmd' : 'pi.mjs')
  writeFileSync(join(cwd, 'fixture.json'), JSON.stringify(config))
  writeFileSync(
    piCommand,
    process.platform === 'win32'
      ? `@echo off\r\n"${process.execPath}" "${fileURLToPath(fixtureUrl)}" %*\r\n`
      : `#!${process.execPath}\nimport ${JSON.stringify(fixtureUrl.href)}\n`,
    { mode: 0o755 }
  )
  const pid = () => Number(readFileSync(join(cwd, 'pid'), 'utf8'))
  const commands = (): Array<Record<string, unknown>> =>
    existsSync(join(cwd, 'commands.jsonl'))
      ? readFileSync(join(cwd, 'commands.jsonl'), 'utf8')
          .trim()
          .split('\n')
          .filter(Boolean)
          .map(line => JSON.parse(line) as Record<string, unknown>)
      : []
  t.after(async () => {
    if (existsSync(join(cwd, 'pid')) && alive(pid())) {
      process.kill(pid(), 'SIGKILL')
      await waitFor(() => !alive(pid()), 'fixture cleanup exits')
    }
    rmSync(cwd, { recursive: true, force: true })
  })
  return { cwd, piCommand, pid, commands, startupTimeoutMs: 1000 }
}

function startupError(code: string) {
  return (error: unknown) => error instanceof PiRpcSpawnError && error.code === code
}

for (const method of ['confirm', 'select', 'input', 'editor']) {
  test(`startup ${method} fails closed without an extension UI response`, { timeout: 6000 }, async t => {
    const f = fixture(t, { mode: 'startup-ui', method })
    await assert.rejects(PiRpcProcess.spawn(f), startupError('PI_RPC_STARTUP_UI_UNSUPPORTED'))
    await waitFor(() => !alive(f.pid()), 'startup UI child exits')
    assert.ok(f.commands().every(command => command.type !== 'extension_ui_response'))
  })
}

test('startup UI fails promptly even when pi has not attached its stdin reader', { timeout: 6000 }, async t => {
  const f = fixture(t, { mode: 'startup-ui-unreadable' })
  await assert.rejects(PiRpcProcess.spawn(f), startupError('PI_RPC_STARTUP_UI_UNSUPPORTED'))
  await waitFor(() => !alive(f.pid()), 'unreadable startup child exits')
})

test(
  'silent startup times out, clears the handshake, and force-kills a child ignoring SIGTERM',
  { timeout: 6000 },
  async t => {
    const f = fixture(t, { mode: 'silent', ignoreTerm: true })
    await assert.rejects(PiRpcProcess.spawn(f), startupError('PI_RPC_STARTUP_TIMEOUT'))
    await waitFor(() => !alive(f.pid()), 'timed-out child exits despite ignoring SIGTERM')
    assert.equal(f.commands().length, 1)
  }
)

test('startup child exit is surfaced instead of returning a dead process', { timeout: 6000 }, async t => {
  const f = fixture(t, { mode: 'exit' })
  await assert.rejects(PiRpcProcess.spawn(f), startupError('PI_RPC_STARTUP_FAILED'))
  await waitFor(() => !alive(f.pid()), 'exited child stays dead')
})

test('startup RPC failure is surfaced and the child is cleaned up', { timeout: 6000 }, async t => {
  const f = fixture(t, { mode: 'reject' })
  await assert.rejects(PiRpcProcess.spawn(f), /fixture startup failed/)
  await waitFor(() => !alive(f.pid()), 'rejected-handshake child exits')
})

test('stdout closing without process exit rejects startup and cleans up', { timeout: 6000 }, async t => {
  const f = fixture(t, { mode: 'stdout-end' })
  await assert.rejects(PiRpcProcess.spawn(f), /pi stdout closed/)
  await waitFor(() => !alive(f.pid()), 'child with closed stdout exits')
})

test(
  'early events are buffered and replayed once in order, with JSONL Unicode preserved',
  { timeout: 6000 },
  async t => {
    const f = fixture(t, { mode: 'early-events' })
    const proc = await PiRpcProcess.spawn(f)
    t.after(() => proc.dispose())
    assert.deepEqual(proc.consumePreludeLines(), ['Startup prelude'])
    assert.deepEqual(proc.consumePreludeLines(), [])
    const events: PiRpcEvent[] = []
    const unsubscribe = proc.onEvent(event => events.push(event))
    assert.deepEqual(events, [
      { type: 'session_info_changed', name: 'early 🦊\u2028title\u2029' },
      { type: 'extension_ui_request', id: 'notice', method: 'notify', message: 'Ready soon' }
    ])
    unsubscribe()
    const laterEvents: PiRpcEvent[] = []
    proc.onEvent(event => laterEvents.push(event))
    assert.equal(laterEvents.length, 0)
    assert.equal(proc.isAlive(), true)
    await proc.prompt('inert prompt')
    await waitFor(() => laterEvents.length === 3, 'normal prompt events arrive')
    assert.deepEqual(
      laterEvents.map(event => event.type),
      ['agent_start', 'message_update', 'agent_settled']
    )
  }
)

test('startup event buffering is bounded and fails closed on overflow', { timeout: 6000 }, async t => {
  const f = fixture(t, { mode: 'event-overflow' })
  await assert.rejects(PiRpcProcess.spawn(f), startupError('PI_RPC_STARTUP_EVENT_OVERFLOW'))
  await waitFor(() => !alive(f.pid()), 'overflowing child exits')
})

test(
  'same-chunk UI immediately after readiness is retained and still requires client permission',
  { timeout: 6000 },
  async t => {
    const f = fixture(t, { mode: 'post-ready-ui' })
    const proc = await PiRpcProcess.spawn(f)
    t.after(() => proc.dispose())
    const conn = new FakeAgentSideConnection()
    conn.nextPermissionResponse = { outcome: { outcome: 'selected', optionId: 'no' } }
    const session = new PiAcpSession({
      sessionId: 'fixture-session',
      cwd: f.cwd,
      mcpServers: [],
      proc,
      conn: asAgentConn(conn),
      deferStartupEvents: true
    })
    assert.equal(conn.permissionRequests.length, 0)
    assert.equal(await session.prompt('inert prompt'), 'end_turn')
    await waitFor(() => f.commands().some(command => command.type === 'extension_ui_response'), 'UI answer arrives')
    assert.equal(conn.permissionRequests.length, 1)
    assert.deepEqual(
      f.commands().find(command => command.type === 'extension_ui_response'),
      {
        type: 'extension_ui_response',
        id: 'after-ready',
        confirmed: false
      }
    )
  }
)

test(
  'SessionManager uses the bounded handshake state and normal ACP turns still complete',
  { timeout: 6000 },
  async t => {
    const f = fixture(t)
    const manager = new SessionManager()
    t.after(() => manager.disposeAll())
    const conn = new FakeAgentSideConnection()
    const session = await manager.create({
      cwd: f.cwd,
      piCommand: f.piCommand,
      mcpServers: [],
      conn: asAgentConn(conn)
    })
    assert.equal(session.sessionId, 'fixture-session')
    assert.equal(f.commands().filter(command => command.type === 'get_state').length, 1)
    assert.equal(await session.prompt('inert prompt'), 'end_turn')
    assert.ok(conn.updates.some(message => message.update.sessionUpdate === 'agent_message_chunk'))
    manager.disposeAll()
    await waitFor(() => !alive(f.pid()), 'normal child exits')
  }
)

test('dispose rejects in-flight and future RPC calls promptly and is idempotent', { timeout: 6000 }, async t => {
  const f = fixture(t, { mode: 'pending' })
  const proc = await PiRpcProcess.spawn(f)
  const pending = assert.rejects(proc.abort(), /disposed/)
  proc.dispose()
  proc.dispose()
  await pending
  assert.equal(proc.isAlive(), false)
  await assert.rejects(proc.getState(), /disposed/)
  await assert.rejects(proc.sendExtensionUiResponse({ id: 'never-sent', confirmed: true }), /disposed/)
  await waitFor(() => !alive(f.pid()), 'disposed child exits')
})

test('invalid startup timeouts are rejected before spawning', async () => {
  for (const startupTimeoutMs of [0, -1, Infinity, NaN, 1.5, 600001]) {
    await assert.rejects(
      PiRpcProcess.spawn({ cwd: process.cwd(), piCommand: 'must-not-start', startupTimeoutMs }),
      startupError('PI_RPC_INVALID_STARTUP_TIMEOUT')
    )
  }
})

test('buffered startup notifications wait for a prompt and are delivered once', { timeout: 6000 }, async t => {
  const f = fixture(t, { mode: 'early-events' })
  const proc = await PiRpcProcess.spawn(f)
  t.after(() => proc.dispose())
  const conn = new FakeAgentSideConnection()
  const session = new PiAcpSession({
    sessionId: 'fixture-session',
    cwd: f.cwd,
    mcpServers: [],
    proc,
    conn: asAgentConn(conn),
    deferStartupEvents: true
  })
  await delay(0)
  assert.equal(conn.updates.length, 0)
  await session.prompt('first inert prompt')
  await session.prompt('second inert prompt')
  assert.equal(
    conn.updates.filter(
      message =>
        message.update.sessionUpdate === 'agent_message_chunk' &&
        message.update.content.type === 'text' &&
        message.update.content.text === 'Ready soon'
    ).length,
    1
  )
})

test('live startup UI after subscription waits until the client has an active prompt', { timeout: 6000 }, async t => {
  const f = fixture(t, { mode: 'post-ready-delayed-ui' })
  const manager = new SessionManager()
  t.after(() => manager.disposeAll())
  const conn = new FakeAgentSideConnection()
  conn.nextPermissionResponse = { outcome: { outcome: 'selected', optionId: 'no' } }
  const session = await manager.create({ cwd: f.cwd, piCommand: f.piCommand, mcpServers: [], conn: asAgentConn(conn) })
  await delay(150)
  assert.equal(conn.permissionRequests.length, 0)
  assert.equal(conn.updates.length, 0)
  assert.equal(await session.prompt('inert prompt'), 'end_turn')
  await waitFor(
    () => f.commands().some(command => command.type === 'extension_ui_response'),
    'deferred UI answer arrives'
  )
  assert.equal(conn.permissionRequests.length, 1)
  assert.deepEqual(
    f.commands().find(command => command.type === 'extension_ui_response'),
    {
      type: 'extension_ui_response',
      id: 'after-ready',
      confirmed: false
    }
  )
})

test('deferred permissions resolve before the first prompt is sent', { timeout: 6000 }, async t => {
  const f = fixture(t, { mode: 'post-ready-ui' })
  const proc = await PiRpcProcess.spawn(f)
  t.after(() => proc.dispose())
  const conn = new FakeAgentSideConnection()
  let answer!: (value: typeof conn.nextPermissionResponse) => void
  conn.requestPermission = async params => {
    conn.permissionRequests.push(params)
    return new Promise(resolve => {
      answer = resolve
    })
  }
  const session = new PiAcpSession({
    sessionId: 'fixture-session',
    cwd: f.cwd,
    mcpServers: [],
    proc,
    conn: asAgentConn(conn),
    deferStartupEvents: true
  })
  const turn = session.prompt('inert prompt')
  await waitFor(() => conn.permissionRequests.length === 1, 'permission requested')
  assert.equal(f.commands().filter(command => command.type === 'prompt').length, 0)
  answer({ outcome: { outcome: 'selected', optionId: 'no' } })
  assert.equal(await turn, 'end_turn')
  const commands = f.commands()
  assert.ok(
    commands.findIndex(command => command.type === 'extension_ui_response') <
      commands.findIndex(command => command.type === 'prompt')
  )
})

test('normal process exit drains final events before closing pending RPC calls', () => {
  const child = Object.assign(new EventEmitter(), {
    stdin: new PassThrough(),
    stdout: new PassThrough(),
    stderr: new PassThrough(),
    kill: () => true
  })
  // Exercise a legal Node event order deterministically: exit can precede stdout drain.
  const ProcessConstructor = PiRpcProcess as unknown as new (child: ChildProcessWithoutNullStreams) => PiRpcProcess
  const proc = new ProcessConstructor(child as unknown as ChildProcessWithoutNullStreams)
  const events: PiRpcEvent[] = []
  proc.onEvent(event => events.push(event))
  child.emit('exit', 0, null)
  child.stdout.emit('data', Buffer.from(JSON.stringify({ type: 'agent_settled' }) + '\n'))
  child.stdout.emit('end')
  child.emit('close', 0, null)
  assert.deepEqual(events, [{ type: 'agent_settled' }])
  assert.equal(proc.isAlive(), false)
})

for (const mode of ['silent', 'ready']) {
  test(
    `ACP disconnect cleans up ${mode === 'silent' ? 'starting' : 'ready'} children before CLI exits`,
    { timeout: 8000 },
    async t => {
      const f = fixture(t, { mode, ignoreTerm: true })
      const adapter = spawn(
        process.execPath,
        ['--import', import.meta.resolve('tsx'), fileURLToPath(new URL('../../src/index.ts', import.meta.url))],
        {
          cwd: f.cwd,
          env: { ...process.env, HOME: f.cwd, PI_ACP_PI_COMMAND: f.piCommand, PI_ACP_STARTUP_TIMEOUT_MS: '30000' },
          stdio: 'pipe'
        }
      )
      let output = ''
      let stderr = ''
      adapter.stdout.on('data', chunk => {
        output += chunk.toString()
      })
      adapter.stderr.on('data', chunk => {
        stderr += chunk.toString()
      })
      t.after(() => {
        if (adapter.exitCode === null && adapter.signalCode === null) adapter.kill('SIGKILL')
      })
      adapter.stdin.write(
        JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: 1 } }) + '\n'
      )
      adapter.stdin.write(
        JSON.stringify({ jsonrpc: '2.0', id: 2, method: 'session/new', params: { cwd: f.cwd, mcpServers: [] } }) + '\n'
      )
      await waitFor(() => existsSync(join(f.cwd, 'pid')), 'CLI starts inert pi child')
      if (mode === 'ready') {
        await waitFor(
          () =>
            output
              .split('\n')
              .filter(Boolean)
              .some(line => {
                const message = JSON.parse(line) as { id?: number; result?: { sessionId?: string } }
                return message.id === 2 && message.result?.sessionId === 'fixture-session'
              }),
          `session/new completed: ${stderr}`
        )
      }
      const closed = once(adapter, 'close')
      adapter.stdin.end()
      const [code] = await closed
      assert.equal(code, 0, stderr)
      assert.equal(alive(f.pid()), false, 'pi child is gone before adapter exits')
    }
  )
}

test(
  'cancel while awaiting startup permission settles the turn and ignores a late approval',
  { timeout: 6000 },
  async t => {
    const f = fixture(t, { mode: 'post-ready-ui' })
    const proc = await PiRpcProcess.spawn(f)
    t.after(() => proc.dispose())
    const conn = new FakeAgentSideConnection()
    let answer!: (value: typeof conn.nextPermissionResponse) => void
    conn.requestPermission = async params => {
      conn.permissionRequests.push(params)
      return new Promise(resolve => {
        answer = resolve
      })
    }
    const session = new PiAcpSession({
      sessionId: 'fixture-session',
      cwd: f.cwd,
      mcpServers: [],
      proc,
      conn: asAgentConn(conn),
      deferStartupEvents: true
    })
    const turn = session.prompt('must never run')
    await waitFor(() => conn.permissionRequests.length === 1, 'permission requested')
    await session.cancel()
    assert.equal(await turn, 'cancelled')
    answer({ outcome: { outcome: 'selected', optionId: 'yes' } })
    await delay(20)
    assert.equal(f.commands().filter(command => command.type === 'prompt').length, 0)
    assert.deepEqual(
      f.commands().filter(command => command.type === 'extension_ui_response'),
      [{ type: 'extension_ui_response', id: 'after-ready', cancelled: true }]
    )
    assert.equal(await session.prompt('next inert prompt'), 'end_turn')
  }
)
