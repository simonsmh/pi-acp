import test from 'node:test'
import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
import { PassThrough } from 'node:stream'
import { mkdtempSync, rmSync, writeFileSync, chmodSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { ChildProcessWithoutNullStreams } from 'node:child_process'
import { RequestError } from '@agentclientprotocol/sdk'
import { PiAcpSession } from '../../src/acp/session.js'
import { PiRpcProcess } from '../../src/pi-rpc/process.js'
import { FakeAgentSideConnection, FakePiRpcProcess, asAgentConn } from '../helpers/fakes.js'

test('PiAcpSession: rejects in-flight and queued prompts when pi exits before agent_settled', async () => {
  const conn = new FakeAgentSideConnection()
  const proc = new FakePiRpcProcess()

  const session = new PiAcpSession({
    sessionId: 's-oom',
    cwd: process.cwd(),
    mcpServers: [],
    proc: proc as any,
    conn: asAgentConn(conn),
    fileCommands: []
  })

  const first = session.prompt('first turn')
  const second = session.prompt('queued turn')

  await new Promise(r => setTimeout(r, 0))
  proc.emit({ type: 'agent_start' })
  proc.emit({
    type: 'message_update',
    assistantMessageEvent: { type: 'text_delta', delta: 'partial output' }
  })

  proc.emitExit(new Error('pi process exited (code=null, signal=SIGKILL)'))

  await assert.rejects(first, (err: unknown) => {
    assert.ok(err instanceof RequestError)
    assert.match(err.message, /SIGKILL/)
    assert.deepEqual(err.data, { code: 'PI_PROCESS_EXITED' })
    return true
  })

  await assert.rejects(second, (err: unknown) => {
    assert.ok(err instanceof RequestError)
    assert.deepEqual(err.data, { code: 'PI_PROCESS_EXITED' })
    return true
  })

  const textChunks = conn.updates
    .filter(u => u.update.sessionUpdate === 'agent_message_chunk')
    .map(u => (u.update as any).content?.text)
  assert.ok(textChunks.includes('partial output'))

  const lastInfo = [...conn.updates].reverse().find(u => u.update.sessionUpdate === 'session_info_update')
  assert.deepEqual((lastInfo?.update as any)?._meta?.piAcp, { queueDepth: 0, running: false })
})

test('PiAcpSession: preserves settled turn when pi exits during usage publish, and rejects queued turn', async () => {
  const conn = new FakeAgentSideConnection()
  const proc = new FakePiRpcProcess()

  let releaseStats!: () => void
  proc.getSessionStats = async () => {
    await new Promise<void>(resolve => {
      releaseStats = resolve
    })
    return {}
  }

  const session = new PiAcpSession({
    sessionId: 's-settling',
    cwd: process.cwd(),
    mcpServers: [],
    proc: proc as any,
    conn: asAgentConn(conn),
    fileCommands: []
  })

  const first = session.prompt('first turn')
  const second = session.prompt('queued turn')

  await new Promise(r => setTimeout(r, 0))
  proc.emit({ type: 'agent_settled' })
  await new Promise(r => setTimeout(r, 0))

  // Process dies after agent_settled while publishContextUsage is still awaiting stats.
  proc.emitExit(new Error('pi process exited (code=0, signal=null)'))
  releaseStats()

  assert.equal(await first, 'end_turn')
  await assert.rejects(second, (err: unknown) => {
    assert.ok(err instanceof RequestError)
    assert.deepEqual(err.data, { code: 'PI_PROCESS_EXITED' })
    return true
  })
})

test('PiAcpSession: resolves cancelled turn as cancelled when pi exits after cancel()', async () => {
  const conn = new FakeAgentSideConnection()
  const proc = new FakePiRpcProcess()

  const session = new PiAcpSession({
    sessionId: 's-cancel',
    cwd: process.cwd(),
    mcpServers: [],
    proc: proc as any,
    conn: asAgentConn(conn),
    fileCommands: []
  })

  const turn = session.prompt('cancel me')
  await new Promise(r => setTimeout(r, 0))
  await session.cancel()
  proc.emitExit(new Error('pi process exited (code=null, signal=SIGTERM)'))

  assert.equal(await turn, 'cancelled')
})

test('PiRpcProcess: exit grace timer fails process even when stdout never reaches EOF', async () => {
  const child = Object.assign(new EventEmitter(), {
    stdin: new PassThrough(),
    stdout: new PassThrough(),
    stderr: new PassThrough(),
    kill: () => true
  })

  const ProcessConstructor = PiRpcProcess as unknown as new (child: ChildProcessWithoutNullStreams) => PiRpcProcess
  const proc = new ProcessConstructor(child as unknown as ChildProcessWithoutNullStreams)

  let exitErr: Error | null = null
  const exited = new Promise<Error>(resolve => {
    proc.onExit(err => {
      exitErr = err
      resolve(err)
    })
  })

  // Simulate pi getting SIGKILLed while an orphaned grandchild still holds stdout open.
  child.emit('exit', null, 'SIGKILL')
  const err = await exited
  assert.match(err.message, /signal=SIGKILL/)
  assert.equal(exitErr, err)
  assert.equal(proc.isAlive(), false)
})

test(
  'PiAcpSession + PiRpcProcess: real child SIGKILL during active prompt rejects promptly',
  { timeout: 6000 },
  async t => {
    const dir = mkdtempSync(join(tmpdir(), 'pi-acp-sigkill-'))
    t.after(() => rmSync(dir, { recursive: true, force: true }))

    const scriptPath = join(dir, 'fake-pi.mjs')
    const wrapperPath = join(dir, 'fake-pi')
    const pidFile = join(dir, 'child.pid')

    writeFileSync(
      scriptPath,
      [
        "import { writeFileSync } from 'node:fs'",
        "import { StringDecoder } from 'node:string_decoder'",
        `writeFileSync(${JSON.stringify(pidFile)}, String(process.pid))`,
        "const decoder = new StringDecoder('utf8')",
        "let buf = ''",
        "process.stdin.on('data', chunk => {",
        '  buf += decoder.write(chunk)',
        '  let nl',
        "  while ((nl = buf.indexOf('\\n')) !== -1) {",
        '    const line = buf.slice(0, nl).trim()',
        '    buf = buf.slice(nl + 1)',
        '    if (!line) continue',
        '    const msg = JSON.parse(line)',
        "    if (msg.type === 'get_state') {",
        "      process.stdout.write(JSON.stringify({ type: 'response', id: msg.id, command: 'get_state', success: true, data: { sessionId: 'real-sigkill', sessionFile: null, thinkingLevel: 'medium', model: { provider: 'test', id: 'm1' } } }) + '\\n')",
        "    } else if (msg.type === 'prompt') {",
        "      process.stdout.write(JSON.stringify({ type: 'response', id: msg.id, command: 'prompt', success: true }) + '\\n')",
        "      process.stdout.write(JSON.stringify({ type: 'agent_start' }) + '\\n')",
        '      // Intentionally never emit agent_settled; wait to be SIGKILLed.',
        '    }',
        '  }',
        '})'
      ].join('\n'),
      'utf8'
    )
    writeFileSync(wrapperPath, `#!/usr/bin/env bash\nexec "${process.execPath}" "${scriptPath}" "$@"\n`, 'utf8')
    chmodSync(wrapperPath, 0o755)

    const proc = await PiRpcProcess.spawn({ cwd: dir, piCommand: wrapperPath })
    t.after(() => proc.dispose())

    const conn = new FakeAgentSideConnection()
    const session = new PiAcpSession({
      sessionId: 'real-sigkill',
      cwd: dir,
      mcpServers: [],
      proc,
      conn: asAgentConn(conn),
      fileCommands: []
    })

    const turn = session.prompt('hang until OOM')
    // Allow prompt RPC acceptance and agent_start to complete before killing child.
    await new Promise(r => setTimeout(r, 150))

    const { readFileSync } = await import('node:fs')
    const childPid = Number(readFileSync(pidFile, 'utf8').trim())
    process.kill(childPid, 'SIGKILL')

    await assert.rejects(turn, (err: unknown) => {
      assert.ok(err instanceof RequestError)
      assert.deepEqual(err.data, { code: 'PI_PROCESS_EXITED' })
      assert.match(err.message, /SIGKILL|closed/)
      return true
    })
    assert.equal(proc.isAlive(), false)
  }
)
