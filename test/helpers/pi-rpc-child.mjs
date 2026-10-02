#!/usr/bin/env node
// Inert RPC fixture: only local JSONL and files, no pi, credentials, models, or MCP servers.
import { appendFileSync, readFileSync, writeFileSync } from 'node:fs'
import { createInterface } from 'node:readline'

const config = JSON.parse(readFileSync('fixture.json', 'utf8'))
const emit = event => process.stdout.write(`${JSON.stringify(event)}\n`)
const keepAlive = setInterval(() => {}, 1000)
writeFileSync('pid', String(process.pid))
if (config.ignoreTerm) process.on('SIGTERM', () => {})
if (config.mode === 'exit') process.exit(23)
if (config.mode === 'stdout-end') process.stdout.end()
if (config.mode === 'early-events') {
  process.stdout.write('\u001b[32mStartup prelude\u001b[0m\n')
  emit({ type: 'session_info_changed', name: 'early 🦊\u2028title\u2029' })
  emit({ type: 'extension_ui_request', id: 'notice', method: 'notify', message: 'Ready soon' })
}
if (config.mode === 'event-overflow') {
  for (let i = 0; i < 257; i++) emit({ type: 'session_info_changed', name: String(i) })
}
if (config.mode === 'startup-ui' || config.mode === 'startup-ui-unreadable') {
  emit({ type: 'extension_ui_request', id: 'trust', method: config.method ?? 'confirm', title: 'Trust fixture?' })
}
if (config.mode === 'startup-ui-unreadable') {
  // Models pi versions that await extension startup before attaching the stdin reader.
  await new Promise(() => {})
}

const input = createInterface({ input: process.stdin })
input.on('line', line => {
  appendFileSync('commands.jsonl', `${line}\n`)
  const command = JSON.parse(line)
  if (command.type === 'extension_ui_response') return
  if (['silent', 'startup-ui', 'event-overflow', 'stdout-end'].includes(config.mode)) return
  if (command.type === 'get_state' && config.mode === 'reject') {
    emit({ type: 'response', id: command.id, command: command.type, success: false, error: 'fixture startup failed' })
    return
  }
  if (command.type === 'abort' && config.mode === 'pending') return
  const data =
    command.type === 'get_state'
      ? {
          sessionId: 'fixture-session',
          sessionFile: config.sessionFile,
          thinkingLevel: 'medium',
          model: { provider: 'fixture', id: 'inert' }
        }
      : command.type === 'get_available_models'
        ? { models: [{ provider: 'fixture', id: 'inert', name: 'Inert fixture' }] }
        : command.type === 'get_available_thinking_levels'
          ? { levels: ['off', 'medium', 'max'] }
          : command.type === 'get_commands'
            ? { commands: [] }
            : {}
  const response = { type: 'response', id: command.id, command: command.type, success: true, data }
  if (command.type === 'get_state' && config.mode === 'post-ready-ui') {
    process.stdout.write(
      `${JSON.stringify(response)}\n${JSON.stringify({ type: 'extension_ui_request', id: 'after-ready', method: 'confirm', title: 'Proceed?' })}\n`
    )
  } else emit(response)
  if (command.type === 'get_state' && config.mode === 'post-ready-delayed-ui') {
    setTimeout(
      () => emit({ type: 'extension_ui_request', id: 'after-ready', method: 'confirm', title: 'Proceed?' }),
      50
    )
  }
  if (command.type === 'prompt') {
    emit({ type: 'agent_start' })
    emit({ type: 'message_update', assistantMessageEvent: { type: 'text_delta', delta: 'Fixture reply' } })
    emit({ type: 'agent_settled' })
  }
})
input.on('close', () => clearInterval(keepAlive))
