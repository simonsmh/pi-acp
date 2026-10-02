import test from 'node:test'
import assert from 'node:assert/strict'
import { toUsageUpdate } from '../../src/acp/usage.js'

const stats = { cost: 1.25, tokens: { total: 1169521 }, contextUsage: { tokens: 52709, contextWindow: 1000000 } }
const state = {
  model: {
    provider: 'Lumi',
    id: 'MiniMax-M3',
    usageMetadata: {
      currency: 'CNY',
      costAvailable: true,
      contextWindowSource: 'gateway' as const
    }
  }
}

test('usage preserves CNY and separates cumulative tokens from occupancy', () => {
  const update = toUsageUpdate(stats, state)!
  assert.equal(update.sessionUpdate, 'usage_update')
  assert.deepEqual((update as { cost?: unknown }).cost, { amount: 1.25, currency: 'CNY' })
  assert.deepEqual(update._meta?.usage, {
    estimated: true,
    contextWindowSource: 'gateway',
    costAvailable: true,
    currency: 'CNY',
    totalTokens: 1169521
  })
})

test('missing cache pricing suppresses monetary totals, not token updates', () => {
  const update = toUsageUpdate(stats, {
    model: {
      ...state.model,
      usageMetadata: { ...state.model.usageMetadata, costAvailable: false, contextWindowSource: 'unknown' }
    }
  })!
  assert.equal('cost' in update, false)
  assert.equal((update._meta?.usage as { contextWindowSource: string }).contextWindowSource, 'unknown')
})

test('unidentified model never invents a currency', () => {
  const update = toUsageUpdate(stats, {})!
  assert.equal('cost' in update, false)
  assert.equal('currency' in (update._meta!.usage as object), false)
})

test('unverified resumed or mixed-provider histories never report monetary totals', () => {
  assert.equal('cost' in toUsageUpdate(stats, { ...state, costHistoryVerifiable: false })!, false)
  assert.deepEqual((toUsageUpdate(stats, { ...state, costHistoryVerifiable: true }) as { cost?: unknown }).cost, {
    amount: 1.25,
    currency: 'CNY'
  })
})

test('custom providers without currency metadata do not silently become USD', () => {
  const update = toUsageUpdate(stats, { model: { provider: 'custom', id: 'model' } })!
  assert.equal('cost' in update, false)
  assert.equal('currency' in (update._meta!.usage as object), false)
})
