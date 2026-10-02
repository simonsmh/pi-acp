import type { SessionUpdate } from '@agentclientprotocol/sdk'
import type { PiSessionStats } from '../pi-rpc/process.js'

type UsageModel = {
  provider?: string
  id?: string
  usageMetadata?: {
    contextWindowSource?: 'gateway' | 'configured' | 'unknown'
    currency?: string
    costAvailable?: boolean
  }
}

export function toUsageUpdate(stats: PiSessionStats | null | undefined, state: unknown): SessionUpdate | null {
  const used = stats?.contextUsage?.tokens
  const size = stats?.contextUsage?.contextWindow
  if (typeof used !== 'number' || !Number.isSafeInteger(used) || used < 0) return null
  if (typeof size !== 'number' || !Number.isSafeInteger(size) || size <= 0) return null

  const usageState = state as { model?: UsageModel; costHistoryVerifiable?: boolean } | null
  const model = usageState?.model
  const metadata = model?.usageMetadata
  const currency = metadata?.currency
  const costAvailable = usageState?.costHistoryVerifiable !== false && metadata?.costAvailable === true
  const amount = stats?.cost
  const cost =
    costAvailable &&
    currency &&
    /^[A-Z]{3}$/.test(currency) &&
    typeof amount === 'number' &&
    Number.isFinite(amount) &&
    amount >= 0
      ? { amount, currency }
      : undefined
  const totalTokens = stats?.tokens?.total
  return {
    sessionUpdate: 'usage_update',
    used,
    size,
    ...(cost ? { cost } : {}),
    _meta: {
      usage: {
        estimated: true,
        contextWindowSource: metadata?.contextWindowSource ?? (model?.provider ? 'model' : 'unknown'),
        costAvailable: !!cost,
        ...(currency ? { currency } : {}),
        ...(typeof totalTokens === 'number' && Number.isSafeInteger(totalTokens) && totalTokens >= 0
          ? { totalTokens }
          : {})
      }
    }
  }
}
