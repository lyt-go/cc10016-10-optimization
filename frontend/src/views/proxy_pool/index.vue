<template>
  <section class="page" data-module="proxy_pool">
    <header class="page-head">
      <div>
        <h2>代理池调度管理</h2>
        <p class="page-desc">维护代理节点的注册、健康检查与轮替调度，为代理池管理提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记代理节点</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出代理池调度清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <section class="snapshot-panel" data-testid="health-snapshot-panel">
      <header class="snapshot-head">
        <div>
          <h3>代理健康快照</h3>
          <p class="page-desc">主数据链产物：失败次数、响应延迟、成功率共同驱动评分；空池给出空态原因，恢复后自动重新排名。</p>
        </div>
        <div class="page-actions">
          <label class="filter-item">
            <span>版本方案</span>
            <select v-model="snapshotPolicy" data-testid="snapshot-policy">
              <option value="history_first">历史优先（失败立即降权）</option>
              <option value="failure_filter">失败过滤后排名</option>
            </select>
          </label>
          <button class="btn primary" data-testid="generate-snapshot" type="button" @click="generateSnapshot(false)">生成快照</button>
          <button class="btn" data-testid="interrupt-snapshot" type="button" @click="generateSnapshot(true)">模拟中断</button>
          <button class="btn ghost" data-testid="resume-snapshot" type="button" @click="resumeSnapshot">中断续做</button>
        </div>
      </header>

      <div v-if="snapshotMessage" class="snapshot-message" :class="{ 'error-text': snapshotError }" data-testid="snapshot-message">
        {{ snapshotMessage }}
      </div>

      <article v-if="snapshot" class="snapshot-meta">
        <span>版本：<strong data-testid="snapshot-version">{{ snapshot.version ?? '未生成' }}</strong></span>
        <span>状态：<strong :data-testid="`snapshot-state-${snapshot.state_label}`">{{ snapshot.state_label }}</strong></span>
        <span>策略：{{ snapshot.policy_label ?? snapshot.policy }}</span>
        <span>候选数：{{ snapshot.candidate_count ?? 0 }}</span>
        <span v-if="snapshot.generated_at">生成时间：{{ snapshot.generated_at }}</span>
        <span v-if="snapshot.previous_version">上一版：{{ snapshot.previous_version }}</span>
      </article>

      <div v-if="snapshot?.pool_empty" class="empty-state snapshot-empty" data-testid="snapshot-empty">
        <strong>空态：当前没有可调度代理</strong>
        <p>{{ snapshot.empty_reason || '暂无可用代理，等待代理恢复后重新排名' }}</p>
      </div>

      <table v-else-if="snapshot && snapshot.candidate_count" class="data-table snapshot-table">
        <thead>
          <tr>
            <th>最终排名</th>
            <th>旧排名</th>
            <th>节点编号</th>
            <th>健康评分</th>
            <th>成功率</th>
            <th>响应延迟</th>
            <th>失败次数</th>
            <th>最近结果</th>
            <th>数据说明</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="candidate in snapshot.candidates" :key="String(candidate.proxy_id)"
              :data-testid="`snapshot-row-${candidate.proxy_id}`" :data-demoted="String(candidate.demoted)">
            <td>{{ candidate.rank }}</td>
            <td>{{ candidate.previous_rank ?? '—' }}{{ candidate.demoted ? ' ⬇降权' : '' }}</td>
            <td>{{ candidate['节点编号'] }}</td>
            <td>{{ candidate.health_score }}</td>
            <td>{{ formatRate(candidate.success_rate) }}</td>
            <td>{{ formatLatency(candidate.latency_ms) }}</td>
            <td>{{ candidate.failures }}（超时 {{ candidate.timeouts }}）</td>
            <td>{{ candidate.last_outcome ? (outcomeLabels[candidate.last_outcome] ?? candidate.last_outcome) : '暂无请求' }}</td>
            <td>
              <span v-if="candidate.stale" class="warn-text">陈旧：{{ candidate.missing_metrics.join('、') }} 缺失</span>
              <span v-else>指标完整</span>
            </td>
          </tr>
        </tbody>
      </table>

      <div v-else-if="snapshot" class="empty-state" data-testid="snapshot-no-data">
        暂无排名数据：{{ snapshot.empty_reason || '没有数据' }}
      </div>

      <div v-if="snapshot?.filtered_out?.length" class="snapshot-filtered" data-testid="snapshot-filtered">
        <h4>失败过滤移除（{{ snapshot.filtered_out.length }}）</h4>
        <ul>
          <li v-for="item in snapshot.filtered_out" :key="String(item.proxy_id)">
            {{ item['节点编号'] }}：{{ item.reason }}
          </li>
        </ul>
      </div>
    </section>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button
              v-for="outcome in requestOutcomes"
              :key="outcome.value"
              class="link"
              type="button"
              :data-testid="`record-${row.id}-${outcome.value}`"
              @click="recordProxyRequest(row, outcome)"
            >
              {{ outcome.label }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无代理池调度数据，可先登记代理节点</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记代理节点</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条代理池调度记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
interface SnapshotCandidate {
  proxy_id: number
  节点编号?: string
  地域标识?: string
  rank: number
  previous_rank: number | null
  health_score: number
  success_rate: number | null
  latency_ms: number | null
  failures: number
  timeouts: number
  stale: boolean
  missing_metrics: string[]
  missing_reasons?: string[]
  last_outcome: string | null
  demoted: boolean
  at?: string
}
interface HealthSnapshot {
  version: string | null
  state_label: string
  policy: string
  policy_label?: string
  pool_empty: boolean
  empty_reason: string
  candidate_count: number
  candidates: SnapshotCandidate[]
  filtered_out?: Array<Record<string, string | number>>
  generated_at?: string
  previous_version?: string
}

const ENDPOINT = '/api/proxy_pool'
const columns = ["节点编号", "代理地址", "端口号", "协议类型", "地域标识", "响应延迟", "成功率", "失效次数"]
const actions = ["提交校验", "标记可用", "标记失效"]
const statuses = ["草稿", "待校验", "可用", "已失效"]
const metricNames = ["代理节点总数", "待校验节点", "可用节点"]
const requestOutcomes = [
  { value: 'success', label: '记录成功', success: true, latency: 120 },
  { value: 'failure', label: '记录失败', success: false, latency: 500 },
  { value: 'timeout', label: '记录超时', success: false, latency: 3500 },
] as const
const outcomeLabels: Record<string, string> = {
  success: '成功',
  failure: '失败',
  timeout: '超时',
  interrupted: '中断',
  error: '异常',
}

function formatRate(rate: number | null): string {
  return rate === null ? '暂无' : `${(rate * 100).toFixed(1)}%`
}

function formatLatency(latency: number | null): string {
  return latency === null ? '暂无' : `${latency}ms`
}

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const snapshot = ref<HealthSnapshot | null>(null)
const snapshotPolicy = ref<'history_first' | 'failure_filter'>('history_first')
const snapshotMessage = ref('')
const snapshotError = ref(false)
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

async function loadSnapshot(forceGenerate = false) {
  snapshotMessage.value = ''
  snapshotError.value = false
  try {
    let response = await request(`${ENDPOINT}/health-snapshot?auto_generate=${forceGenerate ? 'true' : 'false'}`)
    if (!response.ok) throw new Error('代理健康快照读取失败')
    let payload = await response.json()
    if (forceGenerate || !payload.version) {
      response = await request(`${ENDPOINT}/health-snapshot/generate`, {
        method: 'POST',
        body: JSON.stringify({ policy: snapshotPolicy.value }),
      })
      const generated = await response.json()
      if (!response.ok || !generated.ok) throw new Error(generated.message || '快照生成失败')
      payload = generated.snapshot
      snapshotMessage.value = generated.message
    }
    snapshot.value = payload as HealthSnapshot
  } catch (error) {
    snapshotError.value = true
    snapshotMessage.value = error instanceof Error ? error.message : '代理健康快照读取失败'
  }
}

async function generateSnapshot(interrupted: boolean) {
  snapshotMessage.value = ''
  snapshotError.value = false
  try {
    const response = await request(`${ENDPOINT}/health-snapshot/generate`, {
      method: 'POST',
      body: JSON.stringify({ policy: snapshotPolicy.value, interrupted }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '快照生成失败')
    snapshot.value = payload.snapshot as HealthSnapshot
    snapshotMessage.value = interrupted
      ? `快照生成已中断：${payload.snapshot.empty_reason || '进度已保存，可点击中断续做'}`
      : payload.message
    await reload()
  } catch (error) {
    snapshotError.value = true
    snapshotMessage.value = error instanceof Error ? error.message : '快照生成失败'
  }
}

async function resumeSnapshot() {
  snapshotMessage.value = ''
  snapshotError.value = false
  try {
    const response = await request(`${ENDPOINT}/health-snapshot/resume`, { method: 'POST' })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '续做失败')
    snapshot.value = payload.snapshot as HealthSnapshot
    snapshotMessage.value = payload.message
    await reload()
  } catch (error) {
    snapshotError.value = true
    snapshotMessage.value = error instanceof Error ? error.message : '快照续做失败'
  }
}

async function recordProxyRequest(row: Row, outcome: { value: string; label: string; success: boolean; latency: number }) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/record`, {
      method: 'POST',
      body: JSON.stringify({
        success: outcome.success,
        latency: outcome.latency,
        outcome: outcome.value,
        is_retry: false,
      }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.error || '请求结果入账失败')
    await reload()
    await loadSnapshot(false)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '请求结果记录失败'
  }
}

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'proxy_pool.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('代理池调度动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '代理池调度操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('代理节点列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '代理池调度列表读取失败'
  }
}

onMounted(() => {
  void reload()
  void loadSnapshot(false)
})
</script>

<style scoped>
.snapshot-panel {
  border: 1px solid var(--border, #d9dde3);
  border-radius: 8px;
  padding: 12px 16px;
  margin-bottom: 16px;
  background: var(--panel-bg, #fafbfc);
}
.snapshot-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 12px;
  flex-wrap: wrap;
}
.snapshot-meta {
  display: flex;
  gap: 16px;
  flex-wrap: wrap;
  font-size: 13px;
  color: var(--muted);
  margin: 10px 0;
}
.snapshot-empty {
  border: 1px dashed var(--warn, #d97706);
  border-radius: 6px;
  padding: 12px;
  background: #fff7ed;
}
.snapshot-table {
  margin-top: 8px;
}
.snapshot-filtered {
  margin-top: 10px;
  font-size: 12px;
  color: var(--muted);
}
.snapshot-filtered ul {
  margin: 6px 0 0;
  padding-left: 18px;
}
.snapshot-message {
  margin: 8px 0;
  font-size: 13px;
}
.warn-text {
  color: var(--warn, #b45309);
}
</style>
