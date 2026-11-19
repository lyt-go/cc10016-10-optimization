<template>
  <section class="page" data-module="session_mgr">
    <header class="page-head">
      <div>
        <h2>会话管理管理</h2>
        <p class="page-desc">维护采集会话的登录凭证、超时与状态轮转，为会话管理提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记采集会话</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出会话管理清单</button>
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

    <section class="snapshot-panel" data-testid="proxy-snapshot-panel">
      <header class="snapshot-head">
        <div>
          <h3>代理健康快照（跨模块链路）</h3>
          <p class="page-desc">会话发起请求前沿「代理池调度 → 代理健康快照」链路读取，读取后同步回写；空态时停止创建请求日志。</p>
        </div>
        <button class="btn primary" data-testid="refresh-snapshot" type="button" @click="refreshSnapshot">读取并回写快照</button>
      </header>

      <div v-if="snapshotView" class="snapshot-meta" data-testid="snapshot-view">
        <span>快照版本：<strong>{{ snapshotView.version ?? '未生成' }}</strong></span>
        <span>状态：<strong :data-testid="`snapshot-state-${snapshotView.state_label}`">{{ snapshotView.state_label }}</strong></span>
        <span>候选代理：{{ snapshotView.candidate_count ?? 0 }}</span>
        <span>同步回写：{{ snapshotView.write_back ? '已回写' : '未回写' }}</span>
        <span v-if="snapshotView.read_at">读取时间：{{ snapshotView.read_at }}</span>
      </div>
      <div v-if="snapshotView?.pool_empty" class="empty-state snapshot-empty" data-testid="snapshot-empty-reason">
        <strong>空态：新请求日志已停写</strong>
        <p>{{ snapshotView.empty_reason || '无可用代理，暂无候选可调度' }}</p>
      </div>

      <div v-if="emptyEvents.length" class="snapshot-filtered" data-testid="empty-events">
        <h4>空态停写记录（{{ emptyEvents.length }}）</h4>
        <ul>
          <li v-for="(event, idx) in emptyEvents" :key="idx">
            会话 #{{ event.session_id }} 于 {{ event.at }} 停写：{{ event.reason }}（快照 {{ event.snapshot_version ?? '无版本' }}）
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
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无会话管理数据，可先登记采集会话</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记采集会话</h3>
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
      <span>共 {{ total }} 条会话管理记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
interface SnapshotView {
  version: string | null
  state_label: string
  pool_empty: boolean
  empty_reason: string
  candidate_count: number
  write_back: boolean
  read_at?: string
}
interface EmptyEvent {
  session_id: number
  url: string
  reason: string
  stage: string
  snapshot_version: string | null
  at: string
}

const ENDPOINT = '/api/session_mgr'
const columns = ["会话编号", "关联源", "登录状态", "最后活动", "请求计数", "Cookie摘要", "UserAgent", "会话时长"]
const actions = ["标记活跃", "标记过期", "标记失效"]
const statuses = ["草稿", "活跃", "已过期", "已失效"]
const metricNames = ["采集会话总数", "活跃会话", "已过期会话"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const snapshotView = ref<SnapshotView | null>(null)
const emptyEvents = ref<EmptyEvent[]>([])
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

async function refreshSnapshot() {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/proxy-health-snapshot`)
    if (!response.ok) throw new Error('代理健康快照读取失败')
    snapshotView.value = (await response.json()) as SnapshotView
    const eventResponse = await request(`${ENDPOINT}/empty-events`)
    if (eventResponse.ok) {
      const eventPayload = await eventResponse.json()
      emptyEvents.value = (eventPayload.items ?? []) as EmptyEvent[]
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '代理健康快照读取失败'
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
    link.download = 'session_mgr.csv'
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
      throw new Error('会话管理动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '会话管理操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('采集会话列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '会话管理列表读取失败'
  }
}

onMounted(() => {
  void reload()
  void refreshSnapshot()
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
.snapshot-filtered {
  margin-top: 10px;
  font-size: 12px;
  color: var(--muted);
}
.snapshot-filtered ul {
  margin: 6px 0 0;
  padding-left: 18px;
}
</style>
