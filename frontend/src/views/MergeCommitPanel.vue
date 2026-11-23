<template>
  <section class="merge-panel" data-testid="merge-commit-panel">
    <header class="merge-head">
      <h3>批量归并提交与续办</h3>
      <p class="page-desc">多选归并键一次提交；逐条核对写入，确认后回执才进入分区快照可读点。</p>
    </header>

    <form class="merge-form" @submit.prevent="submit">
      <label class="merge-field">
        <span>融合作业</span>
        <input v-model="jobCode" data-testid="merge-job" placeholder="如 FUSI-DEMO" required />
      </label>
      <label class="merge-field">
        <span>批次编号（同号重送幂等）</span>
        <input v-model="batchId" data-testid="merge-batch" placeholder="如 T1，留空自动生成" />
      </label>
      <label class="merge-field">
        <span>期望基线提交（可选）</span>
        <input v-model="baseCommit" data-testid="merge-base" placeholder="如 MC-xxxx，过期整判冲突" />
      </label>

      <div class="merge-rows">
        <div v-for="(row, index) in rows" :key="index" class="merge-row" :data-testid="`merge-row-${index}`">
          <input v-model="row.id" data-testid="merge-key" placeholder="归并键，如 K" required />
          <input v-model="row.value" data-testid="merge-value" placeholder="值，如 20" required />
          <button class="btn ghost" type="button" data-testid="remove-key" @click="removeRow(index)">
            移除
          </button>
        </div>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" data-testid="add-key" @click="addRow">增加归并键</button>
        <button class="btn primary" type="submit" data-testid="merge-submit" :disabled="busy">
          {{ busy ? '提交中…' : '一次提交所选键' }}
        </button>
      </div>
    </form>

    <div v-if="feedback" class="merge-feedback" :class="feedbackOk ? 'ok' : 'err'" data-testid="merge-feedback">
      {{ feedback }}
    </div>

    <article v-if="receipt" class="receipt-card" data-testid="merge-receipt">
      <header>
        <strong>归并提交回执 {{ receipt.commit_id }}</strong>
        <span :class="['receipt-status', receipt.status]">{{ receipt.status }}</span>
      </header>
      <p class="page-desc">
        批次 {{ receipt.batch_id }} · 新增 {{ receipt.inserted }} · 更新 {{ receipt.updated }} ·
        无变化 {{ receipt.unchanged }}
      </p>
      <table class="data-table compact">
        <thead>
          <tr><th>归并键</th><th>变更</th><th>版本</th></tr>
        </thead>
        <tbody>
          <tr v-for="change in receipt.changed_keys" :key="change.key">
            <td>{{ change.key }}</td>
            <td>{{ change.op }}</td>
            <td>v{{ change.from_version }} → v{{ change.to_version }}</td>
          </tr>
        </tbody>
      </table>
      <p class="page-desc">
        快照可读时间：{{ receipt.readable_at || '确认后才可读' }}
        <template v-if="receipt.partition_code"> · 分区 {{ receipt.partition_code }}</template>
      </p>
      <div class="page-actions" v-if="receipt.status === '待确认'">
        <button class="btn primary" type="button" data-testid="merge-confirm" @click="confirm">
          确认提交并开放快照
        </button>
      </div>
    </article>

    <article v-if="interrupted" class="receipt-card" data-testid="merge-interrupted">
      <header><strong>批次 {{ interrupted.batch_id }} 写入中断</strong></header>
      <p class="page-desc">
        已写键：{{ interrupted.written_keys.join('、') || '无' }}；
        失败键：{{ interrupted.failed_keys.join('、') }}
      </p>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="merge-resume" @click="resume">
          续办未完成键
        </button>
      </div>
    </article>

    <div class="page-actions merge-tools">
      <button class="btn" type="button" data-testid="merge-snapshot" @click="readSnapshot">
        读取最新分区快照
      </button>
      <input v-model="traceBatchId" data-testid="merge-trace-input" placeholder="输入批次编号追溯" />
      <button class="btn" type="button" data-testid="merge-trace" @click="trace">追溯批次</button>
    </div>

    <pre v-if="snapshotRows" class="snapshot-box" data-testid="merge-snapshot-rows">{{ snapshotRows }}</pre>
  </section>
</template>

<script setup lang="ts">
import { ref } from 'vue'

import {
  confirmMergeCommit,
  readMergeSnapshot,
  resumeMergeBatch,
  submitMergeBatch,
  traceMergeBatch,
} from '@/api/backend-service'

type Receipt = {
  commit_id: string
  batch_id: string
  status: string
  inserted: number
  updated: number
  unchanged: number
  readable_at: string
  partition_code: string
  changed_keys: { key: string; op: string; from_version: number; to_version: number }[]
}

type InterruptedBatch = {
  batch_id: string
  status: string
  written_keys: string[]
  failed_keys: string[]
}

const jobCode = ref('FUSI-DEMO')
const batchId = ref('')
const baseCommit = ref('')
const rows = ref([{ id: 'K', value: '' }])
const busy = ref(false)
const feedback = ref('')
const feedbackOk = ref(true)
const receipt = ref<Receipt | null>(null)
const interrupted = ref<InterruptedBatch | null>(null)
const snapshotRows = ref('')
const traceBatchId = ref('')

function addRow() {
  rows.value.push({ id: '', value: '' })
}

function removeRow(index: number) {
  rows.value.splice(index, 1)
}

function setFeedback(message: string, ok: boolean) {
  feedback.value = message
  feedbackOk.value = ok
}

async function submit() {
  busy.value = true
  receipt.value = null
  interrupted.value = null
  try {
    const result = await submitMergeBatch({
      job_code: jobCode.value,
      batch_id: batchId.value || undefined,
      base_commit_id: baseCommit.value || undefined,
      records: rows.value.map((row) => ({ id: row.id, value: Number(row.value) })),
    })
    setFeedback(String(result.message ?? ''), Boolean(result.ok))
    if (result.receipt) receipt.value = result.receipt as Receipt
    if (!result.ok && result.batch) interrupted.value = result.batch as InterruptedBatch
  } catch (error) {
    setFeedback(error instanceof Error ? error.message : '归并提交失败', false)
  } finally {
    busy.value = false
  }
}

async function resume() {
  if (!interrupted.value) return
  const result = await resumeMergeBatch(interrupted.value.batch_id)
  setFeedback(String(result.message ?? ''), Boolean(result.ok))
  if (result.receipt) {
    receipt.value = result.receipt as Receipt
    interrupted.value = null
  } else if (result.batch) {
    interrupted.value = result.batch as InterruptedBatch
  }
}

async function confirm() {
  if (!receipt.value) return
  const result = await confirmMergeCommit(receipt.value.commit_id)
  if (result.receipt) receipt.value = result.receipt as Receipt
  setFeedback(String(result.message ?? ''), Boolean(result.ok))
}

async function readSnapshot() {
  const result = await readMergeSnapshot({ job_code: jobCode.value })
  setFeedback(String(result.message ?? ''), Boolean(result.ok))
  if (result.ok) snapshotRows.value = JSON.stringify(result.rows, null, 2)
}

async function trace() {
  if (!traceBatchId.value) return
  const result = await traceMergeBatch(traceBatchId.value)
  setFeedback(
    result.ok
      ? `批次 ${traceBatchId.value}：${result.batch?.status}，回执 ${result.receipt?.commit_id ?? '尚未生成'}`
      : String(result.message ?? '追溯失败'),
    Boolean(result.ok),
  )
}
</script>
