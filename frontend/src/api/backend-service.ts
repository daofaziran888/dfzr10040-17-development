import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---- 增量归并：批量归并提交与续办 ----

export type MergeRecord = Record<string, string | number>

export type MergeApiResult = {
  ok: boolean
  message: string
  idempotent_replay?: boolean
  receipt?: Record<string, unknown>
  batch?: Record<string, unknown>
  rows?: Record<string, unknown>[]
  [key: string]: unknown
}

export async function submitMergeBatch(payload: {
  job_code: string
  records: MergeRecord[]
  key_field?: string
  batch_id?: string
  base_commit_id?: string
}): Promise<MergeApiResult> {
  const response = await request('/api/delta_merge/batches', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return await response.json()
}

export async function resumeMergeBatch(
  batchId: string,
  keyField = 'id',
): Promise<MergeApiResult> {
  const response = await request(`/api/delta_merge/batches/${batchId}/resume`, {
    method: 'POST',
    body: JSON.stringify({ key_field: keyField }),
  })
  return await response.json()
}

export async function confirmMergeCommit(commitId: string): Promise<MergeApiResult> {
  const response = await request(`/api/delta_merge/commits/${commitId}/confirm`, {
    method: 'POST',
    body: JSON.stringify({}),
  })
  return await response.json()
}

export async function readMergeSnapshot(payload: {
  job_code: string
  commit_id?: string
  partition_code?: string
}): Promise<MergeApiResult> {
  const response = await request('/api/delta_merge/snapshot/read', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return await response.json()
}

export async function traceMergeBatch(batchId: string): Promise<MergeApiResult> {
  const response = await request(`/api/delta_merge/batches/${batchId}/trace`)
  return await response.json()
}

export async function listMergeBatches(jobCode = ''): Promise<MergeApiResult> {
  const query = new URLSearchParams(jobCode ? { job_code: jobCode } : {}).toString()
  const response = await request(`/api/delta_merge/batches?${query}`)
  return await response.json()
}
