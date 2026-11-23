"""增量归并专用路由：批量归并提交、续办、确认与按回执读快照。

与通用资源路由并存：通用 /api/delta_merge 保留批次登记清单，
本路由承载真实的归并提交台账能力。固定子路径需在资源路由之前注册。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.merge_commit import merge_commit_manager

router = APIRouter(prefix="/api/delta_merge", tags=["增量归并提交"])


class BatchSubmitPayload(BaseModel):
    job_code: str
    records: list[dict[str, Any]] = Field(default_factory=list)
    key_field: str = "id"
    batch_id: str = ""
    base_commit_id: str = ""


class BatchResumePayload(BaseModel):
    key_field: str = "id"


class SnapshotReadPayload(BaseModel):
    job_code: str
    commit_id: str = ""
    partition_code: str = ""


@router.post("/batches")
def submit_batch(payload: BatchSubmitPayload) -> dict[str, Any]:
    """值班多选键后一次提交：逐条核对写入，返回归并提交回执。"""
    return merge_commit_manager.submit_batch(
        job_code=payload.job_code,
        records=payload.records,
        key_field=payload.key_field,
        batch_id=payload.batch_id,
        base_commit_id=payload.base_commit_id,
    )


@router.post("/batches/{batch_id}/resume")
def resume_batch(batch_id: str, payload: BatchResumePayload) -> dict[str, Any]:
    """续办写入中断的批次：已写键跳过，未写键补齐后再出回执。"""
    return merge_commit_manager.resume_batch(batch_id, key_field=payload.key_field)


@router.get("/batches")
def list_batches(job_code: str = "") -> dict[str, Any]:
    items = merge_commit_manager.list_batches(job_code)
    return {"ok": True, "items": items, "total": len(items)}


@router.get("/batches/{batch_id}/trace")
def trace_batch(batch_id: str) -> dict[str, Any]:
    """按输入批次追查台账与对应回执。"""
    return merge_commit_manager.trace_batch(batch_id)


@router.post("/commits/{commit_id}/confirm")
def confirm_commit(commit_id: str) -> dict[str, Any]:
    """确认归并提交，回执转入分区快照可读点。"""
    return merge_commit_manager.confirm_commit(commit_id)


@router.get("/commits/{commit_id}")
def get_receipt(commit_id: str) -> dict[str, Any]:
    """按提交编号查询归并提交回执。"""
    return merge_commit_manager.get_receipt(commit_id)


@router.post("/snapshot/read")
def read_snapshot(payload: SnapshotReadPayload) -> dict[str, Any]:
    """分区快照以归并提交回执读取实际生效的快照行。"""
    return merge_commit_manager.read_snapshot(
        payload.job_code,
        commit_id=payload.commit_id,
        partition_code=payload.partition_code,
    )
