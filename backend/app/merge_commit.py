"""批量归并提交与续办：可复核的归并提交台账。

解决增量归并只有插入/更新计数、没有提交回执的问题：

- 一次提交可带多个归并键，逐条核对写入，未全部写齐的批次保留为「写入中断」，
  不能当作完成交付，可通过续办补齐；
- 同一批次输入（batch_id 相同）的重送按幂等处理，只计一次真实更新，
  历史回执仍能查到对应输入；
- 提交时携带期望基线，基线已被后续归并推进则整批判为「归并冲突」，不产生部分写入；
- 归并提交先以「待确认」状态登记回执，确认后才进入分区快照可读点，
  分区快照始终按回执读取实际生效的快照行，已确认的高版本键持续有效。
"""
from __future__ import annotations

import copy
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class KeyChange:
    """单个归并键在一次归并中的变更明细。"""

    key: str
    op: str                      # inserted / updated / unchanged / failed
    from_version: int
    to_version: int
    before: dict[str, Any] | None
    after: dict[str, Any] | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "op": self.op,
            "from_version": self.from_version,
            "to_version": self.to_version,
            "before": copy.deepcopy(self.before),
            "after": copy.deepcopy(self.after),
        }


@dataclass
class MergeBatch:
    """归并批次：逐条核对已写键，支撑中断续办与批次追溯。"""

    batch_id: str
    job_code: str
    base_commit_id: str
    records: list[dict[str, Any]]
    status: str                  # 待确认 / 已确认 / 写入中断 / 归并冲突
    written_keys: list[str] = field(default_factory=list)
    failed_keys: list[str] = field(default_factory=list)
    attempts: int = 0
    commit_id: str = ""
    last_error: str = ""
    created_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["records"] = copy.deepcopy(self.records)
        return data


@dataclass
class MergeReceipt:
    """归并提交回执：说明变更了哪些键、何时可供分区快照读取。"""

    commit_id: str
    batch_id: str
    job_code: str
    base_commit_id: str
    status: str                  # 待确认 / 已确认
    changed_keys: list[KeyChange]
    inserted: int
    updated: int
    unchanged: int
    failed: int
    snapshot_ready: bool
    committed_at: str
    readable_at: str = ""        # 确认前为空，确认后即分区快照可读时间
    partition_code: str = ""
    confirmed_at: str = ""
    snapshot_rows: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "commit_id": self.commit_id,
            "batch_id": self.batch_id,
            "job_code": self.job_code,
            "base_commit_id": self.base_commit_id,
            "status": self.status,
            "changed_keys": [change.to_dict() for change in self.changed_keys],
            "inserted": self.inserted,
            "updated": self.updated,
            "unchanged": self.unchanged,
            "failed": self.failed,
            "snapshot_ready": self.snapshot_ready,
            "committed_at": self.committed_at,
            "readable_at": self.readable_at,
            "partition_code": self.partition_code,
            "confirmed_at": self.confirmed_at,
            "snapshot_rows": copy.deepcopy(self.snapshot_rows),
        }


# 测试可替换的写入钩子：签名 (job_code, key) -> None，抛异常模拟该键写入失败。
WriteHook = Callable[[str, str], None]


class MergeCommitManager:
    """批量归并提交管理器：提交、续办、确认、快照读取与回执追溯。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._batches: dict[str, MergeBatch] = {}
        self._receipts: dict[str, MergeReceipt] = {}
        # (job_code, key) -> 当前生效行
        self._rows: dict[tuple[str, str], dict[str, Any]] = {}
        # job_code -> 已确认提交序列（分区快照读取链）
        self._committed_chain: dict[str, list[str]] = {}
        self._write_hook: WriteHook | None = None

    # ---- 测试/运维注入 ----

    def set_write_hook(self, hook: WriteHook | None) -> None:
        """注入写入钩子，用于演练写入中断与续办；生产路径保持 None。"""
        self._write_hook = hook

    def reset(self) -> None:
        with self._lock:
            self._batches.clear()
            self._receipts.clear()
            self._rows.clear()
            self._committed_chain.clear()

    # ---- 提交与续办 ----

    def submit_batch(
        self,
        *,
        job_code: str,
        records: list[dict[str, Any]],
        key_field: str = "id",
        batch_id: str = "",
        base_commit_id: str = "",
    ) -> dict[str, Any]:
        """提交一个批量归并。

        - batch_id 相同的重送一律返回原回执/原批次，不重复计真实更新；
        - base_commit_id 非空且不是该作业最近已确认提交时，整批判为归并冲突；
        - 任一键写入失败则批次保持「写入中断」，不生成完成回执，等待续办。
        """
        if not str(job_code or "").strip():
            return {"ok": False, "message": "融合作业编号不能为空"}
        if not records:
            return {"ok": False, "message": "批次至少包含一条归并记录"}

        keys = [str(rec.get(key_field, "")).strip() for rec in records]
        if any(not key for key in keys):
            return {"ok": False, "message": f"存在缺少归并键字段「{key_field}」的记录"}
        if len(set(keys)) != len(keys):
            return {"ok": False, "message": "批次内存在重复归并键，请拆成单条后再提交"}

        bid = batch_id.strip() or f"BATCH-{uuid.uuid4().hex[:12]}"

        with self._lock:
            existing = self._batches.get(bid)
            if existing is not None:
                # 幂等：同一批输入重送只能计一次真实更新，直接回放历史结论。
                return self._replay_existing(existing)

            if base_commit_id:
                latest = self._latest_commit(job_code)
                if latest != base_commit_id:
                    conflict = MergeBatch(
                        batch_id=bid,
                        job_code=job_code,
                        base_commit_id=base_commit_id,
                        records=copy.deepcopy(records),
                        status="归并冲突",
                        last_error=f"期望基线 {base_commit_id} 已被后续提交 {latest or '无'} 推进",
                    )
                    self._batches[bid] = conflict
                    return {
                        "ok": False,
                        "message": "归并冲突：两份提交重复写同一键，基线已过期，整批未写入",
                        "batch": conflict.to_dict(),
                        "receipt": None,
                    }

            batch = MergeBatch(
                batch_id=bid,
                job_code=job_code,
                base_commit_id=base_commit_id,
                records=copy.deepcopy(records),
                status="写入中断",
            )
            self._batches[bid] = batch
            return self._process_batch(batch, key_field)

    def resume_batch(self, batch_id: str, key_field: str = "id") -> dict[str, Any]:
        """续办中断批次：已写键逐条核对跳过，只补写未完成的键。"""
        with self._lock:
            batch = self._batches.get(batch_id)
            if batch is None:
                return {"ok": False, "message": f"批次 {batch_id} 不存在，无法续办"}
            if batch.status in ("待确认", "已确认"):
                return self._replay_existing(batch)
            if batch.status == "归并冲突":
                return {
                    "ok": False,
                    "message": "归并冲突批次不能直接续办，请基于最新基线重新提交",
                    "batch": batch.to_dict(),
                    "receipt": None,
                }
            return self._process_batch(batch, key_field)

    def _process_batch(self, batch: MergeBatch, key_field: str) -> dict[str, Any]:
        """逐条写入并核对；任一键失败则批次保持中断，已写键保留。"""
        batch.attempts += 1
        changes: list[KeyChange] = []
        written = set(batch.written_keys)

        for rec in batch.records:
            key = str(rec.get(key_field, "")).strip()
            if key in written:
                # 续办时核对已写键：不重复写、不重复计数。
                current = self._rows[(batch.job_code, key)]
                changes.append(self._describe_existing(key, current))
                continue

            row_key = (batch.job_code, key)
            before = copy.deepcopy(self._rows.get(row_key))
            from_version = int(before["_version"]) if before else 0
            try:
                if self._write_hook is not None:
                    self._write_hook(batch.job_code, key)
            except Exception as exc:  # 写入失败：中断交付，保留进度
                batch.failed_keys = sorted(set(batch.failed_keys) | {key})
                batch.last_error = f"键 {key} 写入失败：{exc}"
                batch.updated_at = _now_iso()
                return {
                    "ok": False,
                    "message": f"批次 {batch.batch_id} 写入不完整，已保留断点可续办：{batch.last_error}",
                    "batch": batch.to_dict(),
                    "receipt": None,
                }

            incoming = {k: v for k, v in rec.items() if not str(k).startswith("_")}
            if before is None:
                new_row = dict(incoming)
                new_row["_version"] = 1
                self._rows[row_key] = new_row
                op = "inserted"
                to_version = 1
            else:
                new_row = dict(before)
                new_row.update(incoming)
                if new_row == before:
                    # 内容无变化的重送不计真实更新。
                    op = "unchanged"
                    to_version = from_version
                else:
                    new_row["_version"] = from_version + 1
                    op = "updated"
                    to_version = from_version + 1
                self._rows[row_key] = new_row

            batch.written_keys.append(key)
            batch.failed_keys = [item for item in batch.failed_keys if item != key]
            written.add(key)
            changes.append(
                KeyChange(
                    key=key,
                    op=op,
                    from_version=from_version,
                    to_version=to_version,
                    before=before,
                    after=copy.deepcopy(new_row),
                )
            )

        # 全部键逐条核对写齐，才允许生成完成回执（默认待确认，快照尚不可读）。
        batch.status = "待确认"
        batch.failed_keys = []
        batch.last_error = ""
        batch.updated_at = _now_iso()

        commit_id = f"MC-{uuid.uuid4().hex[:12]}"
        receipt = MergeReceipt(
            commit_id=commit_id,
            batch_id=batch.batch_id,
            job_code=batch.job_code,
            base_commit_id=batch.base_commit_id,
            status="待确认",
            changed_keys=changes,
            inserted=sum(1 for c in changes if c.op == "inserted"),
            updated=sum(1 for c in changes if c.op == "updated"),
            unchanged=sum(1 for c in changes if c.op == "unchanged"),
            failed=0,
            snapshot_ready=False,
            committed_at=_now_iso(),
        )
        batch.commit_id = commit_id
        self._receipts[commit_id] = receipt

        return {
            "ok": True,
            "message": self._receipt_summary(receipt, ready=False),
            "batch": batch.to_dict(),
            "receipt": receipt.to_dict(),
        }

    # ---- 确认与快照读取 ----

    def confirm_commit(self, commit_id: str) -> dict[str, Any]:
        """确认归并提交：确认后该回执成为分区快照的可读点。"""
        with self._lock:
            receipt = self._receipts.get(commit_id)
            if receipt is None:
                return {"ok": False, "message": f"归并提交 {commit_id} 不存在"}
            if receipt.status == "已确认":
                # 确认也幂等：重复确认回原回执，不产生新可读点。
                return {
                    "ok": True,
                    "message": self._receipt_summary(receipt, ready=True),
                    "receipt": receipt.to_dict(),
                }

            now = _now_iso()
            receipt.status = "已确认"
            receipt.snapshot_ready = True
            receipt.confirmed_at = now
            receipt.readable_at = now
            receipt.partition_code = f"SNAP-MC-{commit_id.split('-', 1)[1][:8]}"
            # 确认时刻固化快照行：历史回执永远读到当时实际生效的版本
            receipt.snapshot_rows = [
                copy.deepcopy(row)
                for (jc, _key), row in sorted(self._rows.items(), key=lambda item: item[0][1])
                if jc == receipt.job_code
            ]
            self._committed_chain.setdefault(receipt.job_code, []).append(commit_id)

            batch = self._batches.get(receipt.batch_id)
            if batch is not None:
                batch.status = "已确认"
                batch.updated_at = now

            return {
                "ok": True,
                "message": self._receipt_summary(receipt, ready=True),
                "receipt": receipt.to_dict(),
            }

    def read_snapshot(
        self,
        job_code: str,
        *,
        commit_id: str = "",
        partition_code: str = "",
    ) -> dict[str, Any]:
        """按归并提交回执读取实际生效的分区快照行。"""
        with self._lock:
            target = commit_id
            if partition_code and not target:
                target = next(
                    (cid for cid, r in self._receipts.items()
                     if r.partition_code == partition_code),
                    "",
                )
            if target:
                receipt = self._receipts.get(target)
                if receipt is None:
                    return {"ok": False, "message": f"归并提交 {target} 不存在"}
                if receipt.job_code != job_code:
                    return {"ok": False, "message": f"提交 {target} 不属于作业 {job_code}"}
                if receipt.status != "已确认":
                    return {
                        "ok": False,
                        "message": f"提交 {target} 尚未确认，分区快照暂不可读",
                        "readable_at": "",
                    }
                effective_commit = target
            else:
                effective_commit = self._latest_commit(job_code)
                if not effective_commit:
                    return {"ok": False, "message": f"作业 {job_code} 尚无已确认归并提交，快照不可读"}
                receipt = self._receipts[effective_commit]

            # 按回执读取确认时刻固化的快照行，后续归并不改变历史可读点
            rows = copy.deepcopy(receipt.snapshot_rows)
            return {
                "ok": True,
                "message": f"按回执 {effective_commit} 读取分区快照，共 {len(rows)} 行",
                "job_code": job_code,
                "commit_id": effective_commit,
                "partition_code": receipt.partition_code,
                "readable_at": receipt.readable_at,
                "rows": rows,
            }

    # ---- 查询与追溯 ----

    def get_receipt(self, commit_id: str) -> dict[str, Any]:
        with self._lock:
            receipt = self._receipts.get(commit_id)
            if receipt is None:
                return {"ok": False, "message": f"归并提交回执 {commit_id} 不存在"}
            return {"ok": True, "receipt": receipt.to_dict()}

    def list_batches(self, job_code: str = "") -> list[dict[str, Any]]:
        with self._lock:
            batches = [
                batch.to_dict() for batch in self._batches.values()
                if not job_code or batch.job_code == job_code
            ]
            return sorted(batches, key=lambda item: item["created_at"])

    def trace_batch(self, batch_id: str) -> dict[str, Any]:
        """按输入批次追溯：批次台账 + 对应归并提交回执。"""
        with self._lock:
            batch = self._batches.get(batch_id)
            if batch is None:
                return {"ok": False, "message": f"批次 {batch_id} 不存在"}
            receipt = self._receipts.get(batch.commit_id) if batch.commit_id else None
            return {
                "ok": True,
                "batch": batch.to_dict(),
                "receipt": receipt.to_dict() if receipt else None,
            }

    # ---- 内部工具 ----

    def _latest_commit(self, job_code: str) -> str:
        chain = self._committed_chain.get(job_code, [])
        return chain[-1] if chain else ""

    def _replay_existing(self, batch: MergeBatch) -> dict[str, Any]:
        receipt = self._receipts.get(batch.commit_id) if batch.commit_id else None
        if receipt is not None:
            return {
                "ok": True,
                "message": f"批次 {batch.batch_id} 已提交过，按幂等回放原回执，不重复计更新",
                "batch": batch.to_dict(),
                "receipt": receipt.to_dict(),
                "idempotent_replay": True,
            }
        return {
            "ok": False,
            "message": f"批次 {batch.batch_id} 处于{batch.status}，未生成完成回执，请续办后再提交",
            "batch": batch.to_dict(),
            "receipt": None,
            "idempotent_replay": True,
        }

    @staticmethod
    def _describe_existing(key: str, row: dict[str, Any]) -> KeyChange:
        version = int(row.get("_version", 0))
        return KeyChange(
            key=key,
            op="unchanged",
            from_version=version,
            to_version=version,
            before=copy.deepcopy(row),
            after=copy.deepcopy(row),
        )

    @staticmethod
    def _receipt_summary(receipt: MergeReceipt, *, ready: bool) -> str:
        keys = "、".join(
            f"{c.key}({c.op} v{c.from_version}->v{c.to_version})"
            for c in receipt.changed_keys
        )
        timing = (
            f"已于 {receipt.readable_at} 可供分区快照读取（{receipt.partition_code}）"
            if ready
            else "确认后才可供分区快照读取"
        )
        return (
            f"归并提交 {receipt.commit_id} 变更键：{keys or '无'}；"
            f"新增 {receipt.inserted}、更新 {receipt.updated}、无变化 {receipt.unchanged}；{timing}"
        )


# 全局单例，供路由和跨模块消费
merge_commit_manager = MergeCommitManager()
