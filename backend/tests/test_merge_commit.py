"""批量归并提交与续办测试。

覆盖题设完整剧本与边界规则：

- T1 一次提交多键（K=20、L=7），逐条核对，回执说明变更键与快照可读时间；
- 写入中断的批次不算完成交付，续办补齐后才出回执；
- 同批输入重送幂等，只计一次真实更新，历史回执仍可追查输入；
- 基线过期（两份回执重复写 K）整判归并冲突，不产生部分写入；
- 确认后高版本 K 持续有效，分区快照按回执读取实际生效行；
- T2 把 K 改成 30，T1 重送后仍读到 K=30、L=7，两份批次各自可追溯。
"""
from __future__ import annotations

import asyncio
import json
from urllib.parse import urlencode

import pytest

from app.main import app
from app.merge_commit import MergeCommitManager, merge_commit_manager


class AsgiJsonClient:
    """不依赖 httpx 的 ASGI 测试客户端：直接在进程内驱动路由。"""

    def __init__(self, asgi_app) -> None:
        self._app = asgi_app

    def request(self, method: str, path: str, payload: dict | None = None,
                params: dict | None = None) -> dict:
        query_string = b""
        if params:
            query_string = urlencode(params).encode("utf-8")
        body = json.dumps(payload or {}).encode("utf-8")

        async def call() -> dict:
            scope = {
                "type": "http",
                "http_version": "1.1",
                "method": method,
                "scheme": "http",
                "path": path,
                "raw_path": path.encode("utf-8"),
                "query_string": query_string,
                "headers": [(b"content-type", b"application/json")],
                "client": ("testclient", 50000),
                "server": ("testserver", 80),
            }
            received: list[dict] = []

            async def receive() -> dict:
                if not received:
                    received.append({"type": "http.request", "body": body})
                return received[-1]

            status: dict[str, int] = {}
            chunks: list[bytes] = []

            async def send(message: dict) -> None:
                if message["type"] == "http.response.start":
                    status["code"] = message["status"]
                elif message["type"] == "http.response.body":
                    chunks.append(message.get("body", b""))

            await self._app(scope, receive, send)
            return {"status_code": status["code"], "json": json.loads(b"".join(chunks) or b"{}")}

        return asyncio.run(call())

    def get(self, path: str, params: dict | None = None) -> dict:
        return self.request("GET", path, params=params)

    def post(self, path: str, json_payload: dict | None = None) -> dict:
        return self.request("POST", path, payload=json_payload)


JOB = "FUSI-DEMO"


@pytest.fixture
def manager() -> MergeCommitManager:
    manager = MergeCommitManager()
    return manager


def _batch_t1() -> list[dict]:
    return [{"id": "K", "value": 20}, {"id": "L", "value": 7}]


def _batch_t2() -> list[dict]:
    return [{"id": "K", "value": 30}]


class TestBatchSubmitAndReceipt:
    """批量归并提交与回执。"""

    def test_multi_key_submit_writes_every_key_with_receipt(self, manager: MergeCommitManager) -> None:
        result = manager.submit_batch(job_code=JOB, records=_batch_t1(), batch_id="T1")
        assert result["ok"] is True
        receipt = result["receipt"]
        assert receipt["batch_id"] == "T1"
        assert receipt["status"] == "待确认"
        assert receipt["snapshot_ready"] is False
        assert receipt["readable_at"] == ""
        # 回执逐键说明变更内容
        by_key = {item["key"]: item for item in receipt["changed_keys"]}
        assert set(by_key) == {"K", "L"}
        assert by_key["K"]["op"] == "inserted" and by_key["K"]["to_version"] == 1
        assert by_key["L"]["op"] == "inserted" and by_key["L"]["to_version"] == 1
        assert receipt["inserted"] == 2 and receipt["updated"] == 0
        # 批次台账逐条核对已写键
        assert result["batch"]["written_keys"] == ["K", "L"]
        assert result["batch"]["status"] == "待确认"

    def test_duplicate_key_inside_batch_rejected(self, manager: MergeCommitManager) -> None:
        result = manager.submit_batch(
            job_code=JOB,
            records=[{"id": "K", "value": 1}, {"id": "K", "value": 2}],
            batch_id="DUP",
        )
        assert result["ok"] is False
        assert "重复归并键" in result["message"]
        assert manager.list_batches(JOB) == []

    def test_empty_batch_rejected(self, manager: MergeCommitManager) -> None:
        result = manager.submit_batch(job_code=JOB, records=[], batch_id="EMPTY")
        assert result["ok"] is False

    def test_missing_key_field_rejected(self, manager: MergeCommitManager) -> None:
        result = manager.submit_batch(
            job_code=JOB, records=[{"value": 1}], batch_id="NOKEY",
        )
        assert result["ok"] is False


class TestIncompleteBatchAndResume:
    """不完整批次不能当作完成交付，支持续办。"""

    def test_interrupted_batch_kept_without_receipt_and_resumed(self, manager: MergeCommitManager) -> None:
        def fail_l(job_code: str, key: str) -> None:
            if key == "L":
                raise RuntimeError("模拟写入中断")

        manager.set_write_hook(fail_l)
        first = manager.submit_batch(job_code=JOB, records=_batch_t1(), batch_id="T1")
        # 写了 K 之后 L 失败：无完成回执，批次保持中断
        assert first["ok"] is False
        assert first["receipt"] is None
        assert first["batch"]["status"] == "写入中断"
        assert first["batch"]["written_keys"] == ["K"]
        assert "L" in first["batch"]["failed_keys"]

        # 中断期间快照不可读（没有任何已确认提交）
        snap = manager.read_snapshot(JOB)
        assert snap["ok"] is False

        manager.set_write_hook(None)
        resumed = manager.resume_batch("T1")
        # 续办只补写 L，K 作为已写键核对跳过，真实新增仍为 2 个键各一次
        assert resumed["ok"] is True
        receipt = resumed["receipt"]
        ops = {item["key"]: item["op"] for item in receipt["changed_keys"]}
        assert ops == {"K": "unchanged", "L": "inserted"}
        assert receipt["inserted"] == 1
        assert resumed["batch"]["written_keys"] == ["K", "L"]

    def test_resume_unknown_batch(self, manager: MergeCommitManager) -> None:
        result = manager.resume_batch("GHOST")
        assert result["ok"] is False

    def test_attempts_recorded_for_audit(self, manager: MergeCommitManager) -> None:
        manager.set_write_hook(lambda job, key: (_ for _ in ()).throw(RuntimeError("x")))
        manager.submit_batch(job_code=JOB, records=_batch_t1(), batch_id="T1")
        manager.resume_batch("T1")
        trace = manager.trace_batch("T1")
        assert trace["batch"]["attempts"] == 2


class TestIdempotentReplay:
    """同一批输入重试只能计一次真实更新。"""

    def test_resend_same_batch_returns_original_receipt(self, manager: MergeCommitManager) -> None:
        first = manager.submit_batch(job_code=JOB, records=_batch_t1(), batch_id="T1")
        commit_id = first["receipt"]["commit_id"]

        replay = manager.submit_batch(job_code=JOB, records=_batch_t1(), batch_id="T1")
        assert replay["ok"] is True
        assert replay["idempotent_replay"] is True
        assert replay["receipt"]["commit_id"] == commit_id
        # 没有产生第二次真实更新：台账仍只有两个键、版本仍为 1
        snap_pending = manager.trace_batch("T1")
        assert snap_pending["batch"]["attempts"] == 1

    def test_resend_after_confirm_still_idempotent(self, manager: MergeCommitManager) -> None:
        first = manager.submit_batch(job_code=JOB, records=_batch_t1(), batch_id="T1")
        manager.confirm_commit(first["receipt"]["commit_id"])
        replay = manager.submit_batch(job_code=JOB, records=_batch_t1(), batch_id="T1")
        assert replay["receipt"]["commit_id"] == first["receipt"]["commit_id"]
        snap = manager.read_snapshot(JOB)
        rows = {row["id"]: row for row in snap["rows"]}
        assert rows["K"]["_version"] == 1 and rows["L"]["_version"] == 1


class TestStaleBaseConflict:
    """基线过期的重复写 K 整批判冲突，不产生部分写入。"""

    def test_stale_base_whole_batch_conflicts(self, manager: MergeCommitManager) -> None:
        t1 = manager.submit_batch(job_code=JOB, records=_batch_t1(), batch_id="T1")
        base = t1["receipt"]["commit_id"]
        manager.confirm_commit(base)

        t2 = manager.submit_batch(
            job_code=JOB, records=_batch_t2(), batch_id="T2", base_commit_id=base,
        )
        assert t2["ok"] is True
        manager.confirm_commit(t2["receipt"]["commit_id"])

        # T1' 仍拿着旧基线重送（重复写 K）：整判冲突，K 不回退
        stale = manager.submit_batch(
            job_code=JOB, records=_batch_t1(), batch_id="T1-RETRY", base_commit_id=base,
        )
        assert stale["ok"] is False
        assert stale["receipt"] is None
        assert stale["batch"]["status"] == "归并冲突"
        assert stale["batch"]["written_keys"] == []

        snap = manager.read_snapshot(JOB)
        rows = {row["id"]: row for row in snap["rows"]}
        assert rows["K"]["value"] == 30 and rows["K"]["_version"] == 2
        assert rows["L"]["value"] == 7

    def test_conflict_batch_cannot_resume_directly(self, manager: MergeCommitManager) -> None:
        t1 = manager.submit_batch(job_code=JOB, records=_batch_t1(), batch_id="T1")
        base = t1["receipt"]["commit_id"]
        manager.confirm_commit(base)
        t2 = manager.submit_batch(job_code=JOB, records=_batch_t2(), batch_id="T2", base_commit_id=base)
        manager.confirm_commit(t2["receipt"]["commit_id"])
        manager.submit_batch(
            job_code=JOB, records=_batch_t1(), batch_id="OLD", base_commit_id=base,
        )
        result = manager.resume_batch("OLD")
        assert result["ok"] is False
        assert "归并冲突" in result["message"]


class TestConfirmAndSnapshot:
    """确认窗口与按回执读分区快照。"""

    def test_snapshot_unreadable_before_confirm(self, manager: MergeCommitManager) -> None:
        manager.submit_batch(job_code=JOB, records=_batch_t1(), batch_id="T1")
        snap = manager.read_snapshot(JOB)
        assert snap["ok"] is False
        assert "快照" in snap["message"]

    def test_confirm_makes_snapshot_readable_with_timing(self, manager: MergeCommitManager) -> None:
        submitted = manager.submit_batch(job_code=JOB, records=_batch_t1(), batch_id="T1")
        receipt = submitted["receipt"]
        confirmed = manager.confirm_commit(receipt["commit_id"])
        out = confirmed["receipt"]
        assert out["status"] == "已确认"
        assert out["snapshot_ready"] is True
        assert out["readable_at"] != ""
        assert out["partition_code"].startswith("SNAP-MC-")
        # 确认幂等
        again = manager.confirm_commit(receipt["commit_id"])
        assert again["receipt"]["readable_at"] == out["readable_at"]

        snap = manager.read_snapshot(JOB)
        assert snap["ok"] is True
        assert snap["commit_id"] == receipt["commit_id"]
        assert snap["partition_code"] == out["partition_code"]
        rows = {row["id"]: row for row in snap["rows"]}
        assert rows == {
            "K": {"id": "K", "value": 20, "_version": 1},
            "L": {"id": "L", "value": 7, "_version": 1},
        }

    def test_read_snapshot_by_specific_commit_and_partition(self, manager: MergeCommitManager) -> None:
        t1 = manager.submit_batch(job_code=JOB, records=_batch_t1(), batch_id="T1")
        c1 = t1["receipt"]["commit_id"]
        manager.confirm_commit(c1)
        t2 = manager.submit_batch(job_code=JOB, records=_batch_t2(), batch_id="T2", base_commit_id=c1)
        c2 = t2["receipt"]["commit_id"]
        manager.confirm_commit(c2)

        # 指定旧回执：读到 T1 时刻的历史快照行（历史回执仍可查对应输入）
        old = manager.read_snapshot(JOB, commit_id=c1)
        old_rows = {row["id"]: row for row in old["rows"]}
        assert old_rows["K"]["value"] == 20 and old_rows["K"]["_version"] == 1

        # 按分区编号读取与按提交编号读取等价
        partition_code = manager.get_receipt(c2)["receipt"]["partition_code"]
        by_partition = manager.read_snapshot(JOB, partition_code=partition_code)
        assert by_partition["commit_id"] == c2
        new_rows = {row["id"]: row for row in by_partition["rows"]}
        assert new_rows["K"]["value"] == 30 and new_rows["K"]["_version"] == 2
        assert new_rows["L"]["value"] == 7 and new_rows["L"]["_version"] == 1

    def test_unconfirmed_commit_cannot_be_read(self, manager: MergeCommitManager) -> None:
        t1 = manager.submit_batch(job_code=JOB, records=_batch_t1(), batch_id="T1")
        c1 = t1["receipt"]["commit_id"]
        manager.confirm_commit(c1)
        t2 = manager.submit_batch(job_code=JOB, records=_batch_t2(), batch_id="T2", base_commit_id=c1)
        # T2 未确认：默认读点仍是 T1，显式读 T2 被拒绝
        default_snap = manager.read_snapshot(JOB)
        assert default_snap["commit_id"] == c1
        pending = manager.read_snapshot(JOB, commit_id=t2["receipt"]["commit_id"])
        assert pending["ok"] is False
        assert "尚未确认" in pending["message"]


class TestFullScenario:
    """题设端到端剧本：T1 -> 确认窗口1 -> T2 -> 确认窗口2 -> 重送 T1。"""

    def test_t1_t2_two_confirmation_windows_and_resend(self, manager: MergeCommitManager) -> None:
        # 第一批 T1：多选键 K、L 一次提交
        t1 = manager.submit_batch(job_code=JOB, records=_batch_t1(), batch_id="T1")
        assert t1["ok"] is True
        c1 = t1["receipt"]["commit_id"]

        # 第一次确认窗口：确认 T1 并核对完整版本
        manager.confirm_commit(c1)
        window1 = manager.read_snapshot(JOB, commit_id=c1)
        rows1 = {row["id"]: row for row in window1["rows"]}
        assert rows1["K"] == {"id": "K", "value": 20, "_version": 1}
        assert rows1["L"] == {"id": "L", "value": 7, "_version": 1}

        # 第二批 T2：基于 T1 基线更正同一个 K
        t2 = manager.submit_batch(
            job_code=JOB, records=_batch_t2(), batch_id="T2", base_commit_id=c1,
        )
        assert t2["ok"] is True
        receipt2 = t2["receipt"]
        k_change = next(item for item in receipt2["changed_keys"] if item["key"] == "K")
        assert k_change["op"] == "updated"
        assert k_change["from_version"] == 1 and k_change["to_version"] == 2
        c2 = receipt2["commit_id"]

        # 第二次确认窗口：确认 T2，高版本 K 生效，L 保持
        manager.confirm_commit(c2)
        window2 = manager.read_snapshot(JOB, commit_id=c2)
        rows2 = {row["id"]: row for row in window2["rows"]}
        assert rows2["K"] == {"id": "K", "value": 30, "_version": 2}
        assert rows2["L"] == {"id": "L", "value": 7, "_version": 1}

        # 确认 T2 后重送 T1：幂等回放，不回退数据
        replay = manager.submit_batch(job_code=JOB, records=_batch_t1(), batch_id="T1")
        assert replay["idempotent_replay"] is True
        final = manager.read_snapshot(JOB)
        final_rows = {row["id"]: row for row in final["rows"]}
        assert final_rows["K"]["value"] == 30 and final_rows["K"]["_version"] == 2
        assert final_rows["L"]["value"] == 7

        # 两份批次分别可追查，历史回执仍能查到对应输入
        trace1 = manager.trace_batch("T1")
        trace2 = manager.trace_batch("T2")
        assert trace1["receipt"]["commit_id"] == c1
        assert trace2["receipt"]["commit_id"] == c2
        assert {r["id"] for r in trace1["batch"]["records"]} == {"K", "L"}
        assert {r["id"] for r in trace2["batch"]["records"]} == {"K"}
        assert manager.get_receipt(c1)["receipt"]["batch_id"] == "T1"
        assert manager.get_receipt(c2)["receipt"]["batch_id"] == "T2"


@pytest.fixture
def client() -> AsgiJsonClient:
    merge_commit_manager.reset()
    return AsgiJsonClient(app)


class TestDeltaMergeApi:
    """归并提交路由端到端测试。"""

    def test_full_flow_over_http(self, client: AsgiJsonClient) -> None:
        # 提交 T1
        resp = client.post("/api/delta_merge/batches", {
            "job_code": JOB, "records": _batch_t1(), "batch_id": "T1",
        })
        assert resp["status_code"] == 200
        body = resp["json"]
        assert body["ok"] is True
        c1 = body["receipt"]["commit_id"]

        # 待确认时快照不可读
        assert client.post("/api/delta_merge/snapshot/read", {"job_code": JOB})["json"]["ok"] is False

        # 确认 T1，第一次窗口核对
        confirmed = client.post(f"/api/delta_merge/commits/{c1}/confirm")["json"]
        assert confirmed["receipt"]["snapshot_ready"] is True
        snap1 = client.post("/api/delta_merge/snapshot/read", {"job_code": JOB})["json"]
        rows = {row["id"]: row for row in snap1["rows"]}
        assert rows["K"]["value"] == 20 and rows["L"]["value"] == 7

        # T2 基于 T1 基线更正 K
        t2 = client.post("/api/delta_merge/batches", {
            "job_code": JOB, "records": _batch_t2(), "batch_id": "T2", "base_commit_id": c1,
        })["json"]
        c2 = t2["receipt"]["commit_id"]
        client.post(f"/api/delta_merge/commits/{c2}/confirm")
        snap2 = client.post(
            "/api/delta_merge/snapshot/read", {"job_code": JOB, "commit_id": c2},
        )["json"]
        rows2 = {row["id"]: row for row in snap2["rows"]}
        assert rows2["K"]["value"] == 30 and rows2["L"]["value"] == 7

        # 重送 T1 幂等
        replay = client.post("/api/delta_merge/batches", {
            "job_code": JOB, "records": _batch_t1(), "batch_id": "T1",
        })["json"]
        assert replay["idempotent_replay"] is True
        final = client.post("/api/delta_merge/snapshot/read", {"job_code": JOB})["json"]
        final_rows = {row["id"]: row for row in final["rows"]}
        assert final_rows["K"]["value"] == 30 and final_rows["L"]["value"] == 7

        # 追溯接口
        trace = client.get("/api/delta_merge/batches/T2/trace")["json"]
        assert trace["receipt"]["commit_id"] == c2
        listed = client.get("/api/delta_merge/batches", {"job_code": JOB})["json"]
        assert listed["total"] == 2

    def test_resume_endpoint(self, client: AsgiJsonClient) -> None:
        merge_commit_manager.set_write_hook(
            lambda job, key: (_ for _ in ()).throw(RuntimeError("磁盘抖动")) if key == "L" else None
        )
        first = client.post("/api/delta_merge/batches", {
            "job_code": JOB, "records": _batch_t1(), "batch_id": "T1",
        })["json"]
        assert first["ok"] is False
        merge_commit_manager.set_write_hook(None)
        resumed = client.post("/api/delta_merge/batches/T1/resume", {})["json"]
        assert resumed["ok"] is True
        assert resumed["receipt"]["inserted"] == 1

    def test_stale_base_conflict_endpoint(self, client: AsgiJsonClient) -> None:
        t1 = client.post("/api/delta_merge/batches", {
            "job_code": JOB, "records": _batch_t1(), "batch_id": "T1",
        })["json"]
        c1 = t1["receipt"]["commit_id"]
        client.post(f"/api/delta_merge/commits/{c1}/confirm")
        t2 = client.post("/api/delta_merge/batches", {
            "job_code": JOB, "records": _batch_t2(), "batch_id": "T2", "base_commit_id": c1,
        })["json"]
        client.post(f"/api/delta_merge/commits/{t2['receipt']['commit_id']}/confirm")
        stale = client.post("/api/delta_merge/batches", {
            "job_code": JOB, "records": _batch_t1(), "batch_id": "T1-OLD", "base_commit_id": c1,
        })["json"]
        assert stale["ok"] is False
        assert stale["batch"]["status"] == "归并冲突"

    def test_generic_resource_routes_still_available(self, client: AsgiJsonClient) -> None:
        resp = client.get("/api/delta_merge")
        assert resp["status_code"] == 200
        assert resp["json"]["total"] >= 1
