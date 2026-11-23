"""题设剧本手工复核脚本：T1(K=20,L=7) -> 确认窗口1 -> T2(K=30) -> 确认窗口2 -> 重送T1。

运行：.venv/bin/python -I scripts/verify_merge_commit_scenario.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.merge_commit import MergeCommitManager


def show(title: str, payload: dict) -> None:
    print(f"\n===== {title} =====")
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def main() -> None:
    mgr = MergeCommitManager()
    job = "FUSI-DEMO"

    t1 = mgr.submit_batch(
        job_code=job,
        records=[{"id": "K", "value": 20}, {"id": "L", "value": 7}],
        batch_id="T1",
    )
    show("T1 提交回执（待确认，快照尚不可读）", t1["receipt"])
    c1 = t1["receipt"]["commit_id"]

    show("确认前尝试读分区快照", mgr.read_snapshot(job))
    show("确认 T1", mgr.confirm_commit(c1))
    window1 = mgr.read_snapshot(job, commit_id=c1)
    show("第一次确认窗口：分区快照实际生效行", window1)

    t2 = mgr.submit_batch(
        job_code=job,
        records=[{"id": "K", "value": 30}],
        batch_id="T2",
        base_commit_id=c1,
    )
    show("T2 提交回执（K v1->v2）", t2["receipt"])
    c2 = t2["receipt"]["commit_id"]
    show("确认 T2", mgr.confirm_commit(c2))
    window2 = mgr.read_snapshot(job, commit_id=c2)
    show("第二次确认窗口：分区快照实际生效行", window2)

    replay = mgr.submit_batch(
        job_code=job,
        records=[{"id": "K", "value": 20}, {"id": "L", "value": 7}],
        batch_id="T1",
    )
    show("确认 T2 后重送 T1（幂等回放，不计二次更新）", replay)

    final = mgr.read_snapshot(job)
    show("最终分区快照（期望 K=30、L=7）", final)

    show("批次 T1 追溯", mgr.trace_batch("T1"))
    show("批次 T2 追溯", mgr.trace_batch("T2"))

    rows = {row["id"]: row for row in final["rows"]}
    assert rows["K"]["value"] == 30 and rows["K"]["_version"] == 2
    assert rows["L"]["value"] == 7 and rows["L"]["_version"] == 1
    print("\n全部断言通过：最终读到 K=30、L=7，两份批次回执可分别追查。")


if __name__ == "__main__":
    main()
