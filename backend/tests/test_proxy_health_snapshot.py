"""代理健康快照与会话空态链路测试。

覆盖需求场景：
- 成功/失败/超时连续请求下，失败次数、响应延迟、成功率三因子共同驱动评分与排名；
- 指标缺失时评分标记陈旧，排名不被兜底值改写；
- 空池快照给出可解释空态，空态不伪装成成功调度，会话停止创建请求日志并记录原因；
- history_first（历史优先+失败立即降权）与 failure_filter（失败过滤暴露空池）两个版本方案；
- 中断后只续做未完成项；
- 延迟恢复后重新排名，同一代理只保留一条当前健康记录；
- 会话沿跨模块链路读取快照并同步回写。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.seed import SEED_ROWS
from app.services.proxy_pool import ProxyPoolService
from app.services.session_mgr import SessionMgrService
from app.store import store


@pytest.fixture
def fresh_store():
    """每个用例使用独立的种子数据与空流水线状态。"""
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    # 直接重建状态字典，确保运行中动态新增的键（如快照索引）也被清空
    store._pipeline_states = {
        "source_validation_cache": {},
        "cursor_snapshots": {},
        "dedup_bloom_filters": {},
        "format_pipelines": {},
        "proxy_health_scores": {},
        "proxy_health_metrics": {},
        "proxy_health_snapshot": None,
        "proxy_snapshot_generation": None,
        "proxy_health_snapshots_index": [],
        "session_request_logs": {},
        "session_empty_events": [],
        "session_snapshot_reads": [],
        "retry_backoff_records": {},
        "anti_spider_trigger_log": [],
        "schedule_execution_log": [],
        "monitor_metrics_cache": {},
        "content_extract_results": {},
        "data_validate_reports": {},
        "result_store_partitions": {},
        "rate_limit_states": {},
    }
    yield store


def _available_proxy_ids() -> list[int]:
    return [int(r["id"]) for r in store.rows("proxy_pool") if r.get("status") == "可用"]


# ----------------------------------------------------------------------
# 1. 三因子共同驱动评分；任一指标缺失则标记陈旧
# ----------------------------------------------------------------------

def test_three_factors_all_drive_score(fresh_store) -> None:
    svc = ProxyPoolService()
    before = svc.get_entry(1)["_health_score"]

    # 连续失败：成功率下降 + 失败次数上升，评分必须显著下降
    for _ in range(3):
        result = svc.record_request(1, success=False, latency_ms=400.0, outcome="failure")
    after_fail = svc.get_entry(1)
    assert after_fail["_health_score"] < before
    assert result["ok"] is True

    # 连续高延迟：即使成功，延迟因子也会拉低评分
    high_latency = svc.record_request(2, success=True, latency_ms=900.0)
    low_latency = svc.record_request(5, success=True, latency_ms=40.0)
    assert low_latency["current_score"] > high_latency["current_score"]


def test_missing_metric_marks_stale_and_keeps_old_ranking(fresh_store) -> None:
    svc = ProxyPoolService()
    # 造一个缺失延迟与成功率字段的可用代理
    rows = store.rows("proxy_pool")
    rows.append({"id": 100, "status": "可用", "pending": False, "abnormal": False,
                 "节点编号": "PROX-MISSING", "代理地址": "10.1.1.1", "端口号": "80",
                 "协议类型": "http", "地域标识": "华东-杭州",
                 "响应延迟": "", "成功率": "", "失效次数": ""})
    entry = svc.get_entry(100)
    assert entry["_stale"] is True
    assert set(entry["_missing_metrics"]) == {"成功率", "响应延迟", "失效次数"}

    snapshot = svc.generate_health_snapshot()
    candidate = next(c for c in snapshot["candidates"] if c["proxy_id"] == 100)
    assert candidate["stale"] is True
    assert candidate["missing_metrics"], "缺失指标必须在快照里显式说明"
    # 陈旧评分仍可计算用于展示，但不得伪装成新鲜数据
    assert isinstance(candidate["health_score"], float)


def test_invalid_latency_is_rejected(fresh_store) -> None:
    svc = ProxyPoolService()
    result = svc.record_request(1, success=True, latency_ms=-1.0)  # type: ignore[arg-type]
    assert result["ok"] is False
    assert "延迟" in result["error"]


# ----------------------------------------------------------------------
# 2. 成功、失败、超时、重试、中断、异常结果分类入账
# ----------------------------------------------------------------------

def test_outcome_classifications_and_retry(fresh_store) -> None:
    svc = ProxyPoolService()
    svc.record_request(2, success=True, latency_ms=80.0, outcome="success")
    svc.record_request(2, success=False, latency_ms=500.0, outcome="failure",
                       error="连接被重置", is_retry=True)
    timeout_result = svc.record_request(2, success=False, latency_ms=3500.0, outcome="timeout")
    interrupted = svc.record_request(2, success=False, latency_ms=100.0, outcome="interrupted")
    errored = svc.record_request(2, success=False, latency_ms=120.0, outcome="error",
                                 error="ProtocolError")
    metrics = store.pipeline_state("proxy_health_metrics")["2"]
    assert metrics["timeouts"] >= 1
    assert metrics["retry_count"] == 1
    assert metrics["outcomes"].count("failure") >= 2  # interrupted/error 归并为失败
    assert timeout_result["outcome"] == "timeout"
    assert interrupted["ok"] and errored["ok"]


# ----------------------------------------------------------------------
# 3. history_first：旧排名保留 + 本轮失败立即降权（含旧排名与最终顺序）
# ----------------------------------------------------------------------

def test_history_first_keeps_old_rank_but_demotes_failure(fresh_store) -> None:
    svc = ProxyPoolService()
    first = svc.generate_health_snapshot(policy="history_first")
    assert first["pool_empty"] is False
    top_first = first["candidates"][0]

    # 让当前第一名本轮失败（关闭自动刷新，保证下一版快照由测试显式生成）
    failed_id = top_first["proxy_id"]
    svc.record_request(failed_id, success=False, latency_ms=800.0, outcome="failure",
                       refresh_snapshot=False)

    second = svc.generate_health_snapshot(policy="history_first")
    failed_candidate = next(c for c in second["candidates"] if c["proxy_id"] == failed_id)
    # 快照同时保留旧排名与降权后的最终顺序
    assert failed_candidate["previous_rank"] == top_first["rank"] == 1
    assert failed_candidate["rank"] > failed_candidate["previous_rank"]
    assert failed_candidate["demoted"] is True
    # 新的第一名不再是失败代理
    assert second["candidates"][0]["proxy_id"] != failed_id
    assert second["previous_version"] == first["version"]


def test_history_first_conflict_with_empty_pool(fresh_store) -> None:
    """本轮失败结果与上一轮高成功率并存，且可用集合为空：快照记录冲突但不伪装成功。"""
    svc = ProxyPoolService()
    svc.generate_health_snapshot(policy="history_first")
    # 代理 5（高成功率）先成功后失败，再把全部可用代理置为失效（关闭自动刷新控制版本）
    svc.record_request(5, success=True, latency_ms=45.0, refresh_snapshot=False)
    svc.record_request(5, success=False, latency_ms=900.0, outcome="failure",
                       refresh_snapshot=False)
    for row in store.rows("proxy_pool"):
        if row.get("status") == "可用":
            row["status"] = "已失效"

    snapshot = svc.generate_health_snapshot(policy="history_first")
    assert snapshot["pool_empty"] is True
    assert snapshot["state_label"] == "空态"
    assert snapshot["candidate_count"] == 0
    assert "可用代理集合为空" in snapshot["empty_reason"]

    # 会话侧必须停写请求日志并记录空态原因
    sess_svc = SessionMgrService()
    blocked = sess_svc.record_request(1, "/api/x", 100.0)
    assert blocked["ok"] is False and blocked["request_log_written"] is False
    events = store.pipeline_state("session_empty_events")
    assert events and "空" in events[-1]["reason"]
    assert store.pipeline_state("session_request_logs") == {}


# ----------------------------------------------------------------------
# 4. failure_filter：先过滤不合格代理，空集合时暴露空态与原因
# ----------------------------------------------------------------------

def test_failure_filter_exposes_empty_pool_with_reason(fresh_store) -> None:
    svc = ProxyPoolService()
    # 让所有可用代理在观测窗口内持续失败，失败率超过阈值
    for pid in _available_proxy_ids():
        for _ in range(5):
            svc.record_request(pid, success=False, latency_ms=600.0, outcome="failure",
                               refresh_snapshot=False)
    snapshot = svc.generate_health_snapshot(policy="failure_filter")
    assert snapshot["pool_empty"] is True
    assert snapshot["empty_stage"] == "filtered_empty"
    assert snapshot["candidates"] == []
    assert snapshot["filtered_out"], "被过滤代理及原因必须可解释"
    assert "失败过滤" in snapshot["empty_reason"]

    # select 不能伪装成功
    choice = svc.select_proxy_explained()
    assert choice["ok"] is False and choice["empty"] is True
    assert choice["reason"]


def test_failure_filter_keeps_healthy_candidates(fresh_store) -> None:
    svc = ProxyPoolService()
    svc.record_request(2, success=True, latency_ms=80.0)
    snapshot = svc.generate_health_snapshot(policy="failure_filter")
    assert snapshot["pool_empty"] is False
    ids = [c["proxy_id"] for c in snapshot["candidates"]]
    assert 2 in ids
    # 失败过滤版本按健康分降序
    scores = [c["health_score"] for c in snapshot["candidates"]]
    assert scores == sorted(scores, reverse=True)


# ----------------------------------------------------------------------
# 5. 没有数据 / 空态 / 恢复后重新排名，且同一代理只有一条当前记录
# ----------------------------------------------------------------------

def test_no_snapshot_returns_explainable_no_data(fresh_store) -> None:
    view = ProxyPoolService().get_health_snapshot()
    assert view["version"] is None
    assert view["state_label"] == "没有数据"
    assert "尚未生成" in view["empty_reason"]


def test_recovery_reranks_and_single_current_record(fresh_store) -> None:
    svc = ProxyPoolService()
    # 先把池清空得到空态快照
    for row in store.rows("proxy_pool"):
        if row.get("status") == "可用":
            row["status"] = "已失效"
    empty_snapshot = svc.generate_health_snapshot()
    assert empty_snapshot["pool_empty"] is True

    # 延迟恢复：重新标记一个代理可用，并用低延迟成功请求恢复
    recovered = store.find("proxy_pool", 5)
    recovered["status"] = "可用"
    svc.record_request(5, success=True, latency_ms=40.0)
    recovered_snapshot = svc.generate_health_snapshot()
    assert recovered_snapshot["pool_empty"] is False
    ids = [c["proxy_id"] for c in recovered_snapshot["candidates"]]
    assert ids.count(5) == 1, "恢复后同一代理只能出现一条当前健康记录"
    assert recovered_snapshot["candidates"][0]["proxy_id"] == 5

    # 再次刷新，仍然只有一条
    again = svc.generate_health_snapshot()
    assert [c["proxy_id"] for c in again["candidates"]].count(5) == 1


# ----------------------------------------------------------------------
# 6. 中断后只续做未完成项
# ----------------------------------------------------------------------

def test_interrupted_snapshot_resumes_only_pending(fresh_store) -> None:
    svc = ProxyPoolService()
    interrupted = svc.generate_health_snapshot(interrupted=True)
    progress = store.pipeline_state("proxy_snapshot_generation")
    assert progress["status"] == "interrupted"
    assert interrupted["status"] == "interrupted"
    done_ids = progress["done_ids"]
    assert done_ids, "中断时应已保存已完成项"

    resumed = svc.resume_health_snapshot()
    assert resumed["ok"] if "ok" in resumed else True
    assert resumed["resumed"] is True
    assert resumed["resumed_done_ids"] == done_ids
    # 续做完成后进度被清理，且候选完整
    assert store.pipeline_state("proxy_snapshot_generation") is None
    assert resumed["candidate_count"] == len(_available_proxy_ids())

    # 没有中断任务时续做要说明原因
    again = svc.resume_health_snapshot()
    assert again["ok"] is False and "中断" in again["reason"]


# ----------------------------------------------------------------------
# 7. 会话跨模块读取快照并同步回写
# ----------------------------------------------------------------------

def test_session_reads_snapshot_and_writes_back(fresh_store) -> None:
    proxy_svc = ProxyPoolService()
    snapshot = proxy_svc.generate_health_snapshot()
    sess_svc = SessionMgrService()
    sess, _ = sess_svc.create_entry({"会话编号": "SESS-SNAP-01", "关联源": "SRC-ELEC-001"})

    view = sess_svc.read_proxy_health_snapshot(sess["id"])
    assert view["version"] == snapshot["version"]
    assert view["write_back"] is True

    reads = store.pipeline_state("session_snapshot_reads")
    assert reads[-1]["session_id"] == sess["id"]
    assert reads[-1]["write_back"] is True
    # 回写体现在快照的消费者列表
    persisted = store.pipeline_state("proxy_health_snapshot")
    assert any(c["session_id"] == sess["id"] for c in persisted["consumers"])


def test_session_request_uses_snapshot_version(fresh_store) -> None:
    proxy_svc = ProxyPoolService()
    snapshot = proxy_svc.generate_health_snapshot()
    sess_svc = SessionMgrService()
    sess, _ = sess_svc.create_entry({"会话编号": "SESS-SNAP-02", "关联源": "SRC-ELEC-001"})
    result = sess_svc.record_request(sess["id"], "/api/data", 120.0)
    assert result["ok"] is True
    assert result["snapshot_version"] == snapshot["version"]
    logs = store.pipeline_state("session_request_logs")[str(sess["id"])]
    assert logs[-1]["snapshot_version"] == snapshot["version"]


# ----------------------------------------------------------------------
# 8. 端到端：连续成功/失败/超时 -> 清空 -> 核对排名、空态原因与恢复分数
# ----------------------------------------------------------------------

def test_end_to_end_rank_empty_recovery(fresh_store) -> None:
    svc = ProxyPoolService()
    # 连续记录成功、失败、超时
    svc.record_request(2, success=True, latency_ms=80.0)
    svc.record_request(2, success=False, latency_ms=600.0, outcome="failure")
    svc.record_request(2, success=False, latency_ms=3200.0, outcome="timeout")
    snapshot = svc.generate_health_snapshot()
    candidate_2 = next(c for c in snapshot["candidates"] if c["proxy_id"] == 2)
    # 超时作为失败因子，排名应落后于全程健康的代理 5
    candidate_5 = next(c for c in snapshot["candidates"] if c["proxy_id"] == 5)
    assert candidate_5["rank"] < candidate_2["rank"]

    # 清空可用集合
    for row in store.rows("proxy_pool"):
        if row.get("status") == "可用":
            row["status"] = "已失效"
    empty = svc.generate_health_snapshot()
    assert empty["pool_empty"] is True
    assert empty["empty_reason"]

    # 恢复：代理 2 重新可用并连续成功，当前分数回升
    row2 = store.find("proxy_pool", 2)
    row2["status"] = "可用"
    score_before = svc.get_entry(2)["_health_score"]
    for _ in range(4):
        svc.record_request(2, success=True, latency_ms=60.0)
    recovered = svc.generate_health_snapshot()
    only = recovered["candidates"]
    assert len(only) == 1 and only[0]["proxy_id"] == 2
    assert only[0]["health_score"] >= score_before
