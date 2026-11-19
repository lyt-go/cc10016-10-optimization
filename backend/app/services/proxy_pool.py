"""代理池调度业务规则：健康检查、评分衰减、轮替调度与代理健康快照。

代理健康快照（proxy_health_snapshot）是本模块对外的主数据链产物：
- 每次读取代理请求结果后，按失败次数、响应延迟、成功率三个指标共同重算健康评分；
- 任一指标缺失时评分标记为 stale（陈旧），排名保留旧值，避免排名长期停留在旧值的误判；
- 无可用代理时快照不伪装成成功调度，而是给出可解释的空态原因；
- 延迟恢复后重新生成快照并重新排名，同一代理只保留一条当前健康记录；
- 支持 history_first（历史优先，失败立即降权）与 failure_filter（失败过滤后再排名）两种版本方案；
- 快照生成支持中断，中断后只续做未完成项。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.store import store

MODULE = "proxy_pool"
LIST_FIELDS = ["节点编号", "代理地址", "端口号", "协议类型", "地域标识", "响应延迟", "成功率", "失效次数"]
REQUIRED_FIELDS = ["节点编号", "代理地址", "端口号"]
STATUS_ORDER = ["草稿", "待校验", "可用", "已失效"]
ACTION_RULES = {"提交校验": "待校验", "标记可用": "可用", "标记失效": "已失效"}
NEGATIVE_ACTIONS = ["标记失效"]

SNAPSHOT_STATE = "proxy_health_snapshot"
METRICS_STATE = "proxy_health_metrics"
GENERATION_STATE = "proxy_snapshot_generation"
WINDOW_SIZE = 20          # 滚动观测窗口
HISTORY_LIMIT = 50        # 每代理评分历史上限
TIMEOUT_THRESHOLD_MS = 3000.0
FAILURE_RATE_LIMIT = 0.6  # failure_filter 版本下，观测失败率超过该值即判定不合格
VERSION_POLICIES = ("history_first", "failure_filter")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ----------------------------------------------------------------------
# 指标解析：显式区分「值为 0」与「指标缺失」，缺失必须可解释
# ----------------------------------------------------------------------

def _parse_success_rate(value: Any) -> tuple[float | None, str | None]:
    """解析成功率（返回 0-1 的小数）；无法解析时返回 (None, 原因)。"""
    if value is None or str(value).strip() == "":
        return None, "成功率字段为空，暂无观测数据"
    try:
        rate = float(str(value).replace("%", "").strip())
    except (ValueError, TypeError):
        return None, f"成功率字段「{value}」无法解析"
    if rate > 1:
        rate = rate / 100.0
    return rate, None


def _parse_latency_ms(value: Any) -> tuple[float | None, str | None]:
    """解析响应延迟（毫秒）；无法解析时返回 (None, 原因)。"""
    if value is None or str(value).strip() == "":
        return None, "响应延迟字段为空，暂无观测数据"
    try:
        latency = float(str(value).replace("ms", "").replace("毫秒", "").strip())
    except (ValueError, TypeError):
        return None, f"响应延迟字段「{value}」无法解析"
    if latency < 0:
        return None, f"响应延迟「{latency}ms」为负值，属于异常数据"
    return latency, None


def _parse_failure_count(value: Any) -> tuple[int | None, str | None]:
    """解析失效/失败次数；无法解析时返回 (None, 原因)。"""
    if value is None or str(value).strip() == "":
        return None, "失效次数字段为空，暂无观测数据"
    try:
        count = int(float(str(value).strip()))
    except (ValueError, TypeError):
        return None, f"失效次数字段「{value}」无法解析"
    if count < 0:
        return None, f"失效次数「{count}」为负值，属于异常数据"
    return count, None


def _observed_metrics(entry_id: int) -> dict[str, Any]:
    """读取某代理的滚动观测指标（由 record_request 写入）。"""
    metrics = store.pipeline_state(METRICS_STATE) or {}
    return dict(metrics.get(str(entry_id), {}))


def _collect_indicators(entry: dict[str, Any]) -> dict[str, Any]:
    """汇总三因子指标：优先使用滚动观测值，观测缺失时回退到节点登记字段。

    返回结构同时携带 missing（缺失指标清单）与 reasons（缺失原因），
    保证「任一指标缺失」不会被静默吞掉。
    """
    entry_id = int(entry["id"])
    observed = _observed_metrics(entry_id)
    outcomes: list[str] = observed.get("outcomes", [])
    latencies: list[float] = observed.get("latencies", [])

    missing: list[str] = []
    reasons: list[str] = []

    # 因子1：成功率 —— 观测窗口优先
    if outcomes:
        success_rate = sum(1 for o in outcomes if o == "success") / len(outcomes)
        rate_source = "observed"
    else:
        success_rate, reason = _parse_success_rate(entry.get("成功率"))
        rate_source = "field"
        if success_rate is None:
            missing.append("成功率")
            reasons.append(reason or "成功率缺失")

    # 因子2：响应延迟 —— 观测窗口均值优先
    if latencies:
        latency = round(sum(latencies) / len(latencies), 2)
        latency_source = "observed"
    else:
        latency, reason = _parse_latency_ms(entry.get("响应延迟"))
        latency_source = "field"
        if latency is None:
            missing.append("响应延迟")
            reasons.append(reason or "响应延迟缺失")

    # 因子3：失败次数 —— 观测窗口失败计数优先，叠加登记的历史失效次数
    obs_failures = sum(1 for o in outcomes if o in ("failure", "timeout"))
    field_failures, field_reason = _parse_failure_count(entry.get("失效次数"))
    if not outcomes and field_failures is None:
        missing.append("失效次数")
        reasons.append(field_reason or "失效次数缺失")
        failures = obs_failures
    else:
        failures = obs_failures + (field_failures or 0)

    return {
        "proxy_id": entry_id,
        "success_rate": success_rate,
        "latency_ms": latency,
        "failures": failures,
        "timeouts": observed.get("timeouts", 0),
        "observed_requests": len(outcomes),
        "missing": missing,
        "reasons": reasons,
        "rate_source": rate_source,
        "latency_source": latency_source,
        "last_outcome": outcomes[-1] if outcomes else None,
    }


def _compute_health_score(entry: dict[str, Any], indicators: dict[str, Any] | None = None) -> float:
    """基于成功率、延迟、失败次数三因子计算健康评分（0-1）。

    缺失指标采用中性兜底值参与计算，但调用方应同时依据 indicators["missing"]
    将评分标记为陈旧（stale），不得把兜底评分当作新排名依据。
    """
    indicators = indicators or _collect_indicators(entry)
    success_rate = indicators["success_rate"]
    if success_rate is None:
        success_rate = 0.95
    latency = indicators["latency_ms"]
    if latency is None:
        latency = 200.0
    failures = indicators["failures"]
    latency_score = max(0.0, 1.0 - latency / 1000.0)
    failure_penalty = min(0.5, failures * 0.1)
    score = success_rate * 0.5 + latency_score * 0.3 - failure_penalty
    return round(max(0.0, min(1.0, score)), 3)


def _eligible_for_pool(entry: dict[str, Any], indicators: dict[str, Any]) -> tuple[bool, str]:
    """判定代理是否有资格进入健康快照候选集。"""
    if entry.get("status") != "可用":
        return False, f"代理状态为「{entry.get('status')}」，不在可用状态"
    if indicators["observed_requests"]:
        failure_rate = indicators["failures"] / indicators["observed_requests"]
        if failure_rate > FAILURE_RATE_LIMIT:
            return False, f"观测失败率 {failure_rate:.0%} 超过阈值 {FAILURE_RATE_LIMIT:.0%}，失败过滤移除"
        if indicators["last_outcome"] == "timeout":
            return False, "最近一次请求超时，暂从候选集过滤"
    if (indicators["failures"] or 0) >= 10:
        return False, f"累计失败 {indicators['failures']} 次达到熔断上限（10 次）"
    return True, ""


def _next_snapshot_id(policy: str) -> str:
    snapshots = store.pipeline_state("proxy_health_snapshots_index") or []
    seq = len(snapshots) + 1
    prefix = "PHS-H" if policy == "history_first" else "PHS-F"
    return f"{prefix}-{seq:04d}"


class ProxyPoolService:
    # ------------------------------------------------------------------
    # 常规 CRUD（保持原有契约）
    # ------------------------------------------------------------------
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("节点编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        enriched = []
        scores = store.pipeline_state("proxy_health_scores") or {}
        for row in rows[start:start + size]:
            d = dict(row)
            indicators = _collect_indicators(d)
            d["_health_score"] = _compute_health_score(d, indicators)
            d["_stale"] = bool(indicators["missing"])
            d["_missing_metrics"] = indicators["missing"]
            d["_history"] = scores.get(str(d["id"]), [])
            enriched.append(d)
        return enriched, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        d = dict(entry)
        indicators = _collect_indicators(d)
        d["_health_score"] = _compute_health_score(d, indicators)
        d["_stale"] = bool(indicators["missing"])
        d["_missing_metrics"] = indicators["missing"]
        scores = store.pipeline_state("proxy_health_scores") or {}
        d["_history"] = scores.get(str(entry_id), [])
        return d

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        entry["_health_score"] = _compute_health_score(entry)
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"代理节点 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于代理池调度可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        if action == "标记失效":
            failures = int(str(entry.get("失效次数") or "0")) + 1
            entry["失效次数"] = str(failures)
            self._append_score_history(entry_id, entry, note="失效")
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        # 状态变化后刷新快照，保证恢复/失效都能在当前快照里体现
        self.generate_health_snapshot()
        return entry, f"代理节点已{action}"

    def _append_score_history(self, entry_id: int, entry: dict[str, Any], *, note: str | None = None,
                              outcome: str | None = None, latency_ms: float | None = None) -> float:
        indicators = _collect_indicators(entry)
        score = _compute_health_score(entry, indicators)
        scores = store.pipeline_state("proxy_health_scores") or {}
        hist = scores.setdefault(str(entry_id), [])
        record: dict[str, Any] = {
            "score": score,
            "stale": bool(indicators["missing"]),
            "missing": indicators["missing"],
            "at": _now(),
        }
        if note:
            record["action"] = note
        if outcome:
            record["outcome"] = outcome
        if latency_ms is not None:
            record["latency"] = latency_ms
        hist.append(record)
        scores[str(entry_id)] = hist[-HISTORY_LIMIT:]
        store.set_pipeline_state("proxy_health_scores", scores)
        return score

    # ------------------------------------------------------------------
    # 请求结果读取：成功 / 失败 / 超时 / 重试 / 异常 统一入口
    # ------------------------------------------------------------------
    def record_request(
        self,
        entry_id: int,
        success: bool,
        latency_ms: float,
        *,
        outcome: str | None = None,
        error: str | None = None,
        is_retry: bool = False,
        refresh_snapshot: bool = True,
    ) -> dict[str, Any]:
        """记录代理请求结果并更新滚动观测指标与健康评分。

        outcome 显式区分 success / failure / timeout / interrupted / error；
        未显式给出时按 success 布尔与延迟阈值推断，失败与超时都会计入失败次数。
        指标缺失（如延迟为 None）会被记录原因，评分标记为陈旧。
        """
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": f"代理节点 {entry_id} 不存在或已归档，请求结果无法入账"}
        if not isinstance(latency_ms, (int, float)) or latency_ms < 0:
            return {"ok": False, "error": f"响应延迟「{latency_ms}」非法（缺失或为负），拒绝写入以避免污染健康指标"}

        if outcome is None:
            if success:
                outcome = "success"
            else:
                outcome = "timeout" if latency_ms >= TIMEOUT_THRESHOLD_MS else "failure"
        if outcome not in ("success", "failure", "timeout", "interrupted", "error"):
            return {"ok": False, "error": f"未知请求结果类型「{outcome}」"}
        # interrupted/error 统一按失败参与评分，但保留原始类型用于解释
        effective_outcome = "success" if outcome == "success" else ("timeout" if outcome == "timeout" else "failure")

        metrics = store.pipeline_state(METRICS_STATE) or {}
        metric = dict(metrics.get(str(entry_id), {}))
        outcomes: list[str] = list(metric.get("outcomes", []))
        latencies: list[float] = list(metric.get("latencies", []))
        outcomes.append(effective_outcome)
        latencies.append(round(float(latency_ms), 2))
        metric["outcomes"] = outcomes[-WINDOW_SIZE:]
        metric["latencies"] = latencies[-WINDOW_SIZE:]
        metric["timeouts"] = int(metric.get("timeouts", 0)) + (1 if effective_outcome == "timeout" else 0)
        metric["last_at"] = _now()
        metric["last_error"] = error
        metric["retry_count"] = int(metric.get("retry_count", 0)) + (1 if is_retry else 0)
        # 同步登记字段，保证列表页与快照看到同一份当前值
        window = metric["outcomes"]
        entry["成功率"] = f"{sum(1 for o in window if o == 'success') / len(window) * 100:.1f}%"
        entry["响应延迟"] = f"{sum(metric['latencies']) / len(metric['latencies']):.0f}ms"
        if effective_outcome == "failure":
            entry["失效次数"] = str(int(str(entry.get("失效次数") or "0")) + 1)
        metrics[str(entry_id)] = metric
        store.set_pipeline_state(METRICS_STATE, metrics)

        score = self._append_score_history(
            entry_id, entry, outcome=outcome, latency_ms=float(latency_ms))
        result = {
            "ok": True,
            "outcome": outcome,
            "is_retry": is_retry,
            "current_score": score,
            "latency_ms": round(float(latency_ms), 2),
            "window_size": len(metric["outcomes"]),
        }
        if refresh_snapshot:
            snapshot = self.generate_health_snapshot()
            result["snapshot_version"] = snapshot["version"]
            result["pool_empty"] = snapshot["pool_empty"]
        return result

    # ------------------------------------------------------------------
    # 代理选择：读取当前快照排名；空池时给出可解释原因而不是伪装成功
    # ------------------------------------------------------------------
    def select_proxy(self, region: str | None = None) -> dict[str, Any] | None:
        """按健康快照排名选择最优可用代理；无快照时即时生成。"""
        snapshot = self.get_health_snapshot(auto_generate=True)
        candidates = snapshot.get("candidates", [])
        if region:
            candidates = [c for c in candidates if region in str(c.get("地域标识") or "")]
        if not candidates:
            return None
        top = candidates[0]
        entry = store.find(MODULE, int(top["proxy_id"]))
        if entry is None:
            return None
        best = dict(entry)
        best["_selected_score"] = top["health_score"]
        best["_rank"] = top["rank"]
        return best

    def select_proxy_explained(self, region: str | None = None) -> dict[str, Any]:
        """选择代理并携带可解释结果，空池/地域无匹配时说明原因。"""
        snapshot = self.get_health_snapshot(auto_generate=True)
        if snapshot["pool_empty"]:
            return {"ok": False, "empty": True, "reason": snapshot["empty_reason"],
                    "snapshot_version": snapshot["version"]}
        candidates = snapshot["candidates"]
        if region:
            candidates = [c for c in candidates if region in str(c.get("地域标识") or "")]
        if not candidates:
            return {"ok": False, "empty": True,
                    "reason": f"当前快照无地域匹配「{region}」的可用代理（共 {len(snapshot['candidates'])} 个候选）",
                    "snapshot_version": snapshot["version"]}
        top = candidates[0]
        return {"ok": True, "proxy_id": top["proxy_id"], "节点编号": top.get("节点编号"),
                "health_score": top["health_score"], "rank": top["rank"],
                "snapshot_version": snapshot["version"]}

    # ------------------------------------------------------------------
    # 代理健康快照：生成 / 中断续做 / 读取
    # ------------------------------------------------------------------
    def _build_candidate(self, entry: dict[str, Any], rank: int, previous_rank: int | None) -> dict[str, Any]:
        indicators = _collect_indicators(entry)
        score = _compute_health_score(entry, indicators)
        return {
            "proxy_id": int(entry["id"]),
            "节点编号": entry.get("节点编号"),
            "地域标识": entry.get("地域标识"),
            "rank": rank,
            "previous_rank": previous_rank,
            "health_score": score,
            "success_rate": round(indicators["success_rate"], 4) if indicators["success_rate"] is not None else None,
            "latency_ms": indicators["latency_ms"],
            "failures": indicators["failures"],
            "timeouts": indicators["timeouts"],
            "stale": bool(indicators["missing"]),
            "missing_metrics": indicators["missing"],
            "missing_reasons": indicators["reasons"],
            "last_outcome": indicators["last_outcome"],
            "demoted": bool(previous_rank is not None and rank > previous_rank),
            "at": _now(),
        }

    def generate_health_snapshot(
        self,
        *,
        policy: str = "history_first",
        region: str | None = None,
        interrupted: bool = False,
    ) -> dict[str, Any]:
        """生成代理健康快照（主数据链产物）。

        policy:
        - history_first：保留上一版排名作为旧排名，本轮失败的代理立即降权，
          快照同时包含旧排名（previous_rank）与降权后的最终顺序；
        - failure_filter：先按失败率/超时/熔断过滤不合格代理，再对剩余候选排名，
          过滤后为空集时直接暴露空池并触发空态说明。
        interrupted=True 时模拟生成中断：保存进度，已完成项不重算。
        """
        if policy not in VERSION_POLICIES:
            raise ValueError(f"未知快照版本方案「{policy}」，可选：{VERSION_POLICIES}")

        previous = store.pipeline_state(SNAPSHOT_STATE)
        previous_order = [int(c["proxy_id"]) for c in previous.get("candidates", [])] if previous else []
        previous_rank_map = {pid: idx + 1 for idx, pid in enumerate(previous_order)}

        available = [r for r in store.rows(MODULE) if r.get("status") == "可用"]
        if region:
            available = [r for r in available if region in str(r.get("地域标识") or "")]

        filtered: list[dict[str, Any]] = []
        filter_notes: list[dict[str, Any]] = []
        for entry in available:
            indicators = _collect_indicators(entry)
            eligible, reason = _eligible_for_pool(entry, indicators)
            if policy == "failure_filter" and not eligible:
                filter_notes.append({"proxy_id": int(entry["id"]), "节点编号": entry.get("节点编号"),
                                     "reason": reason, "at": _now()})
                continue
            filtered.append(entry)

        # 空态：绝不伪装成成功调度
        if not filtered:
            if not available:
                empty_reason = "可用代理集合为空：所有代理均处于草稿/待校验/已失效状态，暂无候选可调度"
                empty_stage = "empty_pool"
            else:
                empty_reason = (
                    f"失败过滤后候选为空：{len(available)} 个可用代理全部不合格，"
                    "已移除代理：" + "；".join(n["reason"] for n in filter_notes)
                )
                empty_stage = "filtered_empty"
            snapshot = self._persist_snapshot(
                policy, candidates=[], pool_empty=True, empty_reason=empty_reason,
                empty_stage=empty_stage, filter_notes=filter_notes, interrupted=False,
                resumed_from=None, generation_status="completed")
            self._clear_generation()
            return snapshot

        # 评分与排名
        scored = [(entry, _compute_health_score(entry, _collect_indicators(entry))) for entry in filtered]
        if policy == "history_first":
            # 历史优先：健康代理先按上一版旧排名排（旧排名缺失时按分数补位），
            # 本轮发生失败/超时的代理立即降权，整体排到健康代理之后，再按分数排；
            # 快照候选同时携带 previous_rank（旧排名）与 rank（降权后最终顺序）。
            failed_ids = {
                int(e["id"]) for e in filtered
                if _collect_indicators(e)["last_outcome"] in ("failure", "timeout")
            }
            rank_index = {pid: idx for idx, pid in enumerate(previous_order)}

            def sort_key(item: tuple[dict[str, Any], float]) -> tuple[int, int, float]:
                entry, score = item
                pid = int(entry["id"])
                demoted = 1 if pid in failed_ids else 0
                old_rank = rank_index.get(pid)
                # 健康代理优先保留旧排名；无旧排名或已降权的代理按分数排序（分数高在前）
                if demoted == 0 and old_rank is not None:
                    return (0, old_rank, -score)
                return (1, old_rank if old_rank is not None else 10**6, -score)

            scored.sort(key=sort_key)
        else:
            scored.sort(key=lambda item: item[1], reverse=True)

        candidates = [
            self._build_candidate(entry, rank=i + 1,
                                  previous_rank=previous_rank_map.get(int(entry["id"])))
            for i, (entry, _score) in enumerate(scored)
        ]

        resumed_from = None
        generation_status = "completed"
        progress = store.pipeline_state(GENERATION_STATE)
        if interrupted:
            # 中断：只落已完成项的进度，快照保持上一版不变，等待续做
            done_ids = [c["proxy_id"] for c in candidates]
            progress = {
                "policy": policy,
                "region": region,
                "done_ids": done_ids,
                "pending_ids": [int(e["id"]) for e in filtered if int(e["id"]) not in done_ids],
                "started_at": _now(),
                "status": "interrupted",
                "reason": "快照生成过程被中断，已保存进度，续做时只处理未完成项",
            }
            store.set_pipeline_state(GENERATION_STATE, progress)
            generation_status = "interrupted"
        elif progress and progress.get("status") == "interrupted":
            resumed_from = {
                "resumed_at": _now(),
                "done_ids": list(progress.get("done_ids", [])),
                "pending_count": len(progress.get("pending_ids", [])),
            }
            self._clear_generation()

        snapshot = self._persist_snapshot(
            policy, candidates=candidates, pool_empty=False, empty_reason="",
            empty_stage="", filter_notes=filter_notes,
            interrupted=interrupted, resumed_from=resumed_from,
            generation_status=generation_status)
        return snapshot

    def resume_health_snapshot(self) -> dict[str, Any]:
        """中断后续做：只处理未完成项，已完成项不重算。"""
        progress = store.pipeline_state(GENERATION_STATE)
        if not progress or progress.get("status") != "interrupted":
            return {"ok": False, "reason": "没有处于中断状态的快照生成任务，无需续做"}
        policy = progress.get("policy", "history_first")
        region = progress.get("region")
        snapshot = self.generate_health_snapshot(policy=policy, region=region, interrupted=False)
        snapshot["resumed"] = True
        snapshot["resumed_done_ids"] = list(progress.get("done_ids", []))
        return snapshot

    def _persist_snapshot(self, policy: str, *, candidates: list[dict[str, Any]],
                          pool_empty: bool, empty_reason: str, empty_stage: str,
                          filter_notes: list[dict[str, Any]], interrupted: bool,
                          resumed_from: dict[str, Any] | None,
                          generation_status: str) -> dict[str, Any]:
        previous = store.pipeline_state(SNAPSHOT_STATE)
        version = _next_snapshot_id(policy)
        snapshot: dict[str, Any] = {
            "version": version,
            "policy": policy,
            "policy_label": "历史优先（失败立即降权）" if policy == "history_first" else "失败过滤后排名",
            "generated_at": _now(),
            "status": generation_status,
            "pool_empty": pool_empty,
            "empty_stage": empty_stage,
            "empty_reason": empty_reason,
            "candidate_count": len(candidates),
            "candidates": candidates,
            "filtered_out": filter_notes,
            "interrupted": interrupted,
            "resumed_from": resumed_from,
            "previous_version": previous.get("version") if previous else None,
        }
        if pool_empty:
            snapshot["state_label"] = "空态"
        elif interrupted:
            snapshot["state_label"] = "中断"
        elif not candidates:
            snapshot["state_label"] = "暂无数据"
        else:
            snapshot["state_label"] = "正常"
        store.set_pipeline_state(SNAPSHOT_STATE, snapshot)
        index = store.pipeline_state("proxy_health_snapshots_index") or []
        index.append({"version": version, "policy": policy, "at": snapshot["generated_at"],
                      "pool_empty": pool_empty, "candidate_count": len(candidates)})
        store.set_pipeline_state("proxy_health_snapshots_index", index[-100:])
        return snapshot

    def _clear_generation(self) -> None:
        store.set_pipeline_state(GENERATION_STATE, None)

    def get_health_snapshot(self, *, auto_generate: bool = False,
                            policy: str = "history_first") -> dict[str, Any]:
        """读取当前代理健康快照；不存在时返回可解释的「没有数据」空态。"""
        snapshot = store.pipeline_state(SNAPSHOT_STATE)
        if snapshot is None:
            if auto_generate:
                return self.generate_health_snapshot(policy=policy)
            return {
                "version": None,
                "policy": policy,
                "state_label": "没有数据",
                "pool_empty": True,
                "empty_stage": "no_snapshot",
                "empty_reason": "尚未生成代理健康快照：没有任何调度链接触发健康评分，暂无排名数据",
                "candidate_count": 0,
                "candidates": [],
                "filtered_out": [],
                "generated_at": None,
            }
        return snapshot

    def mark_snapshot_consumed(self, consumer: str, *, session_id: int | None = None,
                               source_chain: str = "schedule-session-proxy") -> dict[str, Any]:
        """会话管理跨模块读取快照后同步回写消费标记。"""
        snapshot = store.pipeline_state(SNAPSHOT_STATE)
        reads = store.pipeline_state("session_snapshot_reads") or []
        record = {
            "consumer": consumer,
            "session_id": session_id,
            "source_chain": source_chain,
            "snapshot_version": snapshot.get("version") if snapshot else None,
            "pool_empty": snapshot.get("pool_empty", True) if snapshot else True,
            "read_at": _now(),
            "write_back": True,
        }
        reads.append(record)
        store.set_pipeline_state("session_snapshot_reads", reads[-100:])
        if snapshot is not None:
            consumers = list(snapshot.get("consumers", []))
            consumers.append({"consumer": consumer, "session_id": session_id, "at": record["read_at"]})
            snapshot["consumers"] = consumers[-50:]
            store.set_pipeline_state(SNAPSHOT_STATE, snapshot)
        return record
