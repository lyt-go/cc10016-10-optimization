"""会话管理业务规则：会话超时轮转、Cookie摘要刷新、请求计数追踪。

跨模块链路约定：会话发起请求前必须沿「代理池调度 -> 代理健康快照」链路读取当前
健康快照，读取后同步回写消费标记；快照处于空态（无可用代理）时停止创建新的请求
日志，并把空态原因记录到 session_empty_events，空态绝不伪装成成功调度。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.store import store

MODULE = "session_mgr"
LIST_FIELDS = ["会话编号", "关联源", "登录状态", "最后活动", "请求计数", "Cookie摘要", "UserAgent", "会话时长"]
REQUIRED_FIELDS = ["会话编号", "关联源"]
STATUS_ORDER = ["草稿", "活跃", "已过期", "已失效"]
ACTION_RULES = {"标记活跃": "活跃", "标记过期": "已过期", "标记失效": "已失效"}
NEGATIVE_ACTIONS = ["标记失效"]


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _check_session_expiry(entry: dict[str, Any]) -> bool:
    """检查会话是否超过时长限制（模拟）。"""
    try:
        duration = int(str(entry.get("会话时长") or "3600").replace("s", "").replace("秒", ""))
    except (ValueError, TypeError):
        duration = 3600
    req_count = int(str(entry.get("请求计数") or "0"))
    # 模拟：请求数超过阈值或固定超时即过期
    return req_count > duration / 10


class SessionMgrService:
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
            rows = [row for row in rows if keyword.casefold() in str(row.get("会话编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        enriched = []
        logs = store.pipeline_state("session_request_logs") or {}
        empty_events = store.pipeline_state("session_empty_events") or []
        snapshot = store.pipeline_state("proxy_health_snapshot")
        for row in rows[start:start + size]:
            d = dict(row)
            expired = _check_session_expiry(d)
            d["_expired"] = expired
            d["_request_log"] = logs.get(str(d["id"]), [])
            # 列表只做轻量预览，不逐行回写；显式读取与请求链路才回写
            d["_proxy_snapshot"] = self.peek_proxy_health_snapshot() if snapshot is not None else None
            d["_empty_events"] = [e for e in empty_events if e.get("session_id") == d["id"]]
            enriched.append(d)
        return enriched, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        d = dict(entry)
        d["_expired"] = _check_session_expiry(d)
        logs = store.pipeline_state("session_request_logs") or {}
        d["_request_log"] = logs.get(str(entry_id), [])
        empty_events = store.pipeline_state("session_empty_events") or []
        d["_empty_events"] = [e for e in empty_events if e.get("session_id") == entry_id]
        d["_proxy_snapshot"] = self.read_proxy_health_snapshot(entry_id)
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
        entry["请求计数"] = "0"
        rows.append(entry)
        # 绑定代理
        proxy_id = values.get("绑定代理ID")
        if proxy_id:
            try:
                pid = int(proxy_id)
                store.add_relation("session_to_proxy", MODULE, entry["id"], "proxy_pool", pid)
            except (ValueError, TypeError):
                pass
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"采集会话 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于会话管理可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"采集会话已{action}"

    def _snapshot_view(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        return {
            "version": snapshot.get("version"),
            "state_label": snapshot.get("state_label"),
            "pool_empty": snapshot.get("pool_empty", True),
            "empty_reason": snapshot.get("empty_reason", ""),
            "candidate_count": snapshot.get("candidate_count", 0),
            "top_candidate": (snapshot.get("candidates") or [None])[0],
            "read_at": _now(),
            "write_back": False,
        }

    def peek_proxy_health_snapshot(self) -> dict[str, Any]:
        """只读预览代理健康快照，不产生回写（用于列表/明细展示）。"""
        from app.services.proxy_pool import ProxyPoolService

        return self._snapshot_view(ProxyPoolService().get_health_snapshot(auto_generate=False))

    def read_proxy_health_snapshot(self, session_id: int | None = None) -> dict[str, Any]:
        """沿跨模块链路读取代理健康快照，读取后同步回写消费标记。

        会话管理不自己生成快照（主数据链由代理池调度负责），只读取当前版本；
        无快照时返回可解释的「没有数据」空态并同样回写，保证链路可追溯。
        """
        from app.services.proxy_pool import ProxyPoolService

        proxy_svc = ProxyPoolService()
        snapshot = proxy_svc.get_health_snapshot(auto_generate=False)
        proxy_svc.mark_snapshot_consumed(
            consumer=MODULE, session_id=session_id,
            source_chain="proxy_pool.health_snapshot->session_mgr")
        view = self._snapshot_view(snapshot)
        view["write_back"] = True
        return view

    def _record_empty_event(self, session_id: int, url: str, reason: str,
                            snapshot_version: str | None, stage: str) -> dict[str, Any]:
        """空态停写：记录原因但不创建请求日志。"""
        events = store.pipeline_state("session_empty_events") or []
        event = {
            "session_id": session_id,
            "url": url,
            "reason": reason,
            "stage": stage,
            "snapshot_version": snapshot_version,
            "at": _now(),
            "request_log_written": False,
        }
        events.append(event)
        store.set_pipeline_state("session_empty_events", events[-100:])
        return event

    def record_request(self, entry_id: int, url: str, latency_ms: float) -> dict[str, Any]:
        """记录会话请求并更新计数；空态时停止创建新请求日志并记录空态原因。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": "会话不存在", "request_log_written": False}
        if not isinstance(latency_ms, (int, float)) or latency_ms < 0:
            return {"ok": False, "error": "响应延迟缺失或为负值，请求日志未写入",
                    "request_log_written": False}

        # 跨模块链路：先读代理健康快照，读取即同步回写
        snapshot_view = self.read_proxy_health_snapshot(entry_id)
        if snapshot_view["pool_empty"]:
            reason = snapshot_view["empty_reason"] or "代理健康快照为空，无可用代理"
            event = self._record_empty_event(
                entry_id, url, reason, snapshot_view["version"],
                snapshot_view.get("state_label", "空态"))
            return {
                "ok": False,
                "blocked": True,
                "empty": True,
                "error": reason,
                "request_log_written": False,
                "empty_event": event,
                "snapshot_version": snapshot_view["version"],
            }

        try:
            count = int(str(entry.get("请求计数") or "0")) + 1
        except (ValueError, TypeError):
            count = 1
        entry["请求计数"] = str(count)
        entry["最后活动"] = _now()
        logs = store.pipeline_state("session_request_logs") or {}
        log = logs.setdefault(str(entry_id), [])
        log.append({"url": url, "latency": latency_ms, "count": count,
                    "snapshot_version": snapshot_view["version"], "at": _now()})
        if len(log) > 100:
            logs[str(entry_id)] = log[-100:]
        store.set_pipeline_state("session_request_logs", logs)
        return {"ok": True, "request_count": count, "request_log_written": True,
                "snapshot_version": snapshot_view["version"]}

    def rotate_session(self, source_id: int) -> dict[str, Any] | None:
        """为指定采集源轮替到一个活跃会话。"""
        rels = store.get_relations("schedule_to_session", target_module=MODULE)
        for rel in rels:
            sess = store.find(MODULE, rel["target_id"])
            if sess and sess.get("关联源") == str(source_id) and sess.get("status") == "活跃":
                return dict(sess)
        # 查找该源下任意活跃会话
        for row in store.rows(MODULE):
            if row.get("关联源") == str(source_id) and row.get("status") == "活跃":
                return dict(row)
        return None
