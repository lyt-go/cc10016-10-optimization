"""代理池调度接口：维护代理节点，覆盖提交校验、标记可用、标记失效等动作。"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.proxy_pool import ProxyPoolService

router = APIRouter(prefix="/api/proxy_pool", tags=["代理池调度"])

service = ProxyPoolService()

LIST_FIELDS = ["节点编号", "代理地址", "端口号", "协议类型", "地域标识", "响应延迟", "成功率", "失效次数"]
STATUSES = ["草稿", "待校验", "可用", "已失效"]


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按节点编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、可用、已失效"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、节点编号关键字与状态过滤代理池调度列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status"])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in LIST_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="proxy_pool.csv"'})


@router.get("/health-snapshot")
def get_health_snapshot(
    auto_generate: bool = Query(default=False, description="无快照时是否即时生成"),
    policy: str = Query(default="history_first", description="history_first 或 failure_filter"),
) -> dict:
    """读取当前代理健康快照；无快照/空池均给出明确原因（须声明在 /{entry_id} 之前）。"""
    return service.get_health_snapshot(auto_generate=auto_generate, policy=policy)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条代理节点明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"代理节点 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条代理节点，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="代理节点已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条代理节点执行提交校验、标记可用、标记失效；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/select")
def select_proxy(payload: dict[str, object]) -> dict:
    """按地域等条件选择最优代理；空池时返回可解释原因而不是伪装成功。"""
    region = payload.get("region")
    result = service.select_proxy_explained(region=str(region) if region else None)
    if not result.get("ok"):
        return {"ok": False, "error": result.get("reason", "无可用代理"),
                "empty": True, "snapshot_version": result.get("snapshot_version")}
    return {"ok": True, "proxy": result}


@router.post("/health-snapshot/generate")
def generate_health_snapshot(payload: dict[str, object] | None = None) -> dict:
    """主数据链：代理池调度生成代理健康快照。"""
    payload = payload or {}
    policy = str(payload.get("policy") or "history_first")
    region = payload.get("region")
    interrupted = bool(payload.get("interrupted", False))
    if policy not in ("history_first", "failure_filter"):
        return {"ok": False, "message": f"未知快照版本方案「{policy}」，可选 history_first / failure_filter"}
    snapshot = service.generate_health_snapshot(
        policy=policy, region=str(region) if region else None, interrupted=interrupted)
    return {"ok": True, "message": "代理健康快照已生成" if not snapshot["pool_empty"]
            else f"快照已生成但处于空态：{snapshot['empty_reason']}", "snapshot": snapshot}


@router.post("/health-snapshot/resume")
def resume_health_snapshot() -> dict:
    """中断后续做快照，只处理未完成项。"""
    result = service.resume_health_snapshot()
    if not result.get("ok"):
        return {"ok": False, "message": result.get("reason", "无可续做任务")}
    return {"ok": True, "message": "快照已按中断进度续做完成", "snapshot": result}


@router.post("/{entry_id}/record")
def record_request(entry_id: int, payload: dict[str, object]) -> dict:
    """记录代理请求结果并刷新健康评分与快照。

    支持 success/failure/timeout/interrupted/error 五类结果，
    可携带 error 原因与 is_retry 重试标记；延迟非法或节点缺失时说明原因。
    """
    success = bool(payload.get("success", True))
    try:
        latency = float(payload.get("latency", 0))
    except (ValueError, TypeError):
        return {"ok": False, "error": "响应延迟缺失或格式非法，健康评分未更新"}
    outcome = payload.get("outcome")
    error = payload.get("error")
    is_retry = bool(payload.get("is_retry", False))
    return service.record_request(
        entry_id, success, latency,
        outcome=str(outcome) if outcome else None,
        error=str(error) if error else None,
        is_retry=is_retry,
    )
