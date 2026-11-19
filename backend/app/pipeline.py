"""数据采集全链路pipeline协调器。

串联各模块形成完整数据链：
1. 采集源连接校验 -> 种子URL模板解析 -> 解析规则校验 -> 字段抽取模板生成
2. 采集编排 -> 会话创建 -> 代理选择 -> 频次令牌获取 -> 请求执行
3. 反爬检测 -> 重试判定 -> 增量游标推进 -> 内容抽取
4. 去重过滤 -> 数据校验 -> 格式转换 -> 结果落地
5. 监控指标实时聚合
"""
from __future__ import annotations

from typing import Any

from app.store import store


def run_source_config_chain(source_id: int, seed_values: dict[str, Any],
                            parse_values: dict[str, Any]) -> dict[str, Any]:
    """链路1: 采集源->种子->解析规则->字段抽取配置链。"""
    from app.services.crawl_source import CrawlSourceService
    from app.services.seed_config import SeedConfigService
    from app.services.parse_rule import ParseRuleService

    src_svc = CrawlSourceService()
    seed_svc = SeedConfigService()
    parse_svc = ParseRuleService()

    # 校验采集源
    validation = src_svc.validate_connection(source_id)
    if not validation.get("reachable"):
        return {"ok": False, "stage": "source_validation", "error": validation.get("message")}

    # 创建种子并绑定
    seed, missing = seed_svc.create_entry(seed_values)
    if missing:
        return {"ok": False, "stage": "seed_creation", "error": missing}
    seed_svc.bind_source(seed["id"], source_id)

    # 创建解析规则并绑定种子
    parse_values["关联种子ID"] = seed["id"]
    rule, missing = parse_svc.create_entry(parse_values)
    if missing:
        return {"ok": False, "stage": "parse_rule_creation", "error": missing}

    # 生成字段抽取模板
    fields = parse_svc.generate_field_extracts(rule["id"])

    return {
        "ok": True,
        "stage": "complete",
        "source_id": source_id,
        "seed_id": seed["id"],
        "parse_rule_id": rule["id"],
        "field_extracts": [f["id"] for f in fields],
    }


def run_execution_chain(schedule_id: int) -> dict[str, Any]:
    """链路2: 采集编排->会话->代理池->频次控制执行链。"""
    from app.services.crawl_schedule import CrawlScheduleService
    from app.services.session_mgr import SessionMgrService
    from app.services.proxy_pool import ProxyPoolService
    from app.services.rate_limit import RateLimitService

    sched_svc = CrawlScheduleService()
    sess_svc = SessionMgrService()
    proxy_svc = ProxyPoolService()
    rate_svc = RateLimitService()

    # 生成执行计划
    plan = sched_svc.generate_execution_plan(schedule_id)
    if not plan.get("ok"):
        return {"ok": False, "stage": "plan_generation", "error": plan.get("error")}

    # 查找或创建会话
    schedule = store.find("crawl_schedule", schedule_id)
    if schedule is None:
        return {"ok": False, "stage": "schedule_lookup", "error": "计划不存在"}

    # 轮替会话
    source_id = 0
    try:
        source_id = int(str(schedule.get("关联种子") or "0"))
    except (ValueError, TypeError):
        pass
    sess = sess_svc.rotate_session(source_id)
    if sess is None:
        # 创建新会话
        sess_values = {"会话编号": f"SESS-AUTO-{schedule_id}", "关联源": str(source_id)}
        sess, missing = sess_svc.create_entry(sess_values)
        if missing:
            return {"ok": False, "stage": "session_creation", "error": missing}

    # 主数据链：代理池调度生成健康快照，会话管理读取快照（读取后同步回写）
    snapshot = proxy_svc.generate_health_snapshot()
    proxy_choice = proxy_svc.select_proxy_explained()
    if not proxy_choice.get("ok"):
        # 空态：停止创建会话请求日志并记录原因，不伪装成成功调度
        blocked = sess_svc.record_request(sess["id"], f"/api/execute/{schedule_id}", 0.0)
        return {
            "ok": False,
            "stage": "proxy_pool_empty",
            "error": proxy_choice.get("reason"),
            "schedule_id": schedule_id,
            "session_id": sess["id"],
            "snapshot_version": snapshot.get("version"),
            "session_block": blocked,
        }

    # 快照非空：会话请求日志正常写入（内部读取快照并回写）
    session_record = sess_svc.record_request(sess["id"], f"/api/execute/{schedule_id}", 150.0)
    proxy_id = int(proxy_choice["proxy_id"])
    proxy_svc.record_request(proxy_id, True, 150.0)

    # 获取频次令牌
    rate_rels = store.get_relations("source_to_rate_limit", source_module="crawl_source", source_id=source_id)
    token_result = {"allowed": True}
    if rate_rels:
        token_result = rate_svc.acquire_token(rate_rels[-1]["target_id"])

    return {
        "ok": True,
        "stage": "execution_ready",
        "schedule_id": schedule_id,
        "session_id": sess["id"],
        "proxy_id": proxy_id,
        "token": token_result,
        "snapshot_version": snapshot.get("version"),
        "session_record": session_record,
    }


def run_resilience_chain(source_id: int, response_text: str, status_code: int,
                         exc_name: str, attempt: int, cursor_id: int) -> dict[str, Any]:
    """链路3: 反爬检测->重试判定->增量游标推进。"""
    from app.services.anti_spider import AntiSpiderService
    from app.services.retry_policy import RetryPolicyService
    from app.services.incre_cursor import IncreCursorService

    anti_svc = AntiSpiderService()
    retry_svc = RetryPolicyService()
    cursor_svc = IncreCursorService()

    # 反爬检测
    anti_rels = store.get_relations("source_to_anti_spider", source_module="crawl_source", source_id=source_id)
    anti_result = {"triggered": False}
    if anti_rels:
        anti_result = anti_svc.detect_and_respond(anti_rels[-1]["target_id"], response_text, status_code)

    # 重试判定
    retry_rels = store.get_relations("source_to_retry", source_module="crawl_source", source_id=source_id)
    retry_result = {"should_retry": False}
    if retry_rels:
        retry_result = retry_svc.should_retry(retry_rels[-1]["target_id"], exc_name, attempt)

    # 游标推进（仅在成功时）
    cursor_result = {"ok": False}
    if not anti_result.get("triggered") and retry_result.get("should_retry"):
        # 模拟成功后的位置推进
        cursor_result = cursor_svc.advance_cursor(cursor_id, str(int(store.find("incre_cursor", cursor_id).get("当前位置") or "0") + 1))

    return {
        "ok": True,
        "anti_spider": anti_result,
        "retry": retry_result,
        "cursor": cursor_result,
    }


def run_data_processing_chain(dedup_id: int, validate_id: int, convert_id: int,
                              store_id: int, records: list[dict[str, Any]]) -> dict[str, Any]:
    """链路4: 去重->校验->格式转换->结果落地。"""
    from app.services.dedup_filter import DedupFilterService
    from app.services.data_validate import DataValidateService
    from app.services.format_convert import FormatConvertService
    from app.services.result_store import ResultStoreService

    dedup_svc = DedupFilterService()
    val_svc = DataValidateService()
    conv_svc = FormatConvertService()
    res_svc = ResultStoreService()

    # 去重
    dedup_result = dedup_svc.deduplicate(dedup_id, records)
    unique_records = dedup_result.get("unique", [])

    # 校验
    validate_result = val_svc.validate_records(validate_id, unique_records)
    passed_records = validate_result.get("passed", [])

    # 格式转换
    convert_result = conv_svc.run_pipeline(convert_id, passed_records)
    output_records = convert_result.get("output", [])

    # 结果落地
    store_result = res_svc.write_batch(store_id, output_records)

    return {
        "ok": True,
        "dedup": {"unique_count": len(unique_records), "duplicate_count": len(dedup_result.get("duplicates", []))},
        "validate": {"passed_count": len(passed_records), "failed_count": len(validate_result.get("failed", []))},
        "convert": {"output_count": len(output_records)},
        "store": store_result,
    }


def run_monitor_aggregation(source_id: int) -> dict[str, Any]:
    """链路5: 监控指标实时聚合（消费各模块产物）。"""
    from app.services.crawl_monitor import CrawlMonitorService
    mon_svc = CrawlMonitorService()
    metrics = mon_svc.refresh_metrics(source_id)
    return {"ok": True, "metrics": metrics}
