"""数据采集全链路引擎：实现至少8组贯通链路。

关键链路：
1. 采集源 -> 种子 -> 解析规则 -> 字段抽取
2. 采集编排 -> 会话 -> 代理池 -> 频次控制
3. 反爬对抗 -> 重试策略 -> 增量游标
4. 格式转换 -> 去重 -> 校验 -> 结果落地
5. 内容抽取 -> 格式转换
6. 渲染编排 -> 采集源资源过滤
7. 监控 -> 采集编排执行状态
8. 种子 -> 增量游标推进
9. 全链路贯通
"""
from __future__ import annotations

from typing import Any

from app.store import store


def chain_source_seed_parse_field(source_id: int) -> dict[str, Any]:
    """链路1: 采集源 -> 种子 -> 解析规则 -> 字段抽取。"""
    from app.services.crawl_source import CrawlSourceService
    from app.services.seed_config import SeedConfigService
    from app.services.parse_rule import ParseRuleService
    from app.services.field_extract import FieldExtractService

    src_svc = CrawlSourceService()
    seed_svc = SeedConfigService()
    parse_svc = ParseRuleService()
    fe_svc = FieldExtractService()

    source = src_svc.get_entry(source_id)
    if source is None:
        return {"ok": False, "message": "采集源不存在", "source_id": source_id}

    seeds = src_svc.get_linked_seeds(source_id)
    if not seeds:
        return {"ok": False, "message": "采集源无关联种子", "source_id": source_id}

    seed = seeds[0]
    seed_id = int(seed["id"])
    rules = seed_svc.get_linked_parse_rules(seed_id)
    fields: list[dict[str, Any]] = []
    for rule in rules:
        rule_id = int(rule["id"])
        generated = parse_svc.generate_field_extracts(rule_id)
        fields.extend(generated)
        # 同时抽取示例字段
        for fe in generated:
            fe_id = int(fe["id"])
            sample_html = f"<div class='item'><h1 class='title'>示例商品</h1><span class='price'>199.00</span></div>"
            fe_svc.extract_fields(fe_id, sample_html)

    return {
        "ok": True,
        "message": "链路执行成功",
        "source_id": source_id,
        "seed_id": seed_id,
        "parse_rules": [int(r["id"]) for r in rules],
        "field_extracts": [int(f["id"]) for f in fields],
        "seed": {"种子编号": seed.get("种子编号"), "目标URL模式": seed.get("目标URL模式")},
    }


def chain_schedule_session_proxy_rate(schedule_id: int) -> dict[str, Any]:
    """链路2: 采集编排 -> 会话 -> 代理池 -> 频次控制。"""
    from app.services.crawl_schedule import CrawlScheduleService
    from app.services.session_mgr import SessionMgrService
    from app.services.proxy_pool import ProxyPoolService
    from app.services.rate_limit import RateLimitService

    sched_svc = CrawlScheduleService()
    sess_svc = SessionMgrService()
    proxy_svc = ProxyPoolService()
    rate_svc = RateLimitService()

    schedule = store.find("crawl_schedule", schedule_id)
    if schedule is None:
        return {"ok": False, "message": "采集编排不存在", "schedule_id": schedule_id}

    plan = sched_svc.generate_execution_plan(schedule_id)
    if not plan.get("ok"):
        return {"ok": False, "message": f"执行计划生成失败: {plan.get('error')}", "schedule_id": schedule_id}

    # 获取关联源
    source_id = 0
    seed_code = str(schedule.get("关联种子") or "")
    for src in store.rows("crawl_source"):
        if seed_code.split("-")[1] if "-" in seed_code else "" in str(src.get("源标识") or ""):
            source_id = int(src["id"])
            break

    sess = sess_svc.rotate_session(source_id)
    if sess is None:
        sess_values = {"会话编号": f"SESS-AUTO-{schedule_id}", "关联源": str(source_id), "会话时长": "3600"}
        sess, missing = sess_svc.create_entry(sess_values)
        if missing:
            return {"ok": False, "message": f"会话创建失败: {missing}", "schedule_id": schedule_id}

    sess_id = int(sess["id"])

    # 主数据链：代理池调度先生成代理健康快照，会话管理再沿链路读取并回写
    snapshot = proxy_svc.generate_health_snapshot()
    proxy_choice = proxy_svc.select_proxy_explained()
    if not proxy_choice.get("ok"):
        # 空态不能伪装成成功调度：会话侧记录空态原因并停止创建请求日志
        blocked = sess_svc.record_request(sess_id, f"/api/execute/{schedule_id}", 0.0)
        return {
            "ok": False,
            "message": f"代理池空态，链路已阻断：{proxy_choice.get('reason')}",
            "schedule_id": schedule_id,
            "session_id": sess_id,
            "proxy_id": None,
            "pool_empty": True,
            "empty_reason": proxy_choice.get("reason"),
            "snapshot_version": snapshot.get("version"),
            "session_block": blocked,
        }

    # 快照非空：会话正常记录请求（内部会再次读取快照并同步回写）
    session_record = sess_svc.record_request(sess_id, f"/api/execute/{schedule_id}", 120.0)

    proxy_id = int(proxy_choice["proxy_id"])
    proxy_svc.record_request(proxy_id, True, 120.0)
    store.add_relation("session_to_proxy", "session_mgr", sess_id, "proxy_pool", proxy_id)

    rate_rels = store.get_relations("source_to_rate_limit", source_module="crawl_source", source_id=source_id)
    token_result: dict[str, Any] = {"allowed": True, "reason": "未绑定限流策略"}
    if rate_rels:
        token_result = rate_svc.acquire_token(rate_rels[-1]["target_id"])

    return {
        "ok": True,
        "message": "链路执行成功",
        "schedule_id": schedule_id,
        "session_id": sess_id,
        "proxy_id": proxy_id,
        "token": token_result,
        "plan_batches": plan.get("plan", {}).get("batches"),
        "snapshot_version": snapshot.get("version"),
        "session_record": session_record,
    }


def chain_anti_retry_cursor(source_id: int, response_text: str, status_code: int,
                            exc_name: str, attempt: int) -> dict[str, Any]:
    """链路3: 反爬对抗 -> 重试策略 -> 增量游标。"""
    from app.services.anti_spider import AntiSpiderService
    from app.services.retry_policy import RetryPolicyService
    from app.services.incre_cursor import IncreCursorService

    anti_svc = AntiSpiderService()
    retry_svc = RetryPolicyService()
    cursor_svc = IncreCursorService()

    anti_rels = store.get_relations("source_to_anti_spider", source_module="crawl_source", source_id=source_id)
    anti_result = {"triggered": False, "reason": "未绑定反爬策略"}
    if anti_rels:
        anti_result = anti_svc.detect_and_respond(anti_rels[-1]["target_id"], response_text, status_code)

    retry_rels = store.get_relations("source_to_retry", source_module="crawl_source", source_id=source_id)
    retry_result = {"should_retry": False, "reason": "未绑定重试策略"}
    if retry_rels:
        retry_result = retry_svc.should_retry(retry_rels[-1]["target_id"], exc_name, attempt)

    cursor_rels = store.get_relations("seed_to_cursor", source_module="seed_config")
    cursor_result: dict[str, Any] = {"ok": False, "reason": "无关联游标"}
    if cursor_rels:
        # 找与 source_id 关联的种子下的游标
        seed_rels = store.get_relations("source_to_seed", source_module="crawl_source", source_id=source_id)
        for sr in seed_rels:
            for cr in cursor_rels:
                if cr["source_id"] == sr["target_id"]:
                    cursor_id = cr["target_id"]
                    entry = store.find("incre_cursor", cursor_id)
                    if entry:
                        curr = str(entry.get("当前位置") or "0")
                        if anti_result.get("triggered"):
                            cursor_result = {"ok": False, "reason": "反爬触发，暂停游标推进", "cursor_id": cursor_id}
                        elif retry_result.get("should_retry"):
                            cursor_result = cursor_svc.advance_cursor(cursor_id, str(int(curr) + 1) if curr.isdigit() else f"{curr}_next")
                        else:
                            cursor_result = {"ok": False, "reason": "不重试，保持当前位置", "cursor_id": cursor_id, "position": curr}
                        break
            if cursor_result.get("cursor_id"):
                break

    return {
        "ok": True,
        "message": "链路执行成功",
        "anti_spider": anti_result,
        "retry": retry_result,
        "cursor": cursor_result,
    }


def chain_convert_dedup_validate_store(dedup_id: int, validate_id: int, convert_id: int,
                                       store_id: int, records: list[dict[str, Any]]) -> dict[str, Any]:
    """链路4: 格式转换 -> 去重 -> 校验 -> 结果落地。"""
    from app.services.format_convert import FormatConvertService
    from app.services.dedup_filter import DedupFilterService
    from app.services.data_validate import DataValidateService
    from app.services.result_store import ResultStoreService

    conv_svc = FormatConvertService()
    dedup_svc = DedupFilterService()
    val_svc = DataValidateService()
    res_svc = ResultStoreService()

    convert_result = conv_svc.run_pipeline(convert_id, records)
    if not convert_result.get("ok"):
        return {"ok": False, "message": f"格式转换失败: {convert_result.get('error')}", "stage": "convert"}

    dedup_result = dedup_svc.deduplicate(dedup_id, convert_result.get("output", []))
    unique_records = dedup_result.get("unique", [])

    validate_result = val_svc.validate_records(validate_id, unique_records)
    passed_records = validate_result.get("passed", [])

    store_result = res_svc.write_batch(store_id, passed_records)

    return {
        "ok": True,
        "message": "链路执行成功",
        "convert": {"input": len(records), "output": len(convert_result.get("output", []))},
        "dedup": {"unique": len(unique_records), "duplicates": len(dedup_result.get("duplicates", [])), "conflicts": len(dedup_result.get("conflicts", []))},
        "validate": {"passed": len(passed_records), "failed": len(validate_result.get("failed", [])), "blocked": len(validate_result.get("blocked", []))},
        "store": store_result,
    }


def chain_extract_convert(extract_id: int, convert_id: int, html: str) -> dict[str, Any]:
    """链路5: 内容抽取 -> 格式转换。"""
    from app.services.content_extract import ContentExtractService
    from app.services.format_convert import FormatConvertService

    ext_svc = ContentExtractService()
    conv_svc = FormatConvertService()

    extract_result = ext_svc.extract(extract_id, html)
    if not extract_result.get("ok"):
        return {"ok": False, "message": f"内容抽取失败", "extract": extract_result}

    records = [{"summary": extract_result.get("summary", ""), "score": extract_result.get("score", 0)}]
    convert_result = conv_svc.run_pipeline(convert_id, records)

    return {
        "ok": True,
        "message": "链路执行成功",
        "extract": extract_result,
        "convert": convert_result,
    }


def chain_render_source(source_id: int, resource_url: str) -> dict[str, Any]:
    """链路6: 渲染编排 -> 采集源资源过滤。"""
    from app.services.page_render import PageRenderService

    render_svc = PageRenderService()

    render_rels = store.get_relations("source_to_render", source_module="crawl_source", source_id=source_id)
    if not render_rels:
        return {"ok": False, "message": "采集源未绑定渲染配置", "source_id": source_id}

    render_id = render_rels[-1]["target_id"]
    match_result = render_svc.match_resource(render_id, resource_url)

    return {
        "ok": True,
        "message": "链路执行成功",
        "source_id": source_id,
        "render_id": render_id,
        "resource_url": resource_url,
        "allowed": match_result.get("allowed"),
        "reason": match_result.get("reason"),
    }


def chain_monitor_schedule(schedule_id: int) -> dict[str, Any]:
    """链路7: 监控 -> 采集编排执行状态。"""
    from app.services.crawl_monitor import CrawlMonitorService
    from app.services.crawl_schedule import CrawlScheduleService

    mon_svc = CrawlMonitorService()
    sched_svc = CrawlScheduleService()

    schedule = sched_svc.get_entry(schedule_id)
    if schedule is None:
        return {"ok": False, "message": "采集编排不存在", "schedule_id": schedule_id}

    source_id = 0
    seed_code = str(schedule.get("关联种子") or "")
    for src in store.rows("crawl_source"):
        if seed_code.split("-")[1] if "-" in seed_code else "" in str(src.get("源标识") or ""):
            source_id = int(src["id"])
            break

    metrics = mon_svc.refresh_metrics(source_id)
    plan = sched_svc.generate_execution_plan(schedule_id)

    return {
        "ok": True,
        "message": "链路执行成功",
        "schedule_id": schedule_id,
        "source_id": source_id,
        "metrics": metrics,
        "plan": plan.get("plan") if plan.get("ok") else None,
    }


def chain_seed_cursor_advance(seed_id: int, new_position: str) -> dict[str, Any]:
    """链路8: 种子 -> 增量游标推进。"""
    from app.services.seed_config import SeedConfigService
    from app.services.incre_cursor import IncreCursorService

    seed_svc = SeedConfigService()
    cursor_svc = IncreCursorService()

    seed = seed_svc.get_entry(seed_id)
    if seed is None:
        return {"ok": False, "message": "种子不存在", "seed_id": seed_id}

    cursor_rels = store.get_relations("seed_to_cursor", source_module="seed_config", source_id=seed_id)
    if not cursor_rels:
        return {"ok": False, "message": "种子未绑定增量游标", "seed_id": seed_id}

    cursor_id = cursor_rels[-1]["target_id"]
    result = cursor_svc.advance_cursor(cursor_id, new_position)

    return {
        "ok": result.get("ok", False),
        "message": "链路执行成功" if result.get("ok") else result.get("error", "游标推进失败"),
        "seed_id": seed_id,
        "cursor_id": cursor_id,
        "advance": result,
    }


def chain_full_pipeline(source_id: int, html: str, records: list[dict[str, Any]]) -> dict[str, Any]:
    """链路9: 全链路贯通：源配置 -> 抽取 -> 转换 -> 去重 -> 校验 -> 落地。"""
    from app.services.crawl_source import CrawlSourceService
    from app.services.content_extract import ContentExtractService
    from app.services.format_convert import FormatConvertService
    from app.services.dedup_filter import DedupFilterService
    from app.services.data_validate import DataValidateService
    from app.services.result_store import ResultStoreService

    src_svc = CrawlSourceService()
    ext_svc = ContentExtractService()
    conv_svc = FormatConvertService()
    dedup_svc = DedupFilterService()
    val_svc = DataValidateService()
    res_svc = ResultStoreService()

    source = src_svc.get_entry(source_id)
    if source is None:
        return {"ok": False, "message": "采集源不存在", "source_id": source_id}

    # 找一个关联的 content_extract 任务
    extract_rows = store.rows("content_extract")
    extract_id = int(extract_rows[0]["id"]) if extract_rows else 1
    extract_result = ext_svc.extract(extract_id, html)

    # 找一个关联的 format_convert
    convert_rels = store.get_relations("extract_to_convert", source_module="content_extract", source_id=extract_id)
    convert_id = convert_rels[-1]["target_id"] if convert_rels else 1
    records_with_extract = [{"summary": extract_result.get("summary", "")}] + records
    convert_result = conv_svc.run_pipeline(convert_id, records_with_extract)

    # 找 source 关联的 dedup/validate/result
    dedup_rels = store.get_relations("source_to_dedup", source_module="crawl_source", source_id=source_id)
    dedup_id = dedup_rels[-1]["target_id"] if dedup_rels else 1

    val_rels = store.get_relations("source_to_validate", source_module="crawl_source", source_id=source_id)
    validate_id = val_rels[-1]["target_id"] if val_rels else 1

    res_rels = store.get_relations("source_to_result", source_module="crawl_source", source_id=source_id)
    store_id = res_rels[-1]["target_id"] if res_rels else 1

    dedup_result = dedup_svc.deduplicate(dedup_id, convert_result.get("output", []))
    unique_records = dedup_result.get("unique", [])
    validate_result = val_svc.validate_records(validate_id, unique_records)
    passed_records = validate_result.get("passed", [])
    store_result = res_svc.write_batch(store_id, passed_records)

    return {
        "ok": True,
        "message": "全链路执行成功",
        "source_id": source_id,
        "extract": extract_result,
        "convert": {"output_count": len(convert_result.get("output", []))},
        "dedup": {"unique": len(unique_records), "duplicates": len(dedup_result.get("duplicates", [])), "conflicts": len(dedup_result.get("conflicts", []))},
        "validate": {"passed": len(passed_records), "failed": len(validate_result.get("failed", [])), "blocked": len(validate_result.get("blocked", []))},
        "store": store_result,
    }
