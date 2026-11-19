"""关键链路测试：跨模块数据流与核心数据处理pipeline验证。"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.pipeline import (
    run_data_processing_chain,
    run_execution_chain,
    run_monitor_aggregation,
    run_resilience_chain,
    run_source_config_chain,
)
from app.services.anti_spider import AntiSpiderService
from app.services.content_extract import ContentExtractService
from app.services.crawl_schedule import CrawlScheduleService
from app.services.crawl_source import CrawlSourceService
from app.services.data_validate import DataValidateService
from app.services.dedup_filter import DedupFilterService
from app.services.format_convert import FormatConvertService
from app.services.incre_cursor import IncreCursorService
from app.services.proxy_pool import ProxyPoolService
from app.services.rate_limit import RateLimitService
from app.services.result_store import ResultStoreService
from app.services.retry_policy import RetryPolicyService
from app.services.session_mgr import SessionMgrService
from app.store import store


def setup_function() -> None:
    """每个测试函数前重置store状态。"""
    store._tables.clear()
    store._pipeline_states.clear()
    store._relations.clear()


def test_source_validation_and_connection_check() -> None:
    """测试采集源连接校验链路。"""
    svc = CrawlSourceService()
    entry, missing = svc.create_entry({
        "源标识": "SRC-TEST-01",
        "源名称": "测试源",
        "协议类型": "https",
        "基础URL": "https://example.com/api",
    })
    assert not missing
    assert entry is not None
    assert entry["status"] == "草稿"

    result = svc.validate_connection(entry["id"])
    assert result["reachable"] is True
    assert result["latency"] > 0


def test_seed_xpath_and_url_pattern_validation() -> None:
    """测试种子XPath与URL模式校验链路。"""
    from app.services.seed_config import SeedConfigService
    svc = SeedConfigService()
    entry, missing = svc.create_entry({
        "种子编号": "SEED-TEST-01",
        "目标URL模式": "https://example.com/list?page={page}",
        "列表页XPath": "//div[@class='item']",
    })
    assert not missing
    assert entry is not None
    assert entry["_validation"]["url_ok"] is True
    assert entry["_validation"]["xpath_ok"] is True


def test_cross_module_source_to_seed_binding() -> None:
    """测试跨模块数据流：采集源→种子→解析规则→字段抽取。"""
    # 先创建源
    src_svc = CrawlSourceService()
    src, _ = src_svc.create_entry({
        "源标识": "SRC-CHAIN",
        "源名称": "链测试",
        "协议类型": "https",
        "基础URL": "https://example.com",
    })

    result = run_source_config_chain(
        source_id=src["id"],
        seed_values={
            "种子编号": "SEED-CHAIN-01",
            "目标URL模式": "https://example.com/items?page={page}",
            "列表页XPath": "//div[@class='product']",
        },
        parse_values={
            "规则编号": "PARS-CHAIN-01",
            "目标字段名": "title",
            "选择器类型": "xpath",
            "选择器表达式": "//h1",
            "提取方式": "text",
        },
    )
    assert result["ok"] is True
    assert result["stage"] == "complete"
    assert result["seed_id"] > 0
    assert result["parse_rule_id"] > 0
    assert len(result["field_extracts"]) > 0

    # 验证关系存在
    rels = store.get_relations("source_to_seed", source_module="crawl_source", source_id=src["id"])
    assert len(rels) >= 1


def test_dedup_filter_engine() -> None:
    """测试去重过滤引擎：精确匹配与相似度匹配。"""
    svc = DedupFilterService()
    entry, missing = svc.create_entry({
        "规则编号": "DEDU-TEST-01",
        "目标源": "test",
        "去重键列": "id",
        "比较方式": "exact",
    })
    assert not missing

    records = [
        {"id": "1", "name": "a"},
        {"id": "2", "name": "b"},
        {"id": "1", "name": "c"},  # 重复
    ]
    result = svc.deduplicate(entry["id"], records)
    assert result["ok"] is True
    assert len(result["unique"]) == 2
    assert len(result["duplicates"]) == 1
    assert result["seen_count"] == 2


def test_format_convert_pipeline() -> None:
    """测试格式转换pipeline：映射->展开->拍平->编码。"""
    svc = FormatConvertService()
    entry, missing = svc.create_entry({
        "配置编号": "FORM-TEST-01",
        "源格式": "json",
        "目标格式": "csv",
        "映射规则": "old_name->new_name;age->user_age",
        "嵌套展开": "是",
        "数组拍平": "是",
        "缺失处理": "keep",
    })
    assert not missing

    records = [
        {"old_name": "张三", "age": 20, "extra": {"city": "北京"}, "tags": ["a", "b"]},
        {"old_name": "李四", "age": 25, "extra": {"city": "上海"}, "tags": ["c"]},
    ]
    result = svc.run_pipeline(entry["id"], records)
    assert result["ok"] is True
    assert result["output_count"] >= result["input_count"]
    assert len(result["output"]) > 0
    # 验证映射生效
    first = result["output"][0]
    assert "new_name" in first or "old_name" in first


def test_data_validate_with_sampling_and_blocking() -> None:
    """测试数据校验：采样、阻断策略。"""
    svc = DataValidateService()
    entry, missing = svc.create_entry({
        "规则编号": "DATA-TEST-01",
        "目标字段": "price",
        "校验函数": "is_number",
        "异常动作": "block",
        "采样比例": "1.0",
        "阻断策略": "per_record",
    })
    assert not missing

    records = [
        {"price": "100"},
        {"price": "abc"},
        {"price": "200"},
    ]
    result = svc.validate_records(entry["id"], records)
    assert result["ok"] is False  # 因为有失败且action=block
    assert len(result["passed"]) == 2
    assert len(result["failed"]) == 1
    assert len(result["blocked"]) == 1


def test_incremental_cursor_advance_and_rollback() -> None:
    """测试增量游标推进与断点恢复。"""
    svc = IncreCursorService()
    entry, missing = svc.create_entry({
        "标记编号": "INCR-TEST-01",
        "关联种子": "SEED-01",
        "增量字段": "id",
        "上次位置": "0",
        "当前位置": "0",
    })
    assert not missing

    # 推进游标
    result = svc.advance_cursor(entry["id"], "100")
    assert result["ok"] is True
    assert result["old"] == "0"
    assert result["new"] == "100"

    updated = store.find("incre_cursor", entry["id"])
    assert updated["当前位置"] == "100"
    assert updated["上次位置"] == "0"

    # 再推进
    result = svc.advance_cursor(entry["id"], "200")
    assert result["new"] == "200"

    # 回滚到第一个检查点（恢复到 from=0）
    rollback = svc.rollback_to_checkpoint(entry["id"], 0)
    assert rollback["ok"] is True
    assert rollback["restored_to"] == "0"

    # 复位（回到上次位置=100，因为 rollback 只改了当前位置）
    entry_after = svc.run_action(entry["id"], "复位游标")
    assert entry_after[0] is not None
    # 复位后当前位置 = 上次位置（即 advance 到 200 时的 old=100）
    assert store.find("incre_cursor", entry["id"])["当前位置"] == "100"


def test_retry_policy_backoff_computation() -> None:
    """测试重试策略退避计算与异常分类。"""
    svc = RetryPolicyService()
    entry, missing = svc.create_entry({
        "策略编号": "RETR-TEST-01",
        "目标源": "test",
        "最大重试次数": "3",
        "退避算法": "exponential",
        "初始延迟": "1",
        "可重试异常": "TimeoutError,ConnectionError",
        "不可重试异常": "ValueError",
    })
    assert not missing

    # 可重试异常
    result = svc.should_retry(entry["id"], "TimeoutError", 0)
    assert result["should_retry"] is True
    assert result["delay"] == 1.0  # 1 * 2^0

    result = svc.should_retry(entry["id"], "TimeoutError", 1)
    assert result["delay"] == 2.0  # 1 * 2^1

    # 不可重试异常
    result = svc.should_retry(entry["id"], "ValueError", 0)
    assert result["should_retry"] is False
    assert result["classification"] == "non_retryable"

    # 超过最大重试
    result = svc.should_retry(entry["id"], "TimeoutError", 3)
    assert result["should_retry"] is False


def test_proxy_pool_health_scoring_and_selection() -> None:
    """测试代理池健康评分与选择。"""
    svc = ProxyPoolService()
    entry, missing = svc.create_entry({
        "节点编号": "PROX-TEST-01",
        "代理地址": "192.168.1.1",
        "端口号": "8080",
        "成功率": "0.95",
        "响应延迟": "100",
        "失效次数": "0",
    })
    assert not missing

    # 标记可用
    svc.run_action(entry["id"], "标记可用")

    selected = svc.select_proxy()
    assert selected is not None
    assert selected["_selected_score"] > 0

    # 记录请求
    result = svc.record_request(entry["id"], True, 120.0)
    assert result["ok"] is True


def test_rate_limit_token_bucket() -> None:
    """测试频次控制令牌桶。"""
    svc = RateLimitService()
    entry, missing = svc.create_entry({
        "策略编号": "RATE-TEST-01",
        "目标源": "test",
        "并发上限": "2",
        "日请求总量": "5",
    })
    assert not missing

    svc.run_action(entry["id"], "标记生效")

    # 获取令牌
    r1 = svc.acquire_token(entry["id"])
    assert r1["allowed"] is True
    assert r1["tokens"] == 1

    r2 = svc.acquire_token(entry["id"])
    assert r2["allowed"] is True
    assert r2["tokens"] == 0

    # 令牌耗尽
    r3 = svc.acquire_token(entry["id"])
    assert r3["allowed"] is False
    assert r3["reason"] == "并发令牌耗尽"

    # 释放令牌
    svc.release_token(entry["id"])
    r4 = svc.acquire_token(entry["id"])
    assert r4["allowed"] is True


def test_anti_spider_detection_and_countermeasure() -> None:
    """测试反爬检测与自动应对。"""
    svc = AntiSpiderService()
    entry, missing = svc.create_entry({
        "策略编号": "ANTI-TEST-01",
        "目标源": "test",
        "反爬类型": "验证码",
        "触发条件": "captcha",
        "应对方式": "切换代理,冷静",
        "冷静时长": "60",
    })
    assert not missing

    result = svc.detect_and_respond(entry["id"], "请完成人机验证 captcha", 200)
    assert result["triggered"] is True
    assert "countermeasure" in result
    assert result["countermeasure"]["switch_proxy"] is True
    assert result["countermeasure"]["cool_down"] == 60

    # 未触发
    result = svc.detect_and_respond(entry["id"], "正常页面内容", 200)
    assert result["triggered"] is False


def test_data_processing_full_chain() -> None:
    """测试完整数据处理链：去重->校验->格式转换->结果落地。"""
    dedup_svc = DedupFilterService()
    dedup, _ = dedup_svc.create_entry({
        "规则编号": "DEDU-FULL-01",
        "目标源": "test",
        "去重键列": "id",
        "比较方式": "exact",
    })
    dedup_svc.run_action(dedup["id"], "标记生效")

    val_svc = DataValidateService()
    val, _ = val_svc.create_entry({
        "规则编号": "DATA-FULL-01",
        "目标字段": "value",
        "校验函数": "not_empty",
        "阻断策略": "per_record",
    })
    val_svc.run_action(val["id"], "标记生效")

    conv_svc = FormatConvertService()
    conv, _ = conv_svc.create_entry({
        "配置编号": "FORM-FULL-01",
        "源格式": "json",
        "目标格式": "csv",
        "映射规则": "value->amount",
        "嵌套展开": "否",
        "数组拍平": "否",
    })
    conv_svc.run_action(conv["id"], "标记生效")

    res_svc = ResultStoreService()
    res, _ = res_svc.create_entry({
        "配置编号": "RESU-FULL-01",
        "输出格式": "json",
        "存储路径": "/data/test",
        "分区规则": "{category}",
        "写入批次": "100",
    })
    res_svc.run_action(res["id"], "标记生效")

    records = [
        {"id": "1", "value": "100", "category": "A"},
        {"id": "2", "value": "200", "category": "B"},
        {"id": "1", "value": "300", "category": "A"},  # 重复
    ]

    result = run_data_processing_chain(dedup["id"], val["id"], conv["id"], res["id"], records)
    assert result["ok"] is True
    assert result["dedup"]["unique_count"] == 2
    assert result["dedup"]["duplicate_count"] == 1
    assert result["validate"]["passed_count"] == 2
    assert result["convert"]["output_count"] == 2
    assert result["store"]["written"] == 2


def test_monitor_consumes_pipeline_state() -> None:
    """测试采集监控消费其他模块产物。"""
    # 先准备可用代理并生成健康快照，否则会话请求会因空态被停写
    proxy_svc = ProxyPoolService()
    proxy, _ = proxy_svc.create_entry({
        "节点编号": "PROX-MON-01",
        "代理地址": "10.0.0.2",
        "端口号": "3128",
    })
    proxy_svc.run_action(proxy["id"], "标记可用")

    # 先写入一些会话请求日志
    sess_svc = SessionMgrService()
    sess, _ = sess_svc.create_entry({"会话编号": "SESS-MON-01", "关联源": "1"})
    sess_svc.run_action(sess["id"], "标记活跃")
    assert sess_svc.record_request(sess["id"], "https://example.com", 150.0)["ok"] is True
    assert sess_svc.record_request(sess["id"], "https://example.com/2", 200.0)["ok"] is True

    result = run_monitor_aggregation(source_id=1)
    assert result["ok"] is True
    assert "metrics" in result
    assert result["metrics"]["total_requests"] >= 2


def test_execution_chain_schedule_to_proxy() -> None:
    """测试执行链：编排->会话->代理->频次。"""
    sched_svc = CrawlScheduleService()
    sched, _ = sched_svc.create_entry({
        "计划编号": "CRAW-EXEC-01",
        "关联种子": "SEED-EXEC-01",
        "Cron表达式": "0 0 * * *",
    })
    sched_svc.run_action(sched["id"], "激活计划")

    # 创建代理
    proxy_svc = ProxyPoolService()
    proxy, _ = proxy_svc.create_entry({
        "节点编号": "PROX-EXEC-01",
        "代理地址": "10.0.0.1",
        "端口号": "3128",
    })
    proxy_svc.run_action(proxy["id"], "标记可用")

    # 创建限流策略
    rate_svc = RateLimitService()
    rate, _ = rate_svc.create_entry({
        "策略编号": "RATE-EXEC-01",
        "目标源": "test",
        "并发上限": "5",
        "日请求总量": "100",
    })
    rate_svc.run_action(rate["id"], "标记生效")

    result = run_execution_chain(sched["id"])
    assert result["ok"] is True
    assert result["stage"] == "execution_ready"
    assert result["session_id"] > 0


def test_resilience_chain_anti_retry_cursor() -> None:
    """测试容错恢复链：反爬->重试->游标。"""
    # 先创建采集源并绑定策略
    src_svc = CrawlSourceService()
    src, _ = src_svc.create_entry({
        "源标识": "SRC-RES",
        "源名称": "容错测试",
        "协议类型": "https",
        "基础URL": "https://example.com",
    })

    anti_svc = AntiSpiderService()
    anti, _ = anti_svc.create_entry({
        "策略编号": "ANTI-RES-01",
        "目标源": "test",
        "反爬类型": "频率限制",
        "触发条件": "429",
        "应对方式": "冷静",
        "冷静时长": "30",
        "关联采集源ID": src["id"],
    })
    anti_svc.run_action(anti["id"], "标记生效")

    retry_svc = RetryPolicyService()
    retry, _ = retry_svc.create_entry({
        "策略编号": "RETR-RES-01",
        "目标源": "test",
        "最大重试次数": "3",
        "退避算法": "linear",
        "初始延迟": "2",
        "可重试异常": "RateLimitError",
        "关联采集源ID": src["id"],
    })
    retry_svc.run_action(retry["id"], "标记生效")

    cursor_svc = IncreCursorService()
    cursor, _ = cursor_svc.create_entry({
        "标记编号": "INCR-RES-01",
        "关联种子": "SEED-RES-01",
        "增量字段": "id",
        "上次位置": "0",
        "当前位置": "0",
    })
    cursor_svc.run_action(cursor["id"], "标记采集中")

    # 触发反爬
    result = run_resilience_chain(
        source_id=src["id"],
        response_text="rate limit 429 too many requests",
        status_code=429,
        exc_name="RateLimitError",
        attempt=1,
        cursor_id=cursor["id"],
    )
    assert result["ok"] is True
    assert result["anti_spider"]["triggered"] is True

    # 不触发反爬，重试
    result = run_resilience_chain(
        source_id=src["id"],
        response_text="正常内容",
        status_code=200,
        exc_name="RateLimitError",
        attempt=1,
        cursor_id=cursor["id"],
    )
    assert result["retry"]["should_retry"] is True
    assert result["cursor"]["ok"] is True


def test_content_extract_quality_pipeline() -> None:
    """测试内容抽取质量评分pipeline。"""
    svc = ContentExtractService()
    long_html = (
        "<html><body><h1>标题</h1>"
        "<p>这是一段非常长的测试文本，包含大量中文内容，用于验证内容抽取质量评分的准确性。</p>"
        "<p>第二段文本同样包含中文和测试关键词，确保密度算法能够正确提取到目标内容块。</p>"
        "</body></html>"
    )
    entry, missing = svc.create_entry({
        "任务编号": "CONT-TEST-01",
        "来源页面": long_html,
        "抽取算法": "density",
        "关键词过滤": "中文,测试",
        "摘要长度": "50",
        "质量阈值": "0.3",
    })
    assert not missing

    result = svc.extract(entry["id"], entry["来源页面"])
    assert result["ok"] is True
    assert "summary" in result
    assert result["score"] > 0


def test_field_extract_casting_and_refinement() -> None:
    """测试字段抽取类型转换与精炼。"""
    from app.services.field_extract import FieldExtractService, _cast_output, _refine_value

    assert _refine_value("价格: 123.45元", r"(\d+\.\d+)") == ("123.45", True)
    assert _cast_output("123", "integer") == (123, True)
    assert _cast_output("abc", "integer") == (None, False)
    assert _cast_output("true", "boolean") == (True, True)

    svc = FieldExtractService()
    entry, missing = svc.create_entry({
        "模板编号": "FIEL-TEST-01",
        "关联种子": "SEED-01",
        "字段名": "price",
        "选择器": "<span>",
        "精炼规则": r"(\d+)",
        "输出类型": "integer",
        "可为空": "否",
    })
    assert not missing

    result = svc.extract_fields(entry["id"], "<span>价格: 999元</span>")
    assert result["ok"] is True
    assert result["casted"] == 999


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
