"""内存数据仓库：支持跨模块数据链路与状态持久化。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
"""
from __future__ import annotations

from typing import Any

from app.seed import SEED_ROWS


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块链路状态存储
        self._pipeline_states: dict[str, Any] = {
            "source_validation_cache": {},   # crawl_source 连接校验缓存
            "cursor_snapshots": {},          # incre_cursor 断点快照
            "dedup_bloom_filters": {},       # dedup_filter 布隆过滤器状态
            "format_pipelines": {},          # format_convert pipeline 运行状态
            "proxy_health_scores": {},       # proxy_pool 健康评分历史
            "proxy_health_metrics": {},      # proxy_pool 滚动观测指标（成功/失败/超时/延迟）
            "proxy_health_snapshot": None,   # proxy_pool 当前代理健康快照
            "proxy_snapshot_generation": None,  # proxy_pool 快照中断/续做进度
            "session_request_logs": {},      # session_mgr 请求日志
            "session_empty_events": [],      # session_mgr 空态停写记录（含原因）
            "session_snapshot_reads": [],    # session_mgr 跨模块读取快照并回写的记录
            "retry_backoff_records": {},     # retry_policy 退避记录
            "anti_spider_trigger_log": [],   # anti_spider 触发日志
            "schedule_execution_log": [],    # crawl_schedule 执行日志
            "monitor_metrics_cache": {},     # crawl_monitor 实时指标缓存
            "content_extract_results": {},   # content_extract 提取结果
            "data_validate_reports": {},     # data_validate 校验报告
            "result_store_partitions": {},   # result_store 分区元数据
            "rate_limit_states": {},         # rate_limit 令牌桶状态
        }
        # 模块间关系索引（外键映射）——基于 seed 数据自动构建
        self._relations: dict[str, list[dict[str, Any]]] = {
            "source_to_seed": [],      # crawl_source.id -> seed_config
            "seed_to_parse_rule": [],  # seed_config.id -> parse_rule
            "parse_to_field": [],      # parse_rule.id -> field_extract
            "schedule_to_session": [], # crawl_schedule.id -> session_mgr (via seed)
            "session_to_proxy": [],    # session_mgr.id -> proxy_pool
            "source_to_rate_limit": [],# crawl_source.id -> rate_limit
            "source_to_anti_spider": [],# crawl_source.id -> anti_spider
            "source_to_retry": [],     # crawl_source.id -> retry_policy
            "seed_to_cursor": [],      # seed_config.id -> incre_cursor
            "source_to_dedup": [],     # crawl_source.id -> dedup_filter
            "source_to_validate": [],  # crawl_source.id -> data_validate
            "source_to_result": [],    # crawl_source.id -> result_store
            "source_to_render": [],    # crawl_source.id -> page_render
            "schedule_to_monitor": [], # crawl_schedule.id -> crawl_monitor
            "extract_to_convert": [],  # content_extract.id -> format_convert
        }
        self._build_relations()

    def _build_relations(self) -> None:
        """根据 seed 数据中的业务语义自动建立跨模块关系。"""

        def _numeric_suffix(s: str) -> str:
            """提取字符串中的数字标识，如 SRC-ELEC-001 -> 001, PARS-E001-A -> 001。"""
            import re
            m = re.search(r"(\d+)", s)
            return m.group(1) if m else ""

        # source -> seed (通过源标识/关联源匹配)
        source_map: dict[str, int] = {}
        for src in self._tables.get("crawl_source", []):
            source_map[str(src.get("源标识") or "")] = int(src["id"])
        for seed in self._tables.get("seed_config", []):
            sid = str(seed.get("种子编号") or "")
            seed_suffix = _numeric_suffix(sid)
            matched_src_id = None
            if seed_suffix:
                for s_id, src_id in source_map.items():
                    if _numeric_suffix(s_id) == seed_suffix:
                        matched_src_id = src_id
                        break
            if matched_src_id:
                self.add_relation("source_to_seed", "crawl_source", matched_src_id, "seed_config", int(seed["id"]))

        # seed -> parse_rule (通过规则编号中的种子编号匹配)
        seed_map: dict[str, int] = {}
        for seed in self._tables.get("seed_config", []):
            seed_map[str(seed.get("种子编号") or "")] = int(seed["id"])
        for rule in self._tables.get("parse_rule", []):
            rid = str(rule.get("规则编号") or "")
            # PARS-E001-A -> 提取 E001 -> SEED-E001
            rid_suffix = _numeric_suffix(rid)
            for seed_code, seed_id in seed_map.items():
                if _numeric_suffix(seed_code) == rid_suffix:
                    self.add_relation("seed_to_parse_rule", "seed_config", seed_id, "parse_rule", int(rule["id"]))
                    break

        # parse_rule -> field_extract (通过关联种子/模板编号匹配)
        rule_map: dict[str, int] = {}
        for rule in self._tables.get("parse_rule", []):
            rule_map[str(rule.get("规则编号") or "")] = int(rule["id"])
        for fe in self._tables.get("field_extract", []):
            fe_code = str(fe.get("模板编号") or "")
            # FIEL-E001-A -> PARS-E001-A (需要完全后缀匹配)
            fe_suffix = _numeric_suffix(fe_code)
            for rule_code, rule_id in rule_map.items():
                if _numeric_suffix(rule_code) == fe_suffix:
                    self.add_relation("parse_to_field", "parse_rule", rule_id, "field_extract", int(fe["id"]))
                    break

        # crawl_schedule -> session_mgr (通过计划编号中的种子编号匹配会话)
        schedule_seed_map: dict[int, str] = {}
        for sched in self._tables.get("crawl_schedule", []):
            schedule_seed_map[int(sched["id"])] = str(sched.get("关联种子") or "")
        for sess in self._tables.get("session_mgr", []):
            sess_src = str(sess.get("关联源") or "")
            for sched_id, seed_code in schedule_seed_map.items():
                # 通过源标识和种子编号前缀匹配
                seed_part = seed_code.split("-")[1] if "-" in seed_code else ""
                if seed_part and seed_part in sess_src:
                    self.add_relation("schedule_to_session", "crawl_schedule", sched_id, "session_mgr", int(sess["id"]))

        # source -> rate_limit / anti_spider / retry / dedup / validate / result / render
        for module, rel_key in [
            ("rate_limit", "source_to_rate_limit"),
            ("anti_spider", "source_to_anti_spider"),
            ("retry_policy", "source_to_retry"),
            ("dedup_filter", "source_to_dedup"),
            ("data_validate", "source_to_validate"),
            ("result_store", "source_to_result"),
            ("page_render", "source_to_render"),
        ]:
            for row in self._tables.get(module, []):
                target = str(row.get("目标源") or "")
                if target in source_map:
                    self.add_relation(rel_key, "crawl_source", source_map[target], module, int(row["id"]))

        # seed -> incre_cursor (通过关联种子匹配)
        for cursor in self._tables.get("incre_cursor", []):
            seed_code = str(cursor.get("关联种子") or "")
            if seed_code in seed_map:
                self.add_relation("seed_to_cursor", "seed_config", seed_map[seed_code], "incre_cursor", int(cursor["id"]))

        # crawl_schedule -> crawl_monitor (通过关联源匹配)
        for sched in self._tables.get("crawl_schedule", []):
            sched_seed = str(sched.get("关联种子") or "")
            sched_part = sched_seed.split("-")[1] if "-" in sched_seed else ""
            for mon in self._tables.get("crawl_monitor", []):
                mon_src = str(mon.get("关联源") or "")
                # SEED-E001 -> SRC-ELEC-001 (match part like E001 in SRC-ELEC-001)
                if sched_part and sched_part in mon_src:
                    self.add_relation("schedule_to_monitor", "crawl_schedule", int(sched["id"]), "crawl_monitor", int(mon["id"]))

        # session -> proxy (基于地域就近匹配)
        proxies = self._tables.get("proxy_pool", [])
        for sess in self._tables.get("session_mgr", []):
            sess_region = str(sess.get("关联源") or "")
            # 简化：找地域标识包含相同城市代码的代理
            for proxy in proxies:
                proxy_region = str(proxy.get("地域标识") or "")
                if proxy_region and any(city in proxy_region for city in ["华东", "华北", "华南", "海外"]):
                    self.add_relation("session_to_proxy", "session_mgr", int(sess["id"]), "proxy_pool", int(proxy["id"]))
                    break

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def find_by_field(self, module: str, field: str, value: Any) -> dict[str, Any] | None:
        for row in self.rows(module):
            if row.get(field) == value:
                return row
        return None

    def find_all_by_field(self, module: str, field: str, value: Any) -> list[dict[str, Any]]:
        return [row for row in self.rows(module) if row.get(field) == value]

    def pipeline_state(self, key: str) -> Any:
        return self._pipeline_states.get(key)

    def set_pipeline_state(self, key: str, value: Any) -> None:
        self._pipeline_states[key] = value

    def pipeline_states(self) -> dict[str, Any]:
        return dict(self._pipeline_states)

    def add_relation(self, relation_type: str, source_module: str, source_id: int,
                     target_module: str, target_id: int, meta: dict[str, Any] | None = None) -> None:
        rels = self._relations.setdefault(relation_type, [])
        rels.append({
            "source_module": source_module,
            "source_id": source_id,
            "target_module": target_module,
            "target_id": target_id,
            "meta": meta or {},
        })

    def get_relations(self, relation_type: str, source_module: str | None = None,
                      source_id: int | None = None, target_module: str | None = None,
                      target_id: int | None = None) -> list[dict[str, Any]]:
        rels = self._relations.get(relation_type, [])
        result = []
        for r in rels:
            if source_module and r["source_module"] != source_module:
                continue
            if source_id is not None and r["source_id"] != source_id:
                continue
            if target_module and r["target_module"] != target_module:
                continue
            if target_id is not None and r["target_id"] != target_id:
                continue
            result.append(r)
        return result

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()
