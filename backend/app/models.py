"""洞察工坊 · 核心业务数据模型

定义跨模块数据流的真实 dataclass：指标、维度、实验、分群、归因、异常检测等。
所有模型支持确定性序列化，可直接用于分析计算。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


# ============================================================
# 枚举
# ============================================================

class AggregationMethod(str, Enum):
    SUM = "sum"
    AVG = "avg"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    MAX = "max"
    MIN = "min"
    P50 = "p50"
    P90 = "p90"
    P99 = "p99"


class MetricDataSource(str, Enum):
    CLICKHOUSE = "clickhouse"
    DORIS = "doris"
    MYSQL = "mysql"
    KAFKA = "kafka"
    HIVE = "hive"


class MetricPeriod(str, Enum):
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class ExperimentStatus(str, Enum):
    DESIGN = "design"
    RUNNING = "running"
    ENDED = "ended"
    TERMINATED = "terminated"


class AttributionAlgorithm(str, Enum):
    FIRST_TOUCH = "first_touch"
    LAST_TOUCH = "last_touch"
    LINEAR = "linear"
    TIME_DECAY = "time_decay"
    POSITION_BASED = "position_based"
    DATA_DRIVEN = "data_driven"


class AnomalyAlgorithm(str, Enum):
    IQR = "iqr"
    ZSCORE = "zscore"
    MOVING_AVERAGE = "moving_average"
    PROPHET = "prophet"
    ISOLATION_FOREST = "isolation_forest"


class JoinType(str, Enum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"


# ============================================================
# 指标定义
# ============================================================

@dataclass
class MetricDef:
    """指标口径定义：聚合方式、数据来源、统计周期。"""
    metric_id: str
    metric_name: str
    calculation_formula: str
    aggregation: AggregationMethod
    data_source: MetricDataSource
    period: MetricPeriod
    owner: str
    field_name: str
    table_name: str
    dimension_keys: list[str] = field(default_factory=list)
    filter_condition: str = ""
    version: int = 1
    last_computed: str = ""

    def compute(self, rows: list[dict[str, Any]]) -> float:
        """根据聚合方式对行数据进行确定性计算。"""
        if self.aggregation == AggregationMethod.COUNT:
            return float(len(rows))
        if self.aggregation == AggregationMethod.COUNT_DISTINCT:
            raw = [row.get(self.field_name) for row in rows]
            return float(len(set(v for v in raw if v is not None)))

        # Numeric operations
        values: list[float] = []
        for row in rows:
            v = row.get(self.field_name)
            if v is not None:
                try:
                    values.append(float(v))
                except (ValueError, TypeError):
                    pass
        if not values:
            return 0.0
        if self.aggregation == AggregationMethod.SUM:
            return sum(values)
        elif self.aggregation == AggregationMethod.AVG:
            return sum(values) / len(values)
        elif self.aggregation == AggregationMethod.MAX:
            return max(values)
        elif self.aggregation == AggregationMethod.MIN:
            return min(values)
        elif self.aggregation == AggregationMethod.P50:
            return _percentile(values, 50)
        elif self.aggregation == AggregationMethod.P90:
            return _percentile(values, 90)
        elif self.aggregation == AggregationMethod.P99:
            return _percentile(values, 99)
        return 0.0


# ============================================================
# 维度定义
# ============================================================

@dataclass
class DimensionDef:
    """分析维度的层级定义和字段来源。"""
    dimension_id: str
    dimension_name: str
    field_source: str
    hierarchy_levels: list[str] = field(default_factory=list)
    enum_cardinality: int = 0
    linked_metric_count: int = 0
    last_refresh: str = ""

    def build_hierarchy_query(self, level: int) -> str:
        """按层级构建 GROUP BY 子句。"""
        if level < 0 or level >= len(self.hierarchy_levels):
            level = len(self.hierarchy_levels) - 1
        dims = self.hierarchy_levels[:level + 1]
        return f"GROUP BY {', '.join(dims)}"


# ============================================================
# 多维查询
# ============================================================

@dataclass
class OlapQuery:
    """多维分析查询方案：指标、维度、筛选、排序。"""
    query_id: str
    query_name: str
    metrics: list[MetricDef] = field(default_factory=list)
    dimensions: list[DimensionDef] = field(default_factory=list)
    filters: dict[str, str] = field(default_factory=dict)
    order_by: str = ""
    time_range: str = ""
    cache_ttl: int = 300

    def build_sql(self) -> str:
        """生成确定性 SQL 查询语句。"""
        if not self.metrics:
            return "SELECT 0"
        metric_exprs = []
        for m in self.metrics:
            alias = f"_{m.metric_id}"
            if m.aggregation == AggregationMethod.COUNT_DISTINCT:
                metric_exprs.append(f"COUNT(DISTINCT {m.field_name}) AS {alias}")
            elif m.aggregation == AggregationMethod.SUM:
                metric_exprs.append(f"SUM({m.field_name}) AS {alias}")
            elif m.aggregation == AggregationMethod.AVG:
                metric_exprs.append(f"AVG({m.field_name}) AS {alias}")
            elif m.aggregation == AggregationMethod.COUNT:
                metric_exprs.append(f"COUNT({m.field_name}) AS {alias}")
            elif m.aggregation == AggregationMethod.MAX:
                metric_exprs.append(f"MAX({m.field_name}) AS {alias}")
            elif m.aggregation == AggregationMethod.MIN:
                metric_exprs.append(f"MIN({m.field_name}) AS {alias}")
            elif m.aggregation == AggregationMethod.P50:
                metric_exprs.append(f"PERCENTILE({m.field_name}, 0.5) AS {alias}")
            elif m.aggregation == AggregationMethod.P90:
                metric_exprs.append(f"PERCENTILE({m.field_name}, 0.9) AS {alias}")
            elif m.aggregation == AggregationMethod.P99:
                metric_exprs.append(f"PERCENTILE({m.field_name}, 0.99) AS {alias}")

        dim_keys = []
        for d in self.dimensions:
            dims = d.hierarchy_levels if d.hierarchy_levels else [d.field_source]
            dim_keys.extend(dims)

        table = self.metrics[0].table_name if self.metrics else "unknown"
        parts = [f"SELECT {', '.join(dim_keys + metric_exprs)}"]
        parts.append(f"FROM {table}")

        if self.filters:
            where_clauses = [f"{k} = '{v}'" for k, v in self.filters.items()]
            parts.append(f"WHERE {' AND '.join(where_clauses)}")

        if dim_keys:
            parts.append(f"GROUP BY {', '.join(dim_keys)}")

        if self.order_by:
            parts.append(f"ORDER BY {self.order_by}")

        return " ".join(parts)


# ============================================================
# 查询缓存
# ============================================================

@dataclass
class QueryCache:
    """查询缓存条目：哈希、结果行、TTL、命中统计。"""
    cache_id: str
    query_hash: str
    olap_query_ref: str
    result_rows: list[dict[str, Any]] = field(default_factory=list)
    ttl_seconds: int = 300
    hit_count: int = 0
    last_hit: str = ""
    memory_bytes: int = 0

    def is_expired(self) -> bool:
        return self.hit_count == 0 and self.ttl_seconds < 0

    def record_hit(self) -> None:
        self.hit_count += 1


# ============================================================
# 看板定义
# ============================================================

@dataclass
class DashboardDef:
    """数据看板的图表布局、指标绑定和刷新策略。"""
    dashboard_id: str
    dashboard_name: str
    layout_config: dict[str, Any] = field(default_factory=dict)
    bound_metrics: list[MetricDef] = field(default_factory=list)
    bound_queries: list[OlapQuery] = field(default_factory=list)
    subscribers: list[str] = field(default_factory=list)
    refresh_strategy: str = "manual"
    cache_ttl: int = 600
    last_refresh: str = ""

    def collect_cached_results(self, caches: list[QueryCache]) -> dict[str, list[dict[str, Any]]]:
        """关联查询缓存，组装看板数据。"""
        results = {}
        for q in self.bound_queries:
            for cache in caches:
                if cache.olap_query_ref == q.query_id and not cache.is_expired():
                    results[q.query_id] = cache.result_rows
                    break
        return results


# ============================================================
# 下钻路径
# ============================================================

@dataclass
class DrillPath:
    """多维数据下钻的层级路径。"""
    path_id: str
    parent_dimension: DimensionDef
    child_dimension: DimensionDef
    transition_layers: int = 1
    data_granularity: str = "day"
    precompute_mode: str = "on_demand"

    def map_query_dimensions(self, query: OlapQuery) -> OlapQuery:
        """把下钻路径映射到查询维度。"""
        query.dimensions = [self.parent_dimension, self.child_dimension]
        return query


# ============================================================
# 事件追踪
# ============================================================

@dataclass
class EventTrack:
    """业务事件定义：属性、触发条件、归因键。"""
    event_id: str
    event_name: str
    event_properties: dict[str, str] = field(default_factory=dict)
    trigger_condition: str = ""
    attribution_key: str = ""
    period: MetricPeriod = MetricPeriod.DAILY

    def matches_properties(self, conditions: dict[str, str]) -> bool:
        """判断事件属性是否满足圈选条件（AND 逻辑）。"""
        if not conditions:
            return True
        for k, v in conditions.items():
            if k not in self.event_properties or self.event_properties[k] != v:
                return False
        return True


# ============================================================
# 群体圈选
# ============================================================

@dataclass
class Segment:
    """用户群体圈选条件：属性筛选 + 行为条件 + 指标依赖。"""
    segment_id: str
    segment_name: str
    conditions: dict[str, str] = field(default_factory=dict)
    member_count: int = 0
    update_strategy: str = "daily"
    effective_range: str = ""
    bound_events: list[EventTrack] = field(default_factory=list)
    bound_metrics: list[MetricDef] = field(default_factory=list)

    def select_users(self, users: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """根据圈选条件从用户列表中筛选目标群体。"""
        result = []
        for user in users:
            match = True
            for cond_key, cond_val in self.conditions.items():
                user_val = str(user.get(cond_key, ""))
                if cond_val not in user_val:
                    match = False
                    break
            if match:
                result.append(user)
        self.member_count = len(result)
        return result


# ============================================================
# 分群比较
# ============================================================

@dataclass
class UserSegmentCompare:
    """跨人群分组的指标对比和显著性检验。"""
    compare_id: str
    segment_a: Segment
    segment_b: Segment
    compare_metric: MetricDef
    data_a: list[float] = field(default_factory=list)
    data_b: list[float] = field(default_factory=list)

    def compute_ttest(self) -> dict[str, Any]:
        """Welch's t-test 计算两组均值的统计显著性。"""
        import math
        n_a, n_b = len(self.data_a), len(self.data_b)
        if n_a < 2 or n_b < 2:
            return {"t_stat": 0.0, "p_value": 1.0, "significant": False, "reason": "样本不足"}

        mean_a = sum(self.data_a) / n_a
        mean_b = sum(self.data_b) / n_b

        var_a = sum((x - mean_a) ** 2 for x in self.data_a) / (n_a - 1)
        var_b = sum((x - mean_b) ** 2 for x in self.data_b) / (n_b - 1)

        se = math.sqrt(var_a / n_a + var_b / n_b)
        if se == 0:
            return {"t_stat": 0.0, "p_value": 1.0, "significant": False}

        t_stat = (mean_a - mean_b) / se

        # Welch-Satterthwaite 自由度
        df_num = (var_a / n_a + var_b / n_b) ** 2
        df_den = ((var_a / n_a) ** 2) / (n_a - 1) + ((var_b / n_b) ** 2) / (n_b - 1)
        df = df_num / df_den

        p_value = _t_distribution_pvalue(abs(t_stat), df)

        return {
            "t_stat": round(t_stat, 4),
            "p_value": round(p_value, 4),
            "significant": p_value < 0.05,
            "mean_a": round(mean_a, 4),
            "mean_b": round(mean_b, 4),
            "n_a": n_a,
            "n_b": n_b,
        }


# ============================================================
# A/B 实验
# ============================================================

@dataclass
class ABExperiment:
    """A/B 实验方案：分组设计、分流比例、评估指标。"""
    experiment_id: str
    experiment_name: str
    groups: dict[str, float] = field(default_factory=dict)
    split_ratio: dict[str, float] = field(default_factory=dict)
    evaluation_metrics: list[MetricDef] = field(default_factory=list)
    min_sample_size: int = 1000
    start_time: str = ""
    status: ExperimentStatus = ExperimentStatus.DESIGN

    def assign_group(self, user_id: str) -> str:
        """基于用户ID哈希的确定性分流。"""
        import hashlib
        hash_val = int(hashlib.md5(user_id.encode()).hexdigest(), 16)
        ratio_sum = sum(self.split_ratio.values())
        if ratio_sum == 0:
            return "control"
        bucket = hash_val % 10000 / 10000.0
        cumulative = 0.0
        for group, ratio in self.split_ratio.items():
            cumulative += ratio / ratio_sum
            if bucket < cumulative:
                return group
        return list(self.split_ratio.keys())[0]


# ============================================================
# 实验分析
# ============================================================

@dataclass
class ExperimentResult:
    """A/B 实验统计分析报告。"""
    report_id: str
    experiment: ABExperiment
    metric: MetricDef
    control_data: list[float] = field(default_factory=list)
    treatment_data: list[float] = field(default_factory=list)
    control_mean: float = 0.0
    treatment_mean: float = 0.0
    lift: float = 0.0
    p_value: float = 1.0
    significant: bool = False

    def analyze(self) -> ExperimentResult:
        """执行 t-test 并计算提升幅度。"""
        compare = UserSegmentCompare(
            compare_id=self.report_id,
            segment_a=Segment(segment_id="control", segment_name="对照组"),
            segment_b=Segment(segment_id="treatment", segment_name="实验组"),
            compare_metric=self.metric,
            data_a=self.control_data,
            data_b=self.treatment_data,
        )
        result = compare.compute_ttest()
        self.p_value = result["p_value"]
        self.significant = result["significant"]
        if self.control_data:
            self.control_mean = sum(self.control_data) / len(self.control_data)
        if self.treatment_data:
            self.treatment_mean = sum(self.treatment_data) / len(self.treatment_data)
        if self.control_mean > 0:
            self.lift = round((self.treatment_mean - self.control_mean) / self.control_mean, 4)
        return self


# ============================================================
# 归因模型
# ============================================================

@dataclass
class AttributionModel:
    """归因分析模型：算法选择、归因窗口、事件绑定。"""
    model_id: str
    model_name: str
    algorithm: AttributionAlgorithm
    attribution_window_days: int = 30
    dimensions: list[DimensionDef] = field(default_factory=list)
    denominator_event: EventTrack | None = None
    numerator_event: EventTrack | None = None

    def attribute(
        self,
        touchpoints: list[dict[str, Any]],
        conversions: list[dict[str, Any]],
    ) -> dict[str, float]:
        """根据选定算法对触点进行归因权重分配。

        Args:
            touchpoints: 触点列表，每项包含 user_id, channel, timestamp
            conversions: 转化列表，每项包含 user_id, conversion_time

        Returns:
            {channel: attribution_weight}
        """
        if self.algorithm == AttributionAlgorithm.FIRST_TOUCH:
            return _first_touch_attribution(touchpoints, conversions, self.attribution_window_days)
        elif self.algorithm == AttributionAlgorithm.LAST_TOUCH:
            return _last_touch_attribution(touchpoints, conversions, self.attribution_window_days)
        elif self.algorithm == AttributionAlgorithm.LINEAR:
            return _linear_attribution(touchpoints, conversions, self.attribution_window_days)
        elif self.algorithm == AttributionAlgorithm.TIME_DECAY:
            return _time_decay_attribution(touchpoints, conversions, self.attribution_window_days)
        elif self.algorithm == AttributionAlgorithm.POSITION_BASED:
            return _position_based_attribution(touchpoints, conversions, self.attribution_window_days)
        return {}


# ============================================================
# 漏斗分析
# ============================================================

@dataclass
class FunnelDef:
    """用户行为漏斗：步骤序列、转化窗口、转化率计算。"""
    funnel_id: str
    funnel_name: str
    steps: list[EventTrack] = field(default_factory=list)
    conversion_window_hours: int = 24
    step_counts: list[int] = field(default_factory=list)
    step_rates: list[float] = field(default_factory=list)
    total_conversion_rate: float = 0.0

    def compute_funnel(self, user_events: dict[str, list[dict[str, Any]]]) -> FunnelDef:
        """计算每个步骤的用户量和转化率。

        Args:
            user_events: {user_id: [{event_name, timestamp}, ...]}
        """
        if not self.steps:
            return self
        step_event_names = [step.event_name for step in self.steps]

        # Step 1: count users who triggered the first event
        step_users: list[set[str]] = [set()]
        for uid, events in user_events.items():
            if any(e["event_name"] == step_event_names[0] for e in events):
                step_users[0].add(uid)
        self.step_counts = [len(step_users[0])]

        # Subsequent steps: users from previous step who also triggered current step
        for i in range(1, len(step_event_names)):
            current_users = set()
            for uid in step_users[i - 1]:
                if any(e["event_name"] == step_event_names[i] for e in user_events.get(uid, [])):
                    current_users.add(uid)
            step_users.append(current_users)
            self.step_counts.append(len(current_users))

        # Calculate rates
        self.step_rates = []
        for i in range(len(self.step_counts)):
            if i == 0:
                self.step_rates.append(1.0)
            else:
                prev = self.step_counts[i - 1]
                curr = self.step_counts[i]
                self.step_rates.append(round(curr / prev, 4) if prev > 0 else 0.0)

        if self.step_counts and self.step_counts[0] > 0:
            self.total_conversion_rate = round(
                self.step_counts[-1] / self.step_counts[0], 4
            )

        return self


# ============================================================
# 同期群分析
# ============================================================

@dataclass
class CohortAnalysis:
    """同期群划分和留存计算。"""
    cohort_id: str
    cohort_name: str
    partition_period: str = "daily"
    start_event: EventTrack | None = None
    revisit_event: EventTrack | None = None
    observation_windows: list[int] = field(default_factory=lambda: [1, 3, 7, 14, 30])
    member_base_count: int = 0

    def compute_retention(
        self,
        users: list[str],
        user_events: dict[str, list[str]],
    ) -> list[float]:
        """计算各观察窗口的用户留存率。

        Args:
            users: 同期群用户ID列表
            user_events: {user_id: [event_timestamp_groups]}
        """
        self.member_base_count = len(users)
        if self.member_base_count == 0:
            return [0.0] * len(self.observation_windows)

        retentions = []
        user_set = set(users)
        for window in self.observation_windows:
            active_count = 0
            for uid in user_set:
                events = user_events.get(uid, [])
                if any(int(e) <= window for e in events):
                    active_count += 1
            retentions.append(round(active_count / self.member_base_count, 4))
        return retentions

    def build_revisit_ledger(self, members: dict[str, str]) -> CohortRevisitLedger:
        """基于本群观察窗口创建回访台账，从合作方文件按真实成员和观察日恢复留存。"""
        return CohortRevisitLedger(
            cohort_id=self.cohort_id,
            members=dict(members),
            observation_windows=list(self.observation_windows),
        )


# ============================================================
# 同期群回访批次
# ============================================================

class RevisitRejectReason(str, Enum):
    """回访行被拒绝的两类业务原因，复核抽样时单列。"""
    MEMBER_MISMATCH = "成员不符"
    WINDOW_MISMATCH = "观察期不符"


# 天干在中文业务里常用作次序标记（甲、乙、丙…），成员排序按此序列对齐，
# 其余字符退回字符串顺序，保证排序确定且符合业务直觉。
_STEM_ORDER = "甲乙丙丁戊己庚辛壬癸"


def _member_sort_key(member_id: str) -> tuple:
    """成员排序键：天干字符按甲乙丙…次序，其余字符按字符串顺序。"""
    return tuple(
        (0, _STEM_ORDER.index(ch)) if ch in _STEM_ORDER else (1, ch)
        for ch in member_id
    )


@dataclass(frozen=True)
class RevisitFact:
    """回访事实：合作方文件中一行通过校验的成员回访记录。"""
    member_id: str
    observation_day: int
    revisit_date: str
    source_file: str = ""

    @property
    def dedup_key(self) -> tuple[str, int]:
        """幂等键：同一成员同一观察日的回访只登记一次。"""
        return (self.member_id, self.observation_day)


@dataclass
class RevisitRejection:
    """被拒绝的回访行：保留原始内容与拒绝原因，供导出工作台复核。"""
    member_id: str
    observation_day: int
    reason: RevisitRejectReason
    source_file: str = ""
    detail: str = ""


@dataclass
class CohortRevisitBatch:
    """同期群回访批次：一次合作方文件导入的登记结果。

    facts 为通过校验的回访行；duplicates 为重复送达（独立登记，不覆盖已确认
    事实）；rejections 区分成员不符与观察期不符；parse_failures 保留无法解析
    的原始行，存在解析失败的批次不完整，不得确认、不得生成结论。
    """
    batch_id: str
    cohort_id: str
    source_file: str
    status: str = "待确认"
    facts: list[RevisitFact] = field(default_factory=list)
    rejections: list[RevisitRejection] = field(default_factory=list)
    duplicates: list[RevisitFact] = field(default_factory=list)
    parse_failures: list[str] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        """文件完整（无解析失败）才允许确认并参与留存结论。"""
        return not self.parse_failures


@dataclass
class PeriodConclusion:
    """单个观察窗口的留存结论：分子、分母、留存率和发出顺序。"""
    window: int
    numerator: int
    denominator: int
    rate: float
    round_id: int = 0
    issue_seq: int = 0


@dataclass
class RetentionReport:
    """一轮留存报告发布结果：未发布时给出暂缓原因。"""
    cohort_id: str
    round_id: int
    released: bool
    conclusions: list[PeriodConclusion] = field(default_factory=list)
    hold_reason: str = ""


@dataclass
class CohortRevisitLedger:
    """同期群回访台账：跨批次登记回访事实，幂等去重，按窗口计算留存。

    - 导入按 (成员, 观察日) 幂等，重复送达独立登记，不覆盖已确认事实；
    - 拒绝区分成员不符与观察期不符；解析失败使批次不完整、禁止确认；
    - 两次并发导入同一回访只记一次：导入与确认都在锁内完成登记。
    """
    cohort_id: str
    members: dict[str, str] = field(default_factory=dict)  # 成员 -> 起始日
    observation_windows: list[int] = field(default_factory=lambda: [1, 7])
    batches: list[CohortRevisitBatch] = field(default_factory=list)
    confirmed_facts: dict[tuple[str, int], RevisitFact] = field(default_factory=dict)
    issued_conclusions: list[PeriodConclusion] = field(default_factory=list)
    release_round: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    @property
    def max_window(self) -> int:
        return max(self.observation_windows) if self.observation_windows else 0

    def find_batch(self, batch_id: str) -> CohortRevisitBatch | None:
        for batch in self.batches:
            if batch.batch_id == batch_id:
                return batch
        return None

    def _active_index(self) -> dict[tuple[str, int], RevisitFact]:
        """已确认事实 + 待确认批次中事实的索引。

        部分解析失败和已作废的批次不参与去重，其中的行允许由更正文件重新送达。
        """
        index = dict(self.confirmed_facts)
        for batch in self.batches:
            if batch.status == "待确认":
                for fact in batch.facts:
                    index.setdefault(fact.dedup_key, fact)
        return index

    def import_file(self, batch_id: str, file_path: str) -> CohortRevisitBatch:
        """从合作方文件真实读取并导入一个回访批次。"""
        import os
        with open(file_path, encoding="utf-8") as fh:
            lines = fh.read().splitlines()
        return self.import_lines(batch_id, os.path.basename(file_path), lines)

    def import_lines(
        self,
        batch_id: str,
        source_file: str,
        lines: list[str],
    ) -> CohortRevisitBatch:
        """解析合作方文件行，登记事实、重复、拒绝和解析失败。"""
        batch = CohortRevisitBatch(
            batch_id=batch_id, cohort_id=self.cohort_id, source_file=source_file
        )
        with self._lock:
            active = self._active_index()
            for raw in lines:
                line = raw.strip()
                if not line:
                    continue
                parts = [part.strip() for part in line.split(",")]
                if parts and parts[0].casefold() == "member_id":
                    continue  # 表头
                parsed = self._parse_row(parts)
                if parsed is None:
                    batch.parse_failures.append(line)
                    continue
                member_id, observation_day, revisit_date = parsed
                if member_id not in self.members:
                    batch.rejections.append(RevisitRejection(
                        member_id=member_id,
                        observation_day=observation_day,
                        reason=RevisitRejectReason.MEMBER_MISMATCH,
                        source_file=source_file,
                        detail="回访成员不属于本同期群",
                    ))
                    continue
                if observation_day < 1 or observation_day > self.max_window:
                    batch.rejections.append(RevisitRejection(
                        member_id=member_id,
                        observation_day=observation_day,
                        reason=RevisitRejectReason.WINDOW_MISMATCH,
                        source_file=source_file,
                        detail="观察日超出本群观察期",
                    ))
                    continue
                record = RevisitFact(
                    member_id=member_id,
                    observation_day=observation_day,
                    revisit_date=revisit_date,
                    source_file=source_file,
                )
                if record.dedup_key in active:
                    batch.duplicates.append(record)  # 重复送达独立登记
                    continue
                active[record.dedup_key] = record
                batch.facts.append(record)
            batch.status = "待确认" if batch.complete else "部分解析失败"
            self.batches.append(batch)
        return batch

    @staticmethod
    def _parse_row(parts: list[str]) -> tuple[str, int, str] | None:
        """解析一行 member_id,observation_day,revisit_date，失败返回 None。"""
        if len(parts) != 3:
            return None
        member_id, day_raw, date_raw = parts
        if not member_id:
            return None
        try:
            observation_day = int(day_raw)
        except ValueError:
            return None
        try:
            datetime.strptime(date_raw, "%Y-%m-%d")
        except ValueError:
            return None
        return member_id, observation_day, date_raw

    def confirm_batch(self, batch_id: str) -> tuple[CohortRevisitBatch | None, str]:
        """确认批次：不完整文件不得确认；确认时在锁内再次去重，保证幂等。"""
        with self._lock:
            batch = self.find_batch(batch_id)
            if batch is None:
                return None, f"回访批次 {batch_id} 不存在"
            if batch.status == "已确认":
                return batch, f"回访批次 {batch_id} 已确认，无需重复操作"
            if batch.status == "已作废":
                return None, f"回访批次 {batch_id} 已作废，不得确认"
            if not batch.complete:
                return None, (
                    f"回访批次 {batch_id} 存在 {len(batch.parse_failures)} 行解析失败，"
                    "不完整文件不得确认"
                )
            accepted: list[RevisitFact] = []
            for fact in batch.facts:
                if fact.dedup_key in self.confirmed_facts:
                    batch.duplicates.append(fact)  # 并发确认时后到的记为重复
                else:
                    self.confirmed_facts[fact.dedup_key] = fact
                    accepted.append(fact)
            batch.facts = accepted
            batch.status = "已确认"
            return batch, f"回访批次 {batch_id} 已确认，登记 {len(accepted)} 条回访事实"

    def void_batch(self, batch_id: str) -> tuple[CohortRevisitBatch | None, str]:
        """作废批次：已确认批次不得作废；作废后不再阻塞发布、不参与去重。"""
        with self._lock:
            batch = self.find_batch(batch_id)
            if batch is None:
                return None, f"回访批次 {batch_id} 不存在"
            if batch.status == "已确认":
                return None, f"回访批次 {batch_id} 已确认，不得作废"
            if batch.status == "已作废":
                return batch, f"回访批次 {batch_id} 已作废，无需重复操作"
            batch.status = "已作废"
            return batch, f"回访批次 {batch_id} 已作废"

    def period_figures(self, window: int) -> tuple[int, int]:
        """某观察窗口的 (分子, 分母)。

        分子为观察日 <= 窗口的已确认事实覆盖的去重本群成员数（按观察日截断），
        分母为本群成员基数。
        """
        hit = {
            fact.member_id
            for fact in self.confirmed_facts.values()
            if fact.observation_day <= window and fact.member_id in self.members
        }
        return len(hit), len(self.members)

    def retention_curve(self) -> list[dict[str, Any]]:
        """按回访批次确认的分子分母计算各期留存曲线。"""
        curve = []
        for window in sorted(self.observation_windows):
            numerator, denominator = self.period_figures(window)
            curve.append({
                "window": window,
                "numerator": numerator,
                "denominator": denominator,
                "rate": round(numerator / denominator, 4) if denominator else 0.0,
            })
        return curve

    def recompute_window(self, window: int) -> dict[str, Any]:
        """按历史口径重算某期已确认的分子分母，用于核发前复核。"""
        numerator, denominator = self.period_figures(window)
        return {
            "window": window,
            "numerator": numerator,
            "denominator": denominator,
            "rate": round(numerator / denominator, 4) if denominator else 0.0,
        }

    def issue_report(self) -> RetentionReport:
        """本轮发布留存报告：各期结论按窗口升序发出。

        存在待确认或部分解析失败的批次时，本轮不生成任何结论。
        """
        with self._lock:
            blocking = [
                batch for batch in self.batches
                if batch.status in ("待确认", "部分解析失败")
            ]
            if blocking:
                reasons = "、".join(f"{b.batch_id}({b.status})" for b in blocking)
                return RetentionReport(
                    cohort_id=self.cohort_id,
                    round_id=self.release_round,
                    released=False,
                    hold_reason=f"批次 {reasons} 未完成确认，本期结论暂缓",
                )
            self.release_round += 1
            conclusions: list[PeriodConclusion] = []
            for window in sorted(self.observation_windows):
                numerator, denominator = self.period_figures(window)
                conclusion = PeriodConclusion(
                    window=window,
                    numerator=numerator,
                    denominator=denominator,
                    rate=round(numerator / denominator, 4) if denominator else 0.0,
                    round_id=self.release_round,
                    issue_seq=len(self.issued_conclusions) + 1,
                )
                self.issued_conclusions.append(conclusion)
                conclusions.append(conclusion)
            return RetentionReport(
                cohort_id=self.cohort_id,
                round_id=self.release_round,
                released=True,
                conclusions=conclusions,
            )

    def review_sampling(
        self,
        window: int,
        columns: list[str] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """导出工作台复核抽样：读取实际参与该期计算的已确认事实。

        按回访日期和成员排序，支持按列投影和限定行数；拒绝原因单列。
        """
        facts = [
            fact for fact in self.confirmed_facts.values()
            if fact.observation_day <= window
        ]
        facts.sort(key=lambda f: (f.revisit_date, _member_sort_key(f.member_id)))
        rows = [
            {
                "成员": fact.member_id,
                "观察日": fact.observation_day,
                "回访日期": fact.revisit_date,
                "来源文件": fact.source_file,
            }
            for fact in facts
        ]
        if columns:
            rows = [{key: row[key] for key in columns if key in row} for row in rows]
        if limit is not None:
            rows = rows[:limit]
        rejections = [
            {
                "成员": rejection.member_id,
                "观察日": rejection.observation_day,
                "拒绝原因": rejection.reason.value,
                "来源文件": rejection.source_file,
            }
            for batch in self.batches
            for rejection in batch.rejections
        ]
        return {
            "cohort_id": self.cohort_id,
            "window": window,
            "样本": rows,
            "拒绝明细": rejections,
        }


# ============================================================
# 异常检测
# ============================================================

@dataclass
class AnomalyDetect:
    """指标异常检测规则：算法、基线、阈值。"""
    rule_id: str
    target_metric: MetricDef
    algorithm: AnomalyAlgorithm
    baseline_period_days: int = 30
    threshold_multiplier: float = 3.0
    sensitivity: float = 0.05

    def detect(self, time_series: list[float]) -> list[int]:
        """检测时间序列中的异常点索引。

        Returns:
            异常点索引列表
        """
        if self.algorithm == AnomalyAlgorithm.IQR:
            return _iqr_anomaly_detect(time_series, self.threshold_multiplier)
        elif self.algorithm == AnomalyAlgorithm.ZSCORE:
            return _zscore_anomaly_detect(time_series, self.threshold_multiplier)
        elif self.algorithm == AnomalyAlgorithm.MOVING_AVERAGE:
            return _moving_average_anomaly_detect(time_series, self.threshold_multiplier)
        return []


# ============================================================
# 数据导出 / 快照
# ============================================================

@dataclass
class DataExport:
    """分析结果的按需导出任务。"""
    task_id: str
    source_query: OlapQuery
    export_format: str = "csv"
    field_selection: list[str] = field(default_factory=list)
    row_limit: int = 100000
    order_by: str = ""
    file_path: str = ""

    def build_row_subset(
        self, rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """按字段筛选和行数限制生成导出子集。"""
        if self.field_selection:
            rows = [
                {k: v for k, v in row.items() if k in self.field_selection}
                for row in rows
            ]
        if self.order_by:
            reverse = self.order_by.startswith("-")
            key = self.order_by.lstrip("-")
            rows = sorted(rows, key=lambda x: str(x.get(key, "")), reverse=reverse)
        return rows[:self.row_limit]


@dataclass
class DataSnapshot:
    """分析结果的历史快照版本。"""
    snapshot_id: str
    analysis_plan_id: str
    data_rows: list[dict[str, Any]] = field(default_factory=list)
    schema_summary: str = ""
    reason: str = ""
    retention_days: int = 90
    dependency_version: str = ""
    created_at: str = ""


# ============================================================
# 数据集关联
# ============================================================

@dataclass
class DatasetJoin:
    """多数据集间的关联查询方案。"""
    join_id: str
    primary_dataset: str
    join_dataset: str
    join_keys: list[str] = field(default_factory=list)
    join_type: JoinType = JoinType.INNER
    field_mapping: dict[str, str] = field(default_factory=dict)

    def execute_join(
        self,
        primary_rows: list[dict[str, Any]],
        join_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """执行数据集关联操作（确定性 INNER/LEFT JOIN）。"""
        result = []
        join_index = {}
        for jr in join_rows:
            key = tuple(str(jr.get(k, "")) for k in self.join_keys)
            if key not in join_index:
                join_index[key] = jr

        for pr in primary_rows:
            key = tuple(str(pr.get(k, "")) for k in self.join_keys)
            matched = join_index.get(key)
            if matched:
                merged = dict(pr)
                for src_field, tgt_field in self.field_mapping.items():
                    merged[tgt_field] = matched.get(src_field, "")
                result.append(merged)
            elif self.join_type == JoinType.LEFT:
                merged = dict(pr)
                for src_field, tgt_field in self.field_mapping.items():
                    merged[tgt_field] = ""
                result.append(merged)

        return result


# ============================================================
# 排行
# ============================================================

@dataclass
class RankingBoard:
    """按指标的排行计算方案。"""
    rank_id: str
    rank_metric: MetricDef
    rank_dimensions: list[DimensionDef] = field(default_factory=list)
    time_range: str = ""
    top_n: int = 10
    update_frequency: str = "daily"

    def compute_ranking(
        self,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """按指定指标对行数据进行排序并返回 TOP N。"""
        sorted_rows = sorted(
            rows,
            key=lambda x: float(x.get(self.rank_metric.field_name, 0)),
            reverse=True,
        )
        return sorted_rows[:self.top_n]


# ============================================================
# 报告生成
# ============================================================

@dataclass
class ReportTemplate:
    """分析报告的模板定义和指标引用。"""
    template_id: str
    report_name: str
    referenced_metrics: list[MetricDef] = field(default_factory=list)
    layout_format: str = "pdf"
    generation_period: str = "weekly"
    push_channel: str = "email"

    def generate_report_data(
        self,
        query_results: dict[str, list[dict[str, Any]]],
    ) -> dict[str, Any]:
        """根据引用的指标和查询结果组装报告数据。"""
        report = {
            "template_id": self.template_id,
            "report_name": self.report_name,
            "metrics": {},
        }
        for metric in self.referenced_metrics:
            metric_id = metric.metric_id
            if metric_id in query_results:
                rows = query_results[metric_id]
                computed = metric.compute(rows)
                report["metrics"][metric_id] = {
                    "name": metric.metric_name,
                    "value": computed,
                    "aggregation": metric.aggregation.value,
                }
        return report


# ============================================================
# 辅助函数
# ============================================================

def _percentile(values: list[float], p: float) -> float:
    """计算百分位数（线性插值方法）。"""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    if n == 1:
        return sorted_vals[0]
    rank = (n - 1) * p / 100.0
    lo = int(rank)
    hi = min(lo + 1, n - 1)
    frac = rank - lo
    return sorted_vals[lo] + frac * (sorted_vals[hi] - sorted_vals[lo])


def _t_distribution_pvalue(t_stat: float, df: float) -> float:
    """基于 t 分布 CDF 计算双尾 p 值。

    使用正则化不完全 Beta 函数的连分数实现。
    参考：Numerical Methods 3rd Edition, Section 6.14.4
    """
    import math
    if df <= 0:
        return 1.0
    abs_t = abs(t_stat)
    x = df / (df + abs_t * abs_t)
    p_tail = 0.5 * _betai(df / 2.0, 0.5, x)
    p = 2.0 * min(p_tail, 1.0 - p_tail)
    return min(max(p, 0.0), 1.0)


def _betai(a: float, b: float, x: float) -> float:
    """正则化不完全 Beta 函数 I_x(a,b)。

    使用连分数方法（Lentz 算法变体）。
    参考：Press et al., Numerical Methods, betacf.
    """
    import math

    if x < 0.0 or x > 1.0:
        return 0.0

    if x == 0.0:
        return 0.0
    if x == 1.0:
        return 1.0

    # 系数计算 - 如果 x > (a+1)/(a+b+2) 则使用对称关系 I_x(a,b) = 1 - I_{1-x}(b,a)
    if x > (a + 1.0) / (a + b + 2.0):
        return 1.0 - _betai(b, a, 1.0 - x)

    # 前置因子
    # I_x(a,b) = x^a * (1-x)^b * exp(ln_beta(a,b)) / a * betacf(a,b,x)
    # 其中 betacf 是连分数评估结果
    log_beta = math.lgamma(a) + math.lgamma(b) - math.lgamma(a + b)
    front = math.exp(math.log(x) * a + math.log(1.0 - x) * b - math.log(a) - log_beta)

    # 连分数
    fpmin = 1e-30
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    c_val = 1.0
    d_val = 1.0 - qab * x / qap
    if abs(d_val) < fpmin:
        d_val = fpmin
    d_val = 1.0 / d_val
    h = d_val

    for m in range(1, 200):
        m2 = 2.0 * m
        # 偶数步
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d_val = 1.0 + aa * d_val
        if abs(d_val) < fpmin:
            d_val = fpmin
        c_val = 1.0 + aa / c_val
        if abs(c_val) < fpmin:
            c_val = fpmin
        d_val = 1.0 / d_val
        h = h * d_val * c_val

        # 奇数步
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d_val = 1.0 + aa * d_val
        if abs(d_val) < fpmin:
            d_val = fpmin
        c_val = 1.0 + aa / c_val
        if abs(c_val) < fpmin:
            c_val = fpmin
        d_val = 1.0 / d_val
        del_val = d_val * c_val
        h = h * del_val

    return front * h


# ============================================================
# 归因算法实现
# ============================================================

def _first_touch_attribution(
    touchpoints: list[dict[str, Any]],
    conversions: list[dict[str, Any]],
    window_days: int,
) -> dict[str, float]:
    """首次触点归因：100% 权重给第一个触点。"""
    weights: dict[str, float] = {}
    converted_users = {c["user_id"] for c in conversions}
    user_first_touch: dict[str, str] = {}
    for tp in sorted(touchpoints, key=lambda x: x.get("timestamp", "")):
        uid = tp["user_id"]
        if uid in converted_users and uid not in user_first_touch:
            user_first_touch[uid] = tp.get("channel", "unknown")
    for channel in user_first_touch.values():
        weights[channel] = weights.get(channel, 0.0) + 1.0
    total = sum(weights.values())
    if total > 0:
        for ch in weights:
            weights[ch] = round(weights[ch] / total, 4)
    return weights


def _last_touch_attribution(
    touchpoints: list[dict[str, Any]],
    conversions: list[dict[str, Any]],
    window_days: int,
) -> dict[str, float]:
    """末次触点归因：100% 权重给最后一个触点。"""
    weights: dict[str, float] = {}
    converted_users = {c["user_id"] for c in conversions}
    seen_users: set[str] = set()
    for tp in reversed(touchpoints):
        uid = tp["user_id"]
        if uid in converted_users and uid not in seen_users:
            seen_users.add(uid)
            ch = tp.get("channel", "unknown")
            weights[ch] = weights.get(ch, 0.0) + 1.0
    total = sum(weights.values())
    if total > 0:
        for ch in weights:
            weights[ch] = round(weights[ch] / total, 4)
    return weights


def _linear_attribution(
    touchpoints: list[dict[str, Any]],
    conversions: list[dict[str, Any]],
    window_days: int,
) -> dict[str, float]:
    """线性归因：权重平均分配给所有触点。"""
    weights: dict[str, float] = {}
    converted_users = {c["user_id"] for c in conversions}
    user_touchpoints: dict[str, list[str]] = {}
    for tp in touchpoints:
        uid = tp["user_id"]
        if uid in converted_users:
            user_touchpoints.setdefault(uid, []).append(tp.get("channel", "unknown"))
    for uid, channels in user_touchpoints.items():
        weight_per_touch = 1.0 / len(channels)
        for ch in channels:
            weights[ch] = weights.get(ch, 0.0) + weight_per_touch
    total = sum(weights.values())
    if total > 0:
        for ch in weights:
            weights[ch] = round(weights[ch] / total, 4)
    return weights


def _time_decay_attribution(
    touchpoints: list[dict[str, Any]],
    conversions: list[dict[str, Any]],
    window_days: int,
) -> dict[str, float]:
    """时间衰减归因：离转化越近的触点权重越高。"""
    weights: dict[str, float] = {}
    converted_users = {
        c["user_id"]: c.get("conversion_time", "") for c in conversions
    }
    for tp in touchpoints:
        uid = tp["user_id"]
        if uid not in converted_users:
            continue
        ch = tp.get("channel", "unknown")
        conv_time = converted_users[uid]
        tp_time = tp.get("timestamp", conv_time)
        if tp_time == conv_time:
            w = 1.0
        else:
            # Simple inverse time decay
            from datetime import datetime
            try:
                d1 = datetime.fromisoformat(tp_time.replace("Z", "+00:00"))
                d2 = datetime.fromisoformat(conv_time.replace("Z", "+00:00"))
                diff_days = abs((d2 - d1).total_seconds()) / 86400.0
                half_life = 7.0
                w = 2.0 ** (-diff_days / half_life)
            except (ValueError, TypeError):
                w = 0.5
        weights[ch] = weights.get(ch, 0.0) + w
    total = sum(weights.values())
    if total > 0:
        for ch in weights:
            weights[ch] = round(weights[ch] / total, 4)
    return weights


def _position_based_attribution(
    touchpoints: list[dict[str, Any]],
    conversions: list[dict[str, Any]],
    window_days: int,
) -> dict[str, float]:
    """位置归因：40% 首个 + 40% 末次 + 20% 中间平均。"""
    first_rate = 0.4
    last_rate = 0.4
    middle_rate = 0.2

    weights: dict[str, float] = {}
    converted_users = {c["user_id"] for c in conversions}
    user_tps: dict[str, list[dict[str, Any]]] = {}
    for tp in sorted(touchpoints, key=lambda x: x.get("timestamp", "")):
        uid = tp["user_id"]
        if uid in converted_users:
            user_tps.setdefault(uid, []).append(tp)

    for uid, tps in user_tps.items():
        n = len(tps)
        for i, tp in enumerate(tps):
            ch = tp.get("channel", "unknown")
            if i == 0:
                w = first_rate
            elif i == n - 1:
                w = last_rate
            else:
                w = middle_rate / (n - 2) if n > 2 else 0
            weights[ch] = weights.get(ch, 0.0) + w
    total = sum(weights.values())
    if total > 0:
        for ch in weights:
            weights[ch] = round(weights[ch] / total, 4)
    return weights


# ============================================================
# 异常检测算法实现
# ============================================================

def _iqr_anomaly_detect(
    time_series: list[float],
    multiplier: float = 3.0,
) -> list[int]:
    """IQR (四分位距) 异常检测。"""
    if len(time_series) < 4:
        return []
    sorted_data = sorted(time_series)
    n = len(sorted_data)
    q1_idx = n // 4
    q3_idx = (3 * n) // 4
    q1 = sorted_data[q1_idx]
    q3 = sorted_data[min(q3_idx, n - 1)]
    iqr = q3 - q1
    if iqr == 0:
        return []
    lower = q1 - multiplier * iqr
    upper = q3 + multiplier * iqr
    return [i for i, v in enumerate(time_series) if v < lower or v > upper]


def _zscore_anomaly_detect(
    time_series: list[float],
    multiplier: float = 3.0,
) -> list[int]:
    """Z-Score 异常检测。"""
    if len(time_series) < 4:
        return []
    n = len(time_series)
    mean = sum(time_series) / n
    variance = sum((x - mean) ** 2 for x in time_series) / n
    if variance == 0:
        return []
    std = variance ** 0.5
    return [i for i, v in enumerate(time_series) if abs(v - mean) / std > multiplier]


def _moving_average_anomaly_detect(
    time_series: list[float],
    multiplier: float = 3.0,
    window: int = 7,
) -> list[int]:
    """移动平均异常检测（基于残差的 Z-Score）。"""
    if len(time_series) < window + 2:
        return []
    residuals = []
    for i in range(window, len(time_series)):
        ma = sum(time_series[i - window:i]) / window
        residuals.append(abs(time_series[i] - ma))
    if not residuals:
        return []
    mean_res = sum(residuals) / len(residuals)
    var_res = sum((r - mean_res) ** 2 for r in residuals) / len(residuals)
    if var_res == 0:
        return []
    std_res = var_res ** 0.5
    anomalies = []
    for i, r in enumerate(residuals):
        if r > mean_res + multiplier * std_res:
            anomalies.append(i + window)
    return anomalies