"""洞察工坊 · 同期群回访批次测试

覆盖合作方文件导入与重送、批次确认、留存分子分母、复核抽样和并发幂等：
- 甲、乙同日起始，导入甲第 1 天、乙第 4 天、甲第 1 天重送及非本群的丙；
- 确认后第 1 天为 1/2、第 7 天为 2/2，重复与越界独立登记；
- 第 7 天文件部分解析失败不得生成结论，更正文件送达后两期报告本轮发布；
- 两次并发导入同一回访只记一次。
"""
from __future__ import annotations

import threading

import pytest

from app.analysis_pipeline import (
    pipeline_cohort_revisit_export,
    run_full_analysis_pipeline,
)
from app.models import (
    CohortAnalysis,
    CohortRevisitLedger,
    RevisitRejectReason,
)
from app.services import service_for

START = "2026-11-01"
MEMBERS = {"甲": START, "乙": START}
WINDOWS = [1, 7]
HEADER = "member_id,observation_day,revisit_date"


def write_partner_file(tmp_path, name, rows):
    """把合作方文件内容真实落盘，返回文件路径。"""
    path = tmp_path / name
    path.write_text("\n".join([HEADER, *rows]) + "\n", encoding="utf-8")
    return str(path)


@pytest.fixture
def ledger():
    return CohortRevisitLedger(
        cohort_id="COH_TEST",
        members=dict(MEMBERS),
        observation_windows=list(WINDOWS),
    )


@pytest.fixture
def day1_file(tmp_path):
    """第 1 天文件：甲第 1 天、乙第 4 天、甲第 1 天重送、非本群的丙。"""
    return write_partner_file(tmp_path, "revisit_d1.csv", [
        "甲,1,2026-11-02",
        "乙,4,2026-11-05",
        "甲,1,2026-11-02",
        "丙,1,2026-11-02",
    ])


@pytest.fixture
def day7_partial_file(tmp_path):
    """第 7 天文件（不完整）：乙的回访行损坏。"""
    return write_partner_file(tmp_path, "revisit_d7_partial.csv", [
        "甲,7,2026-11-08",
        "乙,7,not-a-date",
    ])


@pytest.fixture
def day7_fix_file(tmp_path):
    """第 7 天更正文件（完整）。"""
    return write_partner_file(tmp_path, "revisit_d7_fix.csv", [
        "甲,7,2026-11-08",
        "乙,7,2026-11-08",
    ])


@pytest.fixture
def confirmed_ledger(ledger, day1_file):
    """第 1 天文件已导入并确认的台账。"""
    ledger.import_file("B-D1", day1_file)
    batch, _ = ledger.confirm_batch("B-D1")
    assert batch is not None
    return ledger


# ============================================================
# 文件导入：事实、重复、拒绝、解析失败
# ============================================================

class TestRevisitImport:
    def test_import_registers_facts_duplicates_and_rejections(self, ledger, day1_file):
        batch = ledger.import_file("B-D1", day1_file)
        assert batch.status == "待确认"
        assert batch.complete
        assert [(f.member_id, f.observation_day) for f in batch.facts] == [("甲", 1), ("乙", 4)]
        # 重送的甲第 1 天独立登记为重复
        assert [(f.member_id, f.observation_day) for f in batch.duplicates] == [("甲", 1)]
        # 非本群的丙独立登记为成员不符
        assert len(batch.rejections) == 1
        rejection = batch.rejections[0]
        assert rejection.member_id == "丙"
        assert rejection.reason == RevisitRejectReason.MEMBER_MISMATCH
        assert batch.parse_failures == []

    def test_window_mismatch_is_distinct_from_member_mismatch(self, ledger, tmp_path):
        path = write_partner_file(tmp_path, "revisit_window.csv", [
            "甲,30,2026-12-01",  # 超出观察期
            "乙,0,2026-11-01",   # 观察日非法
            "丙,1,2026-11-02",   # 非本群
        ])
        batch = ledger.import_file("B-WIN", path)
        reasons = {r.member_id: r.reason for r in batch.rejections}
        assert reasons["甲"] == RevisitRejectReason.WINDOW_MISMATCH
        assert reasons["乙"] == RevisitRejectReason.WINDOW_MISMATCH
        assert reasons["丙"] == RevisitRejectReason.MEMBER_MISMATCH
        assert batch.facts == []

    def test_parse_failure_marks_batch_incomplete(self, ledger, day7_partial_file):
        batch = ledger.import_file("B-D7", day7_partial_file)
        assert batch.status == "部分解析失败"
        assert not batch.complete
        assert batch.parse_failures == ["乙,7,not-a-date"]
        assert [(f.member_id, f.observation_day) for f in batch.facts] == [("甲", 7)]


# ============================================================
# 留存分子分母
# ============================================================

class TestRetentionFigures:
    def test_confirmed_retention_day1_and_day7(self, confirmed_ledger):
        assert confirmed_ledger.period_figures(1) == (1, 2)
        assert confirmed_ledger.period_figures(7) == (2, 2)
        curve = confirmed_ledger.retention_curve()
        assert [(c["window"], c["numerator"], c["denominator"]) for c in curve] == [
            (1, 1, 2),
            (7, 2, 2),
        ]
        assert [c["rate"] for c in curve] == [0.5, 1.0]

    def test_unconfirmed_facts_do_not_count(self, ledger, day1_file):
        ledger.import_file("B-D1", day1_file)
        assert ledger.period_figures(1) == (0, 2)  # 未确认不计入分子

    def test_retention_curve_is_deterministic(self, confirmed_ledger):
        assert confirmed_ledger.retention_curve() == confirmed_ledger.retention_curve()


# ============================================================
# 批次确认与结论发布
# ============================================================

class TestConfirmAndIssue:
    def test_incomplete_batch_cannot_confirm(self, ledger, day7_partial_file):
        ledger.import_file("B-D7", day7_partial_file)
        batch, message = ledger.confirm_batch("B-D7")
        assert batch is None
        assert "不完整" in message
        assert ledger.confirmed_facts == {}

    def test_incomplete_file_blocks_day7_conclusion(self, confirmed_ledger, day7_partial_file):
        confirmed_ledger.import_file("B-D7", day7_partial_file)
        report = confirmed_ledger.issue_report()
        assert not report.released
        assert report.conclusions == []
        assert "B-D7" in report.hold_reason
        # 已确认的第 1 天留存仍可核实
        assert confirmed_ledger.recompute_window(1)["numerator"] == 1

    def test_confirm_is_idempotent(self, confirmed_ledger):
        batch, message = confirmed_ledger.confirm_batch("B-D1")
        assert batch is not None
        assert "无需重复" in message
        assert len(confirmed_ledger.confirmed_facts) == 2

    def test_void_rules(self, ledger, day7_partial_file):
        ledger.import_file("B-D7", day7_partial_file)
        batch, message = ledger.void_batch("B-D7")
        assert batch is not None and batch.status == "已作废"
        # 已作废批次不得确认
        batch, message = ledger.confirm_batch("B-D7")
        assert batch is None
        assert "已作废" in message

    def test_confirmed_batch_cannot_be_voided(self, confirmed_ledger):
        batch, message = confirmed_ledger.void_batch("B-D1")
        assert batch is None
        assert "不得作废" in message

    def test_full_round_workflow(self, ledger, day1_file, day7_partial_file, day7_fix_file):
        # 第 1 天文件导入并确认，业务方领取可核实的第 1 天留存
        ledger.import_file("B-D1", day1_file)
        ledger.confirm_batch("B-D1")
        round1 = ledger.issue_report()
        assert round1.released
        assert [(c.window, c.numerator, c.denominator) for c in round1.conclusions] == [
            (1, 1, 2),
            (7, 2, 2),
        ]

        # 第 7 天文件部分解析失败：不得确认，本轮不生成任何结论
        ledger.import_file("B-D7", day7_partial_file)
        batch, _ = ledger.confirm_batch("B-D7")
        assert batch is None
        blocked = ledger.issue_report()
        assert not blocked.released
        assert blocked.conclusions == []

        # 不完整的旧文件作废，完整的更正文件送达：确认后两个时期的报告都在本轮发布
        ledger.void_batch("B-D7")
        ledger.import_file("B-D7-FIX", day7_fix_file)
        ledger.confirm_batch("B-D7-FIX")
        round2 = ledger.issue_report()
        assert round2.released
        assert round2.round_id == 2
        assert [(c.window, c.numerator, c.denominator) for c in round2.conclusions] == [
            (1, 1, 2),
            (7, 2, 2),
        ]

        # 已确认的分子分母按历史口径重算一致
        for conclusion in round1.conclusions:
            again = ledger.recompute_window(conclusion.window)
            assert (again["numerator"], again["denominator"]) == (
                conclusion.numerator,
                conclusion.denominator,
            )

        # 各期结论按窗口升序发出，发出序号递增
        seqs = [c.issue_seq for c in ledger.issued_conclusions]
        assert seqs == sorted(seqs)
        windows = [c.window for c in round2.conclusions]
        assert windows == sorted(windows)


# ============================================================
# 旧文件重送
# ============================================================

class TestResend:
    def test_old_file_resend_registers_duplicates_only(self, confirmed_ledger, day1_file):
        batch = confirmed_ledger.import_file("B-RESEND", day1_file)
        assert batch.facts == []
        assert [(f.member_id, f.observation_day) for f in batch.duplicates] == [
            ("甲", 1),
            ("乙", 4),
            ("甲", 1),
        ]
        assert [r.member_id for r in batch.rejections] == ["丙"]
        # 已确认事实不被重送覆盖
        assert len(confirmed_ledger.confirmed_facts) == 2

    def test_day7_sampling_after_resend(self, confirmed_ledger, day1_file):
        confirmed_ledger.import_file("B-RESEND", day1_file)
        sampling = confirmed_ledger.review_sampling(7, columns=["成员", "观察日"], limit=1)
        assert sampling["样本"] == [{"成员": "甲", "观察日": 1}]


# ============================================================
# 复核抽样
# ============================================================

class TestReviewSampling:
    def test_sampling_sorted_by_date_and_member(self, ledger, day1_file, day7_fix_file):
        ledger.import_file("B-D1", day1_file)
        ledger.confirm_batch("B-D1")
        ledger.import_file("B-D7", day7_fix_file)
        ledger.confirm_batch("B-D7")
        sampling = ledger.review_sampling(7)
        assert [(r["成员"], r["观察日"]) for r in sampling["样本"]] == [
            ("甲", 1),
            ("乙", 4),
            ("甲", 7),
            ("乙", 7),
        ]

    def test_sampling_reads_only_facts_of_that_period(self, confirmed_ledger):
        sampling = confirmed_ledger.review_sampling(1)
        assert [(r["成员"], r["观察日"]) for r in sampling["样本"]] == [("甲", 1)]

    def test_sampling_projection_and_limit(self, confirmed_ledger):
        sampling = confirmed_ledger.review_sampling(7, columns=["成员", "观察日"], limit=1)
        assert sampling["样本"] == [{"成员": "甲", "观察日": 1}]
        # 抽样数值来自已确认事实
        fact = confirmed_ledger.confirmed_facts[("甲", 1)]
        assert fact.member_id == "甲"
        assert fact.observation_day == 1

    def test_rejections_listed_separately(self, confirmed_ledger):
        sampling = confirmed_ledger.review_sampling(7)
        assert sampling["拒绝明细"] == [
            {"成员": "丙", "观察日": 1, "拒绝原因": "成员不符", "来源文件": "revisit_d1.csv"}
        ]


# ============================================================
# 并发幂等
# ============================================================

class TestConcurrentImport:
    def test_concurrent_import_records_once(self, ledger, day1_file):
        def worker(batch_id):
            ledger.import_file(batch_id, day1_file)

        threads = [
            threading.Thread(target=worker, args=(f"B-{i}",)) for i in range(2)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        for batch_id in ("B-0", "B-1"):
            ledger.confirm_batch(batch_id)

        # 两次并发导入同一回访只记一次
        assert set(ledger.confirmed_facts) == {("甲", 1), ("乙", 4)}
        # 后到的批次把同一回访独立登记为重复
        total_duplicates = sum(len(b.duplicates) for b in ledger.batches)
        assert total_duplicates >= 2


# ============================================================
# 跨模块管道
# ============================================================

class TestPipeline:
    def test_pipeline_cohort_revisit_export(self, confirmed_ledger):
        result = pipeline_cohort_revisit_export(
            [confirmed_ledger],
            review_window=7,
            sample_columns=["成员", "观察日"],
            sample_limit=1,
        )
        assert result["pipeline"] == "cohort_analysis -> cohort_revisit -> data_export"
        cohort = result["cohorts"][0]
        assert cohort["retention_curve"] == [
            {"window": 1, "numerator": 1, "denominator": 2, "rate": 0.5},
            {"window": 7, "numerator": 2, "denominator": 2, "rate": 1.0},
        ]
        assert cohort["sample_rows"] == [{"成员": "甲", "观察日": 1}]
        assert cohort["rejections"][0]["拒绝原因"] == "成员不符"
        assert cohort["export_task"] == "EXP_COH_TEST_D7"

    def test_full_pipeline_includes_flow11(self, confirmed_ledger):
        result = run_full_analysis_pipeline(
            metrics=[],
            dimensions=[],
            events=[],
            experiments=[],
            attribution_models=[],
            anomaly_rules=[],
            segments=[],
            ranking_boards=[],
            joins=[],
            report_templates=[],
            sample_data={
                "cohort_ledgers": [confirmed_ledger],
                "cohort_review_window": 7,
                "cohort_sample_columns": ["成员", "观察日"],
                "cohort_sample_limit": 1,
            },
        )
        assert "flow11_cohort_revisit" in result
        assert result["flow11_cohort_revisit"]["total_cohorts"] == 1


# ============================================================
# 模块登记
# ============================================================

class TestModuleRegistration:
    def test_cohort_builds_revisit_ledger(self):
        cohort = CohortAnalysis(
            cohort_id="COH_X", cohort_name="测试群", observation_windows=[1, 7]
        )
        ledger = cohort.build_revisit_ledger(MEMBERS)
        assert ledger.cohort_id == "COH_X"
        assert ledger.observation_windows == [1, 7]
        assert ledger.members == MEMBERS

    def test_service_registered(self):
        service = service_for("cohort_revisit")
        assert service is not None
        assert service.ENTITY == "回访批次"
        assert service.STATUS_ORDER[0] == "待确认"

    def test_seed_rows_present(self):
        from app.store import store
        rows = store.rows("cohort_revisit")
        assert len(rows) >= 3
        statuses = {row["status"] for row in rows}
        assert "部分解析失败" in statuses
