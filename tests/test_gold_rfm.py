from datetime import date, timedelta
from decimal import Decimal

from pyspark.sql import functions as F

from ancora import gold
from tests.conftest import read_delta

ELIGIBLE = {10001, 10003, 10004, 10005, 10006, 10007, 10009, 10010, 10011, 10012}

# customer: (recency_days, frequency, monetary, rfm, segment) at snapshot 2011-03-31
EXPECTED = {
    10001: (0, 5, "465.780", "555", "champions"),
    10003: (29, 2, "142.800", "324", "loyal"),
    10004: (24, 2, "108.160", "323", "loyal"),
    10005: (24, 2, "153.000", "325", "loyal"),
    10006: (10, 2, "150.000", "424", "loyal"),
    10007: (58, 2, "35.400", "121", "hibernating"),
    10009: (118, 1, "88.800", "112", "hibernating"),
    10010: (3, 1, "9.500", "511", "promising"),
    10011: (49, 2, "88.800", "222", "hibernating"),
    10012: (56, 3, "61.020", "252", "at_risk"),
}


def by_customer(df):
    return {r.customer_id: r for r in df.collect()}


def test_snapshot_defaults_to_the_last_day_in_the_data(run):
    assert run.snapshot == date(2011, 3, 31)
    assert run.rfm.select("snapshot_date").distinct().collect()[0][0] == date(2011, 3, 31)


def test_who_is_a_customer(run):
    assert set(by_customer(run.rfm)) == ELIGIBLE
    assert 10002 not in by_customer(run.rfm), "bought and cancelled everything"
    assert 10008 not in by_customer(run.rfm), "only a refund in the window"


def test_a_null_customer_id_is_absent_from_rfm(run):
    assert run.rfm.filter(F.col("customer_id").isNull()).count() == 0


def test_a_cancellation_reduces_the_right_customer(run):
    """10001 bought 491.28 and cancelled 25.50 of it."""
    assert by_customer(run.rfm)[10001].monetary == Decimal("465.780")


def test_a_duplicated_row_does_not_inflate_frequency_or_monetary(run):
    row = by_customer(run.rfm)[10003]
    assert row.frequency == 2
    assert row.monetary == Decimal("142.800")


def test_scores_and_segments(run):
    got = {
        r.customer_id: (r.recency_days, r.frequency, str(r.monetary), r.rfm, r.segment)
        for r in run.rfm.collect()
    }
    assert got == EXPECTED


def test_ties_share_a_score(run):
    rows = by_customer(run.rfm)
    assert len({rows[c].f_score for c in (10003, 10004, 10005, 10006, 10007, 10011)}) == 1
    assert rows[10004].r_score == rows[10005].r_score  # both last bought on 7 March
    assert rows[10009].m_score == rows[10011].m_score  # both spent 88.80


def test_recency_is_anchored_to_the_snapshot_not_the_clock(run):
    """Move the snapshot a week: every recency moves by exactly a week and
    nothing else changes. Whatever day this test runs, the answer is the same."""
    later = gold.customer_rfm(run.sales, run.snapshot + timedelta(days=7))
    base = by_customer(run.rfm)
    moved = by_customer(later)
    assert set(moved) == set(base)
    for customer, row in moved.items():
        assert row.recency_days == base[customer].recency_days + 7
        assert (row.frequency, row.monetary, row.rfm, row.segment) == (
            base[customer].frequency, base[customer].monetary, base[customer].rfm, base[customer].segment,
        )


def test_the_same_input_produces_the_same_segments(run):
    first = sorted(map(tuple, gold.customer_rfm(run.sales, run.snapshot).collect()))
    second = sorted(map(tuple, gold.customer_rfm(run.sales, run.snapshot).collect()))
    assert first == second == sorted(map(tuple, run.rfm.collect()))


def test_revenue_by_segment_reconciles_with_identified_revenue(run):
    """sum(monetary) equals identified revenue minus the net of customers
    excluded for non-positive spend (10002: 0.00, 10008: −51.00)."""
    by_segment = run.rfm.groupBy("segment").agg(F.sum("monetary").alias("m"))
    segments_total = by_segment.agg(F.sum("m")).first()[0]
    identified = run.sales.filter("customer_id IS NOT NULL").agg(F.sum("line_amount")).first()[0]
    excluded = (
        run.sales.filter("customer_id IS NOT NULL")
        .groupBy("customer_id").agg(F.sum("line_amount").alias("net"))
        .filter("net <= 0").agg(F.sum("net")).first()[0]
    )
    assert segments_total == Decimal("1303.260")
    assert identified == Decimal("1252.260")
    assert segments_total == identified - excluded


def test_an_earlier_snapshot_sees_only_what_existed_then(run):
    """As of 28 February, March has not happened: 10010 is not a customer yet
    and 10001 has three invoices, not five."""
    as_of = by_customer(gold.customer_rfm(run.sales, date(2011, 2, 28)))
    assert 10010 not in as_of
    assert as_of[10001].frequency == 3
    assert as_of[10001].monetary == Decimal("302.880")
    assert as_of[10001].recency_days == 20


def test_merge_converges_and_keeps_other_snapshots(spark, run, tmp_path):
    table = tmp_path / "customer_rfm"
    args = dict(keys=["customer_id", "snapshot_date"])

    march = gold.customer_rfm(run.sales, date(2011, 3, 31))
    gold.upsert(spark, march, table, scope="t.snapshot_date = DATE '2011-03-31'", **args)
    gold.upsert(spark, march, table, scope="t.snapshot_date = DATE '2011-03-31'", **args)
    assert read_delta(spark, table).count() == 10, "a re-run adds nothing"

    february = gold.customer_rfm(run.sales, date(2011, 2, 28))
    gold.upsert(spark, february, table, scope="t.snapshot_date = DATE '2011-02-28'", **args)
    stored = read_delta(spark, table)
    assert stored.filter("snapshot_date = DATE '2011-03-31'").count() == 10, "March untouched"
    assert stored.filter("snapshot_date = DATE '2011-02-28'").count() == february.count()

    # A customer that stops being eligible disappears from that snapshot only.
    shrunk = march.filter("customer_id <> 10010")
    gold.upsert(spark, shrunk, table, scope="t.snapshot_date = DATE '2011-03-31'", **args)
    stored = read_delta(spark, table)
    assert stored.filter("snapshot_date = DATE '2011-03-31'").count() == 9
    assert stored.filter("snapshot_date = DATE '2011-02-28'").count() == february.count()
