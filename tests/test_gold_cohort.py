from datetime import date
from decimal import Decimal

DEC, JAN, FEB, MAR = date(2010, 12, 1), date(2011, 1, 1), date(2011, 2, 1), date(2011, 3, 1)

EXPECTED = {
    (DEC, 0): (4, 4, "1.0000"),
    (DEC, 1): (4, 2, "0.5000"),  # 10001, 10012
    (DEC, 2): (4, 3, "0.7500"),  # 10001, 10011, 10012
    (DEC, 3): (4, 1, "0.2500"),  # 10001
    (JAN, 0): (5, 5, "1.0000"),  # 10003..10007; 10002 cancelled everything
    (JAN, 1): (5, 1, "0.2000"),  # 10007, 1 February 00:10
    (JAN, 2): (5, 4, "0.8000"),  # 10003, 10004, 10005, 10006
    (MAR, 0): (1, 1, "1.0000"),  # 10010
}


def test_cohort_table(run):
    got = {
        (r.cohort_month, r.month_index): (r.cohort_size, r.customers, str(r.retention_rate))
        for r in run.cohort.collect()
    }
    assert got == EXPECTED


def test_month_index_counts_calendar_months(run):
    """31 January 23:55 → 1 February 00:10 is month 1, not month 0."""
    row = run.cohort.filter("cohort_month = DATE '2011-01-01' AND month_index = 1").first()
    assert row.customers == 1


def test_a_fully_cancelled_customer_founds_no_cohort(run):
    january = run.cohort.filter("cohort_month = DATE '2011-01-01' AND month_index = 0").first()
    assert january.cohort_size == 5, "10002 bought in January and cancelled it all"
    assert run.cohort.filter("cohort_month = DATE '2011-02-01'").count() == 0, "10008 only refunded"


def test_retention_is_relative_to_cohort_size(run):
    for r in run.cohort.collect():
        assert r.retention_rate == (Decimal(r.customers) / Decimal(r.cohort_size)).quantize(Decimal("0.0001"))
