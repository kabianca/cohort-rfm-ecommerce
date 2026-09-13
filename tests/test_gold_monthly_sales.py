from datetime import date
from decimal import Decimal


def rows(df):
    return {r.month: r for r in df.collect()}


def test_one_row_per_month_with_net_revenue(run):
    got = rows(run.monthly)
    assert sorted(got) == [date(2010, 12, 1), date(2011, 1, 1), date(2011, 2, 1), date(2011, 3, 1)]
    assert [got[m].revenue for m in sorted(got)] == [
        Decimal("306.420"),
        Decimal("438.020"),
        Decimal("109.440"),
        Decimal("639.880"),
    ]


def test_unidentified_customers_count_in_revenue(run):
    """The notebook's dropna understated revenue by a quarter. Here the null
    CustomerID rows are absent from RFM but present in the month."""
    got = rows(run.monthly)
    assert got[date(2010, 12, 1)].unidentified_revenue == Decimal("51.000")
    assert got[date(2011, 2, 1)].unidentified_revenue == Decimal(
        "25.500"
    )  # 51.00 sold, 25.50 refunded
    assert got[date(2011, 3, 1)].unidentified_revenue == Decimal("165.000")
    for r in got.values():
        assert r.revenue == r.identified_revenue + r.unidentified_revenue


def test_a_cancellation_is_negative_revenue_not_lost_revenue(run):
    """February: +102.00 +17.70 +44.40 +20.34 +51.00 −49.50 −51.00 −25.50."""
    feb = rows(run.monthly)[date(2011, 2, 1)]
    assert feb.revenue == Decimal("109.440")
    assert feb.cancellations == 3
    assert feb.orders == 5


def test_orders_and_customers_count_purchases_only(run):
    got = rows(run.monthly)
    assert [(got[m].orders, got[m].customers) for m in sorted(got)] == [
        (5, 4),
        (8, 8),
        (5, 4),
        (8, 6),
    ]
