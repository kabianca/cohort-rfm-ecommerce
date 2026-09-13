"""Gold: three tables, each updated in place with a Delta MERGE.

`customer_rfm` is parameterised by `snapshot_date`: recency is the distance
from the customer's last purchase to that date, never to the wall clock, and
only invoices on or before it are considered. Two runs with the same snapshot
therefore produce the same rows on any day. Different snapshots coexist in
the table, keyed by (customer_id, snapshot_date).

`cohort_retention` and `monthly_sales` are calendars over the whole history
and are keyed by their natural months.

A customer belongs to the gold layer when their net spend, purchases minus
cancellations, is positive. Someone who bought and returned everything did
not become a customer; someone whose only row is a refund of a purchase made
before the data starts is not one either. Their amounts still count in
`monthly_sales`, because revenue is revenue whoever it came from.
"""

from datetime import date
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ancora import rfm


def purchase() -> F.Column:
    """Built on demand: a Column needs a live session, and importing this
    module must not."""
    return ~F.col("is_cancellation")


def resolve_snapshot_date(sales: DataFrame, requested: date | None) -> date:
    """Explicit parameter first; otherwise the last day in the data. Never today."""
    if requested is not None:
        return requested
    return sales.agg(F.max("invoice_date")).first()[0]


def _net_spend(sales: DataFrame) -> DataFrame:
    return (
        sales.filter(F.col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(F.sum("line_amount").alias("net_spend"))
    )


def eligible_customers(sales: DataFrame) -> DataFrame:
    return _net_spend(sales).filter(F.col("net_spend") > 0).select("customer_id")


def customer_rfm(sales: DataFrame, snapshot_date: date) -> DataFrame:
    as_of = sales.filter(F.col("invoice_date") <= F.lit(snapshot_date))
    identified = as_of.filter(F.col("customer_id").isNotNull())

    per_customer = (
        identified.groupBy("customer_id")
        .agg(
            F.sum("line_amount").cast("decimal(14,3)").alias("monetary"),
            F.count_distinct(F.when(purchase(), F.col("invoice_no"))).alias("frequency"),
            F.min(F.when(purchase(), F.col("invoice_date"))).alias("first_purchase_date"),
            F.max(F.when(purchase(), F.col("invoice_date"))).alias("last_purchase_date"),
            F.mode("country", deterministic=True).alias("country"),
        )
        .filter(F.col("monetary") > 0)
        .withColumn("snapshot_date", F.lit(snapshot_date))
        .withColumn("recency_days", F.datediff(F.col("snapshot_date"), F.col("last_purchase_date")))
    )

    scored = (
        per_customer.withColumn(
            "r_score",
            rfm.quintile_score("recency_days", higher_is_better=False, partition="snapshot_date"),
        )
        .withColumn(
            "f_score",
            rfm.quintile_score("frequency", higher_is_better=True, partition="snapshot_date"),
        )
        .withColumn(
            "m_score",
            rfm.quintile_score("monetary", higher_is_better=True, partition="snapshot_date"),
        )
        .withColumn("fm_score", rfm.fm_score())
        .withColumn("rfm", F.concat("r_score", "f_score", "m_score"))
        .withColumn("segment", rfm.segment())
    )
    return scored.select(
        "customer_id",
        "snapshot_date",
        "country",
        "first_purchase_date",
        "last_purchase_date",
        "recency_days",
        "frequency",
        "monetary",
        "r_score",
        "f_score",
        "m_score",
        "rfm",
        "segment",
    )


def cohort_retention(sales: DataFrame) -> DataFrame:
    purchases = sales.filter(purchase() & F.col("customer_id").isNotNull()).join(
        eligible_customers(sales), "customer_id"
    )
    cohorts = purchases.groupBy("customer_id").agg(
        F.trunc(F.min("invoice_date"), "month").alias("cohort_month")
    )
    activity = purchases.select(
        "customer_id", F.trunc("invoice_date", "month").alias("activity_month")
    ).distinct()

    # Whole calendar months, not 30-day windows: 31 January to 1 February is 1.
    month_number = lambda col: F.year(col) * 12 + F.month(col)
    counts = (
        activity.join(cohorts, "customer_id")
        .withColumn("month_index", month_number("activity_month") - month_number("cohort_month"))
        .groupBy("cohort_month", "month_index")
        .agg(F.count_distinct("customer_id").alias("customers"))
    )
    sizes = counts.filter(F.col("month_index") == 0).select(
        "cohort_month", F.col("customers").alias("cohort_size")
    )
    return (
        counts.join(sizes, "cohort_month")
        .withColumn(
            "retention_rate", (F.col("customers") / F.col("cohort_size")).cast("decimal(5,4)")
        )
        .select("cohort_month", "month_index", "cohort_size", "customers", "retention_rate")
    )


def monthly_sales(sales: DataFrame) -> DataFrame:
    identified = F.col("customer_id").isNotNull()
    amount = F.col("line_amount")
    zero = F.lit(0).cast("decimal(14,3)")
    return (
        sales.groupBy(F.trunc("invoice_date", "month").alias("month"))
        .agg(
            F.sum(amount).cast("decimal(14,3)").alias("revenue"),
            F.coalesce(F.sum(F.when(identified, amount)), zero)
            .cast("decimal(14,3)")
            .alias("identified_revenue"),
            F.coalesce(F.sum(F.when(~identified, amount)), zero)
            .cast("decimal(14,3)")
            .alias("unidentified_revenue"),
            F.count_distinct(F.when(purchase(), F.col("invoice_no"))).alias("orders"),
            F.count_distinct(F.when(~purchase(), F.col("invoice_no"))).alias("cancellations"),
            F.count_distinct(F.when(purchase(), F.col("customer_id"))).alias("customers"),
            F.sum("quantity").alias("units"),
        )
        .select(
            "month",
            "revenue",
            "identified_revenue",
            "unidentified_revenue",
            "orders",
            "cancellations",
            "customers",
            "units",
        )
    )


def upsert(
    spark: SparkSession, df: DataFrame, table: Path, keys: list[str], scope: str | None = None
) -> None:
    """MERGE `df` into `table` on `keys`. Rows of the target that the source no
    longer produces are deleted, optionally only within `scope` (a SQL
    predicate over the target aliased `t`), so a re-run converges to exactly
    what the source says instead of accumulating leftovers."""
    if not DeltaTable.isDeltaTable(spark, str(table)):
        df.write.format("delta").save(str(table))
        return
    condition = " AND ".join(f"t.{key} = s.{key}" for key in keys)
    merge = (
        DeltaTable.forPath(spark, str(table))
        .alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
    )
    merge = merge.whenNotMatchedBySourceDelete(condition=scope)
    merge.execute()


def build(
    spark: SparkSession,
    sales: DataFrame,
    snapshot_date: date,
    customer_rfm_table: Path,
    cohort_retention_table: Path,
    monthly_sales_table: Path,
) -> None:
    upsert(
        spark,
        customer_rfm(sales, snapshot_date),
        customer_rfm_table,
        keys=["customer_id", "snapshot_date"],
        scope=f"t.snapshot_date = DATE '{snapshot_date.isoformat()}'",
    )
    upsert(
        spark, cohort_retention(sales), cohort_retention_table, keys=["cohort_month", "month_index"]
    )
    upsert(spark, monthly_sales(sales), monthly_sales_table, keys=["month"])
