"""bronze → silver → gold, end to end, for one source file."""

from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession

from ancora import bronze, gold, silver
from ancora.config import Layout


def run(spark: SparkSession, source: Path, layout: Layout, snapshot_date: date | None = None) -> date:
    """Returns the snapshot date actually used, so a caller that passed None
    can report which day anchored the run."""
    landed = bronze.ingest(spark, source, layout.bronze)
    sales, _ = silver.build(spark, landed, layout.silver_sales, layout.silver_quarantine)
    snapshot = gold.resolve_snapshot_date(sales, snapshot_date)
    gold.build(
        spark,
        sales,
        snapshot,
        layout.gold_customer_rfm,
        layout.gold_cohort_retention,
        layout.gold_monthly_sales,
    )
    return snapshot
