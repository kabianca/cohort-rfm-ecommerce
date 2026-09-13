"""Silver: typed, deduplicated, and honest about what it could not keep.

A bronze row ends up in exactly one of two tables. `sales` holds rows the gold
layer may aggregate. `quarantine` holds the rest, each with the list of
reasons it failed, so "how much did we throw away, and why" is a query rather
than a guess. Nothing is dropped.

Cancellations are *kept* in `sales`, with their negative quantities. Netting
them against purchases is arithmetic the gold layer does; removing them here
would turn a refund into revenue.
"""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ancora.bronze import RAW_COLUMNS

# StockCodes that are fees, adjustments or postage rather than goods. They are
# real rows in the ledger, but not sales of a product to a customer, so they
# count for neither revenue nor RFM. Verified against the full dataset; the
# other non-numeric codes (gift vouchers, `DCGS*`, `PADS`) are products.
NON_PRODUCT_STOCK_CODES = ["POST", "DOT", "M", "C2", "D", "S", "BANK CHARGES", "AMAZONFEE", "CRUK", "B"]

TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

SALES_COLUMNS = [
    "invoice_no",
    "stock_code",
    "description",
    "quantity",
    "invoice_ts",
    "invoice_date",
    "unit_price",
    "customer_id",
    "country",
    "is_cancellation",
    "line_amount",
    "_source_file",
]


def _typed(bronze: DataFrame) -> DataFrame:
    """Typed columns next to the raw ones, which travel inside a `raw` struct:
    Spark resolves names case-insensitively, so `Quantity` and `quantity`
    cannot both be top-level columns. `try_*` yields null instead of raising
    under Spark 4's ANSI mode, and null is what the checks look for."""
    exact_copy = Window.partitionBy(*RAW_COLUMNS).orderBy(F.lit(1))
    return bronze.select(
        F.struct(*RAW_COLUMNS).alias("raw"),
        "_source_file",
        "_ingested_at",
        F.trim("InvoiceNo").alias("invoice_no"),
        F.upper(F.trim("StockCode")).alias("stock_code"),
        F.trim("Description").alias("description"),
        F.col("Quantity").try_cast("int").alias("quantity"),
        F.try_to_timestamp(F.col("InvoiceDate"), F.lit(TIMESTAMP_FORMAT)).alias("invoice_ts"),
        F.col("UnitPrice").try_cast("decimal(10,3)").alias("unit_price"),
        F.col("CustomerID").try_cast("int").alias("customer_id"),
        F.trim("Country").alias("country"),
        F.trim("InvoiceNo").startswith("C").alias("is_cancellation"),
        F.row_number().over(exact_copy).alias("_copy_number"),
    )


def _reasons() -> F.Column:
    """Every rule a row can fail, in the order they are reported. A row keeps
    all the reasons that apply, not only the first one."""
    checks = [
        ("duplicate", F.col("_copy_number") > 1),
        ("unparseable_quantity", F.col("quantity").isNull()),
        ("unparseable_invoice_date", F.col("invoice_ts").isNull()),
        ("unparseable_unit_price", F.col("unit_price").isNull()),
        ("unparseable_customer_id", F.col("raw.CustomerID").isNotNull() & F.col("customer_id").isNull()),
        ("negative_unit_price", F.col("unit_price") < 0),
        ("zero_unit_price", F.col("unit_price") == 0),
        ("non_product_stock_code", F.col("stock_code").isin(NON_PRODUCT_STOCK_CODES)),
        ("negative_quantity_outside_cancellation", (F.col("quantity") < 0) & ~F.col("is_cancellation")),
        ("positive_quantity_in_cancellation", (F.col("quantity") > 0) & F.col("is_cancellation")),
    ]
    return F.array_compact(F.array(*[F.when(cond, F.lit(name)) for name, cond in checks]))


def split(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    flagged = _typed(bronze).withColumn("reasons", _reasons())

    sales = (
        flagged.filter(F.size("reasons") == 0)
        .withColumn("invoice_date", F.to_date("invoice_ts"))
        # Decimal, not double: a sum of doubles depends on the order the
        # partitions arrive in, and the thesis of the project is that it must not.
        .withColumn("line_amount", (F.col("quantity") * F.col("unit_price")).cast("decimal(14,3)"))
        .select(*SALES_COLUMNS)
    )
    quarantine = flagged.filter(F.size("reasons") > 0).select(
        "raw.*", "_source_file", "_ingested_at", "reasons"
    )
    return sales, quarantine


def build(
    spark: SparkSession, bronze: DataFrame, sales_table: Path, quarantine_table: Path
) -> tuple[DataFrame, DataFrame]:
    """Silver is a pure function of bronze, so it is rebuilt, not merged."""
    sales, quarantine = split(bronze)
    sales.write.format("delta").mode("overwrite").save(str(sales_table))
    quarantine.write.format("delta").mode("overwrite").save(str(quarantine_table))
    return (
        spark.read.format("delta").load(str(sales_table)),
        spark.read.format("delta").load(str(quarantine_table)),
    )
