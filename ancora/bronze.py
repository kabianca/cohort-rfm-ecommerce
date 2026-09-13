"""Bronze: the CSV exactly as it arrived, plus where it came from and when.

Every column is read as text on purpose. Letting Spark infer types here would
already be a modelling decision ("ten" becomes null, 30 February becomes null)
taken silently, and the quarantine in silver exists to take it out loud.
"""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

RAW_COLUMNS = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
]
RAW_SCHEMA = T.StructType([T.StructField(name, T.StringType(), True) for name in RAW_COLUMNS])


def read_raw(spark: SparkSession, source: Path) -> DataFrame:
    return (
        spark.read.schema(RAW_SCHEMA)
        .option("header", True)
        # Check the header against the schema instead of trusting position, so
        # a re-published file with reordered columns fails here, not in silver.
        .option("enforceSchema", False)
        .csv(str(source))
        .withColumn("_source_file", F.col("_metadata.file_name"))
        # The only wall-clock value in the project. It is lineage, and nothing
        # downstream reads it.
        .withColumn("_ingested_at", F.current_timestamp())
    )


def ingest(spark: SparkSession, source: Path, table: Path) -> DataFrame:
    """Land one file. Re-landing the same file replaces its rows, so the
    table never holds two copies of one source."""
    raw = read_raw(spark, source)
    (
        raw.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_source_file = '{Path(source).name}'")
        .save(str(table))
    )
    return spark.read.format("delta").load(str(table))
