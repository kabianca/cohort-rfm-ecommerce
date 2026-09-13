"""One SparkSession factory for the pipeline, the tests and the CLI.

Every setting here exists because its default could make two runs of the same
input disagree, or make the container need the network at run time.
"""

import os
from glob import glob
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from ancora.config import IVY_DIR

LOG_CONFIG = Path(__file__).with_name("log4j2.properties")


def get_spark(app_name: str = "ancora") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("ANCORA_SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # The CSV carries naive timestamps. Parsing and date extraction must
        # happen in the same zone on every machine, or a 23:30 invoice can
        # change day between the laptop that built the tables and the CI that
        # reads them. UTC is the only zone nobody has to configure.
        .config("spark.sql.session.timeZone", "UTC")
        # A single laptop's worth of data; 200 shuffle partitions only add
        # scheduling overhead and thousands of tiny files in Delta.
        .config("spark.sql.shuffle.partitions", "8")
        # Local mode never needs the container's hostname, and resolving it is
        # the first thing that breaks on a laptop with odd DNS or a container
        # without network. Bind to loopback and stop asking.
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.extraJavaOptions", f"-Dlog4j2.configurationFile=file:{LOG_CONFIG}")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )

    # The image pre-fetches the Delta jars at build time. Handing them to
    # Spark by path skips Ivy entirely: no resolution banner, no network, and
    # exactly the artefacts that were verified. Outside the image, fall back to
    # letting delta-spark resolve them from Maven.
    jars = sorted(glob(f"{IVY_DIR}/jars/*.jar"))
    if jars:
        return builder.config("spark.jars", ",".join(jars)).getOrCreate()
    return configure_spark_with_delta_pip(builder.config("spark.jars.ivy", IVY_DIR)).getOrCreate()
