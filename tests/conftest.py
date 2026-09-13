"""Shared fixtures.

One SparkSession and one pipeline run per test session: Spark start-up costs
seconds, and every assertion is a pure function of the fixture file, so
sharing the outputs cannot leak state between tests. Tests that need to
mutate tables get their own `tmp_path`.
"""

from pathlib import Path
from types import SimpleNamespace

import pytest

from ancora import pipeline
from ancora.config import Layout
from ancora.session import get_spark

FIXTURE = Path(__file__).parent / "fixtures" / "online_retail_fixture.csv"


def read_delta(spark, path):
    return spark.read.format("delta").load(str(path))


@pytest.fixture(scope="session")
def spark():
    session = get_spark("ancora-tests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def run(spark, tmp_path_factory):
    layout = Layout(tmp_path_factory.mktemp("lakehouse"))
    snapshot = pipeline.run(spark, FIXTURE, layout)
    return SimpleNamespace(
        layout=layout,
        snapshot=snapshot,
        bronze=read_delta(spark, layout.bronze),
        sales=read_delta(spark, layout.silver_sales),
        quarantine=read_delta(spark, layout.silver_quarantine),
        rfm=read_delta(spark, layout.gold_customer_rfm),
        cohort=read_delta(spark, layout.gold_cohort_retention),
        monthly=read_delta(spark, layout.gold_monthly_sales),
    )
