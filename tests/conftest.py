"""Shared fixtures.

One SparkSession per test run: Spark start-up costs seconds, and every test
here is a pure function of a fixture file, so sharing a session cannot leak
state between them.
"""

import pytest

from ancora.session import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("ancora-tests")
    yield session
    session.stop()
