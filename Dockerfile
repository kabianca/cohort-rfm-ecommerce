# syntax=docker/dockerfile:1

# The Java half of the Spark / Delta / Java triple. The Python half (pyspark,
# delta-spark) is pinned in pyproject.toml; the compatibility note there says
# what was verified and when. Spark 4.2 runs on Java 17, 21 or 25; Delta 4.4
# requires 17 or newer. 21 is the current LTS that both list.
ARG PYTHON_VERSION=3.12
ARG JAVA_VERSION=21

FROM python:${PYTHON_VERSION}-slim-trixie
ARG JAVA_VERSION

RUN apt-get update \
 && apt-get install -y --no-install-recommends "openjdk-${JAVA_VERSION}-jre-headless" procps \
 && rm -rf /var/lib/apt/lists/*

# A fixed, non-root UID so files written into the bind-mounted ./data belong
# to the person running `make`, on the common case of a single-user machine.
RUN useradd --create-home --uid 1000 --shell /bin/bash ancora

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SPARK_LOCAL_IP=127.0.0.1 \
    ANCORA_DATA_DIR=/app/data \
    ANCORA_IVY_DIR=/opt/ivy \
    ANCORA_DUCKDB_EXTENSION_DIR=/opt/duckdb-extensions

WORKDIR /app

# Install dependencies from the manifest before copying the code, so editing a
# module does not re-download Spark. The stub package is only there because
# an editable install needs something to point at.
COPY pyproject.toml README.md ./
RUN mkdir -p ancora && touch ancora/__init__.py \
 && pip install --no-cache-dir --editable ".[dev]"

COPY ancora ./ancora

# Fetch the Delta jars and DuckDB's delta extension now, into directories any
# UID can read, so that a run (and the test suite) needs no network. A first
# SparkSession is enough for Ivy to resolve and cache the artefacts.
RUN mkdir -p /opt/ivy /opt/duckdb-extensions \
 && python -c "from ancora.session import get_spark; get_spark().stop()" \
 && python -c "import duckdb, os; c = duckdb.connect(config={'extension_directory': os.environ['ANCORA_DUCKDB_EXTENSION_DIR']}); c.install_extension('delta'); c.load_extension('delta')" \
 && chmod -R a+rwX /opt/ivy /opt/duckdb-extensions \
 && mkdir -p /app/data && chown -R ancora:ancora /app

USER ancora

CMD ["python", "-m", "ancora", "--help"]
