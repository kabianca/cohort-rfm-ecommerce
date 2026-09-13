"""Paths and runtime knobs, all overridable through the environment.

Kept as plain module-level constants so a test can point the whole pipeline at
a temporary directory by setting one variable before the session is built.
"""

import os
from pathlib import Path

# Root of the lakehouse on disk. Bind-mounted from ./data by docker-compose.
DATA_DIR = Path(os.environ.get("ANCORA_DATA_DIR", "data"))

RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
SERVING_DB = DATA_DIR / "serving" / "ancora.duckdb"

# Where Spark resolves the Delta jars. Fixed outside $HOME so the cache warmed at
# image build time is found whatever UID runs the container (bind mounts make
# the UID a deployment detail, not a code one).
IVY_DIR = os.environ.get("ANCORA_IVY_DIR", "/opt/ivy")

# Same reasoning for DuckDB's extension directory: the `delta` extension is
# installed at build time so a run needs no network.
DUCKDB_EXTENSION_DIR = os.environ.get("ANCORA_DUCKDB_EXTENSION_DIR", "/opt/duckdb-extensions")
