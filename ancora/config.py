"""Paths and runtime knobs, all overridable through the environment.

The lakehouse layout is a value, not a set of globals, so a test can run the
whole pipeline inside a temporary directory without touching the environment.
"""

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Layout:
    root: Path

    @property
    def raw(self) -> Path:
        return self.root / "raw"

    @property
    def bronze(self) -> Path:
        return self.root / "bronze" / "online_retail"

    @property
    def silver_sales(self) -> Path:
        return self.root / "silver" / "sales"

    @property
    def silver_quarantine(self) -> Path:
        return self.root / "silver" / "quarantine"

    @property
    def gold_customer_rfm(self) -> Path:
        return self.root / "gold" / "customer_rfm"

    @property
    def gold_cohort_retention(self) -> Path:
        return self.root / "gold" / "cohort_retention"

    @property
    def gold_monthly_sales(self) -> Path:
        return self.root / "gold" / "monthly_sales"

    @property
    def serving_db(self) -> Path:
        return self.root / "serving" / "ancora.duckdb"


# Root of the lakehouse on disk. Bind-mounted from ./data by docker-compose.
DATA_DIR = Path(os.environ.get("ANCORA_DATA_DIR", "data"))
DEFAULT_LAYOUT = Layout(DATA_DIR)
RAW_DIR = DEFAULT_LAYOUT.raw

# Where Spark resolves the Delta jars. Fixed outside $HOME so the cache warmed at
# image build time is found whatever UID runs the container (bind mounts make
# the UID a deployment detail, not a code one).
IVY_DIR = os.environ.get("ANCORA_IVY_DIR", "/opt/ivy")

# Same reasoning for DuckDB's extension directory: the `delta` extension is
# installed at build time so a run needs no network.
DUCKDB_EXTENSION_DIR = os.environ.get("ANCORA_DUCKDB_EXTENSION_DIR", "/opt/duckdb-extensions")
