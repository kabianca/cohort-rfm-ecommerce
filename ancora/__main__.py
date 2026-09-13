"""`python -m ancora run [--snapshot-date YYYY-MM-DD] [--source FILE]`"""

import argparse
from datetime import date
from pathlib import Path

from ancora import pipeline
from ancora.config import DEFAULT_LAYOUT
from ancora.session import get_spark


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(prog="ancora", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="bronze → silver → gold")
    run.add_argument(
        "--source",
        type=Path,
        default=DEFAULT_LAYOUT.raw / "online_retail.csv",
        help="CSV to ingest (default: the file `make data` produces)",
    )
    run.add_argument(
        "--snapshot-date",
        type=date.fromisoformat,
        default=None,
        help="day recency is measured from (default: the last invoice date in the data)",
    )

    args = parser.parse_args(argv)
    spark = get_spark()
    try:
        snapshot = pipeline.run(spark, args.source, DEFAULT_LAYOUT, args.snapshot_date)
        print(f"gold tables written under {DEFAULT_LAYOUT.root}, snapshot_date={snapshot.isoformat()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
