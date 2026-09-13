"""The Spark / Delta / DuckDB triple actually works together.

Version pins are only a claim until something writes a Delta table, merges
into it and reads it back from DuckDB. This is the test that fails first if
one of the three is bumped without the others, and it runs without network.
"""

import os

import duckdb
from delta.tables import DeltaTable

from ancora.config import DUCKDB_EXTENSION_DIR


def test_delta_merge_and_duckdb_read(spark, tmp_path):
    path = str(tmp_path / "customers")

    spark.createDataFrame([(1, "a"), (2, "b")], "id INT, v STRING").write.format("delta").save(path)

    updates = spark.createDataFrame([(2, "B"), (3, "c")], "id INT, v STRING")
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(updates.alias("u"), "t.id = u.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    con = duckdb.connect(config={"extension_directory": DUCKDB_EXTENSION_DIR})
    con.load_extension("delta")
    rows = con.execute(f"select id, v from delta_scan('{path}') order by id").fetchall()

    assert rows == [(1, "a"), (2, "B"), (3, "c")]
    assert os.path.isdir(os.path.join(path, "_delta_log"))
