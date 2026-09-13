from ancora import bronze
from ancora.bronze import RAW_COLUMNS
from tests.conftest import FIXTURE, read_delta


def test_every_raw_column_is_text(run):
    assert [f.dataType.simpleString() for f in run.bronze.schema if f.name in RAW_COLUMNS] == ["string"] * 8


def test_no_row_is_lost_or_coerced(run):
    assert run.bronze.count() == 41
    unparseable = run.bronze.filter("Quantity = 'ten' OR InvoiceDate LIKE '2011-02-30%'").count()
    assert unparseable == 2, "bronze must keep what silver will reject"


def test_lineage_columns(run):
    assert run.bronze.select("_source_file").distinct().collect()[0][0] == FIXTURE.name
    assert run.bronze.filter("_ingested_at IS NULL").count() == 0


def test_relanding_the_same_file_does_not_duplicate_it(spark, tmp_path):
    table = tmp_path / "bronze"
    bronze.ingest(spark, FIXTURE, table)
    bronze.ingest(spark, FIXTURE, table)
    assert read_delta(spark, table).count() == 41
