from decimal import Decimal

from pyspark.sql import functions as F

EXPECTED_QUARANTINE = {
    ("600013", "21755", "6"): ["duplicate"],
    ("600001", "22222", "1"): ["zero_unit_price"],
    ("600002", "POST", "1"): ["non_product_stock_code"],
    ("600010", "M", "1"): ["non_product_stock_code"],
    ("600046", "AMAZONFEE", "1"): ["non_product_stock_code"],
    ("A600099", "B", "1"): ["negative_unit_price", "non_product_stock_code"],
    ("600050", "23343", "-20"): ["zero_unit_price", "negative_quantity_outside_cancellation"],
    ("600047", "21034", "10"): ["unparseable_invoice_date"],
    ("600048", "21034", "ten"): ["unparseable_quantity"],
}


def test_every_bronze_row_lands_in_exactly_one_table(run):
    assert run.sales.count() == 32
    assert run.quarantine.count() == 9
    assert run.sales.count() + run.quarantine.count() == run.bronze.count()


def test_quarantine_carries_every_reason_that_applies(run):
    got = {
        (r.InvoiceNo, r.StockCode, r.Quantity): r.reasons
        for r in run.quarantine.select("InvoiceNo", "StockCode", "Quantity", "reasons").collect()
    }
    assert got == EXPECTED_QUARANTINE


def test_a_duplicate_row_is_kept_once(run):
    copies = run.sales.filter("invoice_no = '600013' AND stock_code = '21755'").count()
    assert copies == 1
    assert run.sales.distinct().count() == run.sales.count()


def test_cancellations_stay_with_their_sign(run):
    row = run.sales.filter("invoice_no = 'C600032'").first()
    assert row.is_cancellation is True
    assert row.quantity == -2
    assert row.line_amount == Decimal("-25.500")


def test_unidentified_customers_are_valid_sales(run):
    assert run.sales.filter(F.col("customer_id").isNull()).count() == 4


def test_typed_schema(run):
    types = {f.name: f.dataType.simpleString() for f in run.sales.schema}
    assert types["quantity"] == "int"
    assert types["invoice_ts"] == "timestamp"
    assert types["invoice_date"] == "date"
    assert types["unit_price"] == "decimal(10,3)"
    assert types["line_amount"] == "decimal(14,3)"
    assert types["customer_id"] == "int"
