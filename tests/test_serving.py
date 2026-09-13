from ancora import serving


def test_gold_tables_are_served_from_one_duckdb_file(run):
    con = serving.connect(run.layout.serving_db)
    try:
        counts = {
            t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in serving.GOLD_TABLES
        }
        reasons = dict(con.execute("SELECT reason, rows_ FROM quarantine_reasons").fetchall())
    finally:
        con.close()
    assert counts == {"customer_rfm": 10, "cohort_retention": 8, "monthly_sales": 4}
    assert reasons["duplicate"] == 1
    assert reasons["non_product_stock_code"] == 4  # POST, M, AMAZONFEE, B
    assert sum(reasons.values()) == 11, "nine rows, two of them with two reasons"


def test_refresh_is_repeatable(run):
    serving.refresh(run.layout)
    con = serving.connect(run.layout.serving_db)
    try:
        assert con.execute("SELECT count(*) FROM customer_rfm").fetchone()[0] == 10
    finally:
        con.close()


def test_charts_are_drawn_from_the_serving_database(run, tmp_path):
    paths = serving.render_charts(run.layout, tmp_path)
    assert [p.name for p in paths] == [
        "monthly_revenue.png",
        "cohort_retention.png",
        "segments.png",
    ]
    for p in paths:
        assert p.stat().st_size > 10_000, p


def test_fingerprint_is_stable(run):
    first = serving.fingerprint(run.layout)
    serving.refresh(run.layout)
    assert serving.fingerprint(run.layout) == first
    assert first[0] == 10
