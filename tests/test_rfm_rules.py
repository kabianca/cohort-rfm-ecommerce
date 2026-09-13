"""The scoring rules on their own, with synthetic data."""

import itertools
import random

from ancora import rfm


def test_every_score_combination_has_exactly_one_segment(spark):
    combos = list(itertools.product(range(1, 6), repeat=3))
    df = (
        spark.createDataFrame(combos, "r_score INT, f_score INT, m_score INT")
        .withColumn("fm_score", rfm.fm_score())
        .withColumn("segment", rfm.segment())
    )
    assert df.filter("segment IS NULL").count() == 0
    assert df.count() == 125
    assert {r.segment for r in df.select("segment").distinct().collect()} == {
        name for name, _ in rfm.SEGMENT_RULES
    }


def test_segment_examples(spark):
    cases = [
        (5, 5, 5, "champions"),
        (1, 1, 1, "hibernating"),
        (3, 1, 1, "promising"),
        (1, 5, 5, "at_risk"),
        (3, 3, 3, "loyal"),
        (4, 3, 4, "champions"),
        (4, 3, 3, "loyal"),
    ]
    df = (
        spark.createDataFrame([c[:3] for c in cases], "r_score INT, f_score INT, m_score INT")
        .withColumn("fm_score", rfm.fm_score())
        .withColumn("segment", rfm.segment())
    )
    assert [r.segment for r in df.collect()] == [c[3] for c in cases]


def test_tied_values_share_a_score_whatever_the_row_order(spark):
    values = [1, 1, 1, 1, 1, 1, 2, 3, 4, 5]
    ids = list(range(len(values)))

    def score(order):
        rows = [(ids[i], values[i], "s") for i in order]
        df = spark.createDataFrame(rows, "id INT, frequency INT, snapshot STRING")
        df = df.withColumn(
            "f", rfm.quintile_score("frequency", higher_is_better=True, partition="snapshot")
        )
        return {r.id: r.f for r in df.collect()}

    forward = score(ids)
    shuffled = score(random.Random(7).sample(ids, len(ids)))
    assert forward == shuffled
    assert {forward[i] for i in range(6)} == {1}, "six tied customers, one score"
    assert forward[9] == 5 and forward[8] == 5  # rank 10/10 → 1.0 capped, rank 9/10 → 0.889


def test_lower_recency_scores_higher(spark):
    df = spark.createDataFrame(
        [(1, 0, "s"), (2, 100, "s"), (3, 50, "s")], "id INT, recency_days INT, snapshot STRING"
    )
    df = df.withColumn(
        "r", rfm.quintile_score("recency_days", higher_is_better=False, partition="snapshot")
    )
    got = {r.id: r.r for r in df.collect()}
    assert got[1] > got[3] > got[2]
