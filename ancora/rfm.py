"""Scoring rules for RFM. Pure column expressions; no I/O.

Two rules live here and both are the product, not a detail:

* A score is a quintile of the customer's *rank*, and customers with the same
  value have the same rank, so they get the same score. `qcut`-style binning
  splits ties by whatever order the rows happened to be in, which in a
  wholesale dataset where hundreds of customers share a frequency of 1 or 2
  means the segment of a real customer depends on the sort order of a file.
  The cost: quintiles are not exactly 20 % each when ties straddle a boundary.

* Segments are an ordered rule list over the recency score and the average of
  the frequency and monetary scores. First match wins; the last rule catches
  everything, so every customer has exactly one segment.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

SCORE_LEVELS = 5

# (segment, rule). R is the recency score; FM is ceil((F + M) / 2).
SEGMENT_RULES = [
    ("champions", "r_score >= 4 AND fm_score >= 4"),
    ("loyal", "r_score >= 3 AND fm_score >= 3"),
    ("promising", "r_score >= 3"),
    ("at_risk", "fm_score >= 3"),
    ("hibernating", "true"),
]


def quintile_score(metric: str, higher_is_better: bool, partition: str) -> F.Column:
    """1..5 from `percent_rank`, which assigns tied values the same rank.

    The window is ordered so that the best value ranks last: for frequency and
    monetary that is ascending, for recency (fewer days is better) descending.
    """
    order = F.col(metric).asc() if higher_is_better else F.col(metric).desc()
    rank = F.percent_rank().over(Window.partitionBy(partition).orderBy(order))
    return F.least(F.lit(SCORE_LEVELS), F.floor(rank * SCORE_LEVELS) + 1).cast("int")


def fm_score() -> F.Column:
    return F.ceil((F.col("f_score") + F.col("m_score")) / 2).cast("int")


def segment() -> F.Column:
    expr = None
    for name, rule in SEGMENT_RULES:
        expr = F.when(F.expr(rule), F.lit(name)) if expr is None else expr.when(F.expr(rule), F.lit(name))
    return expr
