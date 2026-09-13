"""Âncora: deterministic RFM and cohort segmentation.

The thesis of the package is that the same input always produces the same
segments. Everything that could make two runs disagree (the wall clock, tie
order in quantile cuts, the session time zone) is pinned explicitly.
"""

__version__ = "0.1.0"
