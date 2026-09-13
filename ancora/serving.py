"""Serving: the gold tables in one DuckDB file, and the charts drawn from it.

DuckDB reads the Delta tables directly through its `delta` extension and
materialises them into a single `.duckdb` file, so anyone can open the
results in a SQL client without Spark, Java or the lake itself.

The charts are code, not screenshots. Re-running the pipeline re-draws them
from the same tables the tests assert on, which is what lets the README's
figures be trusted.
"""

from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")  # headless: the container has no display

import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

from ancora.config import DUCKDB_EXTENSION_DIR, Layout

# One light surface, drawn explicitly so the figures read the same in a dark
# README. Categorical slots 1 and 2 validated for CVD separation; the gray is
# the de-emphasis role, never a series of its own.
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
DEEMPHASIS = "#d9d8d2"
BLUE = "#2a78d6"
ORANGE = "#eb6834"
BLUE_RAMP = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]

GOLD_TABLES = ("customer_rfm", "cohort_retention", "monthly_sales")


def connect(db: Path) -> duckdb.DuckDBPyConnection:
    db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db), config={"extension_directory": DUCKDB_EXTENSION_DIR})
    con.load_extension("delta")
    return con


def refresh(layout: Layout) -> Path:
    """Materialise the gold tables and a quarantine summary into the serving file."""
    con = connect(layout.serving_db)
    try:
        for table in GOLD_TABLES:
            path = getattr(layout, f"gold_{table}")
            con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM delta_scan('{path}')")
        con.execute(
            "CREATE OR REPLACE TABLE quarantine_reasons AS "
            "SELECT reason, count(*) AS rows_ "
            f"FROM (SELECT unnest(reasons) AS reason FROM delta_scan('{layout.silver_quarantine}')) "
            "GROUP BY reason ORDER BY rows_ DESC"
        )
    finally:
        con.close()
    return layout.serving_db


# --- charts -----------------------------------------------------------------


def _style(ax, *, grid_axis: str = "y") -> None:
    ax.set_facecolor(SURFACE)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(AXIS)
    ax.spines["bottom"].set_linewidth(0.8)
    ax.tick_params(colors=INK_MUTED, labelsize=8.5, length=0)
    if grid_axis:
        ax.grid(axis=grid_axis, color=GRID, linewidth=0.8)
        ax.set_axisbelow(True)


def _title(fig, title: str, subtitle: str) -> None:
    fig.text(0.06, 0.95, title, fontsize=12.5, fontweight="semibold", color=INK, ha="left", va="top")
    fig.text(0.06, 0.895, subtitle, fontsize=9, color=INK_SECONDARY, ha="left", va="top", linespacing=1.5)


def _figure(width: float = 9, height: float = 4.4):
    fig = plt.figure(figsize=(width, height), dpi=200, facecolor=SURFACE)
    return fig


def _save(fig, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, facecolor=SURFACE, bbox_inches="tight", pad_inches=0.25)
    plt.close(fig)
    return path


def chart_monthly_revenue(con, path: Path) -> Path:
    rows = con.execute(
        "SELECT month, identified_revenue, unidentified_revenue FROM monthly_sales ORDER BY month"
    ).fetchall()
    months = [r[0] for r in rows]
    identified = [float(r[1]) for r in rows]
    unidentified = [float(r[2]) for r in rows]
    share = sum(unidentified) / (sum(identified) + sum(unidentified))
    last_day = con.execute("SELECT max(last_purchase_date) FROM customer_rfm").fetchone()[0]

    fig = _figure()
    ax = fig.add_axes([0.06, 0.12, 0.9, 0.68])
    _style(ax)
    x = range(len(months))
    ax.bar(x, identified, width=0.55, color=DEEMPHASIS, label="Identified customer")
    # A 2px stroke in the surface colour is the gap between stacked segments.
    ax.bar(x, unidentified, width=0.55, bottom=identified, color=BLUE,
           edgecolor=SURFACE, linewidth=1.5, label="No CustomerID")
    ax.set_xticks(list(x), [m.strftime("%b\n%Y") if m.month in (1, 12) else m.strftime("%b") for m in months])
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"£{v / 1e6:.1f}M" if v else "0"))
    ax.set_xlim(-0.6, len(months) - 0.4)
    last = len(months) - 1
    ax.text(last, identified[last] + unidentified[last], f"{last_day.day} days\n", ha="center", va="bottom",
            fontsize=7.5, color=INK_MUTED, linespacing=0.6)
    ax.legend(frameon=False, loc="upper left", fontsize=8.5, labelcolor=INK_SECONDARY, handlelength=1, handleheight=1)
    _title(
        fig,
        "Monthly net revenue, by whether the customer is known",
        f"Rows without a CustomerID are {share:.1%} of revenue. A dropna() would have removed them.",
    )
    return _save(fig, path)


def chart_cohort_retention(con, path: Path) -> Path:
    rows = con.execute(
        "SELECT cohort_month, cohort_size, month_index, retention_rate FROM cohort_retention "
        "WHERE month_index >= 1 ORDER BY cohort_month, month_index"
    ).fetchall()
    cohorts = sorted({(r[0], r[1]) for r in rows})
    max_index = max(r[2] for r in rows)
    grid = [[None] * max_index for _ in cohorts]
    row_of = {c[0]: i for i, c in enumerate(cohorts)}
    for cohort_month, _, index, rate in rows:
        grid[row_of[cohort_month]][index - 1] = float(rate)
    vmax = max(v for row in grid for v in row if v is not None)

    fig = _figure(9, 0.42 * len(cohorts) + 1.9)
    ax = fig.add_axes([0.14, 0.08, 0.72, 0.72])
    _style(ax, grid_axis="")
    ax.spines["bottom"].set_visible(False)
    cmap = LinearSegmentedColormap.from_list("blue", BLUE_RAMP).with_extremes(bad=SURFACE)
    masked = [[float("nan") if v is None else v for v in row] for row in grid]
    image = ax.imshow(masked, cmap=cmap, vmin=0, vmax=vmax, aspect="auto")
    for i, row in enumerate(grid):
        for j, value in enumerate(row):
            if value is None:
                continue
            ink = "#ffffff" if value > 0.55 * vmax else INK
            ax.text(j, i, f"{value:.0%}", ha="center", va="center", fontsize=7.5, color=ink)
    ax.set_xticks(range(max_index), [str(i + 1) for i in range(max_index)])
    ax.set_yticks(range(len(cohorts)), [f"{m.strftime('%b %Y')}  ·  {n:,}" for m, n in cohorts])
    # The 2px surface gap between cells, drawn as a minor grid in the surface colour.
    ax.set_xticks([i - 0.5 for i in range(1, max_index)], minor=True)
    ax.set_yticks([i - 0.5 for i in range(1, len(cohorts))], minor=True)
    ax.grid(which="minor", color=SURFACE, linewidth=2)
    ax.tick_params(which="minor", length=0)
    ax.set_xlabel("Months after first purchase", fontsize=8.5, color=INK_MUTED)
    ax.tick_params(colors=INK_SECONDARY)
    bar = fig.colorbar(image, ax=ax, fraction=0.025, pad=0.02)
    bar.outline.set_visible(False)
    bar.ax.tick_params(colors=INK_MUTED, labelsize=7.5, length=0)
    bar.ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0%}"))
    _title(
        fig,
        "Share of each monthly cohort that bought again",
        "Rows: month of first purchase and cohort size. Whole calendar months: 31 Jan → 1 Feb is month 1.\n"
        "The last cell of every row is December 2011, which has nine days of data.",
    )
    return _save(fig, path)


def chart_segments(con, path: Path) -> Path:
    rows = con.execute(
        "SELECT segment, count(*) / sum(count(*)) OVER (), sum(monetary) / sum(sum(monetary)) OVER () "
        "FROM customer_rfm GROUP BY segment ORDER BY 3 DESC"
    ).fetchall()
    labels = [r[0].replace("_", " ") for r in rows]
    customers = [float(r[1]) for r in rows]
    revenue = [float(r[2]) for r in rows]

    fig = _figure(9, 4.2)
    ax = fig.add_axes([0.14, 0.1, 0.8, 0.68])
    _style(ax, grid_axis="x")
    y = list(range(len(labels)))
    thickness = 0.3
    # The axis is inverted below, so the smaller y of each pair is the upper bar:
    # customers first, matching the legend order.
    upper = [v - thickness / 2 - 0.02 for v in y]
    lower = [v + thickness / 2 + 0.02 for v in y]
    ax.barh(upper, customers, height=thickness, color=BLUE, label="Share of customers")
    ax.barh(lower, revenue, height=thickness, color=ORANGE, label="Share of revenue")
    for yu, yl, c, r in zip(upper, lower, customers, revenue):
        ax.text(c + 0.008, yu, f"{c:.0%}", va="center", fontsize=8, color=INK_SECONDARY)
        ax.text(r + 0.008, yl, f"{r:.0%}", va="center", fontsize=8, color=INK_SECONDARY)
    ax.invert_yaxis()
    ax.set_yticks(y, labels)
    ax.tick_params(axis="y", colors=INK_SECONDARY, labelsize=9)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0%}"))
    ax.set_xlim(0, max(max(customers), max(revenue)) * 1.15)
    ax.legend(frameon=False, loc="lower right", fontsize=8.5, labelcolor=INK_SECONDARY, handlelength=1, handleheight=1)
    top = rows[0]
    _title(
        fig,
        "Who the revenue comes from",
        f"{top[0].capitalize()} are {float(top[1]):.0%} of customers and {float(top[2]):.0%} of revenue.",
    )
    return _save(fig, path)


def render_charts(layout: Layout, out_dir: Path) -> list[Path]:
    con = connect(layout.serving_db)
    try:
        return [
            chart_monthly_revenue(con, out_dir / "monthly_revenue.png"),
            chart_cohort_retention(con, out_dir / "cohort_retention.png"),
            chart_segments(con, out_dir / "segments.png"),
        ]
    finally:
        con.close()
