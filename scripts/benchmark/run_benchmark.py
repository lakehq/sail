#!/usr/bin/env python
"""TPC-H / TPC-DS benchmark runner for Sail development.

Subcommands
-----------
* ``run``  - generate (or reuse) the dataset with DuckDB, then run a chosen
  suite of queries against an in-process (or remote) Sail Spark Connect
  server and persist the per-query timings as JSON.
* ``plot`` - compare two (or more) result files and emit a bar chart.  A
  ``--ylim`` cap keeps a single slow query from squashing the rest of the
  chart; bars that exceed the cap are clipped and annotated with the real
  value above the cap line.
* ``list`` - list available queries for a suite.

Layout (everything lives in the gitignored ``opt/`` tree):

* Generated parquet data       : ``opt/benchmark-data/<suite>_sf<sf>/``
* JSON benchmark result files  : ``opt/benchmark-results/``
* Generated comparison plots   : ``opt/benchmark-results/plots/``

The query texts come from ``python/pysail/data/{tpch,tpcds}/queries`` so the
script reuses exactly the SQL we already exercise from pytest.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import statistics
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pyspark.sql import SparkSession


REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = REPO_ROOT / "opt" / "benchmark-data"
RESULTS_ROOT = REPO_ROOT / "opt" / "benchmark-results"
PLOTS_ROOT = RESULTS_ROOT / "plots"
QUERY_ROOTS = {
    "tpch": REPO_ROOT / "python" / "pysail" / "data" / "tpch" / "queries",
    "tpcds": REPO_ROOT / "python" / "pysail" / "data" / "tpcds" / "queries",
}
QUERY_COUNTS = {"tpch": 22, "tpcds": 99}


# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------


def _format_sf(sf: float) -> str:
    if float(sf).is_integer():
        return str(int(sf))
    return str(sf).replace(".", "_")


def dataset_dir(suite: str, sf: float) -> Path:
    return DATA_ROOT / f"{suite}_sf{_format_sf(sf)}"


def generate_dataset(suite: str, sf: float, *, force: bool = False) -> Path:
    """Generate the suite's dataset with DuckDB if not already cached.

    Each table is exported as a single parquet file. A ``.complete`` marker
    file records the generated table list and SF.
    """
    import duckdb  # local import keeps ``--help`` light

    out_dir = dataset_dir(suite, sf)
    marker = out_dir / ".complete"
    if marker.exists() and not force:
        meta = json.loads(marker.read_text())
        print(f"[data] reusing {suite} sf={sf} at {out_dir} ({len(meta['tables'])} tables)")
        return out_dir

    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[data] generating {suite} sf={sf} via DuckDB into {out_dir}")
    conn = duckdb.connect()
    try:
        # DuckDB ships these as built-in extensions and auto-loads them
        # when the call is made.
        if suite == "tpch":
            conn.sql(f"CALL dbgen(sf = {sf})")
        elif suite == "tpcds":
            conn.sql(f"CALL dsdgen(sf = {sf})")
        else:
            msg = f"unknown suite: {suite}"
            raise ValueError(msg)

        tables = [row[0] for row in conn.sql("SHOW TABLES").fetchall()]
        for table in tables:
            target = out_dir / f"{table}.parquet"
            tmp = target.with_suffix(".parquet.tmp")
            if tmp.exists():
                tmp.unlink()
            # Quoting the identifier defends against any future reserved-word table name.
            conn.sql(f"COPY \"{table}\" TO '{tmp.as_posix()}' (FORMAT PARQUET)")
            tmp.replace(target)
            print(f"[data]  wrote {target.name}")
    finally:
        conn.close()

    marker.write_text(json.dumps({"suite": suite, "sf": sf, "tables": tables}, indent=2))
    return out_dir


# ---------------------------------------------------------------------------
# Spark / Sail setup
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def sail_session(remote: str | None) -> Iterator[SparkSession]:
    """Yield a Spark session backed by Sail (in-process unless ``remote`` is set)."""
    from pyspark.sql import SparkSession

    server = None
    if remote:
        url = remote
    else:
        from pysail.spark import SparkConnectServer

        server = SparkConnectServer("127.0.0.1", 0)
        server.start(background=True)
        _, port = server.listening_address
        url = f"sc://localhost:{port}"

    spark = SparkSession.builder.remote(url).appName("sail-benchmark").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        yield spark
    finally:
        with contextlib.suppress(Exception):
            spark.stop()
        if server is not None:
            with contextlib.suppress(Exception):
                server.stop()


def load_tables(spark: SparkSession, data_dir: Path) -> list[str]:
    tables: list[str] = []
    for parquet in sorted(data_dir.glob("*.parquet")):
        name = parquet.stem
        spark.read.parquet(str(parquet)).createOrReplaceTempView(name)
        tables.append(name)
    if not tables:
        msg = f"no parquet files found under {data_dir}"
        raise RuntimeError(msg)
    return tables


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------


def parse_query_arg(arg: str | None, suite: str) -> list[str]:
    total = QUERY_COUNTS[suite]
    if not arg or arg == "all":
        return [f"q{i}" for i in range(1, total + 1)]
    out: list[str] = []
    for token in arg.split(","):
        token = token.strip()
        if not token:
            continue
        if "-" in token:
            lo, hi = token.split("-", 1)
            for i in range(int(lo), int(hi) + 1):
                out.append(f"q{i}")
        else:
            out.append(token if token.startswith("q") else f"q{token}")
    # Validate.
    for q in out:
        path = QUERY_ROOTS[suite] / f"{q}.sql"
        if not path.is_file():
            msg = f"query file not found: {path}"
            raise FileNotFoundError(msg)
    return out


def read_query_statements(suite: str, query: str) -> list[str]:
    text = (QUERY_ROOTS[suite] / f"{query}.sql").read_text()
    out: list[str] = []
    for raw in text.split(";"):
        sql = raw.strip().replace("create view", "create temp view")
        if sql:
            out.append(sql)
    return out


@dataclass
class QueryResult:
    runs: list[float] = field(default_factory=list)
    min_s: float | None = None
    median_s: float | None = None
    mean_s: float | None = None
    rows: int | None = None
    error: str | None = None


def run_query(spark: SparkSession, suite: str, query: str) -> tuple[float, int]:
    """Execute one query and return (elapsed_seconds, row_count_of_final_select)."""
    statements = read_query_statements(suite, query)
    rows = 0
    start = time.perf_counter()
    for sql in statements:
        df = spark.sql(sql)
        # ``count`` materializes the whole result without pulling it back to
        # the driver; this gives us a more honest engine timing for large
        # final result sets while still triggering full execution.
        rows = df.count()
    elapsed = time.perf_counter() - start
    return elapsed, rows


def cmd_run(args: argparse.Namespace) -> int:
    suite = args.suite
    sf = args.sf
    queries = parse_query_arg(args.queries, suite)

    data_dir = generate_dataset(suite, sf, force=args.regenerate_data)

    results: dict[str, QueryResult] = {q: QueryResult() for q in queries}

    with sail_session(args.url) as spark:
        tables = load_tables(spark, data_dir)
        print(f"[run] loaded {len(tables)} tables: {', '.join(tables)}")

        # Optional warm-up so the first measured run is not penalized by JIT
        # warm-up, classloading, etc.
        if args.warmup and queries:
            warm = queries[0]
            print(f"[run] warm-up with {warm}")
            with contextlib.suppress(Exception):
                run_query(spark, suite, warm)

        for q in queries:
            qr = results[q]
            for r in range(args.runs):
                try:
                    elapsed, rows = run_query(spark, suite, q)
                except Exception as exc:  # noqa: BLE001
                    qr.error = f"{type(exc).__name__}: {exc}"
                    print(f"[run] {q} run {r + 1}/{args.runs} FAILED: {qr.error}")
                    break
                qr.runs.append(elapsed)
                qr.rows = rows
                print(f"[run] {q} run {r + 1}/{args.runs}: {elapsed:.3f}s ({rows} rows)")
            if qr.runs:
                qr.min_s = min(qr.runs)
                qr.mean_s = statistics.fmean(qr.runs)
                qr.median_s = statistics.median(qr.runs)

    label = args.label or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    payload = {
        "label": label,
        "suite": suite,
        "sf": sf,
        "runs": args.runs,
        "url": args.url,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data_dir": str(data_dir),
        "results": {q: asdict(r) for q, r in results.items()},
    }

    if args.output:
        out_path = Path(args.output)
    else:
        RESULTS_ROOT.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        safe_label = label.replace("/", "_").replace(" ", "_")
        out_path = RESULTS_ROOT / f"{suite}_sf{_format_sf(sf)}_{safe_label}_{stamp}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2))
    print(f"[run] wrote results to {out_path}")
    return 0


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------


def _query_sort_key(q: str) -> tuple[int, str]:
    if q.startswith("q") and q[1:].isdigit():
        return (int(q[1:]), q)
    return (10**9, q)


def _summary_value(qr: dict, stat: str) -> float | None:
    key = {"min": "min_s", "median": "median_s", "mean": "mean_s"}[stat]
    return qr.get(key)


def cmd_plot(args: argparse.Namespace) -> int:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib is required for `plot`. Install with: pip install matplotlib", file=sys.stderr)
        return 2

    files = [Path(p) for p in args.files]
    if len(files) < 1:
        print("plot requires at least one --file", file=sys.stderr)
        return 2

    datasets: list[dict] = []
    for f in files:
        d = json.loads(f.read_text())
        d["_path"] = str(f)
        datasets.append(d)

    suites = {d["suite"] for d in datasets}
    if len(suites) > 1:
        print(f"refusing to plot mixed suites: {suites}", file=sys.stderr)
        return 2
    suite = suites.pop()

    # Union of queries across all datasets, in natural q-order.
    all_queries = sorted(
        {q for d in datasets for q in d["results"]},
        key=_query_sort_key,
    )

    n_series = len(datasets)
    n_q = len(all_queries)
    fig_w = max(10.0, 0.32 * n_q + 2.0)
    fig, ax = plt.subplots(figsize=(fig_w, 6.0))

    bar_width = 0.8 / max(n_series, 1)
    x_positions = list(range(n_q))

    cap = args.ylim
    colors = plt.cm.tab10.colors  # type: ignore[attr-defined]

    for i, d in enumerate(datasets):
        label = args.labels[i] if args.labels and i < len(args.labels) else d.get("label") or Path(d["_path"]).stem
        values: list[float] = []
        clipped: list[bool] = []
        errors: list[bool] = []
        for q in all_queries:
            qr = d["results"].get(q)
            if not qr:
                values.append(0.0)
                clipped.append(False)
                errors.append(False)
                continue
            v = _summary_value(qr, args.stat)
            if v is None:
                values.append(0.0)
                clipped.append(False)
                errors.append(qr.get("error") is not None)
                continue
            if cap is not None and v > cap:
                values.append(cap)
                clipped.append(True)
            else:
                values.append(v)
                clipped.append(False)
            errors.append(False)

        offsets = [x + (i - (n_series - 1) / 2) * bar_width for x in x_positions]
        bars = ax.bar(offsets, values, width=bar_width, label=label, color=colors[i % len(colors)])

        # Annotate clipped bars with the real value, and mark errored queries.
        for idx, bar in enumerate(bars):
            qr = d["results"].get(all_queries[idx]) or {}
            real = _summary_value(qr, args.stat)
            if errors[idx]:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    (cap or max(values + [0.001])) * 0.02,
                    "ERR",
                    ha="center",
                    va="bottom",
                    fontsize=7,
                    color="red",
                    rotation=90,
                )
            elif clipped[idx] and real is not None:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    cap,
                    f"{real:.1f}s",
                    ha="center",
                    va="bottom",
                    fontsize=7,
                    color=colors[i % len(colors)],
                )

    if cap is not None:
        ax.set_ylim(0, cap * 1.08)
        ax.axhline(cap, color="gray", linestyle="--", linewidth=0.8, alpha=0.7)

    ax.set_xticks(x_positions)
    ax.set_xticklabels(all_queries, rotation=60, ha="right", fontsize=8)
    ax.set_ylabel(f"{args.stat} runtime (s)")
    title = f"{suite.upper()} benchmark"
    if datasets and "sf" in datasets[0]:
        title += f"  (sf={datasets[0]['sf']})"
    ax.set_title(title)
    ax.grid(axis="y", linestyle=":", alpha=0.5)
    ax.legend(loc="upper right", fontsize=8)
    fig.tight_layout()

    if args.output:
        out = Path(args.output)
    else:
        PLOTS_ROOT.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out = PLOTS_ROOT / f"{suite}_compare_{stamp}.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=140)
    print(f"[plot] wrote {out}")

    # Print a small comparison table when there are >= 2 datasets.
    if len(datasets) >= 2:
        base = datasets[0]
        print(f"\n  query  {'  '.join(d.get('label') or Path(d['_path']).stem for d in datasets)}  vs-base")
        for q in all_queries:
            base_v = _summary_value(base["results"].get(q) or {}, args.stat)
            row = [q.ljust(6)]
            for d in datasets:
                v = _summary_value(d["results"].get(q) or {}, args.stat)
                row.append(f"{v:8.3f}" if v is not None else "    n/a ")
            other = _summary_value(datasets[-1]["results"].get(q) or {}, args.stat)
            if base_v and other:
                pct = (other - base_v) / base_v * 100.0
                row.append(f"{pct:+6.1f}%")
            else:
                row.append("   n/a")
            print("  " + "  ".join(row))
    return 0


# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------


def cmd_list(args: argparse.Namespace) -> int:
    qs = parse_query_arg("all", args.suite)
    print(" ".join(qs))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="run a benchmark suite")
    p_run.add_argument("--suite", choices=["tpch", "tpcds"], required=True)
    p_run.add_argument("--sf", type=float, default=1.0, help="scale factor (default: 1.0)")
    p_run.add_argument(
        "--queries",
        type=str,
        default="all",
        help="comma-separated list / ranges, e.g. 'q1,q3,q5-q8' or 'all' (default: all)",
    )
    p_run.add_argument("--runs", type=int, default=1, help="number of repeated runs per query")
    p_run.add_argument("--warmup", action="store_true", help="run one warm-up query before timing")
    p_run.add_argument("--label", type=str, default=None, help="label embedded in the result file")
    p_run.add_argument(
        "--url",
        type=str,
        default=None,
        help="remote Spark Connect URL (sc://...). Default: start an in-process Sail server.",
    )
    p_run.add_argument("--output", type=str, default=None, help="result JSON path (auto-generated if omitted)")
    p_run.add_argument("--regenerate-data", action="store_true", help="force re-running DuckDB data generation")
    p_run.set_defaults(func=cmd_run)

    p_plot = sub.add_parser("plot", help="plot/compare benchmark result files")
    p_plot.add_argument("--file", dest="files", action="append", required=True, help="result JSON file (repeatable)")
    p_plot.add_argument("--label", dest="labels", action="append", help="optional override label per --file")
    p_plot.add_argument(
        "--ylim",
        type=float,
        default=None,
        help="cap the y-axis at this many seconds; bars exceeding the cap are clipped and annotated",
    )
    p_plot.add_argument(
        "--stat",
        choices=["min", "median", "mean"],
        default="median",
        help="which statistic to plot (default: median)",
    )
    p_plot.add_argument("--output", type=str, default=None, help="output PNG path")
    p_plot.set_defaults(func=cmd_plot)

    p_list = sub.add_parser("list", help="list available queries for a suite")
    p_list.add_argument("--suite", choices=["tpch", "tpcds"], required=True)
    p_list.set_defaults(func=cmd_list)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
