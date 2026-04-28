#!/usr/bin/env python
# ruff: noqa: S608, T201
"""TPC-H / TPC-DS benchmark runner for Sail development.

Subcommands
-----------
* ``run``  - generate (or reuse) the dataset with DuckDB, then run a chosen
    suite of queries against an in-process (or remote) Sail Spark Connect
    server and persist the per-query timings as JSON. The generated data uses
    one folder per table with parquet part files, and row-group sizing follows
    the sampling approach used by LakeBench's DuckDB TPC-DS generator.
* ``plot`` - compare two (or more) result labels or files and emit a bar
    chart.  A ``--ylim`` cap keeps a single slow query from squashing the rest
    of the chart; bars that exceed the cap are clipped and annotated with the
    real value above the cap line.
* ``list`` - list available queries for a suite.
* ``results`` - list benchmark result files and their labels.

Layout (everything lives in the gitignored ``opt/`` tree):

* Generated parquet data       : ``opt/benchmark-data/<suite>_sf<sf>_rg<mb>mb_<codec>/``
* JSON benchmark result files  : ``opt/benchmark-results/``
* Generated comparison plots   : ``opt/benchmark-results/plots/``

The query texts come from ``python/pysail/data/{tpch,tpcds}/queries`` so the
script reuses exactly the SQL we already exercise from pytest.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import re
import shutil
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
DATASET_LAYOUT_VERSION = 2
DEFAULT_TARGET_ROW_GROUP_SIZE_MB = 128
DEFAULT_COMPRESSION = "zstd"
SAMPLE_ROWS = 1_000_000
SF_TOLERANCE = 1e-12
MIN_COMPARISON_DATASETS = 2
QUERY_ROOTS = {
    "tpch": REPO_ROOT / "python" / "pysail" / "data" / "tpch" / "queries",
    "tpcds": REPO_ROOT / "python" / "pysail" / "data" / "tpcds" / "queries",
}
QUERY_COUNTS = {"tpch": 22, "tpcds": 99}
TABLE_NAMES = {
    "tpch": ("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"),
    "tpcds": (
        "call_center",
        "catalog_page",
        "catalog_returns",
        "catalog_sales",
        "customer",
        "customer_address",
        "customer_demographics",
        "date_dim",
        "household_demographics",
        "income_band",
        "inventory",
        "item",
        "promotion",
        "reason",
        "ship_mode",
        "store",
        "store_returns",
        "store_sales",
        "time_dim",
        "warehouse",
        "web_page",
        "web_returns",
        "web_sales",
        "web_site",
    ),
}


# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------


def _format_sf(sf: float) -> str:
    if float(sf).is_integer():
        return str(int(sf))
    return str(sf).replace(".", "_")


def _safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_") or "value"


def _normalize_compression(compression: str) -> str:
    normalized = compression.strip().lower()
    if not re.fullmatch(r"[A-Za-z0-9_]+", normalized):
        msg = f"unsupported parquet compression name: {compression!r}"
        raise ValueError(msg)
    return normalized


def _sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def dataset_dir(
    suite: str,
    sf: float,
    *,
    target_row_group_size_mb: int = DEFAULT_TARGET_ROW_GROUP_SIZE_MB,
    compression: str = DEFAULT_COMPRESSION,
) -> Path:
    codec = _safe_name(_normalize_compression(compression))
    return DATA_ROOT / f"{suite}_sf{_format_sf(sf)}_rg{target_row_group_size_mb}mb_{codec}"


def _parquet_table_paths(data_dir: Path) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    if not data_dir.exists():
        return paths
    for child in sorted(data_dir.iterdir()):
        if child.name.startswith("."):
            continue
        if child.is_file() and child.suffix == ".parquet":
            paths[child.stem] = child
        elif child.is_dir() and any(child.glob("*.parquet")):
            paths[child.name] = child
    return paths


def _expected_dataset_metadata(
    suite: str,
    sf: float,
    target_row_group_size_mb: int,
    compression: str,
) -> dict[str, str | float | int]:
    return {
        "layout_version": DATASET_LAYOUT_VERSION,
        "suite": suite,
        "sf": sf,
        "generator": "duckdb",
        "storage": "table-directories",
        "target_row_group_size_mb": target_row_group_size_mb,
        "compression": _normalize_compression(compression),
    }


def _dataset_is_reusable(data_dir: Path, expected: dict[str, str | float | int]) -> tuple[bool, dict | None]:
    marker = data_dir / ".complete"
    if not marker.exists():
        return False, None
    try:
        meta = json.loads(marker.read_text())
    except json.JSONDecodeError:
        return False, None
    for key, value in expected.items():
        if meta.get(key) != value:
            return False, meta
    tables = meta.get("tables") or []
    table_paths = _parquet_table_paths(data_dir)
    if not tables or any(table not in table_paths for table in tables):
        return False, meta
    return True, meta


def _estimate_target_rows(
    conn,
    table: str,
    out_dir: Path,
    target_row_group_size_mb: int,
    compression: str,
) -> int:
    import pyarrow.parquet as pq

    sample_file = out_dir / f".{table}_sample.parquet"
    if sample_file.exists():
        sample_file.unlink()
    try:
        conn.sql(
            f"COPY (SELECT * FROM {_quote_identifier(table)} LIMIT {SAMPLE_ROWS}) "
            f"TO {_sql_string(sample_file)} "
            f"(FORMAT PARQUET, COMPRESSION {_sql_string(compression)})"
        )
        with pq.ParquetFile(sample_file) as parquet_file:
            total_rows = 0
            total_bytes = 0
            for i in range(parquet_file.metadata.num_row_groups):
                row_group = parquet_file.metadata.row_group(i)
                total_rows += row_group.num_rows
                total_bytes += row_group.total_byte_size
        if total_rows <= 0 or total_bytes <= 0:
            return 1
        avg_row_size = total_bytes / total_rows
        target_size_bytes = target_row_group_size_mb * 1024 * 1024
        return max(int(target_size_bytes / avg_row_size), 1)
    finally:
        if sample_file.exists():
            sample_file.unlink()


def _write_table(conn, table: str, target_dir: Path, target_rows: int, compression: str) -> None:
    if target_dir.exists():
        shutil.rmtree(target_dir)
    conn.sql(
        f"COPY {_quote_identifier(table)} TO {_sql_string(target_dir)} "
        f"(FORMAT PARQUET, COMPRESSION {_sql_string(compression)}, "
        f"ROW_GROUP_SIZE {target_rows}, PER_THREAD_OUTPUT, OVERWRITE)"
    )


def generate_dataset(
    suite: str,
    sf: float,
    *,
    force: bool = False,
    target_row_group_size_mb: int = DEFAULT_TARGET_ROW_GROUP_SIZE_MB,
    compression: str = DEFAULT_COMPRESSION,
    duckdb_threads: int | None = None,
) -> Path:
    """Generate the suite's dataset with DuckDB if not already cached.

    Each table is exported to a directory containing parquet part files. The
    ``ROW_GROUP_SIZE`` passed to DuckDB is estimated from a sample parquet file,
    following LakeBench's DuckDB TPC generator approach.
    """
    import duckdb  # local import keeps ``--help`` light

    if target_row_group_size_mb <= 0:
        msg = "--target-row-group-size-mb must be positive"
        raise ValueError(msg)
    if duckdb_threads is not None and duckdb_threads <= 0:
        msg = "--duckdb-threads must be positive"
        raise ValueError(msg)

    compression = _normalize_compression(compression)
    out_dir = dataset_dir(suite, sf, target_row_group_size_mb=target_row_group_size_mb, compression=compression)
    expected = _expected_dataset_metadata(suite, sf, target_row_group_size_mb, compression)

    reusable, meta = _dataset_is_reusable(out_dir, expected)
    if reusable and not force:
        tables = (meta or {}).get("tables", [])
        print(f"[data] reusing {suite} sf={sf} at {out_dir} ({len(tables)} tables)")
        return out_dir

    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    print(
        f"[data] generating {suite} sf={sf} via DuckDB into {out_dir} "
        f"(row-group target={target_row_group_size_mb} MiB, compression={compression})"
    )
    conn = duckdb.connect()
    try:
        if duckdb_threads is not None:
            conn.sql(f"PRAGMA threads={duckdb_threads}")
        # DuckDB ships these as built-in extensions and auto-loads them
        # when the call is made.
        if suite == "tpch":
            conn.sql(f"CALL dbgen(sf = {sf})")
        elif suite == "tpcds":
            conn.sql(f"CALL dsdgen(sf = {sf})")
        else:
            msg = f"unknown suite: {suite}"
            raise ValueError(msg)

        generated = {row[0] for row in conn.sql("SHOW TABLES").fetchall()}
        tables = [table for table in TABLE_NAMES[suite] if table in generated]
        missing = set(TABLE_NAMES[suite]) - generated
        if missing:
            msg = f"DuckDB did not generate expected {suite} table(s): {sorted(missing)}"
            raise RuntimeError(msg)
        for table in tables:
            target_rows = _estimate_target_rows(conn, table, out_dir, target_row_group_size_mb, compression)
            target_dir = out_dir / table
            _write_table(conn, table, target_dir, target_rows, compression)
            conn.sql(f"DROP TABLE {_quote_identifier(table)}")
            part_count = len(list(target_dir.glob("*.parquet")))
            print(f"[data]  wrote {table}/ ({part_count} parquet file(s), row_group_size={target_rows} rows)")
    finally:
        conn.close()

    marker = out_dir / ".complete"
    marker.write_text(
        json.dumps(
            {
                **expected,
                "tables": tables,
                "sample_rows": SAMPLE_ROWS,
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
            indent=2,
        )
    )
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
    table_paths = _parquet_table_paths(data_dir)
    tables = []
    for name, path in sorted(table_paths.items()):
        spark.read.parquet(str(path)).createOrReplaceTempView(name)
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
    for raw_token in arg.split(","):
        token = raw_token.strip()
        if not token:
            continue
        if "-" in token:
            lo, hi = token.split("-", 1)
            out.extend(f"q{i}" for i in range(int(lo), int(hi) + 1))
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

    data_dir = generate_dataset(
        suite,
        sf,
        force=args.regenerate_data,
        target_row_group_size_mb=args.target_row_group_size_mb,
        compression=args.compression,
        duckdb_threads=args.duckdb_threads,
    )

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
        "data_layout_version": DATASET_LAYOUT_VERSION,
        "target_row_group_size_mb": args.target_row_group_size_mb,
        "compression": _normalize_compression(args.compression),
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


@dataclass(frozen=True)
class ResultRecord:
    path: Path
    label: str
    suite: str
    sf: float | None
    timestamp: str


def _relative_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _read_result_record(path: Path) -> ResultRecord | None:
    if path.name == "index.json":
        return None
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    label = payload.get("label")
    suite = payload.get("suite")
    if not isinstance(label, str) or suite not in QUERY_COUNTS:
        return None
    sf = payload.get("sf")
    return ResultRecord(
        path=path,
        label=label,
        suite=suite,
        sf=float(sf) if sf is not None else None,
        timestamp=str(payload.get("timestamp") or ""),
    )


def _iter_result_records(results_dir: Path) -> list[ResultRecord]:
    records = []
    for path in sorted(results_dir.glob("*.json")):
        record = _read_result_record(path)
        if record is not None:
            records.append(record)
    return records


def _same_sf(left: float | None, right: float | None) -> bool:
    if left is None or right is None:
        return left is right
    return abs(left - right) < SF_TOLERANCE


def _resolve_result_ref(ref: str, *, suite: str | None, sf: float | None, results_dir: Path) -> Path:
    path = Path(ref)
    if path.is_file():
        return path
    if path.suffix == ".json" or "/" in ref or "\\" in ref:
        msg = f"result file not found: {ref}"
        raise FileNotFoundError(msg)

    matches = [record for record in _iter_result_records(results_dir) if record.label == ref]
    if suite is not None:
        matches = [record for record in matches if record.suite == suite]
    if sf is not None:
        matches = [record for record in matches if _same_sf(record.sf, sf)]

    if not matches:
        available = sorted({record.label for record in _iter_result_records(results_dir)})
        hint = f" Available labels: {', '.join(available)}" if available else " No benchmark results found."
        msg = f"could not resolve benchmark result label {ref!r}.{hint}"
        raise FileNotFoundError(msg)

    matches.sort(key=lambda record: (record.timestamp, record.path.name), reverse=True)
    chosen = matches[0]
    if len(matches) > 1:
        print(f"[plot] label {ref!r} matched {len(matches)} result files; using latest: {_relative_path(chosen.path)}")
    else:
        print(f"[plot] label {ref!r} -> {_relative_path(chosen.path)}")
    return chosen.path


def cmd_plot(args: argparse.Namespace) -> int:
    try:
        import matplotlib as mpl

        mpl.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib is required for `plot`. Install with: pip install matplotlib", file=sys.stderr)
        return 2

    refs = [*(args.refs or []), *(args.files or [])]
    if not refs:
        print("plot requires at least one label or --file", file=sys.stderr)
        return 2

    try:
        files = [
            _resolve_result_ref(ref, suite=args.suite, sf=args.sf, results_dir=Path(args.results_dir)) for ref in refs
        ]
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    if args.labels and len(args.labels) != len(files):
        print("the number of --label/--legend-label values must match the number of plotted results", file=sys.stderr)
        return 2

    datasets: list[dict] = []
    for f in files:
        d = json.loads(f.read_text())
        d["_path"] = str(f)
        datasets.append(d)
    display_labels = [
        args.labels[i] if args.labels and i < len(args.labels) else d.get("label") or Path(d["_path"]).stem
        for i, d in enumerate(datasets)
    ]

    suites = {d["suite"] for d in datasets}
    if len(suites) > 1:
        print(f"refusing to plot mixed suites: {suites}", file=sys.stderr)
        return 2
    suite = suites.pop()

    scale_factors = {d.get("sf") for d in datasets}
    if len(scale_factors) > 1:
        print(f"refusing to plot mixed scale factors: {scale_factors}", file=sys.stderr)
        return 2

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
        label = display_labels[i]
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
                    (cap or max([*values, 0.001])) * 0.02,
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
    if len(datasets) >= MIN_COMPARISON_DATASETS:
        base = datasets[0]
        print(f"\n  query  {'  '.join(display_labels)}  vs-base")
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


def cmd_results(args: argparse.Namespace) -> int:
    records = _iter_result_records(Path(args.results_dir))
    if args.suite is not None:
        records = [record for record in records if record.suite == args.suite]
    if args.sf is not None:
        records = [record for record in records if _same_sf(record.sf, args.sf)]
    if args.label is not None:
        records = [record for record in records if record.label == args.label]
    records.sort(key=lambda record: (record.suite, record.sf or -1.0, record.label, record.timestamp))

    if not records:
        print("no benchmark results found")
        return 0

    print(f"{'label':24} {'suite':6} {'sf':8} {'timestamp':35} path")
    for record in records:
        sf = "" if record.sf is None else f"{record.sf:g}"
        print(
            f"{record.label[:24]:24} {record.suite:6} {sf:8} {record.timestamp[:35]:35} {_relative_path(record.path)}"
        )
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
        "--target-row-group-size-mb",
        type=int,
        default=DEFAULT_TARGET_ROW_GROUP_SIZE_MB,
        help=f"target parquet row-group size in MiB for generated data (default: {DEFAULT_TARGET_ROW_GROUP_SIZE_MB})",
    )
    p_run.add_argument(
        "--compression",
        type=str,
        default=DEFAULT_COMPRESSION,
        help=f"DuckDB parquet compression for generated data (default: {DEFAULT_COMPRESSION})",
    )
    p_run.add_argument(
        "--duckdb-threads",
        type=int,
        default=None,
        help="DuckDB thread count used during data generation (default: DuckDB decides)",
    )
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
    p_plot.add_argument(
        "refs",
        nargs="*",
        help="result labels or JSON file paths; labels resolve to the latest matching result",
    )
    p_plot.add_argument("--file", dest="files", action="append", help="result JSON file (repeatable; legacy form)")
    p_plot.add_argument("--suite", choices=["tpch", "tpcds"], default=None, help="suite filter for label lookup")
    p_plot.add_argument("--sf", type=float, default=None, help="scale-factor filter for label lookup")
    p_plot.add_argument("--results-dir", type=str, default=str(RESULTS_ROOT), help="directory to search for labels")
    p_plot.add_argument(
        "--legend-label",
        "--label",
        dest="labels",
        action="append",
        help="optional legend override per plotted result",
    )
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

    p_results = sub.add_parser("results", help="list benchmark result labels and files")
    p_results.add_argument("--suite", choices=["tpch", "tpcds"], default=None)
    p_results.add_argument("--sf", type=float, default=None)
    p_results.add_argument("--label", type=str, default=None)
    p_results.add_argument("--results-dir", type=str, default=str(RESULTS_ROOT))
    p_results.set_defaults(func=cmd_results)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
