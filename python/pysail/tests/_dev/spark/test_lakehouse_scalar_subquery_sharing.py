"""Repeated aggregate scalar subqueries over Delta and Iceberg tables.

Each table has two versions (snapshots), and is partitioned by `p` or not. Count, min and max
are answered from table metadata when no filter is left over ("metadata-complete"), or by
scanning the data files that the file statistics cannot decide ("residual").
"""

import datetime
import json
import re
import uuid
from pathlib import Path

import pytest

from pysail.testing.spark.utils.sql import escape_sql_string_literal

pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Sail physical plans")

SCALAR_SUBQUERY_SLOTS = re.compile(r"ScalarSubqueryExec: subqueries=(\d+)")
DATA_SCANS = re.compile(r"\bDataSourceExec:")

# (p, d, w) rows of version 1; version 2 appends one row.
VERSION_1 = [
    (1, datetime.date(2000, 1, 1), 1),
    (1, datetime.date(2000, 1, 2), 3),
    (2, datetime.date(2000, 1, 3), 2),
    (2, datetime.date(2000, 1, 4), 4),
]
VERSION_2 = [*VERSION_1, (1, datetime.date(2000, 1, 5), 5)]
MIN_W = 2
FILTERS = {"": lambda r: True, "WHERE p = 1": lambda r: r[0] == 1, f"WHERE w > {MIN_W}": lambda r: r[2] > MIN_W}  # noqa: ARG005


def plan_counts(df):
    plan = df._explain_string()  # noqa: SLF001
    return sorted(int(n) for n in SCALAR_SUBQUERY_SLOTS.findall(plan)), len(DATA_SCANS.findall(plan))


def stats(rows, where):
    rows = [r for r in rows if FILTERS[where](r)]
    return {"n": len(rows), "lo": min(r[2] for r in rows), "hi": max(r[2] for r in rows)}


def metadata_complete(where, partitioned):
    return where == "" or (where == "WHERE p = 1" and partitioned)


def values_sql(rows):
    return ", ".join(f"({p}, DATE'{d}', {w})" for p, d, w in rows)


def iceberg_snapshot_id(location):
    """The current snapshot ID of an Iceberg table, from its latest metadata file."""
    files = sorted((Path(location) / "metadata").glob("*.metadata.json"), key=lambda f: f.stat().st_mtime_ns)
    return json.loads(files[-1].read_text())["current-snapshot-id"]


@pytest.fixture(
    params=[("delta", False), ("delta", True), ("iceberg", False), ("iceberg", True)],
    ids=["delta-unpartitioned", "delta-partitioned", "iceberg-unpartitioned", "iceberg-partitioned"],
)
def lake_table(spark, tmp_path, request):
    """Returns (table, partitioned, versions); `versions` maps "current", "v1" and "v2" to the
    time travel clause that reads them, and VERSION_1 / VERSION_2 are the rows of v1 / v2."""
    fmt, partitioned = request.param
    name = f"{fmt}_sharing_{uuid.uuid4().hex[:8]}"
    location = tmp_path / name
    partition = "PARTITIONED BY (p)" if partitioned else ""
    sql_location = escape_sql_string_literal(location.as_uri() if fmt == "iceberg" else str(location))
    spark.sql(f"CREATE TABLE {name} (p INT, d DATE, w INT) USING {fmt} {partition} LOCATION '{sql_location}'")
    spark.sql(f"INSERT INTO {name} VALUES {values_sql(VERSION_1)}")  # noqa: S608
    v1 = "VERSION AS OF 1" if fmt == "delta" else f"VERSION AS OF {iceberg_snapshot_id(location)}"
    spark.sql(f"INSERT INTO {name} VALUES {values_sql(VERSION_2[len(VERSION_1) :])}")  # noqa: S608
    v2 = "VERSION AS OF 2" if fmt == "delta" else f"VERSION AS OF {iceberg_snapshot_id(location)}"
    yield name, partitioned, {"current": "", "v1": v1, "v2": v2}
    spark.sql(f"DROP TABLE IF EXISTS {name}")


ROWS = {"current": VERSION_2, "v1": VERSION_1, "v2": VERSION_2}


@pytest.mark.parametrize("where", list(FILTERS))
@pytest.mark.parametrize("version", list(ROWS))
def test_repeated_aggregates_through_a_cte_share_one_result(spark, lake_table, version, where):
    name, partitioned, versions = lake_table
    df = spark.sql(
        f"WITH c AS (SELECT count(*) AS n, min(w) AS lo, max(w) AS hi FROM {name} {versions[version]} {where}) "  # noqa: S608
        "SELECT (SELECT n FROM c) AS n1, (SELECT n FROM c) AS n2, (SELECT hi FROM c) AS hi1, "
        "(SELECT hi FROM c) AS hi2, (SELECT lo FROM c) AS lo"
    )
    s = stats(ROWS[version], where)
    assert tuple(df.collect()[0]) == (s["n"], s["n"], s["hi"], s["hi"], s["lo"])
    # One result for each distinct body, and one data scan for each when a filter is left over.
    assert plan_counts(df) == ([3], 0 if metadata_complete(where, partitioned) else 3)


@pytest.mark.parametrize("where", list(FILTERS))
@pytest.mark.parametrize("version", list(ROWS))
def test_repeated_aggregates_over_a_table_share_one_result(spark, lake_table, version, where):
    name, partitioned, versions = lake_table
    t = f"{name} {versions[version]} {where}"
    df = spark.sql(
        f"SELECT (SELECT max(w) FROM {t}) AS hi1, (SELECT count(*) FROM {t}) AS n1, "  # noqa: S608
        f"(SELECT max(w) FROM {t}) AS hi2, (SELECT count(*) FROM {t}) AS n2, (SELECT min(w) FROM {t}) AS lo"
    )
    s = stats(ROWS[version], where)
    assert tuple(df.collect()[0]) == (s["hi"], s["n"], s["hi"], s["n"], s["lo"])
    assert plan_counts(df) == ([3], 0 if metadata_complete(where, partitioned) else 3)


@pytest.mark.parametrize("where", list(FILTERS))
def test_aggregates_over_different_versions_are_not_shared(spark, lake_table, where):
    name, _, versions = lake_table
    v1 = versions["v1"]
    df = spark.sql(
        f"SELECT (SELECT max(w) FROM {name} {v1} {where}) AS a, (SELECT max(w) FROM {name} {where}) AS b, "  # noqa: S608
        f"(SELECT max(w) FROM {name} {v1} {where}) AS c"
    )
    old, new = stats(VERSION_1, where)["hi"], stats(VERSION_2, where)["hi"]
    assert old != new
    assert tuple(df.collect()[0]) == (old, new, old)
    assert plan_counts(df)[0] == [2]


@pytest.mark.parametrize("where", list(FILTERS))
@pytest.mark.spark(version=">=4", reason="Uses DataFrame.scalar")
def test_repeated_dataframe_aggregates_share_one_result(spark, lake_table, where):
    from pyspark.sql import functions as F  # noqa: N812

    name, partitioned, _ = lake_table
    condition = where.removeprefix("WHERE ") or "true"

    def table():
        return spark.table(name).where(condition)

    df = spark.range(1).select(
        table().select(F.max("w")).scalar().alias("hi1"),
        table().select(F.max("w")).scalar().alias("hi2"),
        table().select(F.count("*")).scalar().alias("n1"),
        table().select(F.count("*")).scalar().alias("n2"),
    )
    s = stats(VERSION_2, where)
    assert tuple(df.collect()[0]) == (s["hi"], s["hi"], s["n"], s["n"])
    assert plan_counts(df) == ([2], 0 if metadata_complete(where, partitioned) else 2)


def test_differently_named_references_share_one_result(spark, lake_table):
    name, _, _ = lake_table
    catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
    df = spark.sql(
        f"SELECT (SELECT max(w) FROM {name}) AS a, (SELECT max(T.w) FROM {name} AS T) AS b, "  # noqa: S608
        f"(SELECT max(x.w) FROM {name} x) AS c, (SELECT max(w) FROM default.{name}) AS d, "
        f"(SELECT max(w) FROM {catalog}.default.{name}) AS e"
    )
    assert tuple(df.collect()[0]) == (5, 5, 5, 5, 5)
    assert plan_counts(df)[0] == [1]


def test_repeated_aggregates_next_to_input_file_name_share_one_result(spark, lake_table):
    name, _, _ = lake_table
    df = spark.sql(
        f"SELECT input_file_name() <> '' AS f, (SELECT max(w) FROM {name}) AS a, (SELECT max(w) FROM {name}) AS b "  # noqa: S608
        f"FROM {name}"
    )
    assert [tuple(row) for row in df.collect()] == [(True, 5, 5)] * len(VERSION_2)
    assert plan_counts(df)[0] == [1]
