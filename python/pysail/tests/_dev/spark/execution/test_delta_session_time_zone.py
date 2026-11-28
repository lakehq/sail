# SPDX-License-Identifier: Apache-2.0

import json
import re
import shutil
from contextlib import contextmanager
from datetime import UTC, datetime
from urllib.parse import unquote

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.sql import escape_sql_string_literal

# Delta Lake on Spark rejects appending `TIMESTAMP_NTZ` data to a `TIMESTAMP` column,
# while Sail casts it in the session time zone.
pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Sail casts appended TIMESTAMP_NTZ data for Delta tables")


@pytest.fixture(scope="module", params=["local", "local-cluster"])
def remote(request):
    with spark_connect_server(
        envs={
            "SAIL_MODE": request.param,
            "SAIL_EXECUTION__DEFAULT_PARALLELISM": "2",
            "SAIL_CLUSTER__WORKER_MAX_COUNT": "2",
        }
    ) as server:
        yield server.remote


def ntz_rows(spark):
    # Two input partitions make a worker run the Delta writer, rather than the driver task that commits.
    return spark.range(1, 3, 1, 2).selectExpr(
        "CAST(id AS INT) AS id",
        "TIMESTAMP_NTZ '2024-01-01 00:00:00' + make_interval(0, 0, 0, 0, CAST(id AS INT), 0, 0) AS ts",
    )


def check_timestamp_ntz_append(spark, path, wall_clock, micros, partition=""):
    """Appends the TIMESTAMP_NTZ value `wall_clock` to a TIMESTAMP column and checks that it is written as
    the instant `micros`, which is Spark's cast of the value in the session time zone."""
    spark.sql(
        f"CREATE TABLE ntz_cast (id INT, ts TIMESTAMP) USING delta {partition} "
        f"LOCATION '{escape_sql_string_literal(str(path))}'"
    )
    try:
        cast = f"CAST(TIMESTAMP_NTZ '{wall_clock}' AS TIMESTAMP)"
        assert spark.sql(f"SELECT unix_micros({cast}) AS micros").collect()[0].micros == micros
        # Two input partitions make a worker run the Delta writer.
        spark.sql(
            f"SELECT * FROM VALUES (1, TIMESTAMP_NTZ '{wall_clock}'), (2, TIMESTAMP_NTZ '{wall_clock}') AS t(id, ts)"  # noqa: S608
        ).repartition(2).write.format("delta").mode("append").save(str(path))
        rows = spark.sql("SELECT id, unix_micros(ts) AS micros FROM ntz_cast ORDER BY id").collect()
        assert [(row.id, row.micros) for row in rows] == [(1, micros), (2, micros)]
        assert spark.sql(f"SELECT id FROM ntz_cast WHERE ts = {cast}").count() == 2  # noqa: S608
    finally:
        spark.sql("DROP TABLE IF EXISTS ntz_cast").collect()


# A TIMESTAMP partition value that neither Spark's cast nor Arrow's cast parses.
UNPARSED_TIMESTAMP = "2024-01-01T09:00:00 Q"


@contextmanager
def ansi_mode(spark, *, enabled):
    spark.conf.set("spark.sql.ansi.enabled", str(enabled).lower())
    try:
        yield
    finally:
        spark.conf.unset("spark.sql.ansi.enabled")


def utc_micros(hour):
    return int(datetime(2024, 1, 1, hour, tzinfo=UTC).timestamp()) * 1_000_000


def partition_values(logs):
    return {
        action["add"]["partitionValues"]["ts"]
        for log in logs
        for action in map(json.loads, log.read_text().splitlines())
        if "add" in action
    }


@pytest.mark.parametrize("session_timezone", ["America/Los_Angeles"], indirect=True)
def test_delta_append_casts_timestamp_ntz_in_session_time_zone(spark, tmp_path, session_timezone):
    _ = session_timezone
    path = tmp_path / "table"
    spark.sql(
        f"CREATE TABLE ntz_append (id INT, ts TIMESTAMP) USING delta LOCATION '{escape_sql_string_literal(str(path))}'"
    )
    try:
        ntz_rows(spark).write.format("delta").mode("append").save(str(path))
        rows = spark.sql("SELECT id, unix_micros(ts) AS micros FROM ntz_append ORDER BY id").collect()
        # The wall clock is read in the session time zone (UTC-8).
        assert [(row.id, row.micros) for row in rows] == [(1, utc_micros(9)), (2, utc_micros(10))]
    finally:
        spark.sql("DROP TABLE IF EXISTS ntz_append").collect()


@pytest.mark.parametrize("session_timezone", ["America/Los_Angeles"], indirect=True)
@pytest.mark.parametrize("partitioned", [False, True], ids=["flat", "partitioned"])
@pytest.mark.parametrize("write", ["insert", "save-as-table"])
def test_delta_insert_casts_timestamp_ntz_in_session_time_zone(spark, tmp_path, session_timezone, partitioned, write):
    _ = session_timezone
    path = tmp_path / "table"
    partition = "PARTITIONED BY (ts)" if partitioned else ""
    spark.sql(
        f"CREATE TABLE ntz_insert (id INT, ts TIMESTAMP) USING delta {partition} "
        f"LOCATION '{escape_sql_string_literal(str(path))}'"
    )
    try:
        if write == "insert":
            ntz_rows(spark).createOrReplaceTempView("ntz_rows")
            spark.sql("INSERT INTO ntz_insert SELECT * FROM ntz_rows").collect()
        else:
            ntz_rows(spark).write.format("delta").mode("append").saveAsTable("ntz_insert")
        rows = spark.sql("SELECT id, unix_micros(ts) AS micros FROM ntz_insert ORDER BY id").collect()
        # The wall clock is read in the session time zone (UTC-8), as Spark's cast reads it.
        assert [(row.id, row.micros) for row in rows] == [(1, utc_micros(9)), (2, utc_micros(10))]
        if partitioned:
            assert partition_values(path.glob("_delta_log/*.json")) == {
                "2024-01-01T09:00:00.000000Z",
                "2024-01-01T10:00:00.000000Z",
            }
    finally:
        spark.sql("DROP TABLE IF EXISTS ntz_insert").collect()
        spark.sql("DROP VIEW IF EXISTS ntz_rows").collect()


@pytest.mark.parametrize(
    ("session_timezone", "wall_clock", "micros"),
    [
        # Like Spark's cast, a wall clock that occurs twice is the first instant (PDT), and a wall
        # clock that does not occur moves forward by the length of the gap.
        ("America/Los_Angeles", "2024-11-03 01:30:00", 1730622600000000),
        ("America/Los_Angeles", "2024-03-10 02:30:00", 1710066600000000),
        ("+05:30:15", "2024-01-01 09:00:00", 1704079785000000),
    ],
    indirect=["session_timezone"],
    ids=["repeated-hour", "skipped-hour", "offset-with-seconds"],
)
@pytest.mark.parametrize("partitioned", [False, True], ids=["flat", "partitioned"])
def test_delta_append_casts_timestamp_ntz_as_spark_casts(
    spark, tmp_path, session_timezone, wall_clock, micros, partitioned
):
    _ = session_timezone
    partition = "PARTITIONED BY (ts)" if partitioned else ""
    check_timestamp_ntz_append(spark, tmp_path / "table", wall_clock, micros, partition)


@pytest.mark.parametrize(
    ("session_timezone", "wall_clock", "micros"),
    [
        ("+05:00", "-262143-01-01 00:00:00", -8334601246800000000),
        ("America/Los_Angeles", "+262142-12-31 23:59:59", 8210266905599000000),
    ],
    indirect=["session_timezone"],
    ids=["first-chrono-date", "last-chrono-date"],
)
@pytest.mark.parametrize("partitioned", [False, True], ids=["flat", "partitioned"])
def test_delta_append_casts_timestamp_ntz_at_the_ends_of_chrono_range(
    spark, tmp_path, session_timezone, wall_clock, micros, partitioned
):
    _ = session_timezone
    partition = "PARTITIONED BY (ts)" if partitioned else ""
    check_timestamp_ntz_append(spark, tmp_path / "table", wall_clock, micros, partition)


@pytest.mark.parametrize("session_timezone", ["UTC"], indirect=True)
def test_delta_timestamp_partition_values_beyond_chrono_range(spark, tmp_path, session_timezone):
    """TIMESTAMP partition values beyond the years that chrono represents are written as dates, as
    Delta Lake on Spark writes them, so they are not read back as NULL."""
    _ = session_timezone
    path = tmp_path / "table"
    micros = [None, 2**63 - 1, 8210266876800000000, 8210266876799999999, -(2**63), 0]
    rows = " UNION ALL ".join(
        f"SELECT {id_} AS id, "
        + ("CAST(NULL AS TIMESTAMP)" if value is None else f"timestamp_micros(CAST('{value}' AS BIGINT))")
        + " AS ts"
        for id_, value in enumerate(micros, 1)
    )
    spark.sql(rows).write.format("delta").partitionBy("ts").save(str(path))
    values = partition_values((path / "_delta_log").glob("*.json"))
    assert {"+294247-01-10T04:00:54.775807Z", "-290308-12-21T19:59:05.224192Z"} <= values
    table = f"delta.`{path}`"
    frame = spark.sql(f"SELECT id, unix_micros(ts) AS micros FROM {table}")  # noqa: S608
    assert sorted((row.id, row.micros) for row in frame.collect()) == list(enumerate(micros, 1))
    assert [row.id for row in spark.sql(f"SELECT id FROM {table} WHERE ts IS NULL").collect()] == [1]  # noqa: S608
    spark.sql(f"DELETE FROM {table} WHERE ts IS NULL").collect()  # noqa: S608
    assert sorted(row.id for row in spark.sql(f"SELECT id FROM {table}").collect()) == [2, 3, 4, 5, 6]  # noqa: S608


@pytest.mark.parametrize(
    ("session_timezone", "wall_clock"),
    [("America/Los_Angeles", "2024-11-03 01:30:00"), ("+05:30:15", "2024-01-01 09:00:00")],
    indirect=["session_timezone"],
    ids=["repeated-hour", "offset-with-seconds"],
)
@pytest.mark.parametrize("partitioned", [False, True], ids=["flat", "partitioned"])
def test_delta_append_casts_timestamp_to_timestamp_ntz_as_spark_casts(
    spark, tmp_path, session_timezone, wall_clock, partitioned
):
    _ = session_timezone
    path = tmp_path / "table"
    partition = "PARTITIONED BY (ts)" if partitioned else ""
    spark.sql(
        f"CREATE TABLE ltz_cast (id INT, ts TIMESTAMP_NTZ) USING delta {partition} "
        f"LOCATION '{escape_sql_string_literal(str(path))}'"
    )
    try:
        value = f"CAST('{wall_clock}' AS TIMESTAMP)"
        # Like Spark's cast, the session wall clock of the instant is written.
        spark.sql(f"SELECT * FROM VALUES (1, {value}), (2, {value}) AS t(id, ts)").repartition(  # noqa: S608
            2
        ).write.format("delta").mode("append").save(str(path))
        rows = spark.sql("SELECT id, CAST(ts AS STRING) AS ts FROM ltz_cast ORDER BY id").collect()
        assert [(row.id, row.ts) for row in rows] == [(1, wall_clock), (2, wall_clock)]
    finally:
        spark.sql("DROP TABLE IF EXISTS ltz_cast").collect()


@pytest.mark.parametrize("session_timezone", ["America/Los_Angeles"], indirect=True)
def test_delta_timestamp_partition_values_are_utc(spark, tmp_path, session_timezone):
    _ = session_timezone
    path = tmp_path / "table"
    spark.sql(
        "CREATE TABLE ts_partition (id INT, ts TIMESTAMP) USING delta PARTITIONED BY (ts) "
        f"LOCATION '{escape_sql_string_literal(str(path))}'"
    )
    try:
        ntz_rows(spark).write.format("delta").mode("append").save(str(path))
        spark.sql("INSERT INTO ts_partition VALUES (3, TIMESTAMP '2024-01-01 03:00:00')").collect()
        for frame in [spark.table("ts_partition"), spark.read.format("delta").load(str(path))]:
            rows = frame.selectExpr("id", "unix_micros(ts) AS micros").orderBy("id").collect()
            assert [(row.id, row.micros) for row in rows] == [
                (1, utc_micros(9)),
                (2, utc_micros(10)),
                (3, utc_micros(11)),
            ]
            # The literals are wall clocks in the session time zone.
            assert [row.id for row in frame.where("ts = TIMESTAMP '2024-01-01 01:00:00'").collect()] == [1]
            assert sorted(row.id for row in frame.where("ts >= TIMESTAMP '2024-01-01 02:00:00'").collect()) == [2, 3]
        # Like Delta Lake on Spark, the log has UTC partition values with an explicit zone.
        values = partition_values((path / "_delta_log").glob("*.json"))
        assert values == {f"2024-01-01T{hour:02d}:00:00.000000Z" for hour in [9, 10, 11]}
    finally:
        spark.sql("DROP TABLE IF EXISTS ts_partition").collect()


@pytest.mark.parametrize("session_timezone", ["America/Los_Angeles"], indirect=True)
@pytest.mark.parametrize("deletion_vectors", [False, True], ids=["copy-on-write", "deletion-vectors"])
@pytest.mark.parametrize(
    "condition", ["hour(ts) = 0", "dayofweek(ts) = 1", "hour(ts) = 0 OR x < 0", "hour(ts) = 0 AND x > 0", "hour(d) = 0"]
)
def test_delta_delete_removes_the_rows_that_select_returns(
    spark, tmp_path, session_timezone, deletion_vectors, condition
):
    """A DELETE removes the rows its condition selects in a query, with the TIMESTAMP values in the
    session time zone, for a table referenced by name and by path."""
    _ = session_timezone
    properties = "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')" if deletion_vectors else ""
    names = ["delete_by_name", "delete_by_path"]
    try:
        for name in names:
            spark.sql(
                f"CREATE TABLE {name} (id INT, x INT, ts TIMESTAMP, d TIMESTAMP) USING delta PARTITIONED BY (ts) "
                f"LOCATION '{escape_sql_string_literal(str(tmp_path / name))}' {properties}"
            )
            # 16:00 on a Sunday and 00:00 on a Monday in the session time zone, 00:00 and 08:00 on a Monday in UTC.
            spark.sql(
                f"INSERT INTO {name} VALUES "  # noqa: S608
                "(1, 1, TIMESTAMP '2023-12-31 16:00:00', TIMESTAMP '2023-12-31 16:00:00'), "
                "(2, 2, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00')"
            ).collect()
        for table in ["delete_by_name", f"delta.`{tmp_path / 'delete_by_path'}`"]:
            selected = [row.id for row in spark.sql(f"SELECT id FROM {table} WHERE {condition}").collect()]  # noqa: S608
            assert len(selected) == 1
            spark.sql(f"DELETE FROM {table} WHERE {condition}").collect()  # noqa: S608
            left = {row.id for row in spark.sql(f"SELECT id FROM {table}").collect()}  # noqa: S608
            assert {1, 2} - left == set(selected), table
    finally:
        for name in names:
            spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


@pytest.mark.parametrize("session_timezone", ["America/Los_Angeles"], indirect=True)
@pytest.mark.parametrize("partition", ["PARTITIONED BY (ts)", ""], ids=["partitioned", "unpartitioned"])
@pytest.mark.parametrize("condition", ["to_date(ts) = DATE '2024-01-01'", "CAST(ts AS DATE) = DATE '2024-01-01'"])
def test_delta_delete_in_another_session_time_zone_removes_the_rows_that_select_returns(
    spark, tmp_path, session_timezone, partition, condition
):
    """A DELETE in a UTC session of a table created in another session time zone removes the rows its
    condition selects in a query, for the table referenced by name and by path."""
    _ = session_timezone
    names = ["zone_by_name", "zone_by_path"]
    try:
        for name in names:
            spark.sql(
                f"CREATE TABLE {name} (id INT, ts TIMESTAMP) USING delta {partition} "
                f"LOCATION '{escape_sql_string_literal(str(tmp_path / name))}'"
            )
            # 04:00 on January 2 and 12:00 on January 1 in UTC, both on January 1 in the creation time zone.
            spark.sql(
                f"INSERT INTO {name} VALUES "  # noqa: S608
                "(1, TIMESTAMP '2024-01-02 04:00:00+00:00'), (2, TIMESTAMP '2024-01-01 12:00:00+00:00')"
            ).collect()
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        for table in ["zone_by_name", f"delta.`{tmp_path / 'zone_by_path'}`"]:
            selected = {row.id for row in spark.sql(f"SELECT id FROM {table} WHERE {condition}").collect()}  # noqa: S608
            assert selected == {2}
            spark.sql(f"DELETE FROM {table} WHERE {condition}").collect()  # noqa: S608
            left = {row.id for row in spark.sql(f"SELECT id FROM {table}").collect()}  # noqa: S608
            assert {1, 2} - left == selected, table
    finally:
        for name in names:
            spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


@pytest.mark.parametrize("session_timezone", ["America/Los_Angeles", "UTC"], indirect=True)
@pytest.mark.parametrize("partition", ["PARTITIONED BY (ts)", ""], ids=["partitioned", "unpartitioned"])
@pytest.mark.parametrize(
    "condition", ["to_date(ts) = DATE '2024-01-01'", "CAST(ts AS DATE) = DATE '2024-01-01'", "year(ts) = 2024"]
)
def test_delta_delete_by_date_in_session_time_zone_removes_the_rows_that_select_returns(
    spark, tmp_path, session_timezone, partition, condition
):
    """A DELETE in a session time zone other than UTC takes the dates of TIMESTAMP values in the session
    time zone, as a query does, for a table created in any session time zone and referenced by name or by path."""
    _ = session_timezone
    names = ["date_by_name", "date_by_path"]
    try:
        for name in names:
            spark.sql(
                f"CREATE TABLE {name} (id INT, ts TIMESTAMP) USING delta {partition} "
                f"LOCATION '{escape_sql_string_literal(str(tmp_path / name))}'"
            )
            # Both on January 1 in UTC, on December 31 and January 1 in the DELETE session time zone.
            spark.sql(
                f"INSERT INTO {name} VALUES "  # noqa: S608
                "(1, TIMESTAMP '2024-01-01 04:00:00+00:00'), (2, TIMESTAMP '2024-01-01 12:00:00+00:00')"
            ).collect()
        spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
        for table in ["date_by_name", f"delta.`{tmp_path / 'date_by_path'}`"]:
            selected = {row.id for row in spark.sql(f"SELECT id FROM {table} WHERE {condition}").collect()}  # noqa: S608
            assert selected == {2}
            spark.sql(f"DELETE FROM {table} WHERE {condition}").collect()  # noqa: S608
            left = {row.id for row in spark.sql(f"SELECT id FROM {table}").collect()}  # noqa: S608
            assert {1, 2} - left == selected, table
    finally:
        for name in names:
            spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


@pytest.mark.parametrize("session_timezone", ["America/Los_Angeles", "UTC"], indirect=True)
@pytest.mark.parametrize("partition", ["PARTITIONED BY (ts)", ""], ids=["partitioned", "unpartitioned"])
@pytest.mark.parametrize(
    "condition",
    ["to_date(ts) = DATE '2024-01-01'", "CAST(ts AS DATE) = DATE '2024-01-01'", "hour(ts) = 4", "year(ts) = 2024"],
)
@pytest.mark.parametrize("statement", ["option", "sql"])
def test_delta_replace_where_in_session_time_zone_replaces_the_rows_that_select_returns(
    spark, tmp_path, session_timezone, partition, condition, statement
):
    """A conditional overwrite of a table in a session time zone other than UTC replaces the rows its
    condition selects in a query, for a table created in any session time zone."""
    _ = session_timezone
    spark.sql(
        f"CREATE TABLE replace_where (id INT, ts TIMESTAMP) USING delta {partition} "
        f"LOCATION '{escape_sql_string_literal(str(tmp_path / 'replace_where'))}'"
    )
    try:
        # 20:00 on December 31, 04:00 on January 1 and 04:00 on January 2 in the overwrite session time zone.
        spark.sql(
            "INSERT INTO replace_where VALUES (1, TIMESTAMP '2024-01-01 04:00:00+00:00'), "
            "(2, TIMESTAMP '2024-01-01 12:00:00+00:00'), (3, TIMESTAMP '2024-01-02 12:00:00+00:00')"
        ).collect()
        spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
        selected = {row.id for row in spark.sql(f"SELECT id FROM replace_where WHERE {condition}").collect()}  # noqa: S608
        rows = "SELECT 10 AS id, TIMESTAMP '2024-01-01 04:30:00' AS ts"
        if statement == "option":
            spark.sql(rows).write.format("delta").mode("overwrite").option("replaceWhere", condition).saveAsTable(
                "replace_where"
            )
        else:
            spark.sql(f"INSERT INTO replace_where REPLACE WHERE {condition} {rows}").collect()
        left = {row.id for row in spark.sql("SELECT id FROM replace_where").collect()}
        assert left == {1, 2, 3} - selected | {10}
    finally:
        spark.sql("DROP TABLE IF EXISTS replace_where").collect()


@pytest.mark.parametrize("session_timezone", ["America/Los_Angeles"], indirect=True)
@pytest.mark.parametrize("deletion_vectors", [False, True], ids=["copy-on-write", "deletion-vectors"])
def test_delta_delete_by_name_skips_files_by_timestamp_statistics(spark, tmp_path, session_timezone, deletion_vectors):
    """A DELETE of a table referenced by name skips the files that the statistics of a TIMESTAMP data
    column rule out, as for the table referenced by path."""
    _ = session_timezone
    properties = "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')" if deletion_vectors else ""
    names = ["prune_by_name", "prune_by_path"]
    try:
        for name in names:
            spark.sql(
                f"CREATE TABLE {name} (id INT, d TIMESTAMP) USING delta "
                f"LOCATION '{escape_sql_string_literal(str(tmp_path / name))}' {properties}"
            )
            # One file per row.
            for id_ in [1, 2, 3]:
                spark.sql(f"INSERT INTO {name} VALUES ({id_}, TIMESTAMP '2024-01-0{id_} 00:00:00')").collect()  # noqa: S608
        for name, table in [
            ("prune_by_name", "prune_by_name"),
            ("prune_by_path", f"delta.`{tmp_path / 'prune_by_path'}`"),
        ]:
            spark.sql(f"DELETE FROM {table} WHERE d > TIMESTAMP '2024-01-02 12:00:00'").collect()  # noqa: S608
            assert sorted(row.id for row in spark.sql(f"SELECT id FROM {table}").collect()) == [1, 2]  # noqa: S608
            commit = sorted((tmp_path / name / "_delta_log").glob("*.json"))[-1]
            removed = [action for action in map(json.loads, commit.read_text().splitlines()) if "remove" in action]
            assert len(removed) == 1, table
    finally:
        for name in names:
            spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


@pytest.mark.parametrize("session_timezone", ["America/Los_Angeles"], indirect=True)
@pytest.mark.parametrize("deletion_vectors", [False, True], ids=["copy-on-write", "deletion-vectors"])
@pytest.mark.parametrize("condition", ["d IS NULL", "d IS NOT NULL", "isnull(d)", "d IS NULL OR x = 1"])
def test_delta_delete_by_null_checks_of_timestamp_columns(
    spark, tmp_path, session_timezone, deletion_vectors, condition
):
    """A DELETE with a null check of a TIMESTAMP data column removes the rows that the check selects in a
    query, for a table referenced by name and by path."""
    _ = session_timezone
    properties = "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')" if deletion_vectors else ""
    names = ["null_by_name", "null_by_path"]
    try:
        for name in names:
            spark.sql(
                f"CREATE TABLE {name} (id INT, x INT, d TIMESTAMP) USING delta "
                f"LOCATION '{escape_sql_string_literal(str(tmp_path / name))}' {properties}"
            )
            # One file per row.
            spark.sql(f"INSERT INTO {name} VALUES (1, 1, TIMESTAMP '2024-01-01 00:00:00')").collect()  # noqa: S608
            spark.sql(f"INSERT INTO {name} VALUES (2, 2, NULL)").collect()  # noqa: S608
        for table in ["null_by_name", f"delta.`{tmp_path / 'null_by_path'}`"]:
            selected = {row.id for row in spark.sql(f"SELECT id FROM {table} WHERE {condition}").collect()}  # noqa: S608
            spark.sql(f"DELETE FROM {table} WHERE {condition}").collect()  # noqa: S608
            left = {row.id for row in spark.sql(f"SELECT id FROM {table}").collect()}  # noqa: S608
            assert {1, 2} - left == selected, table
    finally:
        for name in names:
            spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


def declare_table_without_offsets(spark, tmp_path, name):
    """Declares `name` over a table whose TIMESTAMP partition values have no offset, as older
    writers wrote them: `2024-01-01 09:00:00.000000` for ids 1, 2 and 5, `10:00` for id 3."""
    source, path = tmp_path / "source", tmp_path / "table"
    # Ids 1 and 5 share a file, so a rewrite of id 1 copies id 5.
    for query in [
        "SELECT * FROM VALUES (1, TIMESTAMP '2024-01-01 01:00:00'), (5, TIMESTAMP '2024-01-01 01:00:00') AS t(id, ts)",
        "SELECT 2 AS id, TIMESTAMP '2024-01-01 01:00:00' AS ts",
        "SELECT 3 AS id, TIMESTAMP '2024-01-01 02:00:00' AS ts",
    ]:
        spark.sql(query).repartition(1).write.format("delta").mode("append").partitionBy("ts").save(str(source))
    # A declared schema is refused over an existing table in a non-UTC session, so the table is declared
    # over an empty location first.
    spark.sql(
        f"CREATE TABLE {name} (id INT, ts TIMESTAMP) USING delta PARTITIONED BY (ts) "
        f"LOCATION '{escape_sql_string_literal(str(path))}'"
    ).collect()
    shutil.rmtree(path, ignore_errors=True)
    shutil.copytree(source, path)
    for log in (path / "_delta_log").glob("*.json"):
        log.write_text(re.sub(r'"(2024-01-01)T(\d\d:00:00\.000000)Z"', r'"\1 \2"', log.read_text()))
    assert partition_values((path / "_delta_log").glob("*.json")) == {
        "2024-01-01 09:00:00.000000",
        "2024-01-01 10:00:00.000000",
    }
    return path


@pytest.mark.parametrize("session_timezone", ["America/Los_Angeles"], indirect=True)
@pytest.mark.parametrize(
    ("statement", "expected"),
    [
        ("OPTIMIZE without_offsets", [(1, 17), (2, 17), (3, 18), (5, 17)]),
        ("DELETE FROM without_offsets WHERE id = 1", [(2, 17), (3, 18), (5, 17)]),
        ("UPDATE without_offsets SET id = 101 WHERE id = 1", [(2, 17), (3, 18), (5, 17), (101, 17)]),
        (
            "MERGE INTO without_offsets AS t USING (SELECT 1 AS id) AS s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET id = 201",
            [(2, 17), (3, 18), (5, 17), (201, 17)],
        ),
        ("DELETE FROM without_offsets WHERE ts = TIMESTAMP '2024-01-01 09:00:00'", [(3, 18)]),
    ],
    ids=["optimize", "delete", "update", "merge", "delete-partition"],
)
def test_delta_timestamp_partition_values_without_offset_are_in_session_time_zone(
    spark, tmp_path, session_timezone, statement, expected
):
    _ = session_timezone
    path = declare_table_without_offsets(spark, tmp_path, "without_offsets")

    def rows(time_zone="America/Los_Angeles"):
        spark.conf.set("spark.sql.session.timeZone", time_zone)
        try:
            frames = [
                spark.table("without_offsets"),
                spark.read.format("delta").load(str(path)),
                # Workers parse the partition values of this scan in the session time zone of the job.
                spark.read.format("delta").option("metadataAsDataRead", "true").load(str(path)),
            ]
            return [
                sorted((row.id, row.micros) for row in frame.selectExpr("id", "unix_micros(ts) AS micros").collect())
                for frame in frames
            ]
        finally:
            spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")

    try:
        # Like Delta Lake on Spark, reads by table name and by path take the wall clock in the
        # session time zone, whatever zone the table was declared in.
        before = [(1, 9), (2, 9), (3, 10), (5, 9)]
        assert rows("UTC") == [[(id_, utc_micros(hour)) for id_, hour in before]] * 3
        before = [(id_, utc_micros(hour + 8)) for id_, hour in before]
        assert rows() == [before] * 3
        selected = spark.table("without_offsets").where("ts = TIMESTAMP '2024-01-01 09:00:00'").collect()
        assert sorted(row.id for row in selected) == [1, 2, 5]
        logs = set((path / "_delta_log").glob("*.json"))
        spark.sql(statement).collect()
        # A rewrite keeps what the session read and writes the value with an offset.
        after = [(id_, utc_micros(hour)) for id_, hour in expected]
        assert rows() == [after] * 3
        assert partition_values(set((path / "_delta_log").glob("*.json")) - logs) <= {
            "2024-01-01T17:00:00.000000Z",
            "2024-01-01T18:00:00.000000Z",
        }
    finally:
        spark.sql("DROP TABLE IF EXISTS without_offsets").collect()


def remove_offset(path, value, legacy):
    """Rewrites the partition value `value` in the log without an offset, as older writers wrote it."""
    for log in (path / "_delta_log").glob("*.json"):
        log.write_text(log.read_text().replace(f'"{value}"', f'"{legacy}"'))
    assert partition_values((path / "_delta_log").glob("*.json")) == {legacy}


@pytest.mark.parametrize(
    ("session_timezone", "value", "written", "legacy", "micros"),
    [
        # 01:30 occurs twice in Los Angeles on 2024-11-03. Like Spark, the literal is the first (PDT).
        (
            "America/Los_Angeles",
            "TIMESTAMP '2024-11-03 01:30:00'",
            "2024-11-03T08:30:00.000000Z",
            "2024-11-03 01:30:00.000000",
            1730622600000000,
        ),
        # TODO: Use a TIMESTAMP literal, which Sail does not resolve yet in an offset with seconds.
        (
            "+05:30:15",
            "CAST('2024-01-01 09:00:00' AS TIMESTAMP)",
            "2024-01-01T03:29:45.000000Z",
            "2024-01-01 09:00:00.000000",
            1704079785000000,
        ),
    ],
    indirect=["session_timezone"],
    ids=["repeated-hour", "offset-with-seconds"],
)
def test_delta_timestamp_partition_value_without_offset(
    spark, tmp_path, session_timezone, value, written, legacy, micros
):
    _ = session_timezone
    path = tmp_path / "table"
    spark.sql(f"SELECT 1 AS id, {value} AS ts").write.format("delta").partitionBy("ts").save(str(path))
    remove_offset(path, written, legacy)
    # Like Delta Lake on Spark, the value without an offset is read as the cast of the string in the
    # session time zone.
    assert spark.sql(f"SELECT unix_micros({value}) AS micros").collect()[0].micros == micros
    for metadata_as_data in ["false", "true"]:
        frame = spark.read.format("delta").option("metadataAsDataRead", metadata_as_data).load(str(path))
        assert [row.micros for row in frame.selectExpr("unix_micros(ts) AS micros").collect()] == [micros]
        assert [row.id for row in frame.where(f"ts = {value}").collect()] == [1]
    spark.sql(f"DELETE FROM delta.`{path}` WHERE ts = {value}").collect()  # noqa: S608
    assert spark.read.format("delta").load(str(path)).count() == 0


@pytest.mark.parametrize("session_timezone", ["America/Los_Angeles"], indirect=True)
def test_delta_checkpoint_parses_timestamp_partition_values_in_session_time_zone(spark, tmp_path, session_timezone):
    _ = session_timezone
    path = tmp_path / "table"
    spark.sql("SELECT 1 AS id, TIMESTAMP '2024-01-01 01:00:00' AS ts").write.format("delta").partitionBy("ts").option(
        "delta.checkpointInterval", "2"
    ).save(str(path))
    remove_offset(path, "2024-01-01T09:00:00.000000Z", "2024-01-01 09:00:00.000000")
    for id_ in [2, 3]:
        spark.sql(f"SELECT {id_} AS id, TIMESTAMP '2024-01-01 0{id_}:00:00' AS ts").write.format("delta").mode(
            "append"
        ).save(str(path))
    frame = spark.read.format("delta").load(str(path)).selectExpr("id", "unix_micros(ts) AS micros")
    read = {row.id: row.micros for row in frame.collect()}
    assert read == {1: utc_micros(17), 2: utc_micros(10), 3: utc_micros(11)}
    # The commit of version 2 writes a checkpoint. Like Delta Lake on Spark, it parses the values in the
    # session time zone, so its typed copy holds the instants that the session reads.
    rows = pq.read_table(path / "_delta_log" / f"{2:020}.checkpoint.parquet").to_pylist()
    parsed = {
        dict(row["add"]["partitionValues"])["ts"]: int(row["add"]["partitionValues_parsed"]["ts"].timestamp())
        * 1_000_000
        for row in rows
        if row["add"]
    }
    assert parsed == {
        "2024-01-01 09:00:00.000000": read[1],
        "2024-01-01T10:00:00.000000Z": read[2],
        "2024-01-01T11:00:00.000000Z": read[3],
    }


@pytest.mark.parametrize("session_timezone", ["UTC"], indirect=True)
@pytest.mark.parametrize("ansi", [True, False], ids=["ansi", "not-ansi"])
def test_delta_timestamp_partition_values_that_do_not_parse_are_null(spark, tmp_path, session_timezone, ansi):
    _ = session_timezone
    path = tmp_path / "table"
    june = int(datetime(2024, 6, 1, 7, tzinfo=UTC).timestamp()) * 1_000_000
    spark.sql(
        "CREATE TABLE unparsed (id INT, ts TIMESTAMP) USING delta PARTITIONED BY (ts) "
        f"LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES ('delta.checkpointInterval' = '7')"
    )
    try:
        for id_, value in [(1, "2024-01-01 09:00:00"), (2, "2024-01-01 09:00:00"), (7, "2024-01-01 09:00:00")] + [
            (id_, "2024-06-01 07:00:00") for id_ in [3, 4, 5]
        ]:
            spark.sql(f"SELECT {id_} AS id, TIMESTAMP '{value}' AS ts").write.format("delta").mode("append").save(
                str(path)
            )
    finally:
        spark.sql("DROP TABLE IF EXISTS unparsed").collect()
    # Neither Spark's cast nor the Arrow cast of earlier Sail versions parses the value, so Delta Lake on
    # Spark reads it as NULL.
    for log in (path / "_delta_log").glob("*.json"):
        log.write_text(log.read_text().replace('"2024-01-01T09:00:00.000000Z"', f'"{UNPARSED_TIMESTAMP}"'))
    with ansi_mode(spark, enabled=ansi):
        check_timestamp_partition_values_that_do_not_parse(spark, path, june)


def check_timestamp_partition_values_that_do_not_parse(spark, path, june):
    def rows():
        frame = spark.read.format("delta").load(str(path)).selectExpr("id", "unix_micros(ts) AS micros")
        return sorted((row.id, row.micros) for row in frame.collect())

    def overwrite(query):
        spark.sql(query).write.format("delta").mode("overwrite").option("partitionOverwriteMode", "dynamic").save(
            str(path)
        )

    for metadata_as_data in ["false", "true"]:
        frame = spark.read.format("delta").option("metadataAsDataRead", metadata_as_data).load(str(path))
        assert sorted(row.id for row in frame.where("ts IS NULL").collect()) == [1, 2, 7]
        assert sorted(row.id for row in frame.where("ts >= TIMESTAMP '2024-06-01 07:00:00' OR id = 1").collect()) == [
            1,
            3,
            4,
            5,
        ]
        (row,) = frame.selectExpr("count(ts) AS n", "unix_micros(max(ts)) AS m").collect()
        assert (row.n, row.m) == (3, june)
    # The append commits version 7, which writes a checkpoint. Like Delta Lake on Spark, its typed copy
    # of the value is NULL.
    spark.sql("SELECT 9 AS id, TIMESTAMP '2024-06-01 07:00:00' AS ts").write.format("delta").mode("append").save(
        str(path)
    )
    checkpoint = pq.read_table(path / "_delta_log" / f"{7:020}.checkpoint.parquet").to_pylist()
    parsed = {
        dict(row["add"]["partitionValues"])["ts"]: row["add"]["partitionValues_parsed"]["ts"]
        for row in checkpoint
        if row["add"]
    }
    assert parsed == {UNPARSED_TIMESTAMP: None, "2024-06-01T07:00:00.000000Z": datetime(2024, 6, 1, 7, tzinfo=UTC)}
    # Like Delta Lake on Spark, a dynamic overwrite of the NULL partition keeps the files whose value does
    # not parse.
    overwrite("SELECT 8 AS id, CAST(NULL AS TIMESTAMP) AS ts")
    assert rows() == [(1, None), (2, None), (3, june), (4, june), (5, june), (7, None), (8, None), (9, june)]
    # OPTIMIZE compacts the files whose value does not parse into a file that keeps the spelling.
    spark.sql(f"OPTIMIZE delta.`{path}`").collect()
    assert rows() == [(1, None), (2, None), (3, june), (4, june), (5, june), (7, None), (8, None), (9, june)]
    assert active_partition_values(path, "ts").count(UNPARSED_TIMESTAMP) == 1
    overwrite("SELECT 6 AS id, TIMESTAMP '2024-06-01 07:00:00' AS ts")
    assert rows() == [(1, None), (2, None), (6, june), (7, None), (8, None)]


def active_adds(path):
    """The Add actions of the files of the latest version of the table at `path`."""
    active = {}
    for log in sorted((path / "_delta_log").glob("*.json")):
        for action in map(json.loads, log.read_text().splitlines()):
            if "add" in action:
                active[action["add"]["path"]] = action["add"]
            elif "remove" in action:
                active.pop(action["remove"]["path"], None)
    return list(active.values())


def active_partition_values(path, column):
    """The raw values of `column` of the files of the latest version of the table at `path`."""
    return [add["partitionValues"][column] for add in active_adds(path)]


def stored_rows(path, column="p", id_column="id"):
    """The sorted `(raw value of column, id, whether a deletion vector applies)` of every row stored in
    the files of the latest version of the table at `path`, including the rows a deletion vector
    deletes. `id_column` is the name of the id column in the files."""
    return sorted(
        (
            (add["partitionValues"][column] or None, id_, add.get("deletionVector") is not None)
            for add in active_adds(path)
            for id_ in pq.ParquetFile(path / unquote(add["path"])).read(columns=[id_column]).column(0).to_pylist()
        ),
        key=str,
    )


def write_partitioned_log(path, data_type, values, *, deletion_vectors=False, rows_per_file=1):
    """Writes a Delta table `(id INT, v INT, p <data_type>) PARTITIONED BY (p)` with one file per raw
    partition value in `values`. The file of the n-th value has the ids n, n + 100 and so on, up to
    `rows_per_file` rows, and `v` is 10 times n."""
    (path / "_delta_log").mkdir(parents=True)
    fields = [
        {"name": name, "type": kind, "nullable": True, "metadata": {}}
        for name, kind in [("id", "integer"), ("v", "integer"), ("p", data_type)]
    ]
    protocol = {"minReaderVersion": 1, "minWriterVersion": 2}
    configuration = {}
    if deletion_vectors:
        features = ["deletionVectors"]
        protocol = {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": features,
            "writerFeatures": features,
        }
        configuration = {"delta.enableDeletionVectors": "true"}
    actions = [
        {"protocol": protocol},
        {
            "metaData": {
                "id": "partition-values",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps({"type": "struct", "fields": fields}),
                "partitionColumns": ["p"],
                "configuration": configuration,
                "createdTime": 0,
            }
        },
    ]
    for id_, value in enumerate(values, 1):
        name = f"part-{id_}.parquet"
        ids = [id_ + 100 * row for row in range(rows_per_file)]
        pq.write_table(
            pa.table({"id": pa.array(ids, pa.int32()), "v": pa.array([id_ * 10] * len(ids), pa.int32())}),
            path / name,
        )
        stats = {
            "numRecords": len(ids),
            "minValues": {"id": ids[0]},
            "maxValues": {"id": ids[-1]},
            "nullCount": {"id": 0},
        }
        actions.append(
            {
                "add": {
                    "path": name,
                    "partitionValues": {"p": value},
                    "size": (path / name).stat().st_size,
                    "modificationTime": 0,
                    "dataChange": True,
                    "stats": json.dumps(stats),
                }
            }
        )
    (path / "_delta_log" / f"{0:020}.json").write_text("".join(json.dumps(action) + "\n" for action in actions))


@pytest.mark.parametrize("session_timezone", ["UTC"], indirect=True)
@pytest.mark.parametrize("ansi", [True, False], ids=["ansi", "not-ansi"])
@pytest.mark.parametrize(
    ("data_type", "bad", "good", "literal"),
    [
        ("decimal(5,2)", "-1.-25", "2.50", "2.50"),
        ("integer", "abc", "2", "2"),
        ("timestamp", UNPARSED_TIMESTAMP, "2024-01-01T09:00:00.000000Z", "TIMESTAMP '2024-01-01 09:00:00'"),
    ],
    ids=["decimal", "integer", "timestamp"],
)
def test_delta_partition_filters_read_values_that_do_not_parse_as_null(
    spark, tmp_path, session_timezone, ansi, data_type, bad, good, literal
):
    """Like Delta Lake on Spark, a partition value that does not parse reads as NULL, and partition
    filters see the value that reads return, whatever the ANSI mode. In ANSI mode, Delta Lake on Spark
    raises a cast error for partition filters instead, which it tracks as a bug
    (https://github.com/delta-io/delta/issues/7930)."""
    _ = session_timezone
    path = tmp_path / "table"
    write_partitioned_log(path, data_type, [bad, bad, good, None])
    with ansi_mode(spark, enabled=ansi):
        frame = spark.read.format("delta").load(str(path))
        assert sorted((row.id, row.p is None) for row in frame.collect()) == [
            (1, True),
            (2, True),
            (3, False),
            (4, True),
        ]
        assert sorted(row.id for row in frame.where("v > 0").collect()) == [1, 2, 3, 4]
        for metadata_as_data in ["false", "true"]:
            frame = spark.read.format("delta").option("metadataAsDataRead", metadata_as_data).load(str(path))
            for condition, expected in [
                ("p IS NULL", [1, 2, 4]),
                (f"p = {literal}", [3]),
                ("p IS NULL AND id = 1", [1]),
                ("id = 3 OR p IS NULL", [1, 2, 3, 4]),
            ]:
                assert sorted(row.id for row in frame.where(condition).collect()) == expected, condition
        spark.sql(f"DELETE FROM delta.`{path}` WHERE p IS NULL").collect()  # noqa: S608
        assert [row.id for row in spark.read.format("delta").load(str(path)).collect()] == [3]


@pytest.mark.parametrize("session_timezone", ["UTC", "America/Los_Angeles"], indirect=True)
@pytest.mark.parametrize("ansi", [True, False], ids=["ansi", "not-ansi"])
def test_delta_timestamp_partition_values_in_spellings_of_earlier_sail_versions(
    spark, tmp_path, session_timezone, ansi
):
    """TIMESTAMP partition values in spellings that only the Arrow cast of earlier Sail versions parses
    keep the instant those versions read, a value without a zone in the session time zone."""
    _ = session_timezone
    path = tmp_path / "table"
    with_zone = [
        "2024-01-01t09:00:00Z",
        "2024-01-01T09:00:00z",
        "2024-01-01 09:00:00z",
        "2024-01-01T090000Z",
        "2024-01-01T09:00Z",
    ]
    without_zone = ["2024-01-01t09:00:00", "2024-01-01 090000"]
    write_partitioned_log(path, "timestamp", with_zone + without_zone)
    nine = utc_micros(9)
    wall_clock = nine if spark.conf.get("spark.sql.session.timeZone") == "UTC" else utc_micros(17)
    expected = dict.fromkeys(range(1, 6), nine) | {6: wall_clock, 7: wall_clock}
    with ansi_mode(spark, enabled=ansi):
        for metadata_as_data in ["false", "true"]:
            frame = spark.read.format("delta").option("metadataAsDataRead", metadata_as_data).load(str(path))
            assert {row.id: row.micros for row in frame.selectExpr("id", "unix_micros(p) AS micros").collect()} == (
                expected
            )
            selected = frame.where(f"p = timestamp_micros({nine})").collect()
            assert sorted(row.id for row in selected) == sorted(
                id_ for id_, micros in expected.items() if micros == nine
            )
        spark.sql(f"OPTIMIZE delta.`{path}`").collect()
        spark.sql(f"DELETE FROM delta.`{path}` WHERE p = timestamp_micros({nine})").collect()  # noqa: S608
        left = spark.read.format("delta").load(str(path)).selectExpr("id", "unix_micros(p) AS micros").collect()
        assert {row.id: row.micros for row in left} == {
            id_: micros for id_, micros in expected.items() if micros != nine
        }


def check_rewritten_table(spark, path, original, expected):
    """Checks the rows of the table at `path` after a statement on a table written by
    `write_partitioned_log`. `original` and `expected` map the id of each row to its raw partition value
    and its `v`, before and after the statement. A row that a file with a deletion vector stores is alive
    unless a rewritten file stores it too, and such a file keeps its value."""
    live = {row.id: row.v for row in spark.read.format("delta").load(str(path)).collect()}
    assert live == {id_: v for id_, (_, v) in expected.items()}
    stored = stored_rows(path)
    written = [(id_, value) for value, id_, deletion_vector in stored if not deletion_vector]
    rewritten = dict(written)
    assert len(rewritten) == len(written)
    assert set(rewritten) <= set(live)
    kept = {id_: original[id_][0] for id_ in live if id_ not in rewritten}
    assert all((value, id_, True) in stored for id_, value in kept.items())
    assert rewritten | kept == {id_: value for id_, (value, _) in expected.items()}
    assert all(value == original[id_][0] for value, id_, deletion_vector in stored if deletion_vector)


@pytest.mark.parametrize("session_timezone", ["UTC"], indirect=True)
@pytest.mark.parametrize("ansi", [True, False], ids=["ansi", "not-ansi"])
@pytest.mark.parametrize("deletion_vectors", [False, True], ids=["copy-on-write", "deletion-vectors"])
@pytest.mark.parametrize(
    ("data_type", "bad", "good", "literal"),
    [
        ("decimal(5,2)", "-1.-25", "2.50", "2.50"),
        ("integer", "abc", "2", "2"),
        ("timestamp", UNPARSED_TIMESTAMP, "2024-01-01T09:00:00.000000Z", "TIMESTAMP '2024-01-01 09:00:00'"),
    ],
    ids=["decimal", "integer", "timestamp"],
)
@pytest.mark.parametrize(
    ("statement", "changes"),
    [
        ("DELETE FROM {t} WHERE id IN (1, 2)", {1: None, 2: None}),
        ("UPDATE {t} SET v = v + 1 WHERE id IN (1, 2, 4)", {1: ("bad", 11), 2: ("bad2", 21), 4: (None, 41)}),
        ("UPDATE {t} SET p = {literal} WHERE id = 1", {1: ("good", 10)}),
        ("UPDATE {t} SET p = NULL WHERE id = 1", {1: (None, 10)}),
        (
            "UPDATE {t} SET p = coalesce(p, {literal}), v = 0 WHERE id IN (1, 101, 3)",
            {1: ("good", 0), 101: ("good", 0), 3: ("good", 0)},
        ),
        (
            "MERGE INTO {t} USING (SELECT explode(array(1, 2)) AS k) s ON t.id = s.k "
            "WHEN MATCHED THEN UPDATE SET v = 0",
            {1: ("bad", 0), 2: ("bad2", 0)},
        ),
        (
            "MERGE INTO {t} USING (SELECT 1 AS id, 0 AS v, CAST({literal} AS {data_type}) AS p) s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET *",
            {1: ("good", 0)},
        ),
        (
            "MERGE INTO {t} USING (SELECT * FROM VALUES (1, 0, CAST(NULL AS {data_type})), "
            "(5, 50, CAST(NULL AS {data_type})) AS s(id, v, p)) s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT *",
            {1: ("bad", 0), 5: (None, 50)},
        ),
        (
            "MERGE INTO {t} USING (SELECT explode(array(1, 2)) AS k) s ON t.id = s.k "
            "WHEN MATCHED AND s.k = 1 THEN UPDATE SET p = {literal} WHEN MATCHED THEN UPDATE SET v = 0",
            {1: ("good", 10), 2: ("bad2", 0)},
        ),
        ("MERGE INTO {t} USING (SELECT 1 AS k) s ON t.id = s.k WHEN MATCHED THEN DELETE", {1: None}),
        (
            "MERGE INTO {t} USING (SELECT 3 AS k) s ON t.id = s.k "
            "WHEN NOT MATCHED BY SOURCE AND t.id = 1 THEN UPDATE SET p = {literal} "
            "WHEN NOT MATCHED BY SOURCE AND t.id IN (2, 4) THEN UPDATE SET v = 0",
            {1: ("good", 10), 2: ("bad2", 0), 4: (None, 0)},
        ),
    ],
    ids=[
        "delete",
        "update",
        "update-assign",
        "update-assign-null",
        "update-reads-null",
        "merge-update",
        "merge-update-star",
        "merge-insert-null",
        "merge-clauses",
        "merge-delete",
        "merge-not-matched-by-source",
    ],
)
def test_delta_rewrites_keep_partition_values_that_do_not_parse(
    spark, tmp_path, session_timezone, ansi, deletion_vectors, data_type, bad, good, literal, statement, changes
):
    """A partition value that does not parse reads as NULL. A rewrite keeps it for every row whose
    partition value the statement does not assign, where Delta Lake on Spark writes NULL instead. An
    assigned value is written as computed, from NULL where the expression reads the column. ANSI mode
    plays no part."""
    _ = session_timezone
    path = tmp_path / "table"
    spellings = {"bad": bad, "bad2": f"{bad}x", "good": good, None: None}
    write_partitioned_log(
        path, data_type, [bad, f"{bad}x", good, None], deletion_vectors=deletion_vectors, rows_per_file=2
    )
    original = {
        id_ + offset: (value, id_ * 10) for id_, value in enumerate(spellings.values(), 1) for offset in [0, 100]
    }
    expected = dict(original)
    for id_, change in changes.items():
        if change is None:
            del expected[id_]
        else:
            expected[id_] = (spellings[change[0]], change[1])
    with ansi_mode(spark, enabled=ansi):
        spark.sql(statement.format(t=f"delta.`{path}` t", literal=literal, data_type=data_type)).collect()
    check_rewritten_table(spark, path, original, expected)


@pytest.mark.parametrize("session_timezone", ["UTC"], indirect=True)
@pytest.mark.parametrize("ansi", [True, False], ids=["ansi", "not-ansi"])
@pytest.mark.parametrize(
    ("data_type", "bad", "good"),
    [
        ("decimal(5,2)", "-1.-25", "2.50"),
        ("integer", "abc", "2"),
        ("timestamp", UNPARSED_TIMESTAMP, "2024-01-01T09:00:00.000000Z"),
    ],
    ids=["decimal", "integer", "timestamp"],
)
def test_delta_optimize_keeps_partition_values_that_do_not_parse(
    spark, tmp_path, session_timezone, ansi, data_type, bad, good
):
    """OPTIMIZE compacts the files of a partition value that does not parse into a file that keeps the
    value, apart from NULL and from other spellings, and changes no data."""
    _ = session_timezone
    path = tmp_path / "table"
    values = [bad, bad, f"{bad}x", f"{bad}x", good, good, None, None]
    write_partitioned_log(path, data_type, values)
    with ansi_mode(spark, enabled=ansi):
        spark.sql(f"OPTIMIZE delta.`{path}`").collect()
    (log,) = sorted((path / "_delta_log").glob("*.json"))[1:]
    actions = list(map(json.loads, log.read_text().splitlines()))
    assert all(not action["add"]["dataChange"] for action in actions if "add" in action)
    assert sorted(active_partition_values(path, "p"), key=str) == sorted([bad, f"{bad}x", good, None], key=str)
    assert stored_rows(path) == sorted(((value, id_, False) for id_, value in enumerate(values, 1)), key=str)


@pytest.mark.parametrize("session_timezone", ["UTC"], indirect=True)
@pytest.mark.parametrize("ansi", [True, False], ids=["ansi", "not-ansi"])
@pytest.mark.parametrize(
    ("data_type", "bad", "good", "literal"),
    [
        ("decimal(5,2)", "-1.-25", "2.50", "2.50"),
        ("integer", "abc", "2", "2"),
        ("timestamp", UNPARSED_TIMESTAMP, "2024-01-01T09:00:00.000000Z", "TIMESTAMP '2024-01-01 09:00:00'"),
    ],
    ids=["decimal", "integer", "timestamp"],
)
def test_delta_replace_where_keeps_partition_values_that_do_not_parse(
    spark, tmp_path, session_timezone, ansi, data_type, bad, good, literal
):
    """An overwrite with `replaceWhere` on data columns keeps the partition value that does not parse for
    the rows it keeps of the files it rewrites."""
    _ = session_timezone
    path = tmp_path / "table"
    write_partitioned_log(path, data_type, [bad, good, None], rows_per_file=2)
    original = {1: (bad, 10), 101: (bad, 10), 2: (good, 20), 102: (good, 20), 3: (None, 30), 103: (None, 30)}
    with ansi_mode(spark, enabled=ansi):
        spark.sql(f"SELECT 1 AS id, 0 AS v, CAST({literal} AS {data_type}) AS p").write.format("delta").mode(
            "overwrite"
        ).option("replaceWhere", "id = 1").save(str(path))
    check_rewritten_table(spark, path, original, original | {1: (good, 0)})


@contextmanager
def table_with_unparsed_partition_value(spark, path, schema, columns, values, properties="", partition_by="p"):
    """A Delta table at `path` with the rows `values` of `columns`, whose files with the partition value 7
    have the value `abc` in the log instead, as a writer with a bug would have written it."""
    spark.sql(
        f"CREATE TABLE unparsed ({schema}) USING delta PARTITIONED BY ({partition_by}) "
        f"LOCATION '{escape_sql_string_literal(str(path))}' {properties}"
    )
    try:
        spark.sql(f"INSERT INTO unparsed ({columns}) VALUES {values}").collect()  # noqa: S608
        for log in (path / "_delta_log").glob("*.json"):
            actions = list(map(json.loads, log.read_text().splitlines()))
            for action in actions:
                for kind in ["add", "remove", "cdc"]:
                    if kind in action:
                        values_ = action[kind]["partitionValues"]
                        action[kind]["partitionValues"] = {k: "abc" if v == "7" else v for k, v in values_.items()}
            log.write_text("".join(json.dumps(action) + "\n" for action in actions))
        yield
    finally:
        spark.sql("DROP TABLE IF EXISTS unparsed").collect()


def change_data_partition_values(path, version, column="p"):
    """The sorted `(raw value of column, change type)` of the rows of the change data files of the commit
    `version`."""
    return sorted(
        {
            (action["cdc"]["partitionValues"][column], change_type)
            for action in map(json.loads, (path / "_delta_log" / f"{version:020}.json").read_text().splitlines())
            if "cdc" in action
            for change_type in pq.ParquetFile(path / unquote(action["cdc"]["path"]))
            .read(columns=["_change_type"])
            .column("_change_type")
            .to_pylist()
        },
        key=str,
    )


def latest_version(path):
    return max(int(log.stem) for log in (path / "_delta_log").glob("*.json"))


@pytest.mark.parametrize("statement", ["update", "merge"])
def test_delta_rewrites_write_generated_partition_values_that_do_not_parse(spark, tmp_path, statement):
    """A rewrite computes a generated partition column for the rows it updates, so they get the computed
    value, NULL included, while the rows it copies keep the value that does not parse."""
    path = tmp_path / "table"
    with table_with_unparsed_partition_value(
        spark, path, "id INT, v INT, p INT GENERATED ALWAYS AS (v DIV 10)", "id, v", "(1, 70), (101, 70), (3, 30)"
    ):
        for id_, assignment in [(1, "id = 1001"), (101, "v = NULL")]:
            if statement == "update":
                spark.sql(f"UPDATE unparsed SET {assignment} WHERE id = {id_}").collect()  # noqa: S608
            else:
                spark.sql(
                    f"MERGE INTO unparsed t USING (SELECT {id_} AS k) s ON t.id = s.k "
                    f"WHEN MATCHED THEN UPDATE SET {assignment}"
                ).collect()
            if id_ == 1:
                assert stored_rows(path) == [("3", 3, False), ("7", 1001, False), ("abc", 101, False)]
        assert stored_rows(path) == [("3", 3, False), ("7", 1001, False), (None, 101, False)]


def test_delta_change_data_keeps_partition_values_that_do_not_parse(spark, tmp_path):
    """The change data of a rewrite keeps the partition value that does not parse for its before images, and
    for its after images unless the statement assigns the partition column. It reads as NULL."""
    path = tmp_path / "table"
    with table_with_unparsed_partition_value(
        spark,
        path,
        "id INT, v INT, p INT",
        "id, v, p",
        "(1, 10, 7), (101, 10, 7), (2, 20, 7), (102, 20, 7), (3, 30, 3)",
        "TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')",
    ):
        update = [("abc", "update_postimage"), ("abc", "update_preimage")]
        for statement, partition_values in [
            ("UPDATE unparsed SET v = 0 WHERE id = 1", update),
            ("DELETE FROM unparsed WHERE id = 101", [("abc", "delete")]),
            ("MERGE INTO unparsed t USING (SELECT 2 AS k) s ON t.id = s.k WHEN MATCHED THEN UPDATE SET v = 1", update),
            (
                "MERGE INTO unparsed t USING (SELECT 102 AS k) s ON t.id = s.k "
                "WHEN MATCHED THEN UPDATE SET p = 5 WHEN NOT MATCHED THEN INSERT (id, v, p) VALUES (s.k, 0, 5)",
                [("5", "update_postimage"), ("abc", "update_preimage")],
            ),
            ("SELECT 3 AS id, 0 AS v, 3 AS p", [("3", "delete"), ("3", "insert"), ("abc", "delete")]),
        ]:
            if statement.startswith("SELECT"):
                # The overwrite deletes the row 2 and keeps the row 1 of their file.
                spark.sql(statement).write.format("delta").mode("overwrite").option(
                    "replaceWhere", "id IN (2, 3)"
                ).save(str(path))
            else:
                spark.sql(statement).collect()
            version = latest_version(path)
            assert change_data_partition_values(path, version) == partition_values, statement
            for row in (
                spark.read.format("delta")
                .option("readChangeFeed", "true")
                .option("startingVersion", version)
                .load(str(path))
                .collect()
            ):
                expected = {(102, "update_postimage"): 5, (3, "delete"): 3, (3, "insert"): 3}
                assert row.p == expected.get((row.id, row["_change_type"])), statement
        assert stored_rows(path) == [("3", 3, False), ("5", 102, False), ("abc", 1, False)]


def test_delta_merge_change_data_with_identity_keeps_partition_values_that_do_not_parse(spark, tmp_path):
    """A MERGE that allocates identity values writes the change data with the table data, and keeps the
    partition value that does not parse in both."""
    path = tmp_path / "table"
    with table_with_unparsed_partition_value(
        spark,
        path,
        "k BIGINT GENERATED ALWAYS AS IDENTITY, id INT, v INT, p INT",
        "id, v, p",
        "(1, 10, 7), (101, 10, 7)",
        "TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')",
    ):
        spark.sql(
            "MERGE INTO unparsed t USING (SELECT explode(array(1, 5)) AS s) s ON t.id = s.s "
            "WHEN MATCHED THEN UPDATE SET v = 0 WHEN NOT MATCHED THEN INSERT (id, v, p) VALUES (s.s, 0, NULL)"
        ).collect()
        assert change_data_partition_values(path, latest_version(path)) == [
            ("abc", "update_postimage"),
            ("abc", "update_preimage"),
            (None, "insert"),
        ]
        assert stored_rows(path) == [("abc", 1, False), ("abc", 101, False), (None, 5, False)]


def test_delta_rewrites_keep_partition_values_that_do_not_parse_with_column_mapping(spark, tmp_path):
    """A rewrite of a table with column mapping keeps the partition value that does not parse under the
    physical name of the column."""
    path = tmp_path / "table"
    with table_with_unparsed_partition_value(
        spark,
        path,
        "id INT, v INT, p INT",
        "id, v, p",
        "(1, 10, 7), (101, 10, 7)",
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
    ):
        metadata = next(
            action["metaData"]
            for action in map(json.loads, (path / "_delta_log" / f"{0:020}.json").read_text().splitlines())
            if "metaData" in action
        )
        (field,) = (field for field in json.loads(metadata["schemaString"])["fields"] if field["name"] == "p")
        physical = field["metadata"]["delta.columnMapping.physicalName"]
        assert physical != "p"
        (id_field,) = (field for field in json.loads(metadata["schemaString"])["fields"] if field["name"] == "id")
        spark.sql("UPDATE unparsed SET v = 0 WHERE id = 1").collect()
        assert stored_rows(path, physical, id_field["metadata"]["delta.columnMapping.physicalName"]) == [
            ("abc", 1, False),
            ("abc", 101, False),
        ]
        assert sorted(tuple(row) for row in spark.table("unparsed").collect()) == [(1, 0, None), (101, 10, None)]


def test_delta_checkpoint_keeps_partition_values_that_do_not_parse(spark, tmp_path):
    """A checkpoint written after a rewrite keeps the partition value that does not parse, so later rewrites
    from the checkpoint keep it too."""
    path = tmp_path / "table"
    with table_with_unparsed_partition_value(
        spark,
        path,
        "id INT, v INT, p INT",
        "id, v, p",
        "(1, 10, 7), (101, 10, 7), (2, 20, 7)",
        "TBLPROPERTIES ('delta.checkpointInterval' = '2')",
    ):
        spark.sql("UPDATE unparsed SET v = 0 WHERE id = 1").collect()
        assert (path / "_delta_log" / f"{2:020}.checkpoint.parquet").exists()
        spark.sql("DELETE FROM unparsed WHERE id = 2").collect()
        spark.sql("UPDATE unparsed SET v = 1 WHERE id = 101").collect()
        assert (path / "_delta_log" / f"{4:020}.checkpoint.parquet").exists()
        checkpoint = pq.read_table(path / "_delta_log" / f"{4:020}.checkpoint.parquet").to_pylist()
        assert sorted(dict(row["add"]["partitionValues"])["p"] for row in checkpoint if row["add"]) == ["abc"]
        spark.sql(f"OPTIMIZE delta.`{path}`").collect()
        assert stored_rows(path) == [("abc", 1, False), ("abc", 101, False)]
        with ansi_mode(spark, enabled=False):
            assert sorted(row.id for row in spark.table("unparsed").where("p IS NULL").collect()) == [1, 101]


def test_delta_merge_schema_evolution_keeps_partition_values_that_do_not_parse(spark, tmp_path):
    """MERGE with schema evolution keeps the partition value that does not parse when the source has no
    partition column, and adds only the source columns to the table."""
    path = tmp_path / "table"
    with table_with_unparsed_partition_value(
        spark, path, "id INT, v INT, p INT", "id, v, p", "(1, 10, 7), (101, 10, 7)"
    ):
        spark.sql(
            "MERGE WITH SCHEMA EVOLUTION INTO unparsed t USING (SELECT 1 AS id, 0 AS v, 'x' AS w) s "
            "ON t.id = s.id WHEN MATCHED THEN UPDATE SET *"
        ).collect()
        assert spark.table("unparsed").columns == ["id", "v", "p", "w"]
        assert sorted(tuple(row) for row in spark.table("unparsed").collect()) == [
            (1, 0, None, "x"),
            (101, 10, None, None),
        ]
        assert stored_rows(path) == [("abc", 1, False), ("abc", 101, False)]


def test_delta_rewrites_keep_partition_values_that_do_not_parse_of_one_of_two_columns(spark, tmp_path):
    """A rewrite keeps the partition value that does not parse of one partition column and writes the value
    of the other column as before."""
    path = tmp_path / "table"
    with table_with_unparsed_partition_value(
        spark,
        path,
        "id INT, v INT, p INT, q STRING, d DATE",
        "id, v, p, q, d",
        "(1, 10, 7, 'x', DATE '2024-01-01'), (101, 10, 7, 'x', DATE '2024-01-01')",
        partition_by="p, q, d",
    ):
        spark.sql("UPDATE unparsed SET v = 0 WHERE id = 1").collect()
        assert sorted((add["partitionValues"]["q"], add["partitionValues"]["d"]) for add in active_adds(path)) == [
            ("x", "2024-01-01")
        ]
        assert stored_rows(path) == [("abc", 1, False), ("abc", 101, False)]
