# SPDX-License-Identifier: Apache-2.0

import json
import re
import shutil
from datetime import UTC, datetime

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
def test_delta_timestamp_partition_values_that_do_not_parse_are_null(spark, tmp_path, session_timezone):
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
    # Spark's cast rejects a lowercase `t` or `z`, so Delta Lake on Spark reads the value as NULL.
    for log in (path / "_delta_log").glob("*.json"):
        log.write_text(log.read_text().replace('"2024-01-01T09:00:00.000000Z"', '"2024-01-01t09:00:00z"'))

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
    # The DELETE commits version 7, which writes a checkpoint. Like Delta Lake on Spark, its typed copy
    # of the value is NULL.
    spark.sql(f"DELETE FROM delta.`{path}` WHERE ts IS NULL AND id = 7").collect()  # noqa: S608
    checkpoint = pq.read_table(path / "_delta_log" / f"{7:020}.checkpoint.parquet").to_pylist()
    parsed = {
        dict(row["add"]["partitionValues"])["ts"]: row["add"]["partitionValues_parsed"]["ts"]
        for row in checkpoint
        if row["add"]
    }
    assert parsed == {"2024-01-01t09:00:00z": None, "2024-06-01T07:00:00.000000Z": datetime(2024, 6, 1, 7, tzinfo=UTC)}
    # Like Delta Lake on Spark, a dynamic overwrite of the NULL partition keeps the files whose value does
    # not parse.
    overwrite("SELECT 8 AS id, CAST(NULL AS TIMESTAMP) AS ts")
    assert rows() == [(1, None), (2, None), (3, june), (4, june), (5, june), (8, None)]
    spark.sql(f"OPTIMIZE delta.`{path}`").collect()
    assert rows() == [(1, None), (2, None), (3, june), (4, june), (5, june), (8, None)]
    overwrite("SELECT 6 AS id, TIMESTAMP '2024-06-01 07:00:00' AS ts")
    assert rows() == [(1, None), (2, None), (6, june), (8, None)]
