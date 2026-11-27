# SPDX-License-Identifier: Apache-2.0

import json
import re
import shutil
from datetime import UTC, datetime

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
            frames = [spark.table("without_offsets"), spark.read.format("delta").load(str(path))]
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
        assert rows("UTC") == [[(id_, utc_micros(hour)) for id_, hour in before]] * 2
        before = [(id_, utc_micros(hour + 8)) for id_, hour in before]
        assert rows() == [before, before]
        selected = spark.table("without_offsets").where("ts = TIMESTAMP '2024-01-01 09:00:00'").collect()
        assert sorted(row.id for row in selected) == [1, 2, 5]
        logs = set((path / "_delta_log").glob("*.json"))
        spark.sql(statement).collect()
        # A rewrite keeps what the session read and writes the value with an offset.
        after = [(id_, utc_micros(hour)) for id_, hour in expected]
        assert rows() == [after, after]
        assert partition_values(set((path / "_delta_log").glob("*.json")) - logs) <= {
            "2024-01-01T17:00:00.000000Z",
            "2024-01-01T18:00:00.000000Z",
        }
    finally:
        spark.sql("DROP TABLE IF EXISTS without_offsets").collect()
