# SPDX-License-Identifier: Apache-2.0

import pytest

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.sql import escape_sql_string_literal

# Delta Lake on Spark rejects appending data between `TIMESTAMP` and `TIMESTAMP_NTZ` columns,
# while Sail casts it in the session time zone.
pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Sail casts appended timestamps for Delta tables")


# Cluster workers do not receive the session time zone, so these tests run in local mode only.
@pytest.fixture(scope="module")
def remote():
    with spark_connect_server(envs={"SAIL_MODE": "local", "SAIL_EXECUTION__DEFAULT_PARALLELISM": "2"}) as server:
        yield server.remote


def check_timestamp_ntz_append(spark, path, wall_clock, micros):
    """Appends the TIMESTAMP_NTZ value `wall_clock` to a TIMESTAMP column and checks that it is written as
    the instant `micros`, which is Spark's cast of the value in the session time zone."""
    spark.sql(
        f"CREATE TABLE ntz_cast (id INT, ts TIMESTAMP) USING delta LOCATION '{escape_sql_string_literal(str(path))}'"
    )
    try:
        cast = f"CAST(TIMESTAMP_NTZ '{wall_clock}' AS TIMESTAMP)"
        assert spark.sql(f"SELECT unix_micros({cast}) AS micros").collect()[0].micros == micros
        spark.sql(
            f"SELECT * FROM VALUES (1, TIMESTAMP_NTZ '{wall_clock}'), (2, TIMESTAMP_NTZ '{wall_clock}') AS t(id, ts)"  # noqa: S608
        ).repartition(2).write.format("delta").mode("append").save(str(path))
        rows = spark.sql("SELECT id, unix_micros(ts) AS micros FROM ntz_cast ORDER BY id").collect()
        assert [(row.id, row.micros) for row in rows] == [(1, micros), (2, micros)]
        assert spark.sql(f"SELECT id FROM ntz_cast WHERE ts = {cast}").count() == 2  # noqa: S608
    finally:
        spark.sql("DROP TABLE IF EXISTS ntz_cast").collect()


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
def test_delta_append_casts_timestamp_ntz_as_spark_casts(spark, tmp_path, session_timezone, wall_clock, micros):
    _ = session_timezone
    check_timestamp_ntz_append(spark, tmp_path / "table", wall_clock, micros)


@pytest.mark.parametrize(
    ("session_timezone", "wall_clock", "micros"),
    [
        ("+05:00", "-262143-01-01 00:00:00", -8334601246800000000),
        ("America/Los_Angeles", "+262142-12-31 23:59:59", 8210266905599000000),
    ],
    indirect=["session_timezone"],
    ids=["first-chrono-date", "last-chrono-date"],
)
def test_delta_append_casts_timestamp_ntz_at_the_ends_of_chrono_range(
    spark, tmp_path, session_timezone, wall_clock, micros
):
    _ = session_timezone
    check_timestamp_ntz_append(spark, tmp_path / "table", wall_clock, micros)


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
