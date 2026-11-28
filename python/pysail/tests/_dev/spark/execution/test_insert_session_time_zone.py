# SPDX-License-Identifier: Apache-2.0

import pytest

from pysail.testing.spark.session import spark_connect_server
from pysail.testing.spark.utils.sql import escape_sql_string_literal

pytestmark = pytest.mark.spark(skip_if_jvm=True, reason="Sail local and local-cluster modes")


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


@pytest.mark.parametrize(
    ("wall_clock", "micros"),
    [
        # The values of Spark 4.2 for a table created in a UTC session and written in an
        # America/Los_Angeles session. Like Spark's cast, a wall clock that occurs twice is the
        # first instant (PDT), a wall clock that does not occur moves forward by the length of
        # the gap, and an old wall clock uses the local mean time of the zone.
        ("2024-01-01 01:00:00", 1704099600000000),
        ("2024-11-03 01:30:00", 1730622600000000),
        ("2024-03-10 02:30:00", 1710066600000000),
        ("1850-06-01 12:00:00", -3773707622000000),
    ],
    ids=["plain", "repeated-hour", "skipped-hour", "local-mean-time"],
)
@pytest.mark.parametrize("write", ["insert", "save-as-table"])
@pytest.mark.parametrize("table_format", ["parquet", "iceberg"])
def test_insert_casts_timestamp_ntz_in_session_time_zone(spark, tmp_path, table_format, write, wall_clock, micros):
    """The TIMESTAMP_NTZ value is read in the session time zone of the write, not in the time zone
    of the session that created the table."""
    zone = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        spark.sql(
            f"CREATE TABLE ntz_insert (id INT, ts TIMESTAMP) USING {table_format} "
            f"LOCATION '{escape_sql_string_literal(str(tmp_path / 'table'))}'"
        )
        spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
        # Two input partitions make a worker run the writer.
        rows = spark.sql(
            f"SELECT CAST(id AS INT) AS id, TIMESTAMP_NTZ '{wall_clock}' AS ts FROM range(1, 3, 1, 2)"  # noqa: S608
        )
        if write == "insert":
            rows.createOrReplaceTempView("ntz_rows")
            spark.sql("INSERT INTO ntz_insert SELECT * FROM ntz_rows")
        else:
            rows.write.format(table_format).mode("append").saveAsTable("ntz_insert")
        actual = spark.sql("SELECT id, unix_micros(ts) AS micros FROM ntz_insert ORDER BY id").collect()
        assert [(row.id, row.micros) for row in actual] == [(1, micros), (2, micros)]
    finally:
        spark.sql("DROP TABLE IF EXISTS ntz_insert").collect()
        spark.sql("DROP VIEW IF EXISTS ntz_rows").collect()
        spark.conf.set("spark.sql.session.timeZone", zone)
