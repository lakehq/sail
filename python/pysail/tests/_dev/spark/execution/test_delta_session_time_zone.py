# SPDX-License-Identifier: Apache-2.0

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


@pytest.mark.parametrize("session_timezone", ["America/Los_Angeles"], indirect=True)
def test_delta_append_casts_timestamp_ntz_in_session_time_zone(spark, tmp_path, session_timezone):
    _ = session_timezone
    path = tmp_path / "table"
    spark.sql(
        f"CREATE TABLE ntz_append (id INT, ts TIMESTAMP) USING delta LOCATION '{escape_sql_string_literal(str(path))}'"
    )
    try:
        # Two input partitions make a worker run the Delta writer, rather than the driver task that commits.
        spark.range(1, 3, 1, 2).selectExpr(
            "CAST(id AS INT) AS id",
            "TIMESTAMP_NTZ '2024-01-01 00:00:00' + make_interval(0, 0, 0, 0, CAST(id AS INT), 0, 0) AS ts",
        ).write.format("delta").mode("append").save(str(path))
        rows = spark.sql("SELECT id, unix_micros(ts) AS micros FROM ntz_append ORDER BY id").collect()
        # The wall clock is read in the session time zone (UTC-8).
        assert [(row.id, row.micros) for row in rows] == [
            (1, int(datetime(2024, 1, 1, 9, tzinfo=UTC).timestamp()) * 1_000_000),
            (2, int(datetime(2024, 1, 1, 10, tzinfo=UTC).timestamp()) * 1_000_000),
        ]
    finally:
        spark.sql("DROP TABLE IF EXISTS ntz_append").collect()
