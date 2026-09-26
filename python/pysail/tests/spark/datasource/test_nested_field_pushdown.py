import re

import pytest
from pyspark.sql import Row

from pysail.testing.spark.session import spark_connect_server, spark_session_factory


@pytest.mark.parametrize("file_format", ["delta", "iceberg"])
@pytest.mark.parametrize("nested", [False, True], ids=["struct", "nested-struct"])
def test_metadata_scan_preserves_nested_decoder_predicates(tmp_path, file_format, nested):
    # Decoder metrics are Sail-specific, and metadata scans create their Parquet readers at runtime.
    with (
        spark_connect_server(envs={"SAIL_PARQUET__PUSHDOWN_FILTERS": "true"}) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        spark = sessions.create()
        data_type = "STRUCT<a:INT>"
        field = "s.a"
        if nested:
            data_type = f"STRUCT<inner:{data_type}>"
            field = "s.inner.a"
        path = tmp_path / "nested_decoder"
        values = [Row(a=value) for value in range(-3, 4)]
        if nested:
            values = [Row(inner=value) for value in values]
        values[3] = None
        (
            spark.createDataFrame([Row(s=value) for value in values], f"s {data_type}")
            .coalesce(1)
            .write.format(file_format)
            .save(path.as_uri())
        )
        (
            spark.read.format(file_format)
            .option("metadataAsDataRead", "true")
            .load(path.as_uri())
            .createOrReplaceTempView("nested_decoder")
        )
        query = f"SELECT {field} AS value FROM nested_decoder WHERE {field} % 2 = 1 ORDER BY value"  # noqa: S608
        assert spark.sql(query).collect() == [Row(value=1), Row(value=3)]
        plan = "\n".join(row[0] for row in spark.sql(f"EXPLAIN ANALYZE {query}").collect())
        # A projection preserves row count, exposing how many rows the runtime reader returned.
        rows = re.search(
            r"ProjectionExec:[^\n]*metrics=\[output_rows=(\d+)[^\n]*\n\s+"
            r"(?:DeltaScanByAddsExec|IcebergScanByDataFilesExec)",
            plan,
        )
        assert rows is not None, plan
        assert int(rows[1]) == 2, plan  # noqa: PLR2004


def test_metadata_delta_predicate_respects_requested_field_order(tmp_path):
    with (
        spark_connect_server(envs={"SAIL_PARQUET__PUSHDOWN_FILTERS": "true"}) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        spark = sessions.create()
        path = tmp_path / "reordered_predicate"
        (
            spark.createDataFrame(
                [Row(s=Row(a=value), t=Row(a=100)) for value in range(-3, 4)],
                "s STRUCT<a:INT>, t STRUCT<a:INT>",
            )
            .coalesce(1)
            .write.format("delta")
            .save(path.as_uri())
        )
        rows = (
            spark.read.schema("t STRUCT<a:INT>, s STRUCT<a:INT>")
            .format("delta")
            .option("metadataAsDataRead", "true")
            .load(path.as_uri())
            .where("s.a % 2 = 1")
            .selectExpr("s.a AS value")
            .orderBy("value")
            .collect()
        )
        assert rows == [Row(value=1), Row(value=3)]
