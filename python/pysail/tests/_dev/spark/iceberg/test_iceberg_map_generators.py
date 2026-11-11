"""Map generators retain persisted Iceberg child-field metadata."""

from uuid import UUID

import pyarrow.parquet as pq
import pytest
from pyspark.sql.types import BinaryType, IntegerType, StringType, StructField, StructType

from pysail.testing.spark.utils.sql import escape_sql_string_literal


@pytest.mark.parametrize("uuid", [False, True])
@pytest.mark.parametrize("generator", ["explode", "explode_outer", "posexplode", "posexplode_outer"])
def test_persisted_map_generators(spark, tmp_path, uuid, generator):
    table = "iceberg_map_generators"
    location = escape_sql_string_literal(str(tmp_path))
    data_type = "UUID" if uuid else "STRING"
    first = "6f1c2d3e-4b5a-4c6d-8e7f-0123456789ab" if uuid else "first"
    second = "00000000-0000-0000-0000-000000000001" if uuid else "second"
    positional = generator.startswith("pos")
    outer = generator.endswith("outer")
    try:
        spark.sql(f"CREATE TABLE {table} (id INT, m MAP<{data_type}, {data_type}>) USING iceberg LOCATION '{location}'")
        spark.createDataFrame(
            [(1, {first: first}), (2, {}), (3, None), (4, {second: None})],
            "id INT, m MAP<STRING, STRING>",
        ).repartition(2).write.format("iceberg").mode("append").save(str(tmp_path))
        stored = pq.read_schema(next(tmp_path.rglob("*.parquet"))).field("m").type
        assert b"PARQUET:field_id" in stored.key_field.metadata
        assert b"PARQUET:field_id" in stored.item_field.metadata
        frame = spark.read.format("iceberg").load(str(tmp_path))
        assert [
            tuple(row) for row in frame.selectExpr("id", "size(m)", "size(map_entries(m))").orderBy("id").collect()
        ] == [(1, 1, 1), (2, 0, 0), (3, None, None), (4, 1, 1)]
        aliases = "(pos, k, v)" if positional else "(k, v)"
        generated = frame.selectExpr("id", f"{generator}(m) AS {aliases}")
        leaf = BinaryType() if uuid else StringType()
        expected_fields = [frame.schema["id"]]
        if positional:
            expected_fields.append(StructField("pos", IntegerType(), outer))
        expected_fields += [StructField("k", leaf, outer), StructField("v", leaf, True)]
        assert generated.schema == StructType(expected_fields)
        columns = ["id", "pos"] if positional else ["id"]
        columns += ["hex(k) AS k", "hex(v) AS v"] if uuid else ["k", "v"]
        rows = [tuple(row) for row in generated.selectExpr(*columns).orderBy("id").collect()]
        first = UUID(first).hex.upper() if uuid else first
        second = UUID(second).hex.upper() if uuid else second
        expected = [(1, 0, first, first), (4, 0, second, None)]
        if outer:
            expected[1:1] = [(2, None, None, None), (3, None, None, None)]
        if not positional:
            expected = [(i, k, v) for i, _, k, v in expected]
        assert rows == expected
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("generator", ["explode", "explode_outer", "posexplode", "posexplode_outer"])
def test_map_generator_required_values(spark, generator):
    outer = generator.endswith("outer")
    positional = generator.startswith("pos")
    aliases = "(pos, k, v)" if positional else "(k, v)"
    frame = spark.sql(f"SELECT {generator}(map('k', 'v')) AS {aliases}")
    fields = [StructField("pos", IntegerType(), outer)] if positional else []
    fields += [StructField("k", StringType(), outer), StructField("v", StringType(), outer)]
    assert frame.schema == StructType(fields)
    assert [tuple(row) for row in frame.collect()] == ([(0, "k", "v")] if positional else [("k", "v")])
