import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType, StructType

from .utils import create_sql_catalog  # noqa: TID252


def test_column_projection_subset_and_order(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.test_projection_subset"
    schema = Schema(
        NestedField(1, "a", IntegerType(), required=False),
        NestedField(2, "b", StringType(), required=False),
        NestedField(3, "c", IntegerType(), required=False),
    )
    table = catalog.create_table(identifier=identifier, schema=schema)
    try:
        table.append(
            pa.table(
                {
                    "a": pa.array([1, 2], type=pa.int32()),
                    "b": pa.array(["x", "y"], type=pa.string()),
                    "c": pa.array([3, 4], type=pa.int32()),
                }
            )
        )
        path = table.location()

        df = spark.read.format("iceberg").load(path).select("c", "a")
        rows = sorted([(r[0], r[1]) for r in df.collect()])
        assert rows == [(3, 1), (4, 2)]
    finally:
        catalog.drop_table(identifier)


def test_nested_struct_projection_and_nulls(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.test_projection_nested"
    inner = StructType(
        NestedField(10, "x", IntegerType(), required=False), NestedField(11, "y", StringType(), required=False)
    )
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=False),
        NestedField(2, "s", inner, required=False),
    )
    table = catalog.create_table(identifier=identifier, schema=schema)
    try:
        struct_type = pa.struct([("x", pa.int32()), ("y", pa.string())])
        t1 = pa.Table.from_arrays(
            [
                pa.array([1], type=pa.int32()),
                pa.array([None], type=struct_type),
            ],
            names=["id", "s"],
        )
        t2 = pa.Table.from_arrays(
            [
                pa.array([2], type=pa.int32()),
                pa.array([{"x": 7, "y": "z"}], type=struct_type),
            ],
            names=["id", "s"],
        )
        table.append(t1)
        table.append(t2)
        path = table.location()

        df = spark.read.format("iceberg").load(path).select("id", "s.x")
        result = sorted([(r[0], r[1]) for r in df.collect()])
        assert result == [(1, None), (2, 7)]
    finally:
        catalog.drop_table(identifier)
