import pyarrow as pa
import pytest
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql import types as T  # noqa: N812

from pysail.testing.spark.utils.common import pyspark_version


def test_map_filter_removes_null_metadata_values(spark):
    source = spark.createDataFrame(
        [({"keep": "1", "drop": None},), ({},), (None,)],
        "attributes map<string,string>",
    )
    result = source.select(F.map_filter("attributes", lambda _key, value: value.isNotNull()).alias("filtered"))
    assert [row.filtered for row in result.collect()] == [{"keep": "1"}, {}, None]


@pytest.mark.parametrize("map_expression", ["NULL", "raise_error('boom')"])
def test_map_filter_preserves_null_typed_map_schema(spark, map_expression):
    result = spark.sql(f"SELECT map_filter({map_expression}, true) AS result")
    assert result.schema == T.StructType(
        [T.StructField("result", T.MapType(T.NullType(), T.NullType(), valueContainsNull=True), nullable=True)]
    )


@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize("value_contains_null", [False, True])
def test_map_filter_preserves_input_schema(spark, nullable, value_contains_null):
    map_type = T.MapType(T.StringType(), T.IntegerType(), valueContainsNull=value_contains_null)
    schema = T.StructType([T.StructField("attributes", map_type, nullable=nullable)])
    source = spark.createDataFrame([({"a": 1, "b": 2},)], schema)
    result = source.select(F.map_filter("attributes", lambda _key, value: value > 1).alias("attributes"))
    assert result.schema == schema
    assert result.collect()[0].attributes == {"b": 2}


def test_map_filter_does_not_inherit_input_column_metadata(spark):
    map_type = T.MapType(T.StringType(), T.IntegerType())
    source = spark.createDataFrame(
        [({"a": 1, "b": 2},)],
        T.StructType([T.StructField("m", map_type, metadata={"source": "test"})]),
    )
    result = source.select(F.map_filter("m", lambda _key, value: value > 1).alias("filtered"))
    assert result.schema == T.StructType([T.StructField("filtered", map_type)])
    assert result.collect()[0].filtered == {"b": 2}


def test_map_filter_captures_column_across_batches(spark):
    source = spark.range(9000, numPartitions=1).select(
        "id",
        F.create_map(F.lit("a"), F.col("id"), F.lit("b"), F.col("id") + 1).alias("attributes"),
    )
    result = source.select(
        "id",
        F.map_filter("attributes", lambda key, value: (key == "b") & (value > F.col("id"))).alias("filtered"),
    )
    assert [(row.id, row.filtered) for row in result.orderBy("id").collect()] == [
        (i, {"b": i + 1}) for i in range(9000)
    ]


@pytest.mark.skipif(pyspark_version() < (4,), reason="Arrow Table input requires PySpark 4+")
def test_map_filter_arrow_map_slice_with_hidden_null_entries(spark):
    # The null row has a physical zero-valued entry. Its predicate must not run;
    # the zeros outside the slice likewise must not participate in filtering.
    maps = pa.MapArray.from_arrays(
        [0, 1, 2, 2, 4, 5],
        ["prefix", "hidden", "keep", "drop", "suffix"],
        [0, 0, 2, 4, 0],
        mask=pa.array([False, True, False, False, False]),
    ).slice(1, 3)
    source = spark.createDataFrame(pa.table({"attributes": maps}))
    threshold = 3
    result = source.select(
        F.map_filter("attributes", lambda _key, value: ((F.lit(1) / value) > 0) & (value < threshold)).alias("filtered")
    )
    assert [row.filtered for row in result.collect()] == [None, {}, {"keep": 2}]
