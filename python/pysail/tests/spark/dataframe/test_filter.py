import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import Row, Window
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version


@pytest.fixture
def filter_source(spark):
    return spark.createDataFrame(
        [("a", "DOMESTIC", 1), ("b", "INTRA", 2), ("c", None, 3)],
        "key string, regionality string, value int",
    )


@pytest.mark.parametrize("predicate_kind", ["column", "sql", "bound-column"])
def test_filter_missing_projection_attribute(filter_source, predicate_kind):
    source = filter_source
    if predicate_kind == "sql":
        predicate = "regionality != 'DOMESTIC'"
    elif predicate_kind == "bound-column":
        predicate = source.regionality != "DOMESTIC"
    else:
        predicate = F.col("regionality") != "DOMESTIC"

    result = source.select("key", "value").where(predicate)

    assert result.collect() == [Row(key="b", value=2)]
    assert result.schema == source.select("key", "value").schema


def test_filter_missing_attribute_with_visible_alias(filter_source):
    result = filter_source.select("key", (F.col("value") + 10).alias("adjusted")).where(
        (F.col("adjusted") == 12) & (F.col("regionality") != "DOMESTIC")
    )
    assert result.collect() == [Row(key="b", adjusted=12)]


@pytest.mark.parametrize("hide_alias", [False, True], ids=["visible-alias", "intermediate-alias"])
def test_filter_missing_attribute_preserves_alias_precedence(filter_source, hide_alias):
    projected = filter_source.select("key", F.lit("CURRENT").alias("regionality"))
    if hide_alias:
        projected = projected.select("key")
    result = projected.where((F.col("regionality") == "CURRENT") & (F.col("value") == 2))
    expected = Row(key="b") if hide_alias else Row(key="b", regionality="CURRENT")
    assert result.collect() == [expected]


def test_filter_missing_attributes_through_nested_projections(filter_source):
    result = (
        filter_source.select("key", "regionality")
        .select("key")
        .where((F.col("value") == 2) & (F.col("regionality") != "DOMESTIC"))
    )
    assert result.collect() == [Row(key="b")]


def test_filter_missing_qualified_attribute(filter_source):
    result = (
        filter_source.alias("origin")
        .select("origin.key", "origin.value")
        .where(F.col("origin.regionality") != "DOMESTIC")
    )
    assert result.collect() == [Row(key="b", value=2)]


def test_filter_missing_struct_attribute_in_expression(spark):
    source = spark.createDataFrame(
        [("a", ("DOMESTIC", "1")), ("b", ("INTRA", "2"))],
        "key string, payload struct<regionality:string,value:string>",
    )
    result = source.select("key").where(
        (F.lower("payload.regionality") != "domestic") & (F.col("payload.value").cast("int") > 1)
    )
    assert result.collect() == [Row(key="b")]


def test_filter_missing_attribute_preserves_nulls_and_output_schema(spark):
    source = spark.createDataFrame(
        [("a", "DOMESTIC", 1), ("b", None, 2)],
        StructType(
            [
                StructField("key", StringType(), False, {"description": "source key"}),
                StructField("regionality", StringType(), True),
                StructField("value", IntegerType(), False),
            ]
        ),
    )
    projected = source.select("value", F.col("key").alias("renamed", metadata={"description": "output key"}))
    result = projected.where(F.col("regionality").eqNullSafe(None))
    assert result.collect() == [Row(value=2, renamed="b")]
    assert result.schema == projected.schema


@pytest.mark.parametrize("operation", ["filter", "limit", "sort", "repartition"])
def test_filter_missing_attribute_through_unary_plan(filter_source, operation):
    projected = filter_source.select("key", "value")
    if operation == "filter":
        projected = projected.where(F.col("value") < 3)
    elif operation == "limit":
        projected = projected.orderBy("key").limit(1)
    elif operation == "sort":
        projected = projected.orderBy(F.desc("value"))
    else:
        projected = projected.repartition(2, "key")

    result = projected.where(F.col("regionality") != "DOMESTIC")
    assert result.collect() == ([] if operation == "limit" else [Row(key="b", value=2)])
    assert result.schema == projected.schema


def test_filter_missing_attribute_preserves_window_input(filter_source):
    projected = filter_source.select("key", "value").withColumn(
        "position", F.row_number().over(Window.orderBy("value"))
    )
    result = projected.where(F.col("regionality") != "DOMESTIC")
    assert result.collect() == [Row(key="b", value=2, position=2)]
    assert result.schema == projected.schema


def test_filter_missing_attribute_through_explode(filter_source):
    projected = filter_source.select("key", F.explode(F.array("value")).alias("item"))
    result = projected.where(F.col("regionality") != "DOMESTIC")
    assert result.collect() == [Row(key="b", item=2)]
    assert result.schema == projected.schema


def test_filter_missing_attribute_from_join_output(spark, filter_source):
    keys = spark.createDataFrame([("a",), ("b",)], "key string")
    result = filter_source.join(keys, ["key"]).select("key").where(F.col("regionality") != "DOMESTIC")
    assert result.collect() == [Row(key="b")]


@pytest.mark.parametrize("boundary", ["alias", "aggregate", "join", "union"])
def test_filter_missing_attribute_rejects_resolution_boundaries(spark, filter_source, boundary):
    projected = filter_source.select("key", "value")
    if boundary == "alias":
        projected = projected.alias("output")
    elif boundary == "aggregate":
        projected = filter_source.groupBy("key").count().select("key")
    elif boundary == "join":
        keys = spark.createDataFrame([("a",), ("b",)], "key string")
        projected = projected.join(keys, ["key"])
    else:
        projected = projected.union(projected)
    with pytest.raises(AnalysisException):
        projected.where(F.col("regionality") != "DOMESTIC").collect()


def test_filter_missing_attribute_rejects_unknown_name(filter_source):
    with pytest.raises(AnalysisException):
        filter_source.select("key").where(F.col("unknown") == 1).collect()


def test_filter_missing_attribute_rejects_ambiguous_name(spark):
    source = spark.createDataFrame([("a", 1, 2)], "key string, value int, value int")
    with pytest.raises(AnalysisException):
        source.select("key").where(F.col("value") == 1).collect()


def test_filter_does_not_resolve_field_of_shadowed_struct(spark):
    source = spark.createDataFrame([((1,),)], "payload struct<x:int>")
    projected = source.select(F.struct(F.lit(1).alias("y")).alias("payload"))
    with pytest.raises(AnalysisException):
        projected.where(F.col("payload.x") == 1).collect()


@pytest.mark.parametrize("failure", ["nested-field", "ambiguous-name"])
@pytest.mark.xfail(
    not is_jvm_spark(),
    reason="Missing-reference recovery across invalid intermediate projections requires staged expression resolution",
    strict=True,
)
def test_filter_missing_attributes_discard_failed_projection_resolution(spark, failure):
    if failure == "nested-field":
        source = spark.createDataFrame([((1,), "SOURCE")], "payload struct<x:int>, marker string")
        intermediate = source.select(F.struct(F.lit(2).alias("y")).alias("payload"), F.lit("NEAR").alias("marker"))
        predicate = (F.col("marker") == "SOURCE") & (F.col("payload.x") == 1)
    else:
        source = spark.createDataFrame([(1, "SOURCE")], "x int, marker string")
        intermediate = source.select(F.lit(2).alias("x"), F.lit(3).alias("x"), F.lit("NEAR").alias("marker"))
        predicate = (F.col("x") == 1) & (F.col("marker") == "SOURCE")

    # Failure to resolve one attribute discards all bindings from the intermediate projection.
    result = intermediate.select(F.lit("keep").alias("keep")).where(predicate)
    assert result.collect() == [Row(keep="keep")]


def test_filter_visible_predicate_preserves_projection(filter_source):
    projected = filter_source.select("key", F.lit("CURRENT").alias("regionality"))
    result = projected.where((F.col("key") == "b") & (F.col("regionality") == "CURRENT"))
    assert result.collect() == [Row(key="b", regionality="CURRENT")]
    assert result.schema == projected.schema


@pytest.mark.parametrize("with_missing_attribute", [False, True])
@pytest.mark.skipif(pyspark_version() < (4,), reason="Wildcard expansion in a filter requires Spark 4+")
def test_filter_wildcard_uses_visible_projection(filter_source, with_missing_attribute):
    predicate = F.to_json(F.struct("*")) == '{"key":"b"}'
    if with_missing_attribute:
        predicate = predicate & (F.col("regionality") != "DOMESTIC")
    result = filter_source.select("key").where(predicate)
    assert result.collect() == [Row(key="b")]


@pytest.mark.skipif(pyspark_version() < (4,), reason="Wildcard expansion in a filter requires Spark 4+")
def test_filter_struct_wildcard_uses_visible_projection(filter_source):
    source = filter_source.select(F.struct("key", "value").alias("payload"))
    projected = source.select(F.struct(F.col("payload.key").alias("key")).alias("payload"))
    result = projected.where(F.to_json(F.struct("payload.*")) == '{"key":"b"}')
    assert result.collect() == [Row(payload=Row(key="b"))]


@pytest.mark.skipif(pyspark_version() < (4,), reason="Column regex expansion in a filter requires Spark 4+")
def test_filter_column_regex_uses_visible_projection(filter_source):
    projected = filter_source.select("key")
    result = projected.where(F.to_json(F.struct(projected.colRegex("`.*`"))) == '{"key":"b"}')
    assert result.collect() == [Row(key="b")]


@pytest.mark.parametrize("columns", ["regionality", "regionality, value"])
def test_filter_missing_attributes_in_subquery(spark, filter_source, columns):
    spark.createDataFrame([("INTRA", 2)], "regionality string, value int").createOrReplaceTempView("filter_lookup")
    try:
        result = filter_source.select("key").where(f"({columns}) IN (SELECT {columns} FROM filter_lookup)")
        assert result.collect() == [Row(key="b")]
    finally:
        spark.catalog.dropTempView("filter_lookup")


@pytest.mark.parametrize("with_missing_attribute", [False, True])
def test_filter_correlated_subquery_preserves_visible_alias(spark, filter_source, with_missing_attribute):
    spark.createDataFrame([(12,)], "lookup_value int").createOrReplaceTempView("filter_correlated_lookup")
    try:
        predicate = "EXISTS (SELECT 1 FROM filter_correlated_lookup WHERE lookup_value = value)"
        if with_missing_attribute:
            predicate += " AND regionality = 'INTRA'"
        result = filter_source.select("key", (F.col("value") + 10).alias("value")).where(predicate)
        assert result.collect() == [Row(key="b", value=12)]
    finally:
        spark.catalog.dropTempView("filter_correlated_lookup")


@pytest.mark.parametrize("predicate_kind", ["column", "sql"])
def test_filter_missing_attributes_in_lambda(spark, predicate_kind):
    source = spark.createDataFrame(
        [("a", [1, 2], 3, 100), ("b", [1, 3], 2, -100), ("c", None, 1, 0), ("d", [None], 2, 0)],
        "key string, values array<int>, threshold int, x int",
    )
    predicate = (
        "exists(values, x -> x > threshold)"
        if predicate_kind == "sql"
        else F.exists("values", lambda x: x > F.col("threshold"))
    )
    result = source.select("key").where(predicate)
    assert result.collect() == [Row(key="b")]


@pytest.mark.xfail(
    not is_jvm_spark(),
    reason="Sail lowers DataFrame deduplication to Distinct, which cannot expose missing attributes",
    strict=True,
)
def test_filter_missing_attribute_through_dataframe_distinct(filter_source):
    result = filter_source.select("key", "value").distinct().where(F.col("regionality") != "DOMESTIC")
    assert result.collect() == [Row(key="b", value=2)]


def test_filter_missing_grouping_attribute(filter_source):
    result = filter_source.groupBy("regionality").count().select("count").where(F.col("regionality") == "INTRA")
    assert result.collect() == [Row(count=1)]
