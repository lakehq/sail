import pandas as pd
import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.errors import AnalysisException, SparkRuntimeException
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
        (F.col("adjusted") == 12) & (F.col("regionality") != "DOMESTIC")  # noqa: PLR2004
    )
    assert result.collect() == [Row(key="b", adjusted=12)]


@pytest.mark.parametrize("hide_alias", [False, True], ids=["visible-alias", "intermediate-alias"])
def test_filter_missing_attribute_preserves_alias_precedence(filter_source, hide_alias):
    projected = filter_source.select("key", F.lit("CURRENT").alias("regionality"))
    if hide_alias:
        projected = projected.select("key")
    result = projected.where((F.col("regionality") == "CURRENT") & (F.col("value") == 2))  # noqa: PLR2004
    expected = Row(key="b") if hide_alias else Row(key="b", regionality="CURRENT")
    assert result.collect() == [expected]


def test_filter_missing_attributes_through_nested_projections(filter_source):
    result = (
        filter_source.select("key", "regionality")
        .select("key")
        .where((F.col("value") == 2) & (F.col("regionality") != "DOMESTIC"))  # noqa: PLR2004
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
        projected = projected.where(F.col("value") < 3)  # noqa: PLR2004
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


@pytest.mark.skipif(pyspark_version() < (4,), reason="DataFrame.exists requires PySpark 4+")
@pytest.mark.parametrize("failure", ["nested-field", "ambiguous-name"])
def test_filter_correlated_subquery_discards_failed_descendant_bindings(spark, failure):
    outer = spark.createDataFrame([(1, "OUTER"), (2, "OUTER")], "x int, marker string").alias("o")
    if failure == "nested-field":
        columns = [F.struct(F.lit(42).alias("y")).alias("o")]
        reference = "o.x"
    else:
        columns = [F.lit(41).alias("x"), F.lit(42).alias("x")]
        reference = "x"

    inner = (
        spark.range(1)
        .select(*columns, F.lit("INNER").alias("marker"))
        .select(F.lit(1).alias("keep"))
        .where((F.col("marker").outer() == "OUTER") & (F.col(reference).outer() == 1))
    )
    assert outer.where(inner.exists()).collect() == [Row(x=1, marker="OUTER")]


@pytest.mark.skipif(pyspark_version() < (4,), reason="DataFrame.exists requires PySpark 4+")
@pytest.mark.parametrize("failure", ["nested-field", "ambiguous-name"])
@pytest.mark.parametrize("reverse_predicate", [False, True])
def test_filter_descendant_fallback_preserves_earlier_bindings(spark, failure, reverse_predicate):
    outer = spark.createDataFrame([(1, "OUTER"), (2, "OUTER")], "x int, marker string").alias("o")
    if failure == "nested-field":
        columns = [F.struct(F.lit(42).alias("y")).alias("o")]
        reference = "o.x"
    else:
        columns = [F.lit(41).alias("x"), F.lit(42).alias("x")]
        reference = "x"

    predicates = [F.col("marker").outer() == "OUTER", F.col(reference).outer() == 1]
    if reverse_predicate:
        predicates.reverse()
    inner = (
        spark.range(1)
        .select(*columns)
        .select(F.lit("NEAR").alias("marker"))
        .select(F.lit(1).alias("keep"))
        .where(predicates[0] & predicates[1])
    )
    assert outer.where(inner.exists()).collect() == []


@pytest.mark.skipif(pyspark_version() < (4,), reason="DataFrame.exists requires PySpark 4+")
def test_filter_descendant_fallback_discards_multiple_failed_outputs(spark):
    outer = spark.createDataFrame([(1, 2)], "x int, z int")
    inner = (
        spark.range(1)
        .select(F.lit(41).alias("x"), F.lit(42).alias("x"))
        .select(F.lit(51).alias("z"), F.lit(52).alias("z"))
        .select(F.lit(1).alias("keep"))
        .where((F.col("x").outer() == 1) & (F.col("z").outer() == 2))  # noqa: PLR2004
    )
    assert outer.where(inner.exists()).collect() == [Row(x=1, z=2)]


@pytest.mark.skipif(pyspark_version() < (4,), reason="DataFrame.exists requires PySpark 4+")
@pytest.mark.parametrize("failure", ["visible-root", "descendant-type"])
def test_filter_descendant_fallback_preserves_resolution_errors(spark, failure):
    if failure == "visible-root":
        outer = spark.createDataFrame([(1,)], "x int").alias("o")
        inner = spark.range(1).select(F.struct(F.lit(42).alias("y")).alias("o")).where(F.col("o.x").outer() == 1)
    else:
        outer = spark.createDataFrame([([1],)], "xs array<int>")
        inner = (
            spark.range(1)
            .select(F.lit(1).alias("xs"))
            .select(F.lit(1).alias("keep"))
            .where(F.array_max(F.col("xs").outer()) == 1)
        )
    with pytest.raises((AnalysisException, SparkRuntimeException)):
        outer.where(inner.exists()).collect()


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
        result = filter_source.select("key").where(f"({columns}) IN (SELECT {columns} FROM filter_lookup)")  # noqa: S608
        assert result.collect() == [Row(key="b")]
    finally:
        spark.catalog.dropTempView("filter_lookup")


@pytest.mark.parametrize(
    "predicate",
    [
        "value > 1 AND EXISTS (SELECT 1 FROM filter_recovered_lookup WHERE lookup_value = value)",
        "EXISTS (SELECT 1 FROM filter_recovered_lookup WHERE lookup_value = value) AND value > 1",
        "value > 1 AND (SELECT MAX(lookup_value) FROM filter_recovered_lookup WHERE lookup_value = value) > 1",
        "value IN (SELECT lookup_value FROM filter_recovered_lookup WHERE lookup_value = value)",
    ],
    ids=["exists-after-local", "exists-before-local", "scalar", "in"],
)
@pytest.mark.xfail(
    not is_jvm_spark(),
    reason="Correlated subqueries cannot yet use inputs recovered by the enclosing filter's predicate",
    strict=True,
)
def test_filter_recovered_attributes_are_visible_to_correlated_subqueries(spark, filter_source, predicate):
    spark.createDataFrame([(2,)], "lookup_value int").createOrReplaceTempView("filter_recovered_lookup")
    try:
        result = filter_source.select("key").where(predicate)
        assert result.collect() == [Row(key="b")]
    finally:
        spark.catalog.dropTempView("filter_recovered_lookup")


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


@pytest.mark.parametrize(
    ("with_replacement", "name"),
    [(False, "rand_value"), (True, "rand_value"), (True, "array_value")],
)
def test_filter_missing_attribute_ignores_sampling_auxiliaries(spark, with_replacement, name):
    source = spark.range(1).select(F.lit("keep").alias("key"), F.lit(100).alias(name))
    sampled = source.select("key").sample(with_replacement, 10.0 if with_replacement else 1.0, 42)
    expected = sampled.collect()
    assert expected

    result = sampled.where(F.col(name) == 100)  # noqa: PLR2004

    assert result.collect() == expected
    assert result.schema == sampled.schema


def test_filter_missing_grouping_attribute(filter_source):
    result = filter_source.groupBy("regionality").count().select("count").where(F.col("regionality") == "INTRA")
    assert result.collect() == [Row(count=1)]


@pytest.fixture(params=["local", "global"])
def filter_temp_view(spark, filter_source, request):
    projected = filter_source.select("key", "regionality")
    name = "filter_projected_view"
    if request.param == "global":
        projected.createOrReplaceGlobalTempView(name)
        try:
            yield f"global_temp.{name}"
        finally:
            spark.catalog.dropGlobalTempView(name)
    else:
        projected.createOrReplaceTempView(name)
        try:
            yield name
        finally:
            spark.catalog.dropTempView(name)


def test_filter_temp_view_rejects_attribute_removed_before_registration(spark, filter_temp_view):
    with pytest.raises(AnalysisException):
        spark.table(filter_temp_view).where(F.col("value") == 2).collect()  # noqa: PLR2004


def test_filter_temp_view_recovers_attribute_removed_after_read(spark, filter_temp_view):
    result = spark.table(filter_temp_view).select("key").where(F.col("regionality") != "DOMESTIC")
    assert result.collect() == [Row(key="b")]


@pytest.mark.parametrize("reference", ["alias", "cte"])
def test_filter_temp_view_visible_qualified_attributes(spark, filter_temp_view, reference):
    if reference == "alias":
        query = f"SELECT v.key FROM {filter_temp_view} v WHERE v.regionality = 'INTRA'"  # noqa: S608
    else:
        query = f"""
            WITH visible AS (SELECT key, regionality FROM {filter_temp_view})
            SELECT visible.key FROM visible WHERE visible.regionality = 'INTRA'
        """  # noqa: S608
    assert spark.sql(query).collect() == [Row(key="b")]


@pytest.mark.parametrize("subquery", ["scalar", "exists"])
def test_filter_temp_views_preserve_correlated_attributes(spark, subquery):
    # Match the pandas-backed registrations used by the TPC-H tests. Their 16/9-column layouts
    # reproduce the stored lineitem/part column identity collision without external data.
    lineitem = spark.createDataFrame(
        pd.DataFrame(
            [
                (10, 1, 1, 1, 1.0, 70.0, 0.0, 0.0, "N", "O", "1996-01-01", "1996-01-01", "1996-01-01", "", "", ""),
                (11, 1, 1, 1, 100.0, 700.0, 0.0, 0.0, "N", "O", "1996-01-01", "1996-01-01", "1996-01-01", "", "", ""),
            ],
            columns=[
                "l_orderkey",
                "l_partkey",
                "l_suppkey",
                "l_linenumber",
                "l_quantity",
                "l_extendedprice",
                "l_discount",
                "l_tax",
                "l_returnflag",
                "l_linestatus",
                "l_shipdate",
                "l_commitdate",
                "l_receiptdate",
                "l_shipinstruct",
                "l_shipmode",
                "l_comment",
            ],
        )
    )
    part = spark.createDataFrame(
        pd.DataFrame(
            [(1, "part", "manufacturer", "Brand#42", "type", 1, "LG BAG", 1.0, "")],
            columns=[
                "p_partkey",
                "p_name",
                "p_mfgr",
                "p_brand",
                "p_type",
                "p_size",
                "p_container",
                "p_retailprice",
                "p_comment",
            ],
        )
    )
    lineitem.createOrReplaceTempView("filter_lineitem")
    part.createOrReplaceTempView("filter_part")
    if subquery == "scalar":
        predicate = """
            l_quantity < (
                SELECT 0.2 * AVG(l_quantity) FROM filter_lineitem WHERE l_partkey = p_partkey
            )
        """
    else:
        predicate = """
            l_quantity = 1 AND EXISTS (
                SELECT 1 FROM filter_lineitem WHERE l_partkey = p_partkey AND l_quantity > 50
            )
        """
    try:
        result = spark.sql(f"""
            SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
            FROM filter_lineitem, filter_part
            WHERE p_partkey = l_partkey AND p_brand = 'Brand#42' AND p_container = 'LG BAG'
                AND {predicate}
        """)  # noqa: S608
        assert result.collect() == [Row(avg_yearly=10.0)]
    finally:
        spark.catalog.dropTempView("filter_lineitem")
        spark.catalog.dropTempView("filter_part")


@pytest.mark.parametrize("reference", ["outer-unqualified", "outer-qualified", "visible"])
def test_filter_physical_column_name_preserves_correlated_scope(spark, tmp_path, reference):
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "part.parquet"
    pq.write_table(pa.table({"#0": [2]}), path)
    outer = "range(1) outer_t" if reference == "outer-qualified" else "range(1)"
    predicates = {"outer-unqualified": "id = 0", "outer-qualified": "outer_t.id = 0", "visible": "`#0` = 2"}
    result = spark.sql(f"""
        SELECT id FROM {outer}
        WHERE EXISTS (SELECT 1 FROM parquet.`{path}` WHERE {predicates[reference]})
    """)  # noqa: S608
    assert result.collect() == [Row(id=0)]


@pytest.mark.skipif(pyspark_version() < (4,), reason="DataFrame.exists requires PySpark 4+")
@pytest.mark.parametrize("with_window", [False, True], ids=["sorted-view", "window-over-sorted-view"])
def test_filter_temp_view_boundary_survives_window_rewrite(spark, with_window):
    name = "filter_sorted_physical_view"
    spark.createDataFrame(pd.DataFrame({"#0": [2]})).orderBy("#0").createOrReplaceTempView(name)
    try:
        inner = spark.table(name)
        if with_window:
            inner = inner.withColumn("rn", F.row_number().over(Window.orderBy("#0")))
        # A window can rebuild the stored sort and its enclosing projections. The
        # filter must still resolve id from the outer range, not the stored #0 field.
        inner = inner.where(F.col("id").outer() == 0)
        assert spark.range(1).where(inner.exists()).collect() == [Row(id=0)]
    finally:
        spark.catalog.dropTempView(name)


def test_filter_empty_view_does_not_hide_unrelated_missing_attribute(spark):
    name = "filter_empty_boundary_view"
    spark.range(1).select().createOrReplaceTempView(name)
    try:
        right = spark.range(1).select().where(F.col("id") == 0)
        result = spark.table(name).crossJoin(right)
        assert result.count() == 1
        assert result.columns == []
    finally:
        spark.catalog.dropTempView(name)


def test_filter_unpivot_rejects_removed_input_attribute(spark):
    source = spark.createDataFrame([(1, 10, 20), (2, 30, 40)], "id int, a int, b int")
    unpivoted = source.unpivot("id", ["a", "b"], "variable", "value")
    with pytest.raises(AnalysisException):
        unpivoted.where(F.col("a") == 10).collect()  # noqa: PLR2004


def test_filter_unpivot_recovers_projected_output_attributes(spark):
    source = spark.createDataFrame([(1, 10, 20), (2, 30, 40)], "id int, a int, b int")
    unpivoted = source.unpivot("id", ["a", "b"], "variable", "value")
    projected = unpivoted.select("id")
    result = projected.where((F.col("variable") == "a") & (F.col("value") == 10))  # noqa: PLR2004
    assert result.collect() == [Row(id=1)]
    assert result.schema == projected.schema


@pytest.mark.parametrize("operation", ["explode", "inline", "monotonic-id", "partition-id", "window"])
@pytest.mark.parametrize("has_empty_column", [False, True], ids=["unknown-name", "missing-input"])
def test_filter_missing_attribute_ignores_internal_auxiliaries(spark, operation, has_empty_column):
    source = spark.createDataFrame([(1, [1, 2])], "id int, values array<int>")
    if has_empty_column:
        source = source.withColumn("", F.lit(100))
    if operation == "explode":
        generated = F.explode("values")
    elif operation == "inline":
        generated = F.inline(F.array(F.struct(F.col("id").alias("item"))))
    elif operation == "monotonic-id":
        generated = F.monotonically_increasing_id()
    elif operation == "partition-id":
        generated = F.spark_partition_id()
    else:
        source = source.orderBy("id")
        generated = F.row_number().over(Window.orderBy("id"))
    projected = source.select(generated.alias("generated"))
    predicate = F.col("") == 100  # noqa: PLR2004

    if has_empty_column:
        expected = projected.collect()
        assert expected
        result = projected.where(predicate)
        assert result.collect() == expected
        assert result.schema == projected.schema
    else:
        with pytest.raises(AnalysisException):
            projected.where(predicate).collect()
