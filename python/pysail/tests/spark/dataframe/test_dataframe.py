import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.errors import AnalysisException
from pyspark.sql import Row
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.types import IntegerType, LongType, StructField, StructType
from pyspark.sql.window import Window


def test_dataframe_drop(spark):
    df = spark.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
    df2 = spark.createDataFrame([Row(height=80, name="Tom"), Row(height=85, name="Bob")])

    assert_frame_equal(
        df.drop("age").sort("name").toPandas(),
        pd.DataFrame({"name": ["Alice", "Bob", "Tom"]}),
    )
    assert_frame_equal(
        df.drop(df.age).sort("name").toPandas(),
        pd.DataFrame({"name": ["Alice", "Bob", "Tom"]}),
    )

    assert_frame_equal(
        df.join(df2, df.name == df2.name, "inner").drop("name").sort("age").toPandas(),
        pd.DataFrame({"age": [14, 16], "height": [80, 85]}),
    )

    df3 = df.join(df2)
    assert_frame_equal(
        df3.select(
            df["age"],
            df["name"].alias("name_left"),
            df2["height"],
            df2["name"].alias("name_right"),
        )
        .sort("name_left", "name_right")
        .toPandas(),
        pd.DataFrame(
            {
                "age": [23, 23, 16, 16, 14, 14],
                "name_left": ["Alice", "Alice", "Bob", "Bob", "Tom", "Tom"],
                "height": [85, 80, 85, 80, 85, 80],
                "name_right": ["Bob", "Tom", "Bob", "Tom", "Bob", "Tom"],
            }
        ),
    )

    assert_frame_equal(
        df3.drop("name").sort("age", "height").toPandas(),
        pd.DataFrame({"age": [14, 14, 16, 16, 23, 23], "height": [80, 85, 80, 85, 80, 85]}),
    )

    with pytest.raises(Exception, match="AMBIGUOUS_REFERENCE"):
        df3.drop(col("name")).toPandas()

    df4 = df.withColumn("a.b.c", lit(1))
    assert_frame_equal(
        df4.sort("age").toPandas(),
        pd.DataFrame({"age": [14, 16, 23], "name": ["Tom", "Bob", "Alice"], "a.b.c": [1, 1, 1]}).astype(
            {"a.b.c": "int32"}
        ),
    )

    assert_frame_equal(
        df4.drop("a.b.c").sort("age").toPandas(),
        pd.DataFrame({"age": [14, 16, 23], "name": ["Tom", "Bob", "Alice"]}),
    )

    assert_frame_equal(
        df4.drop(col("a.b.c")).sort("age").toPandas(),
        pd.DataFrame({"age": [14, 16, 23], "name": ["Tom", "Bob", "Alice"], "a.b.c": [1, 1, 1]}).astype(
            {"a.b.c": "int32"}
        ),
    )


def test_dataframe_with_column_alias(spark):
    df = spark.createDataFrame(
        schema="id INTEGER, value STRING",
        data=[(1, "bar"), (2, "foo")],
    )

    # Using alias and referencing a single column works
    assert_frame_equal(
        df.alias("a").withColumn("col1", col("a.id")).sort("id").toPandas(),
        pd.DataFrame({"id": [1, 2], "value": ["bar", "foo"], "col1": [1, 2]}).astype({"id": "int32", "col1": "int32"}),
    )

    # Using alias and referencing multiple columns in chained withColumn calls
    assert_frame_equal(
        df.alias("a").withColumn("col1", col("a.id")).withColumn("col2", col("a.value")).sort("id").toPandas(),
        pd.DataFrame({"id": [1, 2], "value": ["bar", "foo"], "col1": [1, 2], "col2": ["bar", "foo"]}).astype(
            {"id": "int32", "col1": "int32"}
        ),
    )

    # More than two chained withColumn calls with alias
    assert_frame_equal(
        df.alias("a")
        .withColumn("col1", col("a.id"))
        .withColumn("col2", col("a.value"))
        .withColumn("col3", col("a.id"))
        .sort("id")
        .toPandas(),
        pd.DataFrame(
            {
                "id": [1, 2],
                "value": ["bar", "foo"],
                "col1": [1, 2],
                "col2": ["bar", "foo"],
                "col3": [1, 2],
            }
        ).astype({"id": "int32", "col1": "int32", "col3": "int32"}),
    )


def test_with_metadata(spark):
    df = spark.sql("SELECT 1 AS a")
    assert df.schema["a"].metadata == {}
    assert df.withMetadata("a", {"m": "x"}).schema["a"].metadata == {"m": "x"}
    assert df.withMetadata("a", {"m": "x"}).withMetadata("a", {"n": "y"}).schema["a"].metadata == {"n": "y"}
    assert df.withMetadata("a", {"m": "x"}).withMetadata("a", {}).schema["a"].metadata == {}


def reverse_sorted_map_in_pandas(df):
    def reverse_batches(iterator):
        for pdf in iterator:
            yield pd.DataFrame({"id": pdf["id"].iloc[::-1].to_numpy()})

    return df.orderBy(col("id")).mapInPandas(reverse_batches, schema="id long")


def test_map_in_pandas_reordered_rows_can_be_sorted_again(spark):
    actual = reverse_sorted_map_in_pandas(spark.range(0, 4, 1, 1)).orderBy(col("id")).toPandas()
    expected = pd.DataFrame({"id": [0, 1, 2, 3]}, dtype="int64")

    assert_frame_equal(actual, expected)


def test_map_in_pandas_reordering_does_not_satisfy_window_ordering(spark):
    window = Window.orderBy(col("id"))

    actual = (
        reverse_sorted_map_in_pandas(spark.range(0, 4, 1, 1))
        .select("id", row_number().over(window).alias("rn"))
        .orderBy(col("id"))
        .toPandas()
    )
    expected = pd.DataFrame({"id": [0, 1, 2, 3], "rn": [1, 2, 3, 4]}).astype({"rn": "int32"})

    assert_frame_equal(actual, expected)


@pytest.mark.timeout(60)
def test_to_schema_can_be_sorted_after_a_type_change(spark):
    # The column the reconciliation casts is a new column rather than the one it reads, so it needs
    # a field ID of its own. Naming it after the field it reads leaves the projection with an output
    # field that has the same ID as the one below it, and resolving a sort key against that plan
    # does not terminate: it takes all the memory of the process with it, so the test has a timeout
    # rather than hanging the run.
    df = spark.createDataFrame([(1, 2)], "a int, b int")
    widened = StructType([StructField("a", LongType()), StructField("b", IntegerType())])
    out = df.to(widened)

    assert out.sort("a").count() == 1
    # `Sort` and `Filter` resolve a `df["col"]` reference against the plan that carries the id, so
    # the reconciled column keeps the plan IDs of the column it reads.
    assert out.sort(df["a"]).count() == 1
    assert out.filter(df["a"] == 1).count() == 1
    assert [row.asDict() for row in out.sort("a").collect()] == [{"a": 1, "b": 2}]


def test_to_schema_reports_the_name_of_the_target_schema(spark):
    # Spark names the reconciled column after the target schema, not after the column it reads:
    # `Project.matchSchema` passes the name of the target field to `createNewColumn`. The name has
    # to be read off the rows, since the PySpark client answers `columns` and `schema` from the
    # schema it asked for rather than from the one the server returned.
    df = spark.createDataFrame([(1, 2)], "a int, b int")
    renamed = StructType([StructField("A", LongType()), StructField("B", IntegerType())])

    assert list(df.to(renamed).collect()[0].asDict()) == ["A", "B"]
    assert df.to(renamed).select("*").columns == ["A", "B"]


def test_to_schema_keeps_the_qualifier_of_a_column_it_does_not_change(spark):
    # `Project.matchSchema` renames a pass-through column with `Attribute.withName`, which carries
    # the qualifier of the relation over, so `t.a` still resolves after `to()`.
    df = spark.createDataFrame([(1, 2)], "a int, b int").alias("t")
    unchanged = StructType([StructField("a", IntegerType()), StructField("b", IntegerType())])
    out = df.to(unchanged)

    assert out.select("t.a").collect() == [Row(a=1)]
    assert out.filter("t.a = 1").count() == 1
    assert out.selectExpr("t.b").columns == ["b"]
    assert out.select("t.*").columns == ["a", "b"]


def test_to_schema_drops_the_qualifier_of_a_column_it_casts(spark):
    # A reconciled column is wrapped in an `Alias`, which Spark builds without a qualifier, so
    # `t.a` no longer resolves while `t.b`, which was not cast, still does.
    df = spark.createDataFrame([(1, 2)], "a int, b int").alias("t")
    widened = StructType([StructField("a", LongType()), StructField("b", IntegerType())])
    out = df.to(widened)

    # No `match=`: Sail says the attribute is missing from the schema and Spark says the column
    # cannot be resolved, with no wording in common, so matching would tie this to one engine.
    with pytest.raises(AnalysisException):
        out.select("t.a").collect()
    assert out.select("t.b").collect() == [Row(b=2)]
    assert out.select("t.*").columns == ["b"]


def test_to_schema_rejects_an_ambiguous_input_column(spark):
    # `Project.matchSchema` raises `AMBIGUOUS_COLUMN_OR_FIELD` when more than one input column
    # matches a field of the target schema, rather than reading the first one.
    df = spark.createDataFrame([(1, 2)], "a int, b int")
    duplicated = df.select(df["a"], df["a"])

    with pytest.raises(AnalysisException, match="AMBIGUOUS_COLUMN_OR_FIELD"):
        duplicated.to(StructType([StructField("a", LongType())])).collect()


def test_to_schema_rejects_an_ambiguous_column_from_a_join(spark):
    left = spark.createDataFrame([(1, 2)], "a int, b int")
    right = spark.createDataFrame([(1, 3)], "a int, c int")
    joined = left.join(right, left["a"] == right["a"])

    with pytest.raises(AnalysisException, match="AMBIGUOUS_COLUMN_OR_FIELD"):
        joined.to(StructType([StructField("a", LongType())])).collect()


def test_to_schema_ambiguity_only_looks_at_the_fields_of_the_target(spark):
    # `matchSchema` iterates the target schema, so an input column that is duplicated but absent
    # from the target is never matched and never reported.
    df = spark.createDataFrame([(1, 2)], "a int, b int")
    duplicated = df.select(df["a"], df["a"], df["b"])

    assert duplicated.to(StructType([StructField("b", LongType())])).collect() == [Row(b=2)]


def test_to_schema_ambiguity_follows_case_sensitivity(spark):
    # The input columns match a target field through `conf.resolver`, which honors
    # `spark.sql.caseSensitive`, and the ambiguity check counts those same matches. So `a` and `A`
    # are two matches by default and exactly one when case sensitivity is on.
    df = spark.createDataFrame([(1, 2)], "a int, A int")
    target = StructType([StructField("a", LongType())])

    with pytest.raises(AnalysisException, match="AMBIGUOUS_COLUMN_OR_FIELD"):
        df.to(target).collect()

    original = spark.conf.get("spark.sql.caseSensitive")
    spark.conf.set("spark.sql.caseSensitive", "true")
    try:
        assert df.to(target).collect() == [Row(a=1)]
    finally:
        spark.conf.set("spark.sql.caseSensitive", original)


def test_to_schema_can_read_one_input_column_into_two_target_fields(spark):
    # Two target fields can match the same input column, and each gets a field of its own. On main
    # they collided on a single identifier and the plan failed to build.
    df = spark.createDataFrame([(1,)], "a int")

    out = df.to(StructType([StructField("a", LongType()), StructField("A", LongType())]))

    # `Row` compares by position, so the names need an assertion of their own.
    assert list(out.collect()[0].asDict()) == ["a", "A"]
    assert out.collect() == [Row(a=1, A=1)]


def test_to_schema_ambiguity_does_not_depend_on_case_when_the_names_are_equal(spark):
    # Case sensitivity decides which columns match, not whether a match is ambiguous: two columns
    # with the very same name are ambiguous under either setting.
    df = spark.createDataFrame([(1, 2)], "a int, b int")
    duplicated = df.select(df["a"], df["a"])

    original = spark.conf.get("spark.sql.caseSensitive")
    spark.conf.set("spark.sql.caseSensitive", "true")
    try:
        with pytest.raises(AnalysisException, match="AMBIGUOUS_COLUMN_OR_FIELD"):
            duplicated.to(StructType([StructField("a", LongType())])).collect()
    finally:
        spark.conf.set("spark.sql.caseSensitive", original)


def test_to_schema_matches_an_exact_name_when_case_sensitive(spark):
    df = spark.createDataFrame([(1, 2)], "a int, b int")

    original = spark.conf.get("spark.sql.caseSensitive")
    spark.conf.set("spark.sql.caseSensitive", "true")
    try:
        assert df.to(StructType([StructField("a", LongType())])).collect() == [Row(a=1)]
    finally:
        spark.conf.set("spark.sql.caseSensitive", original)


def test_sort_by_an_alias_that_shadows_the_column_it_reads(spark):
    # The sort key is rebased onto the expression of the projection, and that expression must not
    # be rebased a second time: an alias named after a column it reads would otherwise substitute
    # into itself forever. This is the same shape as the reconciliation of `to()`, reached through
    # a plain projection instead.
    df = spark.createDataFrame([(1, 20), (2, 10), (1, 5)], "a int, b int")

    assert [row.a for row in df.select((col("b") * 10).alias("a")).sort("a").collect()] == [50, 100, 200]
    assert [row.a for row in df.select(col("a").cast("string").alias("a")).sort(col("a").desc()).collect()] == [
        "2",
        "1",
        "1",
    ]
