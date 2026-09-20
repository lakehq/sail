import re

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql import Row
from pyspark.sql.functions import col, count, expr, first, lit, row_number, struct
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import transform as spark_transform
from pyspark.sql.types import IntegerType, LongType, MapType, StringType, StructField, StructType
from pyspark.sql.window import Window

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version
from pysail.tests.spark.dataframe.udt import NamedPythonUDT

# `ıd` and `ς` are deliberately confusable with ASCII names: they are what tells the resolver rule
# apart from the lowercasing one.
# ruff: noqa: RUF001, RUF003


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


def test_drop_column_reports_an_ambiguous_reference_with_a_dotted_alias(spark):
    # Dropping a `Column` resolves it as an attribute and fails on ambiguity, unlike dropping a
    # name, which removes every match. The alias is one part that contains a dot, so it has to be
    # reported whole: rendering the qualifier and splitting it on dots would name `x` and `y`.
    left = spark.createDataFrame([(1,)], ["a"]).alias("x.y")
    right = spark.createDataFrame([(2,)], ["a"]).alias("z")

    with pytest.raises(Exception, match=re.escape("could be: [`x.y`.`a`, `z`.`a`].")):
        _ = left.crossJoin(right).drop(col("a")).columns


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


def test_with_column_matches_name_case_insensitively(spark):
    df = spark.createDataFrame([(1, 10), (2, 20)], ["a", "b"])

    # The existing column is replaced in place, and it takes the new name.
    replaced = df.withColumn("A", col("a") + 1)
    assert replaced.columns == ["A", "b"]
    assert [r.asDict() for r in replaced.orderBy("A").collect()] == [{"A": 2, "b": 10}, {"A": 3, "b": 20}]

    assert df.withColumn("a", col("a") + 1).columns == ["a", "b"]
    assert df.withColumn("zz", lit(1)).columns == ["a", "b", "zz"]
    assert df.withColumns({"A": lit(1), "B": lit(2)}).columns == ["A", "B"]

    with pytest.raises(Exception, match="COLUMN_ALREADY_EXISTS"):
        _ = df.withColumns({"a": lit(1), "A": lit(2)}).columns

    # The first duplicate in alphabetical order is the one reported.
    with pytest.raises(Exception, match="The column `a` already exists"):
        _ = df.withColumns({"z": lit(1), "a": lit(2), "Z": lit(3), "A": lit(4)}).columns


def test_with_columns_reports_a_duplicate_name_the_way_the_analyzer_quotes_it(spark):
    # The name reaches the message as one string, so it is parsed again before each of its parts is
    # quoted: a name that contains a dot is reported as several parts, and a name the user already
    # quoted keeps its back quotes single rather than having them doubled.
    df = spark.createDataFrame([(1,)], ["a"])

    with pytest.raises(Exception, match=r"The column `x`\.`y` already exists"):
        _ = df.withColumns({"x.y": lit(1), "X.Y": lit(2)}).columns

    with pytest.raises(Exception, match=r"The column `x\.y` already exists"):
        _ = df.withColumns({"`x.y`": lit(1), "`X.Y`": lit(2)}).columns


def test_with_columns_renamed_matches_name_case_insensitively(spark):
    df = spark.createDataFrame([(1, 10)], ["a", "b"])

    assert df.withColumnRenamed("A", "z").columns == ["z", "b"]
    assert df.withColumnRenamed("a", "z").columns == ["z", "b"]
    # A name that matches no column is ignored.
    assert df.withColumnRenamed("nope", "z").columns == ["a", "b"]
    assert df.withColumnsRenamed({"A": "z", "B": "y"}).columns == ["z", "y"]

    # The renames are applied in order to the output of the previous one, so the second
    # entry no longer matches the column that the first one renamed.
    assert df.withColumnsRenamed({"A": "z", "a": "y"}).columns == ["z", "b"]
    # Spark 3.5 rejected the resulting duplicate name with COLUMN_ALREADY_EXISTS;
    # Spark 4 allows it, and we follow the latest behavior.
    assert df.withColumnsRenamed({"a": "b", "b": "c"}).columns == ["c", "c"]


def test_with_column_case_sensitive(spark):
    spark.conf.set("spark.sql.caseSensitive", "true")
    try:
        df = spark.createDataFrame([(1, 10)], ["a", "b"])
        # The names no longer match, so the column is appended instead of replaced.
        assert df.withColumn("A", lit(1)).columns == ["a", "b", "A"]
        assert df.withColumns({"a": lit(1), "A": lit(2)}).columns == ["a", "b", "A"]
        # A rename that matches no column is ignored.
        assert df.withColumnRenamed("A", "z").columns == ["a", "b"]
        assert df.withColumnRenamed("a", "z").columns == ["z", "b"]
    finally:
        spark.conf.unset("spark.sql.caseSensitive")


def test_with_column_matches_non_ascii_names(spark):
    assert spark.sql("SELECT 1 AS `Ä`").withColumn("ä", lit(2)).columns == ["ä"]
    assert spark.sql("SELECT 1 AS `ä`").withColumnsRenamed({"Ä": "z"}).columns == ["z"]

    with pytest.raises(Exception, match="COLUMN_ALREADY_EXISTS"):
        _ = spark.range(1).withColumns({"Ä": lit(1), "ä": lit(2)}).columns


def test_with_metadata_matches_name_case_insensitively(spark):
    df = spark.createDataFrame([(1, 10)], ["a", "b"])

    annotated = df.withMetadata("A", {"m": "x"})
    assert annotated.columns == ["A", "b"]
    assert annotated.schema["A"].metadata == {"m": "x"}


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


def test_with_column_matches_name_like_java_case_folding(spark):
    # The Spark analyzer resolver uses `String.equalsIgnoreCase`, which folds a character
    # through its *simple* case mappings. `İ` has a simple lowercase mapping to `i`, even
    # though its full lowercase mapping is `i` followed by a combining dot above.
    assert spark.sql("SELECT 1 AS `İ`").withColumn("i", lit(2)).columns == ["i"]
    assert spark.sql("SELECT 1 AS `İ`").withColumn("i", lit(2)).collect() == [Row(i=2)]
    assert spark.sql("SELECT 1 AS `i`").withColumn("İ", lit(2)).columns == ["İ"]
    assert spark.sql("SELECT 1 AS `İ`").withColumnsRenamed({"i": "z"}).columns == ["z"]

    # Duplicates are detected by lowercasing the names instead of using the resolver, and the
    # full lowercase mappings of `İ` and `i` differ, so these names are not duplicates.
    assert spark.range(1).withColumns({"İ": lit(1), "i": lit(2)}).columns == ["id", "İ", "i"]


def test_with_columns_discards_alias_already_matched_by_another_alias(spark):
    # Both names match the `id` column through the resolver, but they are not duplicates
    # because their lowercase forms differ. Only the first one replaces the column, and the
    # other one is discarded rather than appended.
    # U+0131 is the Turkish dotless i. It is written as an escape so the source stays ASCII:
    # spelling it literally is what the confusable-character lint objects to, and the whole
    # point of these cases is that it looks like an ASCII i without folding to one.
    dotless_id = "\u0131d"
    df = spark.range(1)
    assert df.withColumns({"id": lit(1), dotless_id: lit(2)}).columns == ["id"]
    assert df.withColumns({"id": lit(1), dotless_id: lit(2)}).collect() == [Row(id=1)]
    assert df.withColumns({dotless_id: lit(1), "Id": lit(2)}).columns == [dotless_id]
    assert df.withColumns({dotless_id: lit(1), "Id": lit(2)}).collect() == [Row(**{dotless_id: 1})]


def test_with_columns_does_not_resolve_a_discarded_alias(spark):
    # The alias that another alias already matched is discarded before its expression is resolved,
    # so an expression that cannot be resolved never gets the chance to fail. Reversing the order
    # makes the same expression the surviving alias, and then it does fail.
    dotless_id = "ıd"
    df = spark.range(1)

    assert df.withColumns({"id": lit(1), dotless_id: col("missing")}).collect() == [Row(id=1)]
    with pytest.raises(Exception, match=re.escape("[UNRESOLVED_COLUMN.WITH_SUGGESTION]")):
        df.withColumns({dotless_id: col("missing"), "id": lit(1)}).collect()


def test_with_columns_drops_the_second_alias_that_matches_a_column(spark):
    # `expandStar` gives each existing column the FIRST alias that matches it, and appends only
    # the aliases that matched nothing, so the second one is dropped rather than added. The names
    # have to be ones the duplicate check lets through: it folds to lower case, where `ıd` and
    # `id` differ, while the resolver matches them.
    df = spark.range(1)

    replaced = df.withColumns({"id": lit(1), "ıd": lit(2)})

    assert replaced.columns == ["id"]
    assert replaced.collect() == [Row(id=1)]


def test_with_columns_reports_the_duplicate_a_java_string_orders_first(spark):
    # `checkColumnNameDuplication` picks the duplicate with `sortBy`, which compares UTF-16 code
    # units, so a name outside the basic plane comes BEFORE one that a comparison of UTF-8 bytes
    # would put first. Two duplicated groups are what makes either answer wrong for the other.
    df = spark.range(1)
    deseret, deseret_small = "\U00010400", "\U00010428"
    wide, wide_small = "\uff21", "\uff41"

    with pytest.raises(Exception, match=re.escape(f"The column `{deseret_small}` already exists.")):
        df.withColumns({deseret: lit(1), deseret_small: lit(2), wide: lit(3), wide_small: lit(4)}).collect()


def test_with_columns_rejects_two_aliases_that_fold_to_the_same_name(spark):
    # The duplicate check runs BEFORE the expansion, so two names that fold together never reach
    # the rule above: they are an error rather than one of them winning.
    df = spark.range(1)

    with pytest.raises(Exception, match=re.escape("[COLUMN_ALREADY_EXISTS]")):
        df.withColumns({"id": lit(1), "ID": lit(2)}).collect()


def test_with_columns_reads_the_column_it_replaces(spark):
    # The aliases are resolved against the input, so one that reads a column another alias
    # replaces sees the ORIGINAL value. Replacing and reading in the same call is what tells
    # that apart from applying the aliases one after the other.
    df = spark.sql("SELECT 5 AS k")

    assert df.withColumns({"k": lit(9), "z": col("k")}).collect() == [Row(k=9, z=5)]


def test_with_columns_appends_an_alias_that_matches_no_column(spark):
    # An alias that matches nothing goes to the end, after the columns that were already there,
    # whatever order it was given in.
    df = spark.range(1)

    assert df.withColumns({"z": lit(9), "id": lit(1)}).columns == ["id", "z"]
    assert df.withColumns({"z": lit(9)}).columns == ["id", "z"]


def test_with_columns_does_not_shadow_when_the_analysis_is_case_sensitive(spark):
    # The resolver is what matches an alias to a column, so making it case sensitive stops the
    # two from meeting and both reach the output.
    df = spark.range(1)
    try:
        spark.conf.set("spark.sql.caseSensitive", "true")

        assert df.withColumns({"id": lit(1), "ID": lit(2)}).columns == ["id", "ID"]
    finally:
        spark.conf.unset("spark.sql.caseSensitive")


def metadata_df(spark):
    return spark.range(1).select(col("id").alias("a")).withMetadata("a", {"k": "1"})


def test_with_column_metadata(spark):
    df = metadata_df(spark)
    assert df.schema["a"].metadata == {"k": "1"}

    assert df.withMetadata("a", {}).schema["a"].metadata == {}
    assert df.withMetadata("a", {"m": "2"}).schema["a"].metadata == {"m": "2"}
    # A rename keeps the column, so its metadata is preserved.
    assert df.withColumnRenamed("a", "z").schema["z"].metadata == {"k": "1"}


def test_with_column_does_not_inherit_metadata(spark):
    df = metadata_df(spark)

    assert df.withColumn("a", col("a")).schema["a"].metadata == {}
    assert df.withColumn("A", col("a")).schema["A"].metadata == {}
    assert df.withColumn("c", col("a")).schema["c"].metadata == {}
    assert df.withColumns({"a": col("a")}).schema["a"].metadata == {}


# An alias reports the metadata of its child only when that child is a named expression, which an
# attribute and another alias are and an expression that computes a value is not
# (`Alias.metadata` in `namedExpressions.scala`). The column of the frame carries `{"k": "1"}`, so
# a case that keeps the metadata tells the two rules apart from one that clears it.
# (case, the frame to build, the column to read, the metadata Spark reports)
_ALIAS_METADATA = [
    ("the column itself", lambda df: df.select(col("a")), "a", {"k": "1"}),
    ("an alias of the column", lambda df: df.select(col("a").alias("c")), "c", {"k": "1"}),
    ("an alias of an alias", lambda df: df.select(col("a").alias("b").alias("c")), "c", {"k": "1"}),
    ("a SQL alias", lambda df: df.selectExpr("a AS c"), "c", {"k": "1"}),
    ("a rename", lambda df: df.withColumnRenamed("a", "z"), "z", {"k": "1"}),
    ("a cast", lambda df: df.select(col("a").cast("string").alias("c")), "c", {}),
    ("a SQL cast", lambda df: df.selectExpr("CAST(a AS STRING) AS c"), "c", {}),
    ("an aggregate", lambda df: df.groupBy("id").agg(first("a").alias("c")), "c", {}),
    ("an aggregate over the whole frame", lambda df: df.agg(first("a").alias("c")), "c", {}),
    ("a window function", lambda df: df.selectExpr("first(a) OVER (PARTITION BY id) AS c"), "c", {}),
    # Metadata asked for by name replaces whatever the child had, and an empty ask erases it.
    ("an explicit ask", lambda df: df.select(col("a").alias("c", metadata={"z": "2"})), "c", {"z": "2"}),
    # PySpark only sends an empty ask from 4.1 on: before that the client drops the field
    # because an empty dict is falsy, so the alias reaches the server without metadata and
    # inherits what the child had, which is what Spark reports with that client too.
    (
        "an explicit empty ask",
        lambda df: df.select(col("a").alias("c", metadata={})),
        "c",
        {} if pyspark_version() >= (4, 1) else {"k": "1"},
    ),
    # `withColumn` builds a new column, which Spark Connect gives empty explicit metadata.
    ("a replacement", lambda df: df.withColumn("a", col("a")), "a", {}),
    ("a copy", lambda df: df.withColumn("c", col("a")), "c", {}),
    ("the column beside a copy", lambda df: df.withColumn("c", col("a")), "a", {"k": "1"}),
    ("several columns at once", lambda df: df.withColumns({"a": col("a"), "z": lit(1)}), "a", {}),
    ("a column withColumns leaves alone", lambda df: df.withColumns({"z": lit(1)}), "a", {"k": "1"}),
    (
        "an ask after a replacement",
        lambda df: df.withColumn("a", col("a")).withMetadata("a", {"z": "2"}),
        "a",
        {"z": "2"},
    ),
    # Operations that carry the column through rather than build a new one.
    ("a filter", lambda df: df.filter("id = 0"), "a", {"k": "1"}),
    ("a sort", lambda df: df.sort("id"), "a", {"k": "1"}),
    ("a distinct", lambda df: df.distinct(), "a", {"k": "1"}),
    ("a frame alias", lambda df: df.alias("x").select("x.a"), "a", {"k": "1"}),
    ("a drop of another column", lambda df: df.drop("id"), "a", {"k": "1"}),
    ("a limit", lambda df: df.limit(1), "a", {"k": "1"}),
    ("a repartition", lambda df: df.repartition(2), "a", {"k": "1"}),
    # `first` has a Spark implementation of its own in Sail, so another aggregate goes through
    # different code on the way to the same rule.
    ("another aggregate", lambda df: df.groupBy("id").agg(spark_max("a").alias("c")), "c", {}),
    ("an aggregate of another column", lambda df: df.groupBy("a").agg(count("*").alias("c")), "a", {"k": "1"}),
    # A chain: what the replacement cleared must stay cleared, and what it kept must stay kept.
    ("a replacement twice", lambda df: df.withColumn("a", col("a")).withColumn("a", col("a")), "a", {}),
    ("a replacement then an alias", lambda df: df.withColumn("a", col("a")).select(col("a").alias("c")), "c", {}),
    (
        "an alias then a replacement",
        lambda df: df.select(col("a").alias("c")).withColumn("z", col("c")),
        "c",
        {"k": "1"},
    ),
]

# A set operation reports the metadata of its first input, which is not the same as reporting what
# the inputs agree on: the two sides here carry the same key with a different value.
# (case, the frame to build, the metadata Spark reports)
_SET_OP_METADATA = [
    ("a union", lambda left, right: left.union(right), {"side": "L"}),
    ("a union by name", lambda left, right: left.unionByName(right), {"side": "L"}),
    ("an intersection", lambda left, right: left.intersect(right), {"side": "L"}),
    ("a difference", lambda left, right: left.exceptAll(right), {"side": "L"}),
]


@pytest.mark.parametrize(("case", "build", "expected"), _SET_OP_METADATA)
def test_a_set_operation_reports_the_metadata_of_its_first_input(spark, case, build, expected):  # noqa: ARG001
    df = spark.range(2).select(col("id"), col("id").cast("string").alias("a"))
    # The sides differ in their rows as well, so a difference is not empty.
    left = df.withMetadata("a", {"side": "L"})
    right = df.filter("id = 0").withMetadata("a", {"side": "R"})

    result = build(left, right)

    assert result.schema["a"].metadata == expected
    assert len(result.collect()) >= 1


# A join carries both sides through, and `withColumns` over one builds new columns there too.
# (case, the frame to build, the column to read, the metadata Spark reports)
_JOIN_METADATA = [
    ("the passed through column", lambda j: j, "a", {"k": "1"}),
    ("a replacement over a join", lambda j: j.withColumn("a", col("a")), "a", {}),
    ("a copy over a join", lambda j: j.withColumn("c", col("a")), "c", {}),
    ("several columns over a join", lambda j: j.withColumns({"a": col("a"), "z": lit(1)}), "a", {}),
    ("a column withColumns leaves alone", lambda j: j.withColumns({"z": lit(1)}), "a", {"k": "1"}),
    ("an alias over a join", lambda j: j.select(col("a").alias("c")), "c", {"k": "1"}),
    ("a cast over a join", lambda j: j.select(col("a").cast("string").alias("c")), "c", {}),
    ("a replacement of the join key", lambda j: j.withColumns({"id": col("id") + 1}), "a", {"k": "1"}),
    (
        "a replacement after a cast",
        lambda j: j.select(col("a").cast("string").alias("a")).withColumn("z", lit(1)),
        "a",
        {},
    ),
    ("a join after withColumns", lambda j: j.withColumns({"z": lit(1)}), "a", {"k": "1"}),
]


# An outer join rebuilds its output columns as nullable, which is a path of its own, and a self
# join brings the same metadata in twice.
# (case, the frame to build, the column to read, the metadata Spark reports)
_OUTER_JOIN_METADATA = [
    ("an outer join", lambda left, right: left.join(right, "id", "left_outer"), "a", {"k": "1"}),
    ("a full outer join", lambda left, right: left.join(right, "id", "full_outer"), "a", {"k": "1"}),
    (
        "a replacement over an outer join",
        lambda left, right: left.join(right, "id", "left_outer").withColumn("a", col("a")),
        "a",
        {},
    ),
    (
        "a self join",
        lambda left, _: left.alias("l").join(left.alias("r"), "id").select(col("l.a").alias("c")),
        "c",
        {"k": "1"},
    ),
]


@pytest.mark.parametrize(("case", "build", "column", "expected"), _OUTER_JOIN_METADATA)
def test_an_outer_join_keeps_the_metadata_while_it_changes_nullability(spark, case, build, column, expected):  # noqa: ARG001
    left = spark.range(2).select(col("id"), col("id").cast("string").alias("a")).withMetadata("a", {"k": "1"})
    right = spark.range(1).select(col("id"), col("id").cast("string").alias("b"))

    result = build(left, right)

    assert result.schema[column].metadata == expected
    assert len(result.collect()) >= 1


def test_a_cast_to_a_struct_type_rebuilds_its_fields(spark):
    # The target type is what the cast produces, so the metadata of the fields it reads is not
    # part of it.
    schema = StructType([StructField("s", StructType([StructField("f", IntegerType(), metadata={"k": "1"})]))])
    df = spark.createDataFrame([((1,),)], schema)

    cast = df.select(col("s").cast("struct<f:int>").alias("s"))

    assert [dict(field.metadata) for field in cast.schema["s"].dataType.fields] == [{}]
    assert len(cast.collect()) == 1


@pytest.mark.parametrize(("case", "build", "column", "expected"), _JOIN_METADATA)
def test_a_join_carries_the_metadata_of_both_sides(spark, case, build, column, expected):  # noqa: ARG001
    rows = 2
    left = spark.range(2).select(col("id"), col("id").cast("string").alias("a")).withMetadata("a", {"k": "1"})
    right = spark.range(2).select(col("id"), col("id").cast("string").alias("b"))
    joined = left.join(right, "id")

    result = build(joined)

    assert result.schema[column].metadata == expected
    # The override rides on an alias over a plain column reference, which reaches the schema but
    # not the physical projection, so the rows are what tells a working plan from a broken one.
    assert len(result.collect()) == rows


# Metadata inside a struct follows the same rule one level down: a field that is built anew gets
# none, and the fields around it keep theirs.
# (case, the frame to build, the metadata Spark reports for each field of the struct)
_NESTED_METADATA = [
    ("the struct itself", lambda df: df, {"f": {"k": "1"}, "g": {}}),
    ("a replaced struct", lambda df: df.withColumn("s", col("s")), {"f": {"k": "1"}, "g": {}}),
    (
        "a field replaced by withField",
        lambda df: df.withColumn("s", col("s").withField("f", lit(2))),
        {"f": {}, "g": {}},
    ),
    (
        "a field added by withField",
        lambda df: df.withColumn("s", col("s").withField("h", lit(2))),
        {"f": {"k": "1"}, "g": {}, "h": {}},
    ),
    (
        "a struct built from the column",
        lambda df: df.select(struct(col("s.f").alias("f")).alias("s")),
        {"f": {"k": "1"}},
    ),
    ("a struct built from an expression", lambda df: df.select(struct(lit(1).alias("f")).alias("s")), {"f": {}}),
]


@pytest.mark.parametrize(("case", "build", "expected"), _NESTED_METADATA)
def test_the_metadata_of_a_struct_field(spark, case, build, expected):  # noqa: ARG001
    schema = StructType(
        [
            StructField(
                "s",
                StructType(
                    [
                        StructField("f", IntegerType(), metadata={"k": "1"}),
                        StructField("g", IntegerType()),
                    ]
                ),
            )
        ]
    )
    df = spark.createDataFrame([((1, 2),)], schema)

    result = build(df)

    assert {field.name: dict(field.metadata) for field in result.schema["s"].dataType.fields} == expected
    assert len(result.collect()) == 1


def test_a_nested_field_read_as_a_column_keeps_its_metadata(spark):
    # Reading `s.f` is not an attribute, but Spark reports the metadata of the struct field it
    # reads (`Alias.metadata` has a branch of its own for it).
    schema = StructType([StructField("s", StructType([StructField("f", IntegerType(), metadata={"k": "1"})]))])
    df = spark.createDataFrame([((1,),)], schema)

    assert df.select(col("s.f")).schema["f"].metadata == {"k": "1"}
    assert df.select(col("s.f").alias("c")).schema["c"].metadata == {"k": "1"}
    assert df.withColumn("c", col("s.f")).schema["c"].metadata == {}


@pytest.mark.parametrize(("case", "build", "column", "expected"), _ALIAS_METADATA)
def test_an_alias_reports_the_metadata_of_a_named_child_only(spark, case, build, column, expected):  # noqa: ARG001
    df = spark.range(1).select(col("id"), col("id").cast("string").alias("a")).withMetadata("a", {"k": "1"})

    assert build(df).schema[column].metadata == expected


def test_clearing_the_metadata_keeps_the_user_defined_type(spark):
    # The identity of a user defined type rides in the metadata of the field, so clearing the Spark
    # metadata has to clear that one key and leave the rest: replacing the whole map would turn the
    # column into its storage type without a single error. The column carries metadata of its own,
    # which is what makes the clearing fire at all.
    schema = StructType([StructField("id", IntegerType()), StructField("u", NamedPythonUDT())])
    marked = spark.createDataFrame([], schema).withMetadata("u", {"k": "1"})

    cleared = [
        marked.groupBy("id").agg(first("u").alias("u")),
        marked.selectExpr("first(u) OVER (PARTITION BY id) AS u"),
        marked.withColumn("u", col("u")),
    ]
    for df in cleared:
        field = df.schema["u"]
        assert isinstance(field.dataType, NamedPythonUDT)
        assert field.metadata == {}

    # An alias reads the metadata of the attribute below it, so there both survive.
    aliased = marked.select(col("u").alias("u")).schema["u"]
    assert isinstance(aliased.dataType, NamedPythonUDT)
    assert aliased.metadata == {"k": "1"}


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_a_generated_aggregate_name_is_marked_as_such(spark):
    # Spark names the column of an aggregate that was not aliased, and marks that name in the
    # metadata so that later rules can tell it from a name the user wrote.
    df = spark.range(1).select(col("id"), col("id").cast("string").alias("a"))

    assert df.groupBy("id").agg(first("a")).schema["first(a)"].metadata == {"__autoGeneratedAlias": "true"}


def test_drop_matches_non_ascii_names(spark):
    assert spark.sql("SELECT 1 AS `Ä`").drop("ä").columns == []
    assert spark.sql("SELECT 1 AS `İ`").drop("i").columns == []
    assert spark.sql("SELECT 1 AS `\u0131d`").drop("Id").columns == []
    # U+13A0 is the Cherokee capital letter A; it folds to its lowercase form U+AB70.
    assert spark.sql("SELECT 1 AS `\u13a0`").drop("\uab70").columns == []
    # `ﬁ` has no simple case mapping, so it does not match `FI`.
    assert spark.sql("SELECT 1 AS `ﬁ`").drop("FI").columns == ["ﬁ"]


def test_replace_subset_matches_name_exactly(spark):
    df = spark.createDataFrame([("x",)], ["s"])

    assert df.replace("x", "y", subset=["s"]).collect() == [Row(s="y")]
    # The name is resolved case-insensitively, so it is not an error, but only a column whose
    # name matches exactly is replaced.
    assert df.replace("x", "y", subset=["S"]).collect() == [Row(s="x")]
    assert spark.createDataFrame([("x",)], ["Ä"]).replace("x", "y", subset=["ä"]).collect() == [Row(Ä="x")]

    with pytest.raises(Exception, match=re.escape("[UNRESOLVED_COLUMN.WITH_SUGGESTION]")):
        df.replace("x", "y", subset=["nope"]).collect()


def test_column_resolution_is_case_sensitive_when_configured(spark):
    spark.conf.set("spark.sql.caseSensitive", "true")
    try:
        df = spark.createDataFrame([(1, 10)], ["a", "b"])

        assert df.select(col("a")).columns == ["a"]
        # The name no longer matches, so the column cannot be resolved at all. The condition and
        # the name are both asserted, since the suggestion list mentions every column and would
        # satisfy a pattern that only looks for the name.
        unresolved = r"UNRESOLVED_COLUMN\.WITH_SUGGESTION.*name `A` cannot be resolved"
        with pytest.raises(Exception, match=unresolved):
            df.select(col("A")).collect()
        with pytest.raises(Exception, match=unresolved):
            df.filter(col("A") > 0).collect()
        with pytest.raises(Exception, match=r'CANNOT_RESOLVE_DATAFRAME_COLUMN.*dataframe column "A"'):
            df.withMetadata("A", {"m": "x"}).collect()

        # A name that matches no column is ignored by `drop`.
        assert df.drop("A").columns == ["a", "b"]
        assert df.drop("a").columns == ["b"]
    finally:
        spark.conf.unset("spark.sql.caseSensitive")


def test_attribute_reference_does_not_use_the_resolver_alone(spark):
    # An attribute is looked up in a map keyed by the lowercased name and the candidates are
    # then filtered with the resolver, so a name matches only when it matches both ways.
    # `İ` and `i` match the resolver but their lowercase forms differ, so they do not match.
    df = spark.sql("SELECT 1 AS `İ`")
    for reference in (lambda: df.select("i"), lambda: df.filter(col("i") > 0)):
        with pytest.raises(Exception, match=r"UNRESOLVED_COLUMN\.WITH_SUGGESTION.*name `i` cannot be resolved"):
            reference().collect()

    dotless = spark.sql("SELECT 1 AS `\u0131d`")
    with pytest.raises(Exception, match=r"UNRESOLVED_COLUMN\.WITH_SUGGESTION.*name `Id` cannot be resolved"):
        dotless.select("Id").collect()

    # The operations that select the output columns by name use the resolver alone, so the very
    # same names do match there.
    assert df.drop("i").columns == []
    assert df.withColumn("i", lit(2)).columns == ["i"]
    assert dotless.withColumnsRenamed({"Id": "z"}).columns == ["z"]

    # The lowercase forms agree for ASCII, so both rules accept it.
    assert spark.sql("SELECT 1 AS a").select("A").collect() == [Row(A=1)]


# `groupBy(str)` looks the name up through `DataFrame.__getitem__` until PySpark 4.1, so the name
# reaches the server carrying a plan ID, and a plan ID is what selects
# `CANNOT_RESOLVE_DATAFRAME_COLUMN` over `UNRESOLVED_COLUMN`. The condition therefore depends on
# the client rather than on the engine, which is why only this one is gated.
@pytest.mark.skipif(
    pyspark_version() < (4, 1),
    reason="The client stops attaching a plan ID to the name of `groupBy` from PySpark 4.1 on",
)
def test_group_by_does_not_use_the_resolver_alone(spark):
    df = spark.sql("SELECT 1 AS `\u0130`")

    with pytest.raises(Exception, match=r"UNRESOLVED_COLUMN\.WITH_SUGGESTION.*name `i` cannot be resolved"):
        df.groupBy("i").count().collect()

    # The very same name does match where the resolver is used alone.
    assert spark.sql("SELECT 1 AS a").groupBy("A").count().columns == ["A", "count"]


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_the_schema_of_an_expression_is_available_before_it_is_evaluated(spark):
    # `ConstantFolding` is an optimizer rule (`optimizer/expressions.scala:50`), not an analyzer
    # one, so an expression that cannot be evaluated still reports a schema and only raises once
    # rows are produced. Sail folds it while resolving, so the schema itself is unreachable. This
    # is the resolver rather than any one method: a `select`, a `withColumn` and a plain SQL
    # projection of the same expression all fold it just as eagerly.
    df = spark.sql("SELECT * FROM VALUES (1) AS t(a)")

    assert df.select(expr("1/0")).columns == ["(1 / 0)"]
    assert df.withColumn("c", expr("1/0")).columns == ["a", "c"]

    with pytest.raises(Exception, match="DIVIDE_BY_ZERO"):
        df.select(expr("1/0")).collect()


def test_to_schema_matches_name_like_the_analyzer(spark):
    src = spark.sql("SELECT 1 AS `Ä`, 2 AS b")
    target = StructType([StructField("b", IntegerType()), StructField("ä", IntegerType())])
    # `DataFrame.to` caches the requested schema on the client, so reading `.schema` straight
    # after it answers from that cache and measures nothing. The cache has to go first.
    out = src.to(target)
    out._cached_schema = None  # noqa: SLF001

    assert out.schema.names == ["b", "ä"]
    assert [r.asDict() for r in out.collect()] == [{"b": 2, "ä": 1}]


def test_to_schema_keeps_the_qualifier_of_an_unchanged_column(spark):
    # `Project.reorderFields` keeps an unchanged scalar attribute as it is, so the alias it was
    # read through still qualifies it afterwards.
    df = spark.sql("SELECT 1 AS a, 2 AS b").alias("t")
    reconciled = df.to(df.schema)

    assert reconciled.select("t.a").collect() == [Row(a=1)]
    assert [tuple(row) for row in reconciled.select("t.*").collect()] == [(1, 2)]


def test_to_schema_keeps_the_qualifier_of_a_column_it_only_reorders(spark):
    # Reordering the fields does not rebuild them, so both keep the alias they were read through.
    df = spark.sql("SELECT 1 AS a, 'x' AS b").alias("t")
    target = StructType([StructField("b", StringType()), StructField("a", IntegerType())])

    reconciled = df.to(target)

    assert reconciled.select("t.a").collect() == [Row(a=1)]
    assert reconciled.select("t.b").collect() == [Row(b="x")]


def test_to_schema_drops_the_qualifier_only_of_the_column_it_rebuilds(spark):
    # A column that has to be cast is a new expression, so the alias no longer qualifies it, while
    # the column beside it is untouched and still does. Both in the same frame is what tells the
    # rule apart from "the alias survives" and from "the alias is lost".
    df = spark.sql("SELECT 1 AS a, 'x' AS b").alias("t")
    target = StructType([StructField("a", StringType()), StructField("b", StringType())])

    reconciled = df.to(target)

    assert reconciled.select("t.b").collect() == [Row(b="x")]
    with pytest.raises(Exception, match=re.escape("`t`.`a`")):
        reconciled.select("t.a").collect()


def test_to_schema_gives_no_qualifier_to_a_column_it_fills_with_null(spark):
    # A target field that matches nothing is filled with a literal, which `createNewColumn` wraps
    # in an alias rather than keeping an attribute, so the frame alias does not reach it. The
    # column beside it is untouched and still carries the alias.
    df = spark.sql("SELECT 1 AS a, 'x' AS b").alias("t")
    target = StructType(
        [StructField("a", IntegerType()), StructField("b", StringType()), StructField("c", IntegerType(), True)]
    )

    reconciled = df.to(target)

    assert reconciled.select("c").collect() == [Row(c=None)]
    assert reconciled.select("t.a").collect() == [Row(a=1)]
    with pytest.raises(Exception, match=re.escape("`t`.`c`")):
        reconciled.select("t.c").collect()


def test_to_schema_keeps_the_qualifier_of_a_column_it_only_renames(spark):
    # `createNewColumn` renames the attribute rather than rebuilding it, so the alias still
    # qualifies it under the name the target asked for, and under the one it had.
    df = spark.sql("SELECT 1 AS a, 'x' AS b").alias("t")
    target = StructType([StructField("A", IntegerType()), StructField("b", StringType())])

    reconciled = df.to(target)

    assert reconciled.select("t.A").collect() == [Row(A=1)]
    assert reconciled.select("t.a").collect() == [Row(a=1)]


@pytest.mark.parametrize(
    ("expression", "target"),
    [
        ("1", StructType([StructField("a", LongType())])),
        ("named_struct('x', 1)", None),
    ],
)
def test_to_schema_does_not_qualify_a_column_it_rebuilds(spark, expression, target):
    # A cast or a rebuilt container is a new expression rather than the attribute that was read,
    # so the qualifier does not survive it.
    df = spark.sql(f"SELECT {expression} AS a").alias("t")
    reconciled = df.to(df.schema if target is None else target)

    with pytest.raises(Exception, match=re.escape("[UNRESOLVED_COLUMN.WITH_SUGGESTION]")):
        reconciled.select("t.a").collect()


def test_to_schema_fills_missing_nullable_field(spark):
    # `Project.reorderFields` only rejects a missing target field when it is non-nullable.
    # A nullable one is filled with a NULL literal of the target type.
    src = spark.sql("SELECT 1 AS a")
    target = StructType([StructField("a", IntegerType()), StructField("zz", StringType(), True)])

    assert src.to(target).columns == ["a", "zz"]
    assert src.to(target).collect() == [Row(a=1, zz=None)]


def test_to_schema_rejects_ambiguous_name(spark):
    # The target name is matched against every input column, and more than one match is an error
    # rather than a silent pick of the first one.
    src = spark.sql("SELECT 1 AS a, 2 AS A")
    target = StructType([StructField("a", IntegerType())])

    with pytest.raises(Exception, match="AMBIGUOUS_COLUMN_OR_FIELD"):
        src.to(target).collect()


def test_to_schema_keeps_the_dataframe_column_identity(spark):
    # `createNewColumn` renames the attribute through `withName`, which keeps its `exprId`, and
    # that id is the identity a `df["col"]` reference resolves against. It only gets there while
    # the column is still an attribute, which `reconcileColumnType` leaves alone for a scalar whose
    # type already matches -- a rename included, since `withName` keeps the id either way.
    df = spark.createDataFrame([(1, 2)], "a int, b int")
    target = StructType([StructField("a", IntegerType()), StructField("b", IntegerType())])

    assert df.to(target).select(df["a"]).collect() == [Row(a=1)]
    assert df.to(target).filter(df["a"] == 1).count() == 1
    assert df.to(target).withColumn("z", df["b"]).collect() == [Row(a=1, b=2, z=2)]

    renamed = StructType([StructField("A", IntegerType()), StructField("b", IntegerType())])
    assert df.to(renamed).select(df["a"]).collect() == [Row(A=1)]


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_to_schema_drops_the_identity_of_a_column_it_has_to_cast(spark):
    # A type that has to be reconciled reaches `Alias(other, name)()` instead, which mints a fresh
    # `exprId`, so the reference stops resolving. The untouched sibling still resolves, which is
    # what makes this about the rebuilt column rather than about `to` dropping every id.
    df = spark.createDataFrame([(1, 2)], "a int, b int")
    widened = StructType([StructField("a", LongType()), StructField("b", IntegerType())])

    with pytest.raises(Exception, match="CANNOT_RESOLVE_DATAFRAME_COLUMN"):
        df.to(widened).select(df["a"]).collect()

    # A join condition does not pull the attribute up either: `resolveExprsAndAddMissingAttrs`
    # descends only through a `UnaryNode`, and `Join` is binary.
    other = spark.createDataFrame([(1,)], "k int")
    with pytest.raises(Exception, match="CANNOT_RESOLVE_DATAFRAME_COLUMN"):
        df.to(widened).join(other, df["a"] == other["k"]).count()

    assert df.to(widened).select(df["b"]).collect() == [Row(b=2)]


def test_to_schema_keeps_a_rebuilt_column_reachable_from_filter_and_sort(spark):
    # Spark resolves a `df["col"]` reference against the plan node tagged with the id rather than
    # against the output, and `Filter` and `Sort` then pull the missing attribute up from below the
    # projection. So these two keep working on a column that `select` can no longer reach, and
    # withholding the plan IDs to make `select` agree would reject them -- which is why the
    # narrowing was rejected, and nothing else asserts it. The two engines land on the same answer
    # here only because widening preserves value and order; which column each one actually reads is
    # `test_to_schema_reads_the_reconciled_column_in_filter_and_sort`.
    df = spark.createDataFrame([(1, 2)], "a int, b int")
    widened = StructType([StructField("a", LongType()), StructField("b", IntegerType())])
    out = df.to(widened)

    assert out.filter(df["a"] == 1).count() == 1
    assert out.sort(df["a"]).count() == 1


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_to_schema_reads_the_reconciled_column_in_filter_and_sort(spark):
    # `Filter` and `Sort` resolve the reference to the attribute below the projection, which is the
    # column as it was before `reconcileColumnType` touched it. Sail resolves it to the reconciled
    # output column instead, so the two agree only while the reconciliation preserves value and
    # order, and diverge without either engine raising when it does not.
    df = spark.createDataFrame([(9.0,), (10.0,)], "a double")
    ordered = df.to(StructType([StructField("a", StringType())]))

    # Ordered by the original double, not by the string the cast produced.
    assert [r.a for r in ordered.sort(df["a"]).collect()] == ["9.0", "10.0"]

    narrowed = spark.createDataFrame([(1.5,)], "a double")
    rounded = narrowed.to(StructType([StructField("a", IntegerType())]))
    before_the_cast, after_the_cast = 1.5, 1.0

    assert rounded.filter(narrowed["a"] == before_the_cast).count() == 1
    assert rounded.filter(narrowed["a"] == after_the_cast).count() == 0


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
@pytest.mark.parametrize("expression", ["named_struct('n', 1)", "array(1, 2)", "map('a', 1)"])
def test_to_schema_rebuilds_a_container_even_when_the_schema_is_identical(spark, expression):
    # A struct, an array and a map each reach an arm of `reconcileColumnType` that rebuilds them
    # rather than handing the column back, so the attribute never survives and the identity goes
    # even though the schema is the one the frame already has.
    df = spark.sql(f"SELECT {expression} AS s, 1 AS k")

    with pytest.raises(Exception, match="CANNOT_RESOLVE_DATAFRAME_COLUMN"):
        df.to(df.schema).select(df["s"]).collect()

    assert df.to(df.schema).select(df["k"]).collect() == [Row(k=1)]


def test_to_schema_suggests_by_distance_to_the_raw_field_name(spark):
    # `Project.reorderFields` measures the edit distance against the raw `StructField.name`, unlike
    # the analyzer, which measures it against the name it renders. A target name that needs quoting
    # is what tells the two apart: counting the back quotes moves `c` away from the name asked for.
    src = spark.sql("SELECT 1 AS c, 2 AS abc, 3 AS aaa, 4 AS zzzz, 5 AS cc, 6 AS ccc")
    target = StructType([StructField("a b", IntegerType(), nullable=False)])

    with pytest.raises(Exception, match=re.escape("[`c`, `abc`, `aaa`, `cc`, `ccc`]")):
        src.to(target).collect()


def test_to_schema_suggests_the_same_order_for_a_plain_name(spark):
    # The control for the case above: a plain identifier renders to itself, so both bases agree and
    # the order must come out the same whichever one is measured against.
    src = spark.sql("SELECT 1 AS c, 2 AS abc, 3 AS aaa, 4 AS zzzz, 5 AS cc, 6 AS ccc")
    target = StructType([StructField("abd", IntegerType(), nullable=False)])

    with pytest.raises(Exception, match=re.escape("[`c`, `abc`, `aaa`, `cc`, `ccc`]")):
        src.to(target).collect()


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_to_schema_reorders_nested_struct_fields(spark):
    # The reconciliation recurses into structs, so the nested fields are matched by name, not by
    # position, and they take the name of the target field.
    src = spark.sql("SELECT named_struct('x', 1, 'y', 'a') AS s")
    nested = StructType([StructField("Y", StringType()), StructField("X", IntegerType())])
    target = StructType([StructField("s", nested)])

    assert src.to(target).schema.simpleString() == "struct<s:struct<Y:string,X:int>>"
    assert src.to(target).collect() == [Row(s=Row(Y="a", X=1))]


def test_to_schema_accepts_a_column_spark_considers_non_nullable(spark):
    # Spark reports these four as non-nullable and Sail reports them nullable, so a check that
    # narrows on Sail's own flag would refuse a target Spark answers. Each one is a different
    # branch of that gap: a plain function, a struct literal, a map literal and a `CASE`.
    cases = [
        ("upper('x')", StringType()),
        ("named_struct('a', 1)", StructType([StructField("a", IntegerType(), True)])),
        ("map('k', 1)", MapType(StringType(), IntegerType(), True)),
        ("CASE WHEN 1 > 0 THEN 'x' ELSE 'y' END", StringType()),
    ]
    for expression, data_type in cases:
        source = spark.sql(f"SELECT {expression} AS a")
        target = StructType([StructField("a", data_type, False)])

        assert len(source.to(target).collect()) == 1


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_to_schema_rejects_nullable_column_for_non_nullable_field(spark):
    # A nullable input column cannot be narrowed to a non-nullable target field.
    src = spark.sql("SELECT CAST(NULL AS INT) AS a")
    target = StructType([StructField("a", IntegerType(), False)])

    with pytest.raises(
        Exception,
        match=re.escape(
            "[NULLABLE_COLUMN_OR_FIELD] Column or field `a` is nullable while it's required to be non-nullable."
        ),
    ):
        src.to(target).collect()


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_to_schema_checks_nullability_before_it_fills_a_missing_field(spark):
    # `reconcileColumnType` refuses the narrowing of a column it matched whatever the fields
    # around it are, so a target that also adds a column to fill does not get to return rows with
    # a schema that claims the column cannot be null. A source that is already non-nullable is
    # the other side of the rule, and it must still go through.
    target = StructType([StructField("a", IntegerType(), False), StructField("z", StringType(), True)])
    nullable = spark.createDataFrame([(1,)], "a int")
    non_nullable = spark.sql("SELECT 1 AS a")

    assert nullable.schema["a"].nullable is True
    with pytest.raises(Exception, match=re.escape("[NULLABLE_COLUMN_OR_FIELD]")):
        nullable.to(target).collect()

    assert non_nullable.schema["a"].nullable is False
    assert [tuple(row) for row in non_nullable.to(target).collect()] == [(1, None)]


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_to_schema_rejects_a_nested_field_narrowed_to_non_nullable(spark):
    # The reconciliation walks into a struct and applies the same rule there, naming the whole
    # path it walked. Sail rebuilds a nested target with a cast instead of field by field, so the
    # check never reaches the field.
    src = spark.sql("SELECT named_struct('b', CAST(NULL AS INT)) AS a")
    target = StructType([StructField("a", StructType([StructField("b", IntegerType(), False)]), True)])

    with pytest.raises(
        Exception,
        match=re.escape(
            "[NULLABLE_COLUMN_OR_FIELD] Column or field `a`.`b` is nullable while it's required to be non-nullable."
        ),
    ):
        src.to(target).collect()


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_to_schema_keeps_the_metadata_of_the_target_field(spark):
    # `DataFrame.to` caches the requested schema on the client, so reading `.schema` straight after
    # it answers from that cache and measures nothing. The cache has to go first.
    src = spark.sql("SELECT CAST(1 AS INT) AS a, CAST('x' AS STRING) AS b").withMetadata("a", {"j": "w"})
    target = StructType([StructField("a", IntegerType(), True, {"k": "v"}), StructField("b", StringType(), True)])

    out = src.to(target)
    out._cached_schema = None  # noqa: SLF001

    # The target's metadata is merged over the column's own, the target winning per key.
    assert [dict(field.metadata) for field in out.schema.fields] == [{"j": "w", "k": "v"}, {}]


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_to_schema_keeps_the_metadata_of_a_filled_field(spark):
    # A field the input does not have is filled with NULL, and the target's metadata is the only
    # metadata such a column can carry.
    src = spark.sql("SELECT CAST(1 AS INT) AS a")
    target = StructType([StructField("a", IntegerType(), True), StructField("c", IntegerType(), True, {"k": "v"})])

    out = src.to(target)
    out._cached_schema = None  # noqa: SLF001

    assert [dict(field.metadata) for field in out.schema.fields] == [{}, {"k": "v"}]


def test_fillna_rejects_a_nested_subset_name_that_matches_nothing(spark):
    # A dotted name is resolved like any other column reference, so one that matches nothing is an
    # error rather than a name to skip.
    with pytest.raises(
        Exception,
        match=re.escape(
            "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with "
            "name `nope`.`x` cannot be resolved. Did you mean one of the following? [`a`]."
        ),
    ):
        spark.sql("SELECT CAST(NULL AS INT) AS a").fillna(0, subset=["nope.x"]).collect()


def test_dropna_rejects_a_nested_subset_name_that_matches_nothing(spark):
    with pytest.raises(
        Exception,
        match=re.escape(
            "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with "
            "name `nope`.`x` cannot be resolved. Did you mean one of the following? [`a`]."
        ),
    ):
        spark.sql("SELECT CAST(NULL AS INT) AS a").dropna(subset=["nope.x"]).collect()


def test_replace_rejects_ambiguous_subset_name(spark):
    # The subset name is resolved as an attribute reference, which fails when it matches more
    # than one column of the input.
    df = spark.sql("SELECT 'x' AS a, 'x' AS A")

    with pytest.raises(Exception, match="AMBIGUOUS_REFERENCE"):
        df.replace("x", "y", subset=["a"]).collect()


def test_union_by_name_matches_non_ascii_names(spark):
    left = spark.sql("SELECT 1 AS `ä`")
    right = spark.sql("SELECT 2 AS `Ä`")
    assert sorted(row[0] for row in left.unionByName(right).collect()) == [1, 2]


def test_union_by_name_is_case_sensitive_when_configured(spark):
    spark.conf.set("spark.sql.caseSensitive", "true")
    try:
        left = spark.sql("SELECT 1 AS a, 2 AS b")
        right = spark.sql("SELECT 3 AS B, 4 AS A")
        with pytest.raises(Exception, match="Cannot resolve column name"):
            left.unionByName(right).collect()
    finally:
        spark.conf.unset("spark.sql.caseSensitive")


def test_drop_duplicates_matches_name_with_the_resolver(spark):
    # The subset name selects output columns by name, so it is matched by the resolver alone,
    # which folds `ı` to `I` even though the lowercase forms differ. The rows differ in the
    # column that is NOT the key, so deduplicating by it is what tells a subset that reached the
    # plan apart from one that was ignored: without it both rows are already distinct.
    rows = [("ıd", "Id", "SELECT * FROM VALUES (1, 'x'), (1, 'y') AS t(`ıd`, v)")]
    rows.append(("ς", "Σ", "SELECT * FROM VALUES (1, 'x'), (1, 'y') AS t(`ς`, v)"))
    for name, reference, query in rows:
        df = spark.sql(query)

        assert df.dropDuplicates([reference]).columns == [name, "v"]
        assert len(df.dropDuplicates([reference]).collect()) == 1
        assert len(df.dropDuplicates().collect()) == 2  # noqa: PLR2004


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_drop_duplicates_reports_the_connect_condition_for_a_missing_name(spark):
    # The Connect planner validates the subset itself, so it reports the wrapped condition, unlike
    # `unionByName`, whose identical failure comes from the analyzer and keeps the bare one.
    with pytest.raises(Exception, match=re.escape("CONNECT_INVALID_PLAN.UNRESOLVED_COLUMN_AMONG_FIELD_NAMES")):
        spark.sql("SELECT 1 AS a, 2 AS b").dropDuplicates(["Z"]).collect()


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_describe_names_the_column_as_written(spark):
    # Resolving an attribute renames it to the spelling that was asked for.
    assert spark.sql("SELECT 1 AS id").describe("ID").columns == ["summary", "ID"]
    assert spark.sql("SELECT 1 AS `Ä`").describe("ä").columns == ["summary", "ä"]


def test_fillna_leaves_a_nested_name_alone(spark):
    # `fill(value, cols)` keeps only the names that resolve to an attribute (`toAttributes` ends in
    # `collect { case a: Attribute }`), so a nested one is dropped from the list rather than filled
    # or rejected. A map of ONE entry is sent down that same path by the Connect planner, which
    # splits on `values.length == 1`, so it behaves the same way.
    df = spark.sql("SELECT named_struct('a', CAST(NULL AS INT)) AS s, CAST(NULL AS INT) AS b")

    assert [tuple(row) for row in df.na.fill(1, subset=["s.a"]).collect()] == [(Row(a=None), None)]
    assert [tuple(row) for row in df.na.fill({"s.a": 1}).collect()] == [(Row(a=None), None)]


def test_fillna_rejects_a_nested_name_given_among_several(spark):
    # A map of two entries goes to `fillMap` instead, which resolves each name and refuses the ones
    # that are not an attribute. The same name is ignored with one entry and refused with two,
    # which is what tells the two paths apart.
    df = spark.sql("SELECT named_struct('a', CAST(NULL AS INT)) AS s, CAST(NULL AS INT) AS b")

    with pytest.raises(Exception, match=re.escape("[UNSUPPORTED_FEATURE.REPLACE_NESTED_COLUMN]")):
        df.na.fill({"s.a": 1, "b": 2}).collect()


def test_replace_rejects_a_nested_name(spark):
    # `replace` resolves every name it is given and refuses the ones that are not an attribute,
    # with no path that ignores them.
    df = spark.sql("SELECT named_struct('a', CAST(NULL AS INT)) AS s")

    with pytest.raises(Exception, match=re.escape("[UNSUPPORTED_FEATURE.REPLACE_NESTED_COLUMN]")):
        df.na.replace(1, 9, subset=["s.a"]).collect()


def test_fillna_rejects_a_subset_name_that_matches_nothing(spark):
    # A subset name that resolves to no column is an error rather than being ignored.
    df = spark.sql("SELECT CAST(NULL AS INT) AS a")
    with pytest.raises(Exception, match=re.escape("[UNRESOLVED_COLUMN.WITH_SUGGESTION]")):
        df.fillna(0, subset=["nope"]).collect()
    with pytest.raises(Exception, match=re.escape("[UNRESOLVED_COLUMN.WITH_SUGGESTION]")):
        df.dropna(subset=["nope"]).collect()


def test_na_subset_suggests_a_dotted_column_as_several_quoted_parts(spark):
    # Unlike the suggestion the analyzer orders, which quotes a name only when it needs one, this
    # one parses each field name before quoting it, so a dot inside a name separates two parts.
    df = spark.sql("SELECT 1 AS `x.y`, 2 AS plain")

    for reference in (lambda: df.fillna(0, subset=["nope"]), lambda: df.replace(1, 9, subset=["nope"])):
        with pytest.raises(
            Exception,
            match=r"name `nope` cannot be resolved\. Did you mean one of the following\? \[`x`\.`y`, `plain`\]\.",
        ):
            reference().collect()


def test_names_are_folded_with_the_case_mappings_of_the_jvm(spark):
    # Vithkuqi was assigned in Unicode 14, which OpenJDK 17 does not know, so the two names do not
    # fold into each other and both columns are added.
    assert spark.range(1).withColumns({"𐕰": lit(1), "𐖗": lit(2)}).columns == ["id", "𐕰", "𐖗"]


def test_drop_duplicates_keeps_every_matching_column(spark):
    # The name selects output columns, so every column that matches becomes a key rather than
    # the name being rejected as ambiguous. The two columns named `a` hold different values, so
    # both rows survive only if BOTH of them are keys: keeping just the first would collapse them.
    df = spark.sql("SELECT 1 AS a, 2 AS a UNION ALL SELECT 1, 3")

    assert df.dropDuplicates(["a"]).columns == ["a", "a"]
    assert sorted(tuple(row) for row in df.dropDuplicates(["a"]).collect()) == [(1, 2), (1, 3)]
    assert spark.sql("SELECT 1 AS a").dropDuplicates().columns == ["a"]

    with pytest.raises(Exception, match="Cannot resolve column name"):
        _ = spark.sql("SELECT 1 AS a").dropDuplicates(["nope"]).columns


def test_fillna_and_dropna_without_subset_are_unaffected(spark):
    df = spark.sql("SELECT CAST(NULL AS INT) AS a")
    assert df.fillna(0).collect() == [Row(a=0)]
    assert df.dropna().collect() == []


def test_na_ambiguity_orders_the_references_like_a_java_string(spark):
    # The references are reported through a path of their own, which sorts them by UTF-16 code unit
    # as a Java string compares: a name outside the BMP comes before one in the high part of it.
    df = spark.range(1).alias("\ufb00").crossJoin(spark.range(1).alias("\U0001f600"))

    with pytest.raises(Exception, match=re.escape("could be: [`\U0001f600`.`id`, `\ufb00`.`id`].")):
        df.fillna(0, subset=["id"]).collect()


def test_fillna_rejects_an_ambiguous_subset_name(spark):
    df = spark.sql("SELECT CAST(NULL AS INT) AS a, CAST(NULL AS INT) AS a")
    with pytest.raises(Exception, match="AMBIGUOUS_REFERENCE"):
        df.fillna(0, subset=["a"]).collect()


def test_fillna_rejects_a_map_key_that_matches_nothing(spark):
    with pytest.raises(Exception, match=re.escape("[UNRESOLVED_COLUMN.WITH_SUGGESTION]")):
        spark.sql("SELECT CAST(NULL AS INT) AS a").fillna({"nope": 0}).collect()


def test_fillna_accepts_a_nested_subset_name(spark):
    # A subset name that resolves to a nested field is not a column, so it is discarded and the
    # frame is left untouched rather than the name being rejected.
    df = spark.sql("SELECT named_struct('x', CAST(NULL AS INT)) AS s, 1 AS a")
    assert df.fillna(0, subset=["s.x"]).collect() == [Row(s=Row(x=None), a=1)]


def test_dropna_filters_on_a_nested_subset_name(spark):
    # Unlike `fillna`, `dropna` keeps the resolved nested field and filters on it.
    df = spark.sql("SELECT named_struct('x', CAST(NULL AS INT)) AS s, 1 AS a")
    assert df.dropna(subset=["s.x"]).collect() == []


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_union_by_name_rejects_an_extra_column_on_the_right(spark):
    # Without `allowMissingColumns` the two sides have to name the same columns. Sail unions
    # them on the names they share and lets the extra one through instead of refusing.
    left = spark.sql("SELECT 1 AS a")
    right = spark.sql("SELECT 2 AS a, 3 AS b")
    with pytest.raises(Exception, match="NUM_COLUMNS_MISMATCH"):
        left.unionByName(right).collect()


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_array_struct_field_keeps_the_nullability_of_the_array(spark):
    # Extracting a field from an array of structs inherits the array's nullability, and
    # `containsNull` comes from the array and the field, not from a hardcoded `true`.
    schema = spark.sql("SELECT s.`Ä` AS r FROM (SELECT array(named_struct('ä', 1)) AS s)").schema
    assert schema["r"].nullable is False
    assert schema["r"].dataType.containsNull is False


def test_union_by_name_matches_names_with_allow_missing_columns(spark):
    # The right-side extras are matched with the resolver too, so a case-differing name is not
    # appended twice.
    left = spark.sql("SELECT 1 AS `ä`, 2 AS b")
    right = spark.sql("SELECT 3 AS `Ä`, 4 AS c")
    result = left.unionByName(right, allowMissingColumns=True)
    assert result.columns == ["ä", "b", "c"]


def test_drop_duplicates_accepts_a_repeated_subset_name(spark):
    # The same name twice selects the same column twice, which is not an error.
    df = spark.sql("SELECT * FROM VALUES (1, 'x'), (1, 'y'), (2, 'z') AS t(k, v)")
    assert sorted(r.k for r in df.dropDuplicates(["k", "k"]).collect()) == [1, 2]


def test_union_by_name_fills_missing_columns_with_the_right_type(spark):
    # The padded column keeps the type of the side that has it, rather than becoming untyped.
    left = spark.sql("SELECT 1 AS a, 'p' AS b")
    right = spark.sql("SELECT 2 AS a, 3 AS c")
    assert left.unionByName(right, allowMissingColumns=True).schema.simpleString() == "struct<a:int,b:string,c:int>"


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_union_by_name_rejects_duplicate_names_on_either_side(spark):
    # The names of each side are checked for duplicates before they are matched against each
    # other, so a duplicate on either side is rejected, and the check folds the names.
    with pytest.raises(Exception, match="COLUMN_ALREADY_EXISTS"):
        spark.sql("SELECT 1 AS a").unionByName(spark.sql("SELECT 2 AS a, 3 AS a")).collect()
    with pytest.raises(Exception, match="COLUMN_ALREADY_EXISTS"):
        spark.sql("SELECT 1 AS a, 2 AS a").unionByName(spark.sql("SELECT 3 AS a")).collect()
    with pytest.raises(Exception, match="COLUMN_ALREADY_EXISTS"):
        spark.sql("SELECT 1 AS a, 2 AS A").unionByName(spark.sql("SELECT 3 AS a")).collect()


def test_union_by_name_merges_reordered_nested_struct_fields(spark):
    # The fields of a nested struct are matched by name rather than by position.
    left = spark.sql("SELECT named_struct('x', 1, 'y', 2) AS s")
    right = spark.sql("SELECT named_struct('y', 4, 'x', 3) AS s")
    assert sorted((r.s.asDict() for r in left.unionByName(right).collect()), key=lambda s: s["x"]) == [
        {"x": 1, "y": 2},
        {"x": 3, "y": 4},
    ]


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_union_by_name_fills_a_missing_nested_struct_field(spark):
    # `allowMissingColumns` reaches into a struct as well, filling the field the other side
    # does not have. Sail compares the two structs whole, so the union fails to plan.
    left = spark.sql("SELECT named_struct('x', 1, 'y', 2) AS s")
    right = spark.sql("SELECT named_struct('x', 3) AS s")
    assert [r.s.asDict() for r in left.unionByName(right, allowMissingColumns=True).collect()] == [
        {"x": 1, "y": 2},
        {"x": 3, "y": None},
    ]


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_union_by_name_matches_nested_struct_fields_case_insensitively(spark):
    # The fields of a struct are matched by the resolver, like any other name. Sail compares
    # them literally, so the two structs are different types and the cast is refused.
    left = spark.sql("SELECT named_struct('x', 1) AS s")
    right = spark.sql("SELECT named_struct('X', 3) AS s")
    assert [r.s.asDict() for r in left.unionByName(right).collect()] == [{"x": 1}, {"x": 3}]


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_drop_duplicates_rejects_an_empty_subset(spark):
    # The Connect planner validates the subset before the plan is built, so an empty one is
    # its own condition rather than the message of whoever finds it later.
    with pytest.raises(Exception, match="DEDUPLICATE_REQUIRES"):
        spark.sql("SELECT 1 AS a").dropDuplicates([]).collect()


def test_fillna_skips_a_nested_subset_name(spark):
    # A subset name that resolves to a nested field is discarded rather than filled, so the null
    # inside the struct survives.
    df = spark.sql("SELECT named_struct('a', CAST(NULL AS INT)) AS s")

    assert df.fillna(0, subset=["s.a"]).collect() == [Row(s=Row(a=None))]


def test_to_schema_reports_no_suggestion_when_the_input_has_no_column(spark):
    # The condition carries the suggestion in a sub-condition, so an input with nothing to
    # suggest reports the other one instead of an empty list.
    schema = StructType([StructField("a", IntegerType(), nullable=False)])

    with pytest.raises(
        Exception,
        match=re.escape(
            "[UNRESOLVED_COLUMN.WITHOUT_SUGGESTION] A column, variable, or function parameter "
            "with name `a` cannot be resolved."
        ),
    ):
        spark.range(1).select().to(schema).collect()


def test_ambiguous_column_reference_names_the_way_out(spark):
    # The condition ends with the example that tells the user how to disambiguate, which is the
    # only actionable part of the message.
    df = spark.sql("SELECT 1 AS name")

    with pytest.raises(
        Exception,
        match=re.escape(
            'and specify the column using qualified name, e.g. `df.alias("a").join('
            'df.alias("b"), col("a.id") > col("b.id"))`.'
        ),
    ):
        df.join(df, df.name == df.name, "outer").select(df.name).collect()


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_a_column_of_a_join_of_two_frames_is_an_ambiguous_reference(spark):
    # `AMBIGUOUS_COLUMN_REFERENCE` is for a plan id that names SEVERAL nodes of the tree, which is
    # what a self join makes. Here the id names the join alone and two of its columns match, which
    # is an ordinary ambiguous reference. Sail picks the condition by whether there is a plan id
    # at all, and telling the two apart needs to know how many nodes carry it.
    left = spark.createDataFrame([("Bob", 5)], ["name", "age"])
    right = spark.createDataFrame([("Bob", 85)], ["name", "height"])
    joined = left.join(right, left.name == right.name)

    with pytest.raises(
        Exception,
        match=re.escape("[AMBIGUOUS_REFERENCE] Reference `name` is ambiguous, could be: [`name`, `name`]."),
    ):
        joined.select(joined["name"]).collect()


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_a_union_reports_the_metadata_of_its_first_input_only(spark):
    # `Union.mergeChildOutputs` takes the metadata of the FIRST child verbatim, so a key that only
    # the second one carries does not reach the output. The left side having none is what tells
    # "reports the first" apart from "merges both".
    left = spark.sql("SELECT 1 AS a")
    right = spark.sql("SELECT 2 AS a").withMetadata("a", {"k": "v"})

    union = left.union(right)
    union._cached_schema = None  # noqa: SLF001

    assert [dict(field.metadata) for field in union.schema.fields] == [{}]


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_an_intersection_is_nullable_only_where_both_sides_are(spark):
    # `Intersect.mergeChildOutputs` ands the two nullabilities, so a column that cannot be null on
    # one side cannot be null in the result either.
    left = spark.sql("SELECT CAST(NULL AS INT) AS a")
    right = spark.sql("SELECT 1 AS a")

    result = left.intersect(right)
    result._cached_schema = None  # noqa: SLF001

    assert result.schema["a"].nullable is False


def test_get_field_matches_the_field_with_the_resolver(spark):
    # Reading a field by name through `getField` resolves it the way every other name is
    # resolved, so it folds the case unless the analysis is case sensitive, and a name that
    # matches nothing is a missing field rather than a message of its own.
    df = spark.sql("SELECT named_struct('x', 1) AS s")

    assert [tuple(row) for row in df.select(col("s").getField("X")).collect()] == [(1,)]
    with pytest.raises(Exception, match=re.escape("[FIELD_NOT_FOUND]")):
        df.select(col("s").getField("zz")).collect()

    try:
        spark.conf.set("spark.sql.caseSensitive", "true")

        assert [tuple(row) for row in df.select(col("s").getField("x")).collect()] == [(1,)]
        with pytest.raises(Exception, match=re.escape("[FIELD_NOT_FOUND]")):
            df.select(col("s").getField("X")).collect()
    finally:
        spark.conf.unset("spark.sql.caseSensitive")


def test_get_field_reports_a_field_that_two_names_match(spark):
    # The resolver decides how many fields a name matches, so the same struct is ambiguous when
    # the case is folded and unambiguous when it is not.
    df = spark.sql("SELECT named_struct('x', 1, 'X', 2) AS s")

    with pytest.raises(Exception, match=re.escape("[AMBIGUOUS_REFERENCE_TO_FIELDS]")):
        df.select(col("s").getField("x")).collect()

    try:
        spark.conf.set("spark.sql.caseSensitive", "true")

        assert [tuple(row) for row in df.select(col("s").getField("x")).collect()] == [(1,)]
        assert [tuple(row) for row in df.select(col("s").getField("X")).collect()] == [(2,)]
    finally:
        spark.conf.unset("spark.sql.caseSensitive")


def test_drop_fields_matches_the_field_with_the_resolver(spark):
    # `DropField` keeps the fields the resolver does not match, so it folds the case unless the
    # analysis is case sensitive, it drops every field a folded name matches, and dropping all of
    # them is refused rather than producing a struct with no field at all.
    df = spark.sql("SELECT named_struct('a', 1, 'b', 2) AS s")

    folded = df.select(col("s").dropFields("A"))
    assert folded.schema[0].dataType.simpleString() == "struct<b:int>"
    assert [tuple(row[0]) for row in folded.collect()] == [(2,)]

    with pytest.raises(Exception, match=re.escape("[DATATYPE_MISMATCH.CANNOT_DROP_ALL_FIELDS]")):
        df.select(col("s").dropFields("A", "B")).collect()

    dup = spark.sql("SELECT named_struct('a', 1, 'A', 2, 'b', 3) AS s")
    both = dup.select(col("s").dropFields("a"))
    assert both.schema[0].dataType.simpleString() == "struct<b:int>"
    assert [tuple(row[0]) for row in both.collect()] == [(3,)]

    try:
        spark.conf.set("spark.sql.caseSensitive", "true")

        exact = df.select(col("s").dropFields("A"))
        assert exact.schema[0].dataType.simpleString() == "struct<a:int,b:int>"
        assert [tuple(row[0]) for row in exact.collect()] == [(1, 2)]

        one = dup.select(col("s").dropFields("a"))
        assert one.schema[0].dataType.simpleString() == "struct<A:int,b:int>"
        assert [tuple(row[0]) for row in one.collect()] == [(2, 3)]
    finally:
        spark.conf.unset("spark.sql.caseSensitive")


def test_drop_fields_matches_a_nested_path_with_the_resolver(spark):
    # A nested path is walked one level at a time, and every level is matched with the resolver.
    df = spark.sql("SELECT named_struct('a', named_struct('b', 1, 'c', 2)) AS s")

    # The path is rebuilt as a `WithField` per level, so the level the resolver matched takes the
    # spelling that was asked for, exactly as it does for `withField`.
    folded = df.select(col("s").dropFields("A.B"))
    assert folded.schema[0].dataType.simpleString() == "struct<A:struct<c:int>>"
    assert [row[0].A.c for row in folded.collect()] == [2]

    try:
        spark.conf.set("spark.sql.caseSensitive", "true")

        # The level is looked up before it is rebuilt, so a spelling the resolver no longer
        # matches is a missing field rather than a struct left alone.
        with pytest.raises(Exception, match=re.escape("[FIELD_NOT_FOUND]")):
            df.select(col("s").dropFields("A.B")).collect()
    finally:
        spark.conf.unset("spark.sql.caseSensitive")


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_a_rebuilt_level_takes_the_nullability_of_its_parent(spark):
    # A level of the path is rebuilt as a `WithField` whose value reads that level, so its
    # nullability is the one the READING has — `parent.nullable || field.nullable` — and not the
    # one the field declares. Sail gives it the declared one, so a nullable parent holding a
    # non-nullable child is reported non-nullable.
    schema = StructType(
        [
            StructField(
                "s",
                StructType(
                    [
                        StructField(
                            "a",
                            StructType([StructField("b", IntegerType(), False)]),
                            False,
                        )
                    ]
                ),
                True,
            )
        ]
    )
    df = spark.createDataFrame([(((1,),),)], schema)

    rebuilt = df.select(col("s").withField("a.c", lit(1)))
    assert rebuilt.schema[0].dataType["a"].nullable is True


def test_a_level_that_is_not_a_struct_reports_the_spark_class(spark):
    # A path is rewritten into one `update_fields` per level, and every level but the last is
    # also READ, with an `ExtractValue` built while the plan is. The two steps refuse a level that
    # is not a struct with different classes, so which one answers depends on where the level
    # sits: the parent of the last name is only ever the input of an `update_fields`, while a
    # level before it is read first, and the read is refused first.
    df = spark.sql("SELECT named_struct('b', 1, 'a', named_struct('x', 1)) AS s")

    # The parent of the last name: `update_fields` refuses its input.
    for column in (col("s").withField("b.x", lit(1)), col("s").dropFields("b.x")):
        with pytest.raises(
            Exception,
            match=re.escape('The first parameter requires the "STRUCT" type, however "s.b" has'),
        ):
            df.select(column).collect()

    # It is the parent of the last name wherever the path ends, not the first level.
    with pytest.raises(
        Exception,
        match=re.escape('The first parameter requires the "STRUCT" type, however "s.a.x" has'),
    ):
        df.select(col("s").withField("a.x.y", lit(1))).collect()

    # A level before it is read, and the read is refused with the class the same read raises on
    # its own, not with the one the `update_fields` above it would have raised.
    for column in (col("s").withField("b.x.y", lit(1)), col("s").dropFields("b.x.y")):
        with pytest.raises(
            Exception,
            match=re.escape(
                '[INVALID_EXTRACT_BASE_FIELD_TYPE] Can\'t extract a value from "s.b". '
                'Need a complex type [STRUCT, ARRAY, MAP] but got "INT".'
            ),
        ):
            df.select(column).collect()

    # A NULL base is read as NULL rather than refused, so the refusal is the one `update_fields`
    # raises even when the path has more than one level.
    null = spark.sql("SELECT NULL AS s")
    for column in (col("s").withField("a", lit(1)), col("s").withField("a.b", lit(1))):
        with pytest.raises(Exception, match=re.escape('has the type "VOID"')):
            null.select(column).collect()


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_a_struct_with_two_fields_of_one_name_can_be_read(spark):
    # Spark builds a struct whose fields repeat a name and reads it back. Sail builds the same
    # schema and cannot return the rows: the client refuses to convert a struct with duplicate
    # field names, so what the server sends has to be what a struct with distinct names sends.
    #
    # This is the output path rather than name resolution, and it is reached without any of the
    # operations this file exercises. It is pinned here because folding the case of a field name
    # opens a third way into it: `withField("a")` over `struct<a, A>` now correctly produces
    # `struct<a, a>`, where it used to leave `A` alone and answer the wrong rows.
    written = spark.sql("SELECT named_struct('a', 1, 'a', 2) AS s")
    assert written.schema[0].dataType.simpleString() == "struct<a:int,a:int>"
    assert [tuple(row[0]) for row in written.collect()] == [(1, 2)]

    # The same shape through the DataFrame API, which is the second way in.
    built = spark.sql("SELECT 1 AS x, 2 AS y").select(struct(col("x").alias("a"), col("y").alias("a")).alias("s"))
    assert [tuple(row[0]) for row in built.collect()] == [(1, 2)]


def test_a_lambda_parameter_is_named_the_way_spark_names_it(spark):
    # A lambda parameter has no name a user wrote, so Spark prints it as `namedlambdavariable()`.
    # The name Sail gives one is generated per call, so echoing it would put a name in the
    # message that changes between two identical runs.
    df = spark.sql("SELECT array(named_struct('a', 1)) AS arr")

    # Asserted twice: the name Sail generates counts up per call, so a name that leaked into the
    # message would differ between these two otherwise identical runs.
    for _ in range(2):
        with pytest.raises(Exception, match=re.escape('"namedlambdavariable().a" has the type')):
            df.select(spark_transform(col("arr"), lambda x: x.withField("a.b", lit(9)))).collect()


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_a_level_that_is_complex_but_not_a_struct_names_the_level_spark_names(spark):
    # `ExtractValue` has one arm per complex type, and none of them refuses the read: an array of
    # structs reads the field inside the elements, any other array is indexed, and a map is looked
    # up. The path therefore walks one level further than Sail lets it, and the level the error
    # names is the deeper one.
    arr = spark.sql("SELECT named_struct('a', array(named_struct('x', 1))) AS s")
    mp = spark.sql("SELECT named_struct('m', map('k', named_struct('x', 1))) AS s")

    with pytest.raises(Exception, match=re.escape('however "s.a.x" has the type "ARRAY<INT')):
        arr.select(col("s").withField("a.x.y", lit(1))).collect()

    with pytest.raises(Exception, match=re.escape('Cannot resolve "s.a.x[y]"')):
        arr.select(col("s").withField("a.x.y.z", lit(1))).collect()

    with pytest.raises(Exception, match=re.escape('however "s.m[k].x" has the type "INT"')):
        mp.select(col("s").withField("m.k.x.y", lit(1))).collect()


def test_a_nested_path_reports_an_ambiguous_level(spark):
    # The rule has two halves. The LAST name of the path is only ever written or dropped, so every
    # field it matches is written or dropped and a duplicate name is the correct output. Every
    # level BEFORE it is looked up with `ExtractValue` first, and a name that matches twice there
    # is ambiguous rather than a level to rebuild twice.
    dup = spark.sql("SELECT named_struct('a', named_struct('x', 1, 'y', 2), 'A', named_struct('x', 3, 'y', 4)) AS s")

    for column in (
        col("s").dropFields("a.x"),
        col("s").withField("a.y", lit(9)),
    ):
        with pytest.raises(Exception, match=re.escape("[AMBIGUOUS_REFERENCE_TO_FIELDS]")):
            dup.select(column).collect()

    try:
        spark.conf.set("spark.sql.caseSensitive", "true")

        # Nothing is ambiguous once the case is not folded, so the level is rebuilt as usual.
        dropped = dup.select(col("s").dropFields("a.x"))
        assert dropped.schema[0].dataType.simpleString() == "struct<a:struct<y:int>,A:struct<x:int,y:int>>"
        assert [tuple(row[0]) for row in dropped.collect()] == [((2,), (3, 4))]
    finally:
        spark.conf.unset("spark.sql.caseSensitive")


def test_with_field_renames_every_level_of_a_nested_path(spark):
    # A nested path is rebuilt as a `WithField` at each level, so every level the resolver matched
    # takes the spelling that was asked for, not only the last one. The rows are asserted as well
    # as the schema: renaming the type without moving the column underneath is the failure this
    # pins, and it shows up only when the rows are read.
    df = spark.sql("SELECT named_struct('a', named_struct('b', 1)) AS s")

    outer = df.withColumn("s", col("s").withField("A.b", lit(9)))
    assert outer.schema["s"].dataType.simpleString() == "struct<A:struct<b:int>>"
    assert [row.s.A.b for row in outer.collect()] == [9]

    both = df.withColumn("s", col("s").withField("A.B", lit(9)))
    assert both.schema["s"].dataType.simpleString() == "struct<A:struct<B:int>>"
    assert [row.s.A.B for row in both.collect()] == [9]

    exact = df.withColumn("s", col("s").withField("a.b", lit(9)))
    assert exact.schema["s"].dataType.simpleString() == "struct<a:struct<b:int>>"
    assert [row.s.a.b for row in exact.collect()] == [9]


def test_with_field_matches_the_existing_field_with_the_resolver(spark):
    # `WithField` matches the field it replaces with the resolver and names the result the way it
    # was asked for, so asking in another case replaces the field and renames it. Making the
    # analysis case sensitive stops the two from meeting, and then the field is appended instead,
    # which is what tells "it matched" apart from "it renamed whatever it found".
    df = spark.sql("SELECT named_struct('a', 1) AS s")

    assert df.withColumn("s", col("s").withField("a", lit(9))).schema["s"].dataType.simpleString() == "struct<a:int>"
    assert df.withColumn("s", col("s").withField("A", lit(9))).schema["s"].dataType.simpleString() == "struct<A:int>"
    assert (
        df.withColumn("s", col("s").withField("z", lit(9))).schema["s"].dataType.simpleString() == "struct<a:int,z:int>"
    )

    try:
        spark.conf.set("spark.sql.caseSensitive", "true")
        replaced = df.withColumn("s", col("s").withField("A", lit(9)))

        assert replaced.schema["s"].dataType.simpleString() == "struct<a:int,A:int>"
    finally:
        spark.conf.unset("spark.sql.caseSensitive")


def test_a_using_join_key_is_reachable_through_the_qualifier_of_each_side(spark):
    # The key of a `USING` join is one column in the output, but each side keeps its own hidden
    # copy, so the qualifier of either side still reaches it. Both sides are asserted because
    # keeping only the left one would pass a test that checks just `L`.
    left = spark.sql("SELECT 1 AS k, 10 AS a").alias("L")
    right = spark.sql("SELECT 1 AS k, 20 AS b").alias("R")

    joined = left.join(right, "k")

    assert [tuple(row) for row in joined.select("L.k").collect()] == [(1,)]
    assert [tuple(row) for row in joined.select("R.k").collect()] == [(1,)]


def test_a_join_key_does_not_shift_the_columns_of_a_later_operation(spark):
    # A `USING` join keeps the key of each side as a hidden field, interleaved with the visible
    # ones. An operation that pairs the names of the schema with its columns by POSITION would
    # take the hidden key for a visible column and drop the last one, so this pins that the
    # hidden fields are gone before any of them runs.
    left = spark.sql("SELECT 1 AS k, 10 AS a")
    right = spark.sql("SELECT 1 AS k, 20 AS b")
    joined = left.join(right, ["k"])

    assert [tuple(row) for row in joined.collect()] == [(1, 10, 20)]
    assert [tuple(row) for row in joined.withColumnsRenamed({"b": "z"}).collect()] == [(1, 10, 20)]
    assert joined.withColumn("x", lit(1)).columns == ["k", "a", "b", "x"]
    assert [tuple(row) for row in joined.withColumn("x", lit(1)).collect()] == [(1, 10, 20, 1)]
    assert [tuple(row) for row in joined.to(joined.schema).collect()] == [(1, 10, 20)]


def test_ambiguous_column_reference_is_written_on_several_lines(spark):
    # The condition has four sentences in the catalog and Spark joins them with a newline
    # (`ErrorClassesJSONReader` does `message.mkString("\n")`), so the message the user reads is
    # four lines rather than one long one.
    df = spark.sql("SELECT 1 AS name")

    with pytest.raises(Exception) as error:  # noqa: PT011
        df.join(df, df.name == df.name, "outer").select(df.name).collect()

    condition = str(error.value)
    start = condition.index("[AMBIGUOUS_COLUMN_REFERENCE]")
    end = condition.index('col("b.id"))`.') + len('col("b.id"))`.')
    assert condition[start:end].count("\n") == 3  # noqa: PLR2004


def test_an_ambiguous_aggregate_alias_reports_the_reference_condition(spark):
    # A `HAVING` reads the output of the aggregate, so a name that two aliases both carry is an
    # ambiguous reference there, reported with the same condition as any other one.
    spark.sql("SELECT 1 AS a, 2 AS b").createOrReplaceTempView("t_ambiguous_alias")

    with pytest.raises(
        Exception,
        match=re.escape("[AMBIGUOUS_REFERENCE] Reference `c` is ambiguous, could be: [`c`, `c`]."),
    ):
        spark.sql("SELECT count(*) AS c, sum(a) AS c FROM t_ambiguous_alias GROUP BY b HAVING c > 0").collect()


def test_col_regex_is_case_sensitive_when_configured(spark):
    # The pattern is compiled case-insensitively unless the analysis is case sensitive, so the
    # same regex selects the column under one setting and nothing under the other.
    df = spark.sql("SELECT 1 AS id, 2 AS other")

    try:
        spark.conf.set("spark.sql.caseSensitive", "false")
        assert df.select(df.colRegex("`ID`")).columns == ["id"]
        spark.conf.set("spark.sql.caseSensitive", "true")
        assert df.select(df.colRegex("`ID`")).columns == []
    finally:
        spark.conf.unset("spark.sql.caseSensitive")


def test_join_on_condition_keeps_both_key_columns(spark):
    # Joining on a condition keeps the key of each side, so the output has two columns named `k`
    # and the row carries both values. A comparison that keys the row by column name sees only
    # one of them, which is why the values are read by position here.
    left = spark.sql("SELECT * FROM VALUES (1, 'x') AS l(k, lv)")
    right = spark.sql("SELECT * FROM VALUES (1, 'p') AS r(k, rv)")

    joined = left.join(right, left.k == right.k, "inner")

    assert joined.columns == ["k", "lv", "k", "rv"]
    assert [list(row) for row in joined.collect()] == [[1, "x", 1, "p"]]


def test_left_outer_join_on_condition_keeps_the_unmatched_left_key(spark):
    # The left key of an unmatched row keeps its value while the right key is null. Both columns
    # are named `k`, so keying the row by name drops the left one and reports the null as if it
    # were the value of the key.
    left = spark.sql("SELECT * FROM VALUES (1, 'x'), (2, 'y') AS l(k, lv)")
    right = spark.sql("SELECT * FROM VALUES (1, 'p') AS r(k, rv)")

    joined = left.join(right, left.k == right.k, "left_outer")

    assert joined.columns == ["k", "lv", "k", "rv"]
    assert sorted([list(row) for row in joined.collect()], key=str) == [
        [1, "x", 1, "p"],
        [2, "y", None, None],
    ]


# The name of a `na` or `replace` subset is resolved by `Dataset.resolve`, which reports every
# field of the schema in order, unlike the suggestion of an unresolved column in a query.
_SIX = "SELECT 1 AS zzzzzz, 2 AS nope1, 3 AS c, 4 AS d, 5 AS e, 6 AS f"
_ALL_SIX = re.escape("Did you mean one of the following? [`zzzzzz`, `nope1`, `c`, `d`, `e`, `f`].")


def test_dropna_lists_every_field_of_the_schema(spark):
    with pytest.raises(Exception, match=_ALL_SIX):
        spark.sql(_SIX).dropna(subset=["nope"]).collect()


def test_fillna_lists_every_field_of_the_schema(spark):
    with pytest.raises(Exception, match=_ALL_SIX):
        spark.sql(_SIX).fillna(0, subset=["nope"]).collect()


def test_replace_lists_every_field_of_the_schema(spark):
    with pytest.raises(Exception, match=_ALL_SIX):
        spark.sql(_SIX).replace(1, 2, subset=["nope"]).collect()


def test_select_orders_and_truncates_the_suggestion(spark):
    # The same input through a column reference takes the other overload, which orders the names
    # by similarity and keeps five of them.
    with pytest.raises(
        Exception,
        match=re.escape("Did you mean one of the following? [`nope1`, `c`, `d`, `e`, `f`]."),
    ):
        spark.sql(_SIX).select("nope").collect()


def test_col_regex_does_not_fold_a_non_ascii_name(spark):
    # Java's `(?i)` folds ASCII alone, so a capital A with a diaeresis reaches only the column
    # spelled the same way, and the small one is left alone.
    df = spark.sql("SELECT 1 AS `Ä`, 2 AS `ä`")

    assert df.select(df.colRegex("`Ä`")).columns == ["Ä"]
    assert df.select(df.colRegex("`ä`")).columns == ["ä"]


# Spark matches the name against the pattern with `String.matches`, which is a whole-string match
# and appends nothing, so a pattern that ends in a comment or that alternates loses nothing. Sail
# anchors the pattern itself, so it has to group it first.
# (case, pattern, the columns of `a, ab, b, xb` that Spark selects)
_COL_REGEX_ANCHORING = [
    # Without the grouping the trailing comment would swallow the anchor and `ab` would match too.
    ("a comment at the end", "(?x)a#comment", ["a"]),
    ("a comment after a space", "(?x)a #comment", ["a"]),
    # Without the grouping this reads as "starts with a" or "ends with b", which adds `ab` and `xb`.
    ("an alternation", "a|b", ["a", "b"]),
    ("an alternation and a comment", "(?x)a|b#comment", ["a", "b"]),
    ("a plain name", "a", ["a"]),
    # The comment is a comment only in extended mode, so here it is part of the name to match.
    ("a hash without the extended mode", "a#comment", []),
    ("an escaped hash", "(?x)a\\#", []),
    ("only a comment", "(?x)#comment", []),
]


@pytest.mark.parametrize(("case", "pattern", "expected"), _COL_REGEX_ANCHORING)
@pytest.mark.parametrize("case_sensitive", ["false", "true"])
def test_col_regex_anchors_the_whole_pattern(spark, case_sensitive, case, pattern, expected):  # noqa: ARG001
    try:
        spark.conf.set("spark.sql.caseSensitive", case_sensitive)
        df = spark.sql("SELECT 1 AS a, 2 AS ab, 3 AS b, 4 AS xb")

        assert df.select(df.colRegex(f"`{pattern}`")).columns == expected
    finally:
        spark.conf.unset("spark.sql.caseSensitive")


def test_col_regex_rejects_a_pattern_that_does_not_parse(spark):
    # The pattern is read before it is anchored, so one that does not parse is an error rather
    # than a selection that matches nothing.
    # TODO: Spark reports the message of the JVM engine (`Unclosed group near index 6`), and Sail
    # reports the one of its own, so only the rejection is pinned here.
    df = spark.sql("SELECT 1 AS a")

    with pytest.raises(Exception):  # noqa: B017, PT011
        df.select(df.colRegex("`a(`")).collect()


def test_col_regex_with_a_qualifier_selects_nothing(spark):
    # Spark has a branch for a qualified pattern, but the client sends the whole string as the
    # pattern, so the qualifier never reaches it and the name it compares contains a dot.
    df = spark.sql("SELECT 1 AS a, 2 AS ab").alias("t")

    assert df.select(df.colRegex("`t`.`a`")).columns == []


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_col_regex_folds_only_ascii(spark):
    # Spark compiles the pattern with Java's `(?i)`, which is ASCII-only unless `UNICODE_CASE` is
    # set, so a Greek capital sigma does not reach a column named with a small one.
    df = spark.sql("SELECT 1 AS `\u03c3`")

    assert df.select(df.colRegex("`\u03a3`")).columns == []
    assert df.select(df.colRegex("`\u03c3`")).columns == ["\u03c3"]

    # A metacharacter in the pattern does not exempt it from the rule, which is what makes the gap
    # reachable: `.*` is the shape a pattern is usually written in.
    latin = spark.sql("SELECT 1 AS `\u00e4x`")

    assert latin.select(latin.colRegex("`\u00c4X`")).columns == []
    assert latin.select(latin.colRegex("`\u00c4.*`")).columns == []
    assert latin.select(latin.colRegex("`\u00e4X`")).columns == ["\u00e4x"]
    assert latin.select(latin.colRegex("`\u00e4.*`")).columns == ["\u00e4x"]


# `CANNOT_RESOLVE_DATAFRAME_COLUMN` renders the name through `UnresolvedAttribute.name`, which
# quotes a part only when it contains a dot, unlike the fully quoted form used for a column name.
# (case, the column of the frame, the name as written by the client, the name in the message)
_DATAFRAME_COLUMN_NAMES = [
    ("plain", "plain", "plain", "plain"),
    ("a space", "`a b`", "a b", "a b"),
    ("a dot", "`a.b`", "`a.b`", "`a.b`"),
    ("a back quote", "`a``b`", "`a``b`", "a`b"),
    ("a leading digit", "`1a`", "1a", "1a"),
    ("a non ascii letter", "`\u00e4`", "\u00e4", "\u00e4"),
]


@pytest.mark.parametrize(("case", "column", "written", "rendered"), _DATAFRAME_COLUMN_NAMES)
def test_cannot_resolve_dataframe_column_renders_the_name(spark, case, column, written, rendered):  # noqa: ARG001
    df = spark.sql(f"SELECT 1 AS {column}")
    other = spark.sql("SELECT 2 AS c")

    with pytest.raises(
        Exception,
        match=re.escape(f'Cannot resolve dataframe column "{rendered}".'),
    ):
        other.select(df[written]).collect()


def test_na_subset_resolves_a_quoted_name(spark):
    # The subset entry is resolved as a column reference, so a name that needs quoting is matched
    # by the part it parses to rather than by the string the client wrote.
    df = spark.sql("SELECT CAST(NULL AS INT) AS `a b`")

    assert df.fillna(0, subset=["`a b`"]).collect() == [Row(**{"a b": 0})]


def test_na_subset_resolves_a_quoted_name_containing_a_dot(spark):
    # A dot inside back quotes is part of the name, not a separator, so this is a column and not
    # a walk into one.
    df = spark.sql("SELECT CAST(NULL AS INT) AS `a.b`")

    assert df.fillna(0, subset=["`a.b`"]).collect() == [Row(**{"a.b": 0})]


def test_na_subset_reports_an_empty_suggestion(spark):
    # This path reports every field of the schema rather than a suggestion, and it has no other
    # sub-condition to fall back to, so an input with no column reports an empty list.
    with pytest.raises(
        Exception,
        match=re.escape(
            "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with "
            "name `nope` cannot be resolved. Did you mean one of the following? []."
        ),
    ):
        spark.range(1).select().dropna(subset=["nope"]).collect()


def test_to_schema_suggests_the_columns_in_the_order_of_the_plan(spark):
    # Unlike the suggestion of a name written in a query, this one is not sorted by name first,
    # so names at the same distance keep the order of the input.
    schema = StructType([StructField("nope", IntegerType(), nullable=False)])

    with pytest.raises(
        Exception,
        match=re.escape("Did you mean one of the following? [`zz`, `aa`, `mm`]."),
    ):
        spark.sql("SELECT 1 AS zz, 2 AS aa, 3 AS mm").to(schema).collect()


def test_unresolved_column_suggestion_measures_the_quoted_name(spark):
    # The distance is measured against the name as the analyzer renders it, which quotes a part
    # that is not a plain identifier. Measuring the unquoted name would order `ab` before `a b`.
    with pytest.raises(
        Exception,
        match=re.escape("Did you mean one of the following? [`a b`, `abc`, `ab`]."),
    ):
        spark.sql("SELECT 1 AS `a b`, 2 AS ab, 3 AS abc").select("`a c`").collect()


def test_col_regex_matches_the_whole_name(spark):
    # Spark matches the pattern against the whole name, so an alternation must not escape the
    # anchors: `a|b` selects the column named `a`, not the one ending in `b`.
    df = spark.sql("SELECT 1 AS ab, 2 AS xb, 3 AS a")

    assert df.select(df.colRegex("`a|b`")).columns == ["a"]


# The subset name of an NA operation is parsed as an attribute name before it is looked up, so a
# name the parser rejects is a syntax error rather than a column that could not be found. Each
# entry is one branch of `AttributeNameParser.parseAttributeName`.
# (case, the name as written by the client)
_MALFORMED_NAMES = [
    ("unterminated backtick", "`a"),
    ("backtick after text", "a`b"),
    ("backtick then text", "`a`b"),
    ("leading dot", ".a"),
    ("trailing dot", "a."),
    ("double dot", "a..b"),
    ("only a dot", "."),
]


def _syntax_error(name):
    return re.escape(f"[INVALID_ATTRIBUTE_NAME_SYNTAX] Syntax error in the attribute name: {name}.")


@pytest.mark.parametrize(("case", "name"), _MALFORMED_NAMES)
def test_fillna_rejects_a_malformed_subset_name(spark, case, name):  # noqa: ARG001
    df = spark.sql("SELECT CAST(NULL AS INT) AS a, 1 AS b")

    with pytest.raises(Exception, match=_syntax_error(name)):
        df.fillna(0, subset=[name]).collect()


def test_dropna_rejects_a_malformed_subset_name(spark):
    # The three entry points share the rule, so one spelling is enough for the other two.
    df = spark.sql("SELECT CAST(NULL AS INT) AS a, 1 AS b")

    with pytest.raises(Exception, match=_syntax_error("a.")):
        df.dropna(subset=["a."]).collect()


def test_replace_rejects_a_malformed_subset_name(spark):
    df = spark.sql("SELECT CAST(NULL AS INT) AS a, 1 AS b")

    with pytest.raises(Exception, match=_syntax_error("a..b")):
        df.replace(1, 2, subset=["a..b"]).collect()


# `replace` only works on a top-level column, so any name that resolves to something else gets its
# own condition. (case, the query, the subset name, the name as it reaches the message)
_NESTED_REPLACE = [
    ("struct field", "SELECT named_struct('x', 1) AS s, 1 AS a", "s.x", "`s`.`x`"),
    ("struct field quoted", "SELECT named_struct('x', 1) AS s, 1 AS a", "`s`.`x`", "`s`.`x`"),
    (
        "two levels",
        "SELECT named_struct('t', named_struct('u', 1)) AS s, 1 AS a",
        "s.t.u",
        "`s`.`t`.`u`",
    ),
    (
        "intermediate struct",
        "SELECT named_struct('t', named_struct('u', 1)) AS s, 1 AS a",
        "s.t",
        "`s`.`t`",
    ),
    ("array of struct", "SELECT array(named_struct('x', 1)) AS s, 1 AS a", "s.x", "`s`.`x`"),
]


@pytest.mark.parametrize(("case", "query", "name", "rendered"), _NESTED_REPLACE)
def test_replace_rejects_a_nested_subset_name(spark, case, query, name, rendered):  # noqa: ARG001
    df = spark.sql(query)

    with pytest.raises(
        Exception,
        match=re.escape(
            "[UNSUPPORTED_FEATURE.REPLACE_NESTED_COLUMN] The feature is not supported: The replace "
            f"function does not support nested column {rendered}."
        ),
    ):
        df.replace(1, 2, subset=[name]).collect()


def test_replace_reports_a_missing_root_as_unresolved(spark):
    # The control: with no column to walk into, the name is unresolved like any other.
    df = spark.sql("SELECT named_struct('x', 1) AS s, 1 AS a")

    with pytest.raises(
        Exception,
        match=re.escape(
            "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with "
            "name `nope`.`x` cannot be resolved. Did you mean one of the following? [`s`, `a`]."
        ),
    ):
        df.replace(1, 2, subset=["nope.x"]).collect()


def test_replace_resolves_a_quoted_subset_name_containing_a_dot(spark):
    # A dot inside back quotes is part of the name, so this is a column and not a walk into one.
    df = spark.sql("SELECT 1 AS `a.b`, 2 AS c")

    assert [list(row) for row in df.replace(1, 9, subset=["`a.b`"]).collect()] == [[9, 2]]
