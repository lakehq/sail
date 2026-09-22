import re

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql import Row
from pyspark.sql.functions import col, count, expr, first, lit, row_number, struct
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import transform as spark_transform
from pyspark.sql.types import IntegerType, StructField, StructType
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


def test_a_subset_name_is_resolved_with_its_qualifier(spark):
    # A subset name is resolved as a column reference, and the leading part of one can be the
    # qualifier of a relation rather than a column. `t.s` is the column `s` of `t`, and `t.s.a`
    # walks into it; neither is an unresolved column.
    df = spark.sql("SELECT named_struct('a', CAST(NULL AS INT)) AS s, CAST(NULL AS INT) AS a").alias("t")

    # `fill(value, subset)` keeps only the names that resolve to an attribute, so a struct and a
    # nested field are both left alone.
    for subset in ["t.s", "t.s.a"]:
        assert [tuple(row) for row in df.fillna(9, subset=[subset]).collect()] == [(Row(a=None), None)]

    # A qualified name that resolves to nothing is still an error.
    with pytest.raises(Exception, match=re.escape("[UNRESOLVED_COLUMN.WITH_SUGGESTION]")):
        df.fillna(9, subset=["t.nope"]).collect()

    # `fill(map)` with two entries resolves every name, so it refuses the nested one.
    with pytest.raises(Exception, match=re.escape("[UNSUPPORTED_FEATURE.REPLACE_NESTED_COLUMN]")):
        df.na.fill({"t.s.a": 1, "a": 2}).collect()

    # `dropna` uses a qualified name as a key like any other.
    nulls = spark.sql("SELECT CAST(NULL AS INT) AS a, 1 AS b").alias("t")
    assert nulls.dropna(subset=["t.a"]).collect() == []


def test_fillna_fills_a_qualified_attribute(spark):
    # `t.a` resolves to the column `a`, so `fill(value, subset)` fills it: Spark fills the
    # attributes the names resolve to (`fillValue` compares them with `semanticEquals`), and a
    # qualified name is not the name of any column.
    df = spark.sql("SELECT CAST(NULL AS INT) AS a").alias("t")

    assert df.fillna(9, subset=["t.a"]).collect() == [Row(a=9)]

    # The same root through `fill(map)`, where `t` is also a struct column: the qualifier wins, so
    # the key names the column `a` and fills it. (Two keys that both resolve to `a` are not used
    # here: which value wins then depends on the iteration order of an `AttributeMap`.)
    shadowed = spark.sql("SELECT named_struct('x', 1) AS t, CAST(NULL AS INT) AS a, 1 AS b").alias("t")
    assert [tuple(row)[1] for row in shadowed.na.fill({"t.a": 9, "b": 8}).collect()] == [9]


def test_replace_resolves_a_subset_name_with_its_qualifier(spark):
    # `replace` resolves every name it is given, and `t.a` resolves to the column `a` of `t`, so
    # the value is replaced there. The leading part of a name is not necessarily a column.
    df = spark.sql("SELECT 1 AS a, 1 AS b").alias("t")

    assert [tuple(row) for row in df.na.replace(1, 7, subset=["t.a"]).collect()] == [(7, 1)]
    with pytest.raises(Exception, match=re.escape("[UNRESOLVED_COLUMN.WITH_SUGGESTION]")):
        df.na.replace(1, 7, subset=["t.nope"]).collect()

    # A qualified name that walks into a struct is refused as nested, not reported as unresolved.
    nested = spark.sql("SELECT named_struct('a', 1) AS s, 1 AS a").alias("t")
    with pytest.raises(Exception, match=re.escape("[UNSUPPORTED_FEATURE.REPLACE_NESTED_COLUMN]")):
        nested.na.replace(1, 7, subset=["t.s.a"]).collect()


def test_a_subset_name_reaches_a_join_key_through_its_qualifier(spark):
    # A USING join keeps one key in its output and the other only in the metadata output, where a
    # qualified name still reaches it (`LogicalPlan.resolve` falls back to the metadata output).
    # So `r.id` names a column Spark resolves, not a missing one.
    left = spark.sql("SELECT 1 AS id, CAST(NULL AS INT) AS a").alias("l")
    right = spark.sql("SELECT 1 AS id, 2 AS b").alias("r")
    joined = left.join(right, "id")

    assert [tuple(row) for row in joined.fillna(0, subset=["r.id"]).collect()] == [(1, None, 2)]
    assert [tuple(row) for row in joined.na.fill({"r.id": 0, "a": 5}).collect()] == [(1, 5, 2)]


def test_a_subset_name_takes_the_qualifier_before_a_column_of_the_same_name(spark):
    # `t` is both the relation and a struct column in it. Spark tries the longest qualifier first,
    # so `t.a` is the column `a` of the relation, whether or not the struct has a field `a`, and it
    # is not a nested field to refuse. The other entry of the map is filled.
    for shadow in ("named_struct('x', 1)", "named_struct('a', 1)"):
        df = spark.sql(f"SELECT {shadow} AS t, CAST(NULL AS INT) AS a, CAST(NULL AS INT) AS b").alias("t")
        assert [tuple(row)[2] for row in df.na.fill({"t.a": 9, "b": 8}).collect()] == [8]


@pytest.mark.skipif(
    is_jvm_spark() and pyspark_version() < (4, 2), reason="Before 4.2 Spark refuses a field of a NULL base"
)
def test_a_subset_name_that_walks_through_null_is_not_an_error(spark):
    # Reading a field of a NULL base is NULL rather than an error (`ExtractValue.applyOrNull`), so
    # the name resolves to something that is not a column, and is treated as nested.
    df = spark.sql("SELECT NULL AS n, CAST(NULL AS INT) AS a").alias("t")

    assert [tuple(row) for row in df.fillna(9, subset=["t.n.x"]).collect()] == [(None, None)]
    assert df.dropna(subset=["t.n.x"]).collect() == []
    with pytest.raises(Exception, match=re.escape("[UNSUPPORTED_FEATURE.REPLACE_NESTED_COLUMN]")):
        df.na.replace(1, 7, subset=["t.n.x"]).collect()


def test_a_subset_name_over_a_column_selected_twice_is_not_ambiguous(spark):
    # Selecting the same column twice gives two outputs that are the same attribute, and Spark
    # removes duplicate candidates before it checks for ambiguity (`AttributeSeq.resolve`), so the
    # name reaches both.
    df = spark.sql("SELECT CAST(NULL AS INT) AS a, 1 AS b").select("a", "a", "b")

    assert [tuple(row) for row in df.fillna(9, subset=["a"]).collect()] == [(9, 9, 1)]
    assert [tuple(row) for row in df.na.fill({"a": 9, "b": 2}).collect()] == [(9, 9, 1)]
    assert df.dropna(subset=["a"]).collect() == []
    ones = spark.sql("SELECT 1 AS a").select("a", "a")
    assert [tuple(row) for row in ones.na.replace(1, 7, subset=["a"]).collect()] == [(7, 7)]


def test_a_subset_name_over_two_expressions_of_one_name_is_ambiguous(spark):
    # Two different expressions given the same name are two attributes, so Spark reports the name
    # as ambiguous, where the same column selected twice is one attribute and is not.
    df = spark.sql("SELECT CAST(NULL AS INT) AS a, CAST(NULL AS INT) AS a")

    with pytest.raises(Exception, match=re.escape("[AMBIGUOUS_REFERENCE]")):
        df.fillna(9, subset=["a"]).collect()


def test_a_qualified_subset_name_is_resolved_however_deep_it_walks(spark):
    # A qualified subset name is resolved in full before any operation decides what to do with it,
    # so the same three answers as for a bare nested name hold two levels down: `fill(value,
    # subset)` leaves it alone, `fill(map)` and `replace` refuse it, and `dropna` uses it as a key.
    df = spark.sql(
        "SELECT named_struct('inner', named_struct('x', CAST(NULL AS INT))) AS s, CAST(NULL AS INT) AS a"
    ).alias("t")

    assert [tuple(row) for row in df.fillna(9, subset=["t.s.inner.x"]).collect()] == [(Row(inner=Row(x=None)), None)]
    for refused in (
        lambda: df.na.fill({"t.s.inner.x": 1, "a": 2}),
        lambda: df.na.replace(1, 7, subset=["t.s.inner.x"]),
    ):
        with pytest.raises(Exception, match=re.escape("[UNSUPPORTED_FEATURE.REPLACE_NESTED_COLUMN]")):
            refused().collect()
    assert df.dropna(subset=["t.s.inner.x"]).collect() == []

    # Resolving it in full is also why a field it walks into that does not exist is a missing
    # field, even on the path that would have left the name alone.
    for missing in (lambda: df.fillna(9, subset=["t.s.nope"]), lambda: df.dropna(subset=["t.s.nope"])):
        with pytest.raises(Exception, match=re.escape("[FIELD_NOT_FOUND] No such struct field `nope` in `inner`.")):
            missing().collect()

    # Across a join, the qualifier picks the struct of one side.
    left = spark.sql("SELECT 1 AS id, named_struct('x', CAST(NULL AS INT)) AS s").alias("l")
    right = spark.sql("SELECT 1 AS id, named_struct('x', 5) AS s").alias("r")
    joined = left.join(right, "id")
    assert joined.dropna(subset=["l.s.x"]).collect() == []
    assert [tuple(row) for row in joined.dropna(subset=["r.s.x"]).collect()] == [(1, Row(x=None), Row(x=5))]


def test_replace_and_fill_drop_the_metadata_of_the_columns_they_change(spark):
    # `replaceCol` and `fillCol` rebuild a column as `.as(name)` over a `CaseKeyWhen` or a
    # `coalesce`, with no explicit metadata, so a column they change loses what it carried
    # (`Alias.metadata`). A column they pass through is the attribute itself and keeps it, and so
    # does one in the subset whose type does not match the value, which they do not change.
    def meta(df):
        df._cached_schema = None  # noqa: SLF001
        return {field.name: dict(field.metadata) for field in df.schema.fields}

    def tagged(sql):
        return spark.sql(sql).withMetadata("a", {"k": "a"}).withMetadata("b", {"k": "b"}).withMetadata("c", {"k": "c"})

    changed_a = {"a": {}, "b": {"k": "b"}, "c": {"k": "c"}}
    values = tagged("SELECT 1 AS a, 1 AS b, 'x' AS c")
    assert meta(values.na.replace(1, 7, subset=["a", "c"])) == changed_a
    assert meta(values.na.replace(1, 7)) == {"a": {}, "b": {}, "c": {"k": "c"}}
    # The same holds for a column reached through its qualifier.
    assert meta(values.alias("t").na.replace(1, 7, subset=["t.a"])) == changed_a

    nulls = tagged("SELECT CAST(NULL AS INT) AS a, CAST(NULL AS INT) AS b, CAST(NULL AS STRING) AS c")
    assert meta(nulls.fillna(9, subset=["a", "c"])) == changed_a
    assert meta(nulls.fillna(9)) == {"a": {}, "b": {}, "c": {"k": "c"}}
    # `dropna` filters rows and rebuilds no column.
    assert meta(nulls.dropna(subset=["a"])) == {"a": {"k": "a"}, "b": {"k": "b"}, "c": {"k": "c"}}


def test_a_qualified_subset_name_reaches_only_the_side_it_names(spark):
    # With one relation, `t.a` and `a` are the same column, so a qualifier that was thrown away
    # would pass unnoticed. Across a join both sides have `a`, and the qualifier is what picks one.
    left = spark.sql("SELECT 1 AS id, 1 AS a").alias("l")
    right = spark.sql("SELECT 1 AS id, 1 AS a").alias("r")
    assert [tuple(row) for row in left.join(right, "id").na.replace(1, 7, subset=["l.a"]).collect()] == [(1, 7, 1)]

    left = spark.sql("SELECT 1 AS id, CAST(NULL AS INT) AS a").alias("l")
    right = spark.sql("SELECT 1 AS id, 5 AS a").alias("r")
    joined = left.join(right, "id")
    assert joined.dropna(subset=["l.a"]).collect() == []
    assert [tuple(row) for row in joined.dropna(subset=["r.a"]).collect()] == [(1, None, 5)]


def test_a_qualified_name_reaches_a_subset_but_not_an_operation_that_matches_by_name(spark):
    # Spark has two rules here. A subset name is resolved as a column reference, so a qualifier
    # reaches the column. `drop`, `withColumnRenamed` and `withMetadata` compare the name they are
    # given with the name of each column instead, and `t.a` is not the name of any column, so they
    # leave the frame alone. Resolving the qualifier in the second group would change what they do.
    df = spark.sql("SELECT 1 AS a, 2 AS b").alias("t")

    assert df.drop("t.a").schema.names == ["a", "b"]
    assert df.withColumnRenamed("t.a", "z").schema.names == ["a", "b"]
    assert df.withMetadata("t.a", {"k": "v"}).schema["a"].metadata == {}

    assert [tuple(row) for row in df.na.replace(1, 7, subset=["t.a"]).collect()] == [(7, 2)]


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


def test_union_by_name_rejects_an_extra_column_on_the_right(spark):
    # Without `allowMissingColumns` the two sides have to name the same columns: an extra one on
    # the right is appended to it (`ResolveUnion`), so the two differ in the number of columns.
    left = spark.sql("SELECT 1 AS a")
    right = spark.sql("SELECT 2 AS a, 3 AS b")
    with pytest.raises(Exception, match="NUM_COLUMNS_MISMATCH"):
        left.unionByName(right).collect()


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


def test_union_by_name_fills_a_missing_nested_struct_field(spark):
    # `allowMissingColumns` reaches into a struct as well, filling the field the other side
    # does not have (`ResolveUnion.addFields`).
    left = spark.sql("SELECT named_struct('x', 1, 'y', 2) AS s")
    right = spark.sql("SELECT named_struct('x', 3) AS s")
    rows = left.unionByName(right, allowMissingColumns=True).collect()
    assert sorted((r.s.asDict() for r in rows), key=lambda x: x["x"]) == [
        {"x": 1, "y": 2},
        {"x": 3, "y": None},
    ]


def test_union_by_name_matches_nested_struct_fields_case_insensitively(spark):
    # The fields of a struct are matched by the resolver, like any other name.
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


def test_a_union_reports_the_metadata_of_its_first_input_only(spark):
    # `Union.mergeChildOutputs` takes the metadata of the FIRST child verbatim, so a key that only
    # the second one carries does not reach the output. The left side having none is what tells
    # "reports the first" apart from "merges both".
    left = spark.sql("SELECT 1 AS a")
    right = spark.sql("SELECT 2 AS a").withMetadata("a", {"k": "v"})

    union = left.union(right)
    union._cached_schema = None  # noqa: SLF001

    assert [dict(field.metadata) for field in union.schema.fields] == [{}]


@pytest.mark.parametrize("by_name", [False, True])
def test_a_union_reports_the_metadata_of_the_first_input_after_coercion(spark, by_name):
    # Spark widens the children of a union first, casting a column whose type is narrower than
    # the result as `Alias(Cast(...))` (`WidenSetOperationTypes.widenTypes`), and a cast carries no
    # metadata (`Alias.metadata`). The output takes the metadata of the first child after that, so
    # what decides is whether the LEFT column was cast, not whether the two sides agree.
    def union(left, right):
        result = left.unionByName(right) if by_name else left.union(right)
        result._cached_schema = None  # noqa: SLF001
        return result

    # The left column is narrower, so it is cast and its metadata goes with the cast.
    left = spark.sql("SELECT CAST(1 AS INT) AS a").withMetadata("a", {"side": "L"})
    right = spark.sql("SELECT CAST(2 AS BIGINT) AS a").withMetadata("a", {})
    assert union(left, right).schema["a"].metadata == {}
    assert sorted(tuple(row) for row in union(left, right).collect()) == [(1,), (2,)]

    # Neither side is cast.
    left = spark.sql("SELECT CAST(1 AS INT) AS a").withMetadata("a", {"side": "L"})
    right = spark.sql("SELECT CAST(2 AS INT) AS a").withMetadata("a", {"side": "R"})
    assert union(left, right).schema["a"].metadata == {"side": "L"}

    # The decision is taken per column: in one union, the cast column loses its metadata and the
    # one beside it keeps it. By name, the right side lists the columns in another order, which
    # the metadata has to follow.
    left = (
        spark.sql("SELECT CAST(1 AS INT) AS a, CAST(1 AS INT) AS b")
        .withMetadata("a", {"side": "L"})
        .withMetadata("b", {"side": "L"})
    )
    right = spark.sql(
        "SELECT CAST(2 AS INT) AS b, CAST(2 AS BIGINT) AS a"
        if by_name
        else "SELECT CAST(2 AS BIGINT) AS a, CAST(2 AS INT) AS b"
    )
    right = right.withMetadata("a", {}).withMetadata("b", {"side": "R"})
    result = union(left, right)
    assert result.schema["a"].metadata == {}
    assert result.schema["b"].metadata == {"side": "L"}
    assert sorted(tuple(row) for row in result.collect()) == [(1, 1), (2, 2)]


@pytest.mark.parametrize("by_name", [False, True])
def test_a_union_drops_the_metadata_of_a_cast_column_the_right_side_does_not_tag(spark, by_name):
    # The left column is narrower, so Spark casts it and its metadata goes with the cast, even when
    # the right side carries none to replace it.
    left = spark.sql("SELECT CAST(1 AS INT) AS a").withMetadata("a", {"side": "L"})
    right = spark.sql("SELECT CAST(2 AS BIGINT) AS a")

    result = left.unionByName(right) if by_name else left.union(right)
    result._cached_schema = None  # noqa: SLF001
    assert result.schema["a"].metadata == {}


def test_an_intersection_drops_the_metadata_of_a_cast_left_column(spark):
    # `Intersect` and `Except` widen their inputs too, so a narrower left column is cast and loses
    # its metadata.
    left = spark.sql("SELECT CAST(1 AS INT) AS a").withMetadata("a", {"side": "L"})
    right = spark.sql("SELECT CAST(1 AS BIGINT) AS a")

    for result in (left.intersect(right), left.exceptAll(right), left.subtract(right)):
        result._cached_schema = None  # noqa: SLF001
        assert result.schema["a"].metadata == {}


# Every pair of these types, in both ANSI modes, as Spark answers it. A union widens its inputs
# first (`WidenSetOperationTypes`) and reports the metadata of the left one only where the left
# column is not cast (`Alias.metadata`), so each cell is `L` where the metadata is kept, `.` where
# the left column is cast and loses it, and `x` where Spark refuses the pair with
# `INCOMPATIBLE_COLUMN_TYPE`. The widening itself depends on the mode: with ANSI a string is cast
# to the other side and an integral type with FLOAT widens to DOUBLE
# (`AnsiTypeCoercion.findWiderTypeForTwo`); without it the string wins (`TypeCoercion`).
# Rows are the left type and columns the right one, in the order of `_UNION_TYPES`.
_UNION_TYPES = {
    "null": "NULL",
    "boolean": "true",
    "tinyint": "CAST(1 AS TINYINT)",
    "int": "CAST(1 AS INT)",
    "bigint": "CAST(1 AS BIGINT)",
    "float": "CAST(1 AS FLOAT)",
    "double": "CAST(1 AS DOUBLE)",
    "decimal(5,2)": "CAST(1 AS DECIMAL(5, 2))",
    "decimal(20,0)": "CAST(1 AS DECIMAL(20, 0))",
    "string": "'x'",
    "binary": "X'01'",
    "date": "DATE'2020-01-01'",
    "timestamp": "TIMESTAMP'2020-01-01 00:00:00'",
    "timestamp_ntz": "TIMESTAMP_NTZ'2020-01-01 00:00:00'",
    "interval": "INTERVAL '1' DAY",
}
_UNION_GRID = {
    "true": [
        "L..............",
        "LLxxxxxxxLxxxxx",
        "LxL.......xxxxx",
        "LxLL......xxxxx",
        "LxLLL....Lxxxxx",
        "Lx...L....xxxxx",
        "LxLLLLLLLLxxxxx",
        "LxL....L..xxxxx",
        "LxLLL...L.xxxxx",
        "L........L....x",
        "LxxxxxxxxLLxxxx",
        "LxxxxxxxxLxL..x",
        "LxxxxxxxxLxLLLx",
        "LxxxxxxxxLxL.Lx",
        "LxxxxxxxxxxxxxL",
    ],
    "false": [
        "L..............",
        "LLxxxxxxxxxxxxx",
        "LxL.......xxxxx",
        "LxLL......xxxxx",
        "LxLLL.....xxxxx",
        "LxLLLL....xxxxx",
        "LxLLLLLLL.xxxxx",
        "LxL....L..xxxxx",
        "LxLLL...L.xxxxx",
        "LxLLLLLLLLxLLLL",
        "LxxxxxxxxxLxxxx",
        "Lxxxxxxxx.xL..x",
        "Lxxxxxxxx.xLLLx",
        "Lxxxxxxxx.xL.Lx",
        "Lxxxxxxxx.xxxxL",
    ],
}


def _union_cases():
    for ansi, grid in _UNION_GRID.items():
        for left, row in zip(_UNION_TYPES, grid, strict=False):
            for right, cell in zip(_UNION_TYPES, row, strict=False):
                yield pytest.param(ansi, left, right, cell, id=f"ansi={ansi}-{left}-{right}")


@pytest.mark.parametrize(("ansi", "left", "right", "cell"), _union_cases())
def test_a_union_of_two_types(spark, ansi, left, right, cell):
    try:
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        tagged = spark.sql(f"SELECT {_UNION_TYPES[left]} AS a").withMetadata("a", {"side": "L"})
        result = tagged.union(spark.sql(f"SELECT {_UNION_TYPES[right]} AS a"))
        result._cached_schema = None  # noqa: SLF001
        if cell == "x":
            with pytest.raises(Exception, match=re.escape("[INCOMPATIBLE_COLUMN_TYPE]")):
                _ = result.schema
        else:
            assert result.schema["a"].metadata == ({"side": "L"} if cell == "L" else {})
    finally:
        spark.conf.unset("spark.sql.ansi.enabled")


_INCOMPATIBLE = "can only be performed on tables with compatible column types."
_ANSI_HINT = (
    "\nTo fix the error, you might need to add explicit type casts. If necessary set "
    "spark.sql.ansi.enabled to false to bypass this error."
)


def _refused(operator, ordinal, right, left, hint=""):
    return (
        f"[INCOMPATIBLE_COLUMN_TYPE] {operator} {_INCOMPATIBLE} The {ordinal} column of the second table is "
        f'"{right}" type which is not compatible with "{left}" at the same column of the first table.{hint}.'
    )


def _map_refused(name):
    return (
        "[UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE] The feature is not supported: Cannot have MAP type "
        f'columns in DataFrame which calls set operations (INTERSECT, EXCEPT, etc.), but the type of column `{name}` is "MAP<INT, INT>".'
    )


def _mismatched(operator):
    return (
        f"[NUM_COLUMNS_MISMATCH] {operator} can only be performed on inputs with the same number of columns, "
        "but the first input has 2 columns and the second input has 1 columns."
    )


# How each operation checks that the columns of its two inputs are compatible, after it widens
# them (`CheckAnalysis`, `TypeCoercionValidation.getDataTypesAreCompatibleFn`). A union accepts two
# types that differ only in the names of what they nest; the other operations check the widening
# without ANSI mode, and every operation widens a struct only when the names of its fields match.
# With ANSI mode Spark suggests turning it off when that alone accepts every column. Measured on the
# Spark JVM; `None` is an operation Spark accepts.
_SET_OPERATION_CHECKS = [
    ("union", "true AS a", "1 AS a", _refused("UNION", "first", "INT", "BOOLEAN")),
    ("union", "1 AS a, true AS b", "1 AS a, 1 AS b", _refused("UNION", "second", "INT", "BOOLEAN")),
    ("union", "1 a, 2 b, 3 c, 4 d, true e", "1 a, 2 b, 3 c, 4 d, 1 e", _refused("UNION", "5th", "INT", "BOOLEAN")),
    ("unionByName", "true AS a", "1 AS a", _refused("UNION", "first", "INT", "BOOLEAN")),
    ("unionAll", "true AS a", "1 AS a", _refused("UNION", "first", "INT", "BOOLEAN")),
    ("intersect", "true AS a", "1 AS a", _refused("INTERSECT", "first", "INT", "BOOLEAN")),
    ("intersectAll", "true AS a", "1 AS a", _refused("INTERSECT ALL", "first", "INT", "BOOLEAN")),
    ("subtract", "true AS a", "1 AS a", _refused("EXCEPT", "first", "INT", "BOOLEAN")),
    ("exceptAll", "true AS a", "1 AS a", _refused("EXCEPT ALL", "first", "INT", "BOOLEAN")),
    ("union", "named_struct('x', 1) AS a", "named_struct('y', 1) AS a", None),
    ("union", "named_struct('x', 1) AS a", "named_struct('X', 1L) AS a", None),
    (
        "union",
        "named_struct('x', 1) AS a",
        "named_struct('y', 1L) AS a",
        _refused("UNION", "first", "STRUCT<y: BIGINT NOT NULL>", "STRUCT<x: INT NOT NULL>"),
    ),
    (
        "intersect",
        "named_struct('x', 1) AS a",
        "named_struct('y', 1) AS a",
        _refused("INTERSECT", "first", "STRUCT<y: INT NOT NULL>", "STRUCT<x: INT NOT NULL>"),
    ),
    (
        "union",
        "named_struct('x', CAST(NULL AS INT)) AS a",
        "named_struct('y', CAST(NULL AS BIGINT)) AS a",
        _refused("UNION", "first", "STRUCT<y: BIGINT>", "STRUCT<x: INT>"),
    ),
    (
        "intersect",
        "named_struct('x', CAST(NULL AS INT)) AS a",
        "named_struct('y', CAST(NULL AS INT)) AS a",
        _refused("INTERSECT", "first", "STRUCT<y: INT>", "STRUCT<x: INT>"),
    ),
    ("union", "array(1) AS a", "array(true) AS a", _refused("UNION", "first", "ARRAY<BOOLEAN>", "ARRAY<INT>")),
    ("union", "array(1) AS a", "array('s') AS a", None),
    ("union", "map(1, 1) AS a", "map(1, true) AS a", _refused("UNION", "first", "MAP<INT, BOOLEAN>", "MAP<INT, INT>")),
    ("union", "map(1, 1) AS a", "map(1L, 1) AS a", None),
    ("union", "X'01' AS a", "1 AS a", _refused("UNION", "first", "INT", "BINARY")),
    # The number of columns is checked before their types.
    ("union", "1 AS a, 2 AS b", "true AS a", _mismatched("UNION")),
    ("intersect", "1 AS a, 2 AS b", "true AS a", _mismatched("INTERSECT")),
    ("subtract", "1 AS a, 2 AS b", "true AS a", _mismatched("EXCEPT")),
    # A map is looked for after both, and in the output: a NULL column of the first input holds
    # the map of the second, under its own name.
    ("intersect", "map(1, 2) AS m, 1 AS x", "map(1, 2) AS m", _mismatched("INTERSECT")),
    ("intersect", "1 AS a", "map(1, 2) AS m", _refused("INTERSECT", "first", "MAP<INT, INT>", "INT")),
    ("intersect", "NULL AS a", "map(1, 2) AS m", _map_refused("a")),
]
_SET_OPERATION_CHECKS_BY_MODE = {
    "true": [
        (
            "union",
            "map(1, 1) AS a",
            "map('s', 1) AS a",
            _refused("UNION", "first", "MAP<STRING, INT>", "MAP<INT, INT>", _ANSI_HINT),
        ),
        ("intersect", "'s' AS a", "true AS a", None),
        ("subtract", "'s' AS a", "X'01' AS a", None),
        # The hint judges the columns after ANSI mode widens them, so a string and a boolean count
        # as accepted, while a column that is refused without ANSI mode too takes the hint away.
        (
            "union",
            "'true' AS a, map('s', 1) AS b",
            "true AS a, map(1, 1) AS b",
            _refused("UNION", "second", "MAP<INT, INT>", "MAP<STRING, INT>", _ANSI_HINT),
        ),
        (
            "union",
            "map('s', 1) AS a, true AS b",
            "map(1, 1) AS a, 1 AS b",
            _refused("UNION", "first", "MAP<INT, INT>", "MAP<STRING, INT>"),
        ),
    ],
    "false": [
        ("union", "map(1, 1) AS a", "map('s', 1) AS a", None),
        ("intersect", "'s' AS a", "true AS a", _refused("INTERSECT", "first", "BOOLEAN", "STRING")),
        ("subtract", "'s' AS a", "X'01' AS a", _refused("EXCEPT", "first", "BINARY", "STRING")),
    ],
}


def _set_operation_check_cases():
    for ansi in ["true", "false"]:
        for op, left, right, expected in _SET_OPERATION_CHECKS + _SET_OPERATION_CHECKS_BY_MODE[ansi]:
            yield pytest.param(ansi, op, left, right, expected, id=f"ansi={ansi}-{op}-{left}-{right}")


@pytest.mark.parametrize(("ansi", "op", "left", "right", "expected"), _set_operation_check_cases())
def test_a_set_operation_checks_that_its_columns_are_compatible(spark, ansi, op, left, right, expected):
    try:
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        result = getattr(spark.sql(f"SELECT {left}"), op)(spark.sql(f"SELECT {right}"))
        result._cached_schema = None  # noqa: SLF001
        if expected is None:
            _ = result.schema
        else:
            with pytest.raises(Exception, match=re.escape(expected)):
                _ = result.schema
    finally:
        spark.conf.unset("spark.sql.ansi.enabled")


_MISSING_GROUP_BY = (
    "[MISSING_GROUP_BY] The query does not include a GROUP BY clause. Add GROUP BY or turn it into "
    "the window functions using OVER clauses."
)

# A projection that holds an aggregate is an aggregation without grouping (`GlobalAggregates`), so
# every column it reads outside an aggregate is refused (`CheckAnalysis`). `withColumn` projects
# every column of its input, and is refused unless the aggregate replaces the only one. A window
# function is not an aggregate. Measured on the Spark JVM; a list is the rows Spark returns.
_AGGREGATES_WITHOUT_GROUPING = {
    "withColumn": (lambda df: df.withColumn("x", expr("sum(a)")), _MISSING_GROUP_BY),
    "withColumn replacing": (lambda df: df.withColumn("a", expr("sum(a)")), _MISSING_GROUP_BY),
    "withColumns": (lambda df: df.withColumns({"x": expr("sum(a)")}), _MISSING_GROUP_BY),
    "withColumn with a column": (lambda df: df.withColumn("x", expr("sum(a) + a")), _MISSING_GROUP_BY),
    "withColumn replacing the only column": (
        lambda df: df.select("a").withColumn("a", expr("sum(a)")),
        [(3,)],
    ),
    "withColumn over a window": (
        lambda df: df.withColumn("x", expr("sum(a) OVER ()")),
        [(1, "x", 3), (2, "y", 3)],
    ),
    "select with a column": (lambda df: df.select("a", expr("sum(a)")), _MISSING_GROUP_BY),
    "select": (lambda df: df.select(expr("sum(a)"), lit(1)), [(3, 1)]),
    "selectExpr with a column": (lambda df: df.selectExpr("a", "sum(a)"), _MISSING_GROUP_BY),
    "sql with a column": (
        lambda df: df.sparkSession.sql("SELECT a, sum(a) FROM VALUES (1, 'x'), (2, 'y') AS t(a, b)"),
        _MISSING_GROUP_BY,
    ),
}


@pytest.mark.parametrize("case", list(_AGGREGATES_WITHOUT_GROUPING))
def test_an_aggregate_without_grouping_reads_no_other_column(spark, case):
    build, expected = _AGGREGATES_WITHOUT_GROUPING[case]
    df = spark.sql("SELECT * FROM VALUES (1, 'x'), (2, 'y') AS t(a, b)")
    if isinstance(expected, list):
        assert sorted(tuple(row) for row in build(df).collect()) == expected
    else:
        # A SQL query is analyzed as soon as it is built.
        with pytest.raises(Exception, match=re.escape(expected)):
            build(df).collect()


@pytest.mark.skipif(pyspark_version() < (4, 0), reason="VARIANT is a Spark 4.0 type")
def test_a_set_operation_names_a_nested_variant_as_spark_does(spark):
    # A variant is stored as a struct, which the type in the message must not show.
    left = spark.sql("SELECT named_struct('v', CAST(NULL AS VARIANT)) AS a")
    right = spark.sql("SELECT named_struct('w', CAST(NULL AS VARIANT)) AS a")
    result = left.intersect(right)
    result._cached_schema = None  # noqa: SLF001
    with pytest.raises(
        Exception, match=re.escape(_refused("INTERSECT", "first", "STRUCT<w: VARIANT>", "STRUCT<v: VARIANT>"))
    ):
        _ = result.schema


@pytest.mark.parametrize("op", ["intersect", "subtract", "union"])
def test_a_set_operation_takes_one_binary_type_however_it_is_stored(spark, tmp_path, op):
    # A binary column read from Parquet is stored as another Arrow type than a binary literal, and
    # both are the one BINARY type to Spark.
    path = str(tmp_path / "binary")
    spark.sql("SELECT X'01' AS a").write.parquet(path)
    left = spark.read.parquet(path).withMetadata("a", {"side": "L"})
    right = spark.sql("SELECT X'01' AS a" if op == "intersect" else "SELECT X'02' AS a")

    result = getattr(left, op)(right)
    result._cached_schema = None  # noqa: SLF001
    assert result.schema["a"].metadata == {"side": "L"}
    if op != "union":
        assert [bytes(row.a) for row in result.collect()] == [b"\x01"]


def test_union_by_name_keeps_each_nested_value_under_its_own_name(spark):
    # Both fields are integers, so a match by position instead of by name swaps the values
    # without any error.
    left = spark.createDataFrame([((1, 2),)], "s struct<a:int,b:int>")
    right = spark.createDataFrame([((20, 10),)], "s struct<b:int,a:int>")

    assert sorted(tuple(row.s) for row in left.unionByName(right).collect()) == [(1, 2), (10, 20)]


def test_union_by_name_matches_the_fields_of_a_nested_struct_by_name(spark):
    # `unionByName` reorders the fields of a nested struct to match the first input.
    left = spark.sql("SELECT named_struct('a', 1, 'b', 'x') AS s")
    right = spark.sql("SELECT named_struct('b', 'y', 'a', 2) AS s")

    assert sorted(tuple(row.s) for row in left.unionByName(right).collect()) == [(1, "x"), (2, "y")]


# The columns of the first input are matched one at a time, and the first that fails decides the
# error; a duplicate is reported in order of the names, and a name is quoted part by part. Measured
# on the Spark JVM.
_UNION_BY_NAME_ERRORS = [
    (
        "SELECT 1 AS a, named_struct('x', 1, 'y', 2) AS s",
        "SELECT named_struct('x', 1) AS s, 2 AS b",
        '[UNRESOLVED_COLUMN_AMONG_FIELD_NAMES] Cannot resolve column name "a" among (s, b).',
    ),
    (
        "SELECT 1 AS b, 2 AS b, 3 AS a, 4 AS a",
        "SELECT 1 AS a, 2 AS b",
        "[COLUMN_ALREADY_EXISTS] The column `a` already exists.",
    ),
    (
        "SELECT named_struct('a.b', 1, 'c', 2) AS s",
        "SELECT named_struct('c', 2) AS s",
        "[FIELD_NOT_FOUND] No such struct field `a`.`b` in `c`.",
    ),
]


@pytest.mark.parametrize(("left", "right", "message"), _UNION_BY_NAME_ERRORS)
def test_union_by_name_reports_the_error_spark_reports_first(spark, left, right, message):
    result = spark.sql(left).unionByName(spark.sql(right))
    result._cached_schema = None  # noqa: SLF001
    with pytest.raises(Exception, match=re.escape(message)):
        _ = result.schema


def _field(name, data_type, nullable):
    return {"metadata": {}, "name": name, "nullable": nullable, "type": data_type}


def _struct(*fields):
    return {"fields": list(fields), "type": "struct"}


# A union can hold a NULL wherever either input can, at every level a column nests, and a field
# read out of a struct that can be NULL can be NULL too. Measured on the Spark JVM.
_UNION_NESTED_NULLABILITY = [
    (
        "SELECT named_struct('a', 1) AS s",
        "SELECT named_struct('b', CAST(NULL AS INT)) AS s",
        False,
        _field("s", _struct(_field("a", "integer", True)), False),
        ["Row(s=Row(a=1))", "Row(s=Row(a=None))"],
    ),
    (
        "SELECT named_struct('a', CAST(NULL AS INT)) AS s",
        "SELECT named_struct('b', 1) AS s",
        False,
        _field("s", _struct(_field("a", "integer", True)), False),
        ["Row(s=Row(a=1))", "Row(s=Row(a=None))"],
    ),
    (
        "SELECT array(named_struct('a', 1)) AS s",
        "SELECT array(named_struct('b', CAST(NULL AS INT))) AS s",
        False,
        _field(
            "s",
            {"containsNull": False, "elementType": _struct(_field("a", "integer", True)), "type": "array"},
            False,
        ),
        ["Row(s=[Row(a=1)])", "Row(s=[Row(a=None)])"],
    ),
    (
        "SELECT named_struct('x', named_struct('a', 1)) AS s",
        "SELECT named_struct('x', named_struct('b', 2)) AS s",
        False,
        _field("s", _struct(_field("x", _struct(_field("a", "integer", False)), False)), False),
        ["Row(s=Row(x=Row(a=1)))", "Row(s=Row(x=Row(a=2)))"],
    ),
    (
        "SELECT map(named_struct('x', 1), 1) AS m",
        "SELECT map(named_struct('x', CAST(NULL AS INT)), 1) AS m",
        False,
        _field(
            "m",
            {
                "keyType": _struct(_field("x", "integer", True)),
                "type": "map",
                "valueContainsNull": False,
                "valueType": "integer",
            },
            False,
        ),
        ["Row(m={Row(x=1): 1})", "Row(m={Row(x=None): 1})"],
    ),
    (
        "SELECT named_struct('a', 1, 'b', 2) AS s",
        "SELECT IF(rand() < 2, named_struct('b', 3, 'a', 4), NULL) AS s",
        True,
        _field("s", _struct(_field("a", "integer", True), _field("b", "integer", True)), True),
        ["Row(s=Row(a=1, b=2))", "Row(s=Row(a=4, b=3))"],
    ),
]


@pytest.mark.parametrize(("left", "right", "by_name", "expected", "rows"), _UNION_NESTED_NULLABILITY)
def test_a_union_can_hold_a_null_wherever_either_input_can(spark, left, right, by_name, expected, rows):
    left, right = spark.sql(left), spark.sql(right)
    result = left.unionByName(right) if by_name else left.union(right)
    result._cached_schema = None  # noqa: SLF001
    assert result.schema.jsonValue()["fields"] == [expected]
    assert sorted(repr(row) for row in result.collect()) == rows


def test_a_cast_renames_the_fields_of_a_nested_struct(spark):
    result = spark.sql("SELECT CAST(named_struct('x', named_struct('a', 1)) AS STRUCT<x: STRUCT<b: INT>>) AS s")
    assert result.schema.jsonValue()["fields"] == [
        _field("s", _struct(_field("x", _struct(_field("b", "integer", True)), True)), False)
    ]
    assert result.collect() == [Row(s=Row(x=Row(b=1)))]


# Once one column of a set operation has to be widened, every column whose type is not the wider
# one is cast, even where only what it can hold NULL in differs, and a cast carries no metadata
# (`WidenSetOperationTypes.widenTypes`). Measured on the Spark JVM.
@pytest.mark.parametrize(
    ("op", "rows"),
    [("union", [([1], 1), ([None], 1)]), ("exceptAll", [([1], 1)]), ("intersectAll", [])],
)
def test_a_set_operation_casts_every_column_once_one_is_widened(spark, op, rows):
    left = spark.sql("SELECT array(1) AS a, 1 AS b").withMetadata("a", {"side": "L"})
    right = spark.sql("SELECT array(CAST(NULL AS INT)) AS a, CAST(1 AS BIGINT) AS b")

    result = getattr(left, op)(right)
    result._cached_schema = None  # noqa: SLF001
    assert result.schema.simpleString() == "struct<a:array<int>,b:bigint>"
    assert result.schema["a"].dataType.containsNull
    assert result.schema["a"].metadata == {}
    assert sorted(((list(row.a), row.b) for row in result.collect()), key=repr) == sorted(rows, key=repr)


def _containing_null(spark, contains_null):
    from pyspark.sql.types import ArrayType

    schema = StructType([StructField("a", ArrayType(IntegerType(), contains_null), False)])
    return spark.createDataFrame([([1],)], schema)


# A set operation reports the metadata of the first input unless it has to cast that input to the
# type both widen to, and it casts a container when it casts something the container holds
# (`WidenSetOperationTypes`). When there is no wider type nothing is cast, and a difference in
# what a container can hold NULL in is not a cast. Measured on the Spark JVM.
_CONTAINER_METADATA = [
    ("union", "array(CAST(1 AS BIGINT))", "array(1)", {"side": "L"}),
    ("intersect", "array(CAST(1 AS BIGINT))", "array(1)", {"side": "L"}),
    ("exceptAll", "array(CAST(1 AS BIGINT))", "array(1)", {"side": "L"}),
    ("union", "array(1)", "array(CAST(1 AS BIGINT))", {}),
    ("union", "named_struct('x', 1)", "named_struct('x', CAST(1 AS BIGINT))", {}),
    ("union", "named_struct('x', 1)", "named_struct('y', 1)", {"side": "L"}),
    ("union", "map(1, CAST(1 AS BIGINT))", "map(1, 1)", {"side": "L"}),
    ("union", "map(1, 1)", "map(1, CAST(1 AS BIGINT))", {}),
    ("union", "array containing no NULL", "array containing NULL", {"side": "L"}),
    ("union", "array containing NULL", "array containing no NULL", {"side": "L"}),
    ("intersect", "array containing no NULL", "array containing NULL", {"side": "L"}),
    # Past the maximum precision the wider decimal keeps the integral digits and cuts the fraction.
    ("union", "CAST(1 AS DECIMAL(38,10))", "CAST(1 AS DECIMAL(38,0))", {}),
    ("union", "CAST(1 AS DECIMAL(38,0))", "CAST(1 AS DECIMAL(38,10))", {"side": "L"}),
]


def _container_frame(spark, value):
    if value.startswith("array containing"):
        return _containing_null(spark, value == "array containing NULL")
    return spark.sql(f"SELECT {value} AS a")


@pytest.mark.parametrize(("op", "left", "right", "expected"), _CONTAINER_METADATA)
def test_a_set_operation_keeps_the_metadata_of_a_container_it_does_not_cast(spark, op, left, right, expected):
    tagged = _container_frame(spark, left).withMetadata("a", {"side": "L"})
    result = getattr(tagged, op)(_container_frame(spark, right))
    result._cached_schema = None  # noqa: SLF001
    assert result.schema["a"].metadata == expected


def test_a_set_operation_keeps_the_metadata_of_a_struct_that_is_not_cast_without_ansi(spark):
    # Without ANSI mode a string field wins over an integer one, so the first input is not cast.
    try:
        spark.conf.set("spark.sql.ansi.enabled", "false")
        left = spark.sql("SELECT named_struct('x', CAST(NULL AS STRING)) AS a").withMetadata("a", {"side": "L"})
        result = left.union(spark.sql("SELECT named_struct('x', 1) AS a"))
        result._cached_schema = None  # noqa: SLF001
        assert result.schema["a"].metadata == {"side": "L"}
    finally:
        spark.conf.unset("spark.sql.ansi.enabled")


@pytest.mark.parametrize("by_name", [False, True])
def test_a_union_keeps_the_metadata_of_a_left_column_that_is_already_the_wider(spark, by_name):
    # Only the right column is cast here, so the left one reaches the output unchanged and keeps
    # its metadata.
    left = spark.sql("SELECT CAST(1 AS BIGINT) AS a").withMetadata("a", {"side": "L"})
    right = spark.sql("SELECT CAST(2 AS INT) AS a").withMetadata("a", {})

    result = left.unionByName(right) if by_name else left.union(right)
    result._cached_schema = None  # noqa: SLF001
    assert result.schema["a"].metadata == {"side": "L"}


def test_an_intersection_and_a_difference_report_the_metadata_of_their_first_input(spark):
    # `Intersect` and `Except` take their output from the left child, and they widen their inputs
    # the way a union does (`WidenSetOperationTypes`). With the same type on both sides nothing is
    # cast, so the metadata of the left side reaches the output unchanged, and Sail agrees because
    # it takes the fields of the left input for these two. The case where the left side is cast is
    # `test_an_intersection_drops_the_metadata_of_a_cast_left_column`.
    left = spark.sql("SELECT 1 AS a").withMetadata("a", {"side": "L"})
    right = spark.sql("SELECT 1 AS a").withMetadata("a", {"side": "R"})

    for result in (left.intersect(right), left.exceptAll(right), left.subtract(right)):
        result._cached_schema = None  # noqa: SLF001
        assert result.schema["a"].metadata == {"side": "L"}


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
