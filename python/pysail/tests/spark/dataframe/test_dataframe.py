import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql import Row
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window

from pysail.testing.spark.utils.common import is_jvm_spark


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

    with pytest.raises(Exception, match="UNRESOLVED_COLUMN"):
        df.replace("x", "y", subset=["nope"]).collect()


def test_column_resolution_is_case_sensitive_when_configured(spark):
    spark.conf.set("spark.sql.caseSensitive", "true")
    try:
        df = spark.createDataFrame([(1, 10)], ["a", "b"])

        assert df.select(col("a")).columns == ["a"]
        # The name no longer matches, so the column cannot be resolved at all.
        with pytest.raises(Exception, match=r"[\"`]A[\"`]"):
            df.select(col("A")).collect()
        with pytest.raises(Exception, match=r"[\"`]A[\"`]"):
            df.filter(col("A") > 0).collect()
        with pytest.raises(Exception, match=r"[\"`]A[\"`]"):
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
    for reference in (lambda: df.select("i"), lambda: df.filter(col("i") > 0), lambda: df.groupBy("i").count()):
        with pytest.raises(Exception, match=r"[\"`]i[\"`]"):
            reference().collect()

    dotless = spark.sql("SELECT 1 AS `\u0131d`")
    with pytest.raises(Exception, match=r"[\"`]Id[\"`]"):
        dotless.select("Id").collect()

    # The operations that select the output columns by name use the resolver alone, so the very
    # same names do match there.
    assert df.drop("i").columns == []
    assert df.withColumn("i", lit(2)).columns == ["i"]
    assert dotless.withColumnsRenamed({"Id": "z"}).columns == ["z"]

    # The lowercase forms agree for ASCII, so both rules accept it.
    assert spark.sql("SELECT 1 AS a").select("A").collect() == [Row(A=1)]


def test_to_schema_matches_name_like_the_analyzer(spark):
    src = spark.sql("SELECT 1 AS `Ä`, 2 AS b")
    target = StructType([StructField("b", IntegerType()), StructField("ä", IntegerType())])
    # `columns` is derived by the client from the schema that it passed, and `Row` compares by
    # position rather than by name, so the schema is what tells the two spellings apart.
    assert src.to(target).schema.names == ["b", "ä"]
    assert [r.asDict() for r in src.to(target).collect()] == [{"b": 2, "ä": 1}]


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


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_to_schema_reorders_nested_struct_fields(spark):
    # The reconciliation recurses into structs, so the nested fields are matched by name, not by
    # position, and they take the name of the target field.
    src = spark.sql("SELECT named_struct('x', 1, 'y', 'a') AS s")
    nested = StructType([StructField("Y", StringType()), StructField("X", IntegerType())])
    target = StructType([StructField("s", nested)])

    assert src.to(target).schema.simpleString() == "struct<s:struct<Y:string,X:int>>"
    assert src.to(target).collect() == [Row(s=Row(Y="a", X=1))]


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_to_schema_rejects_nullable_column_for_non_nullable_field(spark):
    # A nullable input column cannot be narrowed to a non-nullable target field.
    src = spark.sql("SELECT CAST(NULL AS INT) AS a")
    target = StructType([StructField("a", IntegerType(), False)])

    with pytest.raises(Exception, match="NULLABLE_COLUMN_OR_FIELD"):
        src.to(target).collect()


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
    # which folds `ı` to `I` even though the lowercase forms differ.
    assert spark.sql("SELECT 1 AS `ıd`").dropDuplicates(["Id"]).columns == ["ıd"]
    assert spark.sql("SELECT 1 AS `ς`").dropDuplicates(["Σ"]).columns == ["ς"]


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_describe_names_the_column_as_written(spark):
    # Resolving an attribute renames it to the spelling that was asked for.
    assert spark.sql("SELECT 1 AS id").describe("ID").columns == ["summary", "ID"]
    assert spark.sql("SELECT 1 AS `Ä`").describe("ä").columns == ["summary", "ä"]


def test_fillna_rejects_a_subset_name_that_matches_nothing(spark):
    # A subset name that resolves to no column is an error rather than being ignored.
    df = spark.sql("SELECT CAST(NULL AS INT) AS a")
    with pytest.raises(Exception, match="UNRESOLVED_COLUMN"):
        df.fillna(0, subset=["nope"]).collect()
    with pytest.raises(Exception, match="UNRESOLVED_COLUMN"):
        df.dropna(subset=["nope"]).collect()


def test_names_are_folded_with_the_case_mappings_of_the_jvm(spark):
    # Vithkuqi was assigned in Unicode 14, which OpenJDK 17 does not know, so the two names do not
    # fold into each other and both columns are added.
    assert spark.range(1).withColumns({"𐕰": lit(1), "𐖗": lit(2)}).columns == ["id", "𐕰", "𐖗"]


def test_drop_duplicates_keeps_every_matching_column(spark):
    # The name selects output columns, so every column that matches becomes a key rather than
    # the name being rejected as ambiguous.
    assert spark.sql("SELECT 1 AS a, 1 AS a").dropDuplicates(["a"]).columns == ["a", "a"]
    assert spark.sql("SELECT 1 AS a").dropDuplicates().columns == ["a"]

    with pytest.raises(Exception, match="Cannot resolve column name"):
        spark.sql("SELECT 1 AS a").dropDuplicates(["nope"]).columns


def test_fillna_and_dropna_without_subset_are_unaffected(spark):
    df = spark.sql("SELECT CAST(NULL AS INT) AS a")
    assert df.fillna(0).collect() == [Row(a=0)]
    assert df.dropna().collect() == []


def test_fillna_rejects_an_ambiguous_subset_name(spark):
    df = spark.sql("SELECT CAST(NULL AS INT) AS a, CAST(NULL AS INT) AS a")
    with pytest.raises(Exception, match="AMBIGUOUS_REFERENCE"):
        df.fillna(0, subset=["a"]).collect()


def test_fillna_rejects_a_map_key_that_matches_nothing(spark):
    with pytest.raises(Exception, match="UNRESOLVED_COLUMN"):
        spark.sql("SELECT CAST(NULL AS INT) AS a").fillna({"nope": 0}).collect()


def test_fillna_accepts_a_nested_subset_name(spark):
    # A subset name that resolves to a nested field is not a column, so it is discarded and the
    # frame is left untouched rather than the name being rejected.
    df = spark.sql("SELECT named_struct('x', CAST(NULL AS INT)) AS s, 1 AS a")
    assert df.fillna(0, subset=["s.x"]).collect() == [Row(s=Row(x=None), a=1)]


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_dropna_filters_on_a_nested_subset_name(spark):
    # Unlike `fillna`, `dropna` keeps the resolved nested field and filters on it.
    df = spark.sql("SELECT named_struct('x', CAST(NULL AS INT)) AS s, 1 AS a")
    assert df.dropna(subset=["s.x"]).collect() == []


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_union_by_name_rejects_an_extra_column_on_the_right(spark):
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
    assert (
        left.unionByName(right, allowMissingColumns=True).schema.simpleString()
        == "struct<a:int,b:string,c:int>"
    )




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
    left = spark.sql("SELECT named_struct('x', 1, 'y', 2) AS s")
    right = spark.sql("SELECT named_struct('x', 3) AS s")
    assert [r.s.asDict() for r in left.unionByName(right, allowMissingColumns=True).collect()] == [
        {"x": 1, "y": 2},
        {"x": 3, "y": None},
    ]


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_union_by_name_matches_nested_struct_fields_case_insensitively(spark):
    left = spark.sql("SELECT named_struct('x', 1) AS s")
    right = spark.sql("SELECT named_struct('X', 3) AS s")
    assert [r.s.asDict() for r in left.unionByName(right).collect()] == [{"x": 1}, {"x": 3}]


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_drop_duplicates_rejects_an_empty_subset(spark):
    with pytest.raises(Exception, match="DEDUPLICATE_REQUIRES"):
        spark.sql("SELECT 1 AS a").dropDuplicates([]).collect()
