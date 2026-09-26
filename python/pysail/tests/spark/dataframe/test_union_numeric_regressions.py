from decimal import Decimal

import pytest
from pyspark.sql import functions as F  # noqa: N812


@pytest.mark.parametrize("ansi", ["true", "false"])
@pytest.mark.parametrize("allow_missing", [False, True])
def test_union_by_name_preserves_numeric_conditional_precision(spark, ansi, allow_missing):
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    try:
        left = spark.sql("SELECT 0 AS id, CAST(1.25 AS FLOAT) AS v, 'left' AS label")
        right = spark.sql("SELECT 'right' AS label, CAST(16777217.25 AS DECIMAL(10,2)) AS v, 1 AS id")
        if allow_missing:
            right = right.selectExpr("*", "'extra' AS extra")
        union = left.unionByName(right, allowMissingColumns=allow_missing)
        result = union.selectExpr(
            "id",
            "NVL2(NULLIF(id, 1), CAST(2 AS FLOAT), v) AS value",
            "label",
        ).orderBy("id")
        assert result.dtypes == [("id", "int"), ("value", "double"), ("label", "string")]
        assert result.schema["value"].nullable is False
        assert result.collect() == [(0, 2.0, "left"), (1, 16777217.25, "right")]
        if allow_missing:
            assert union.orderBy("id").select("extra").collect() == [(None,), ("extra",)]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)


@pytest.mark.parametrize("ansi", ["true", "false"])
@pytest.mark.parametrize("case_sensitive", ["true", "false"])
@pytest.mark.parametrize("allow_missing", [False, True])
@pytest.mark.parametrize(("container", "field"), [("{}", "s"), ("array({})", "s[0]")])
@pytest.mark.parametrize(
    ("right_a_type", "right_a_value", "expected_a_type"),
    [("FLOAT", "2.5", "float"), ("DECIMAL(10,2)", "16777217.25", "double")],
)
def test_union_by_name_promotes_matching_nested_numeric_fields(
    spark, ansi, case_sensitive, allow_missing, container, field, right_a_type, right_a_value, expected_a_type
):
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    original_case_sensitive = spark.conf.get("spark.sql.caseSensitive")
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    spark.conf.set("spark.sql.caseSensitive", case_sensitive)
    try:
        left_struct = "named_struct('a', CAST(1.25 AS FLOAT), 'b', 9007199254740993L)"
        right_struct = (
            f"named_struct('b', CAST(9007199254740993 AS DECIMAL(20,0)), 'a', CAST({right_a_value} AS {right_a_type}))"
        )
        left = spark.sql(f"SELECT 0 AS id, {container.format(left_struct)} AS s")
        right = spark.sql(f"SELECT 1 AS id, {container.format(right_struct)} AS s")
        result = (
            left.unionByName(right, allowMissingColumns=allow_missing)
            .selectExpr("id", f"{field}.a AS a", f"{field}.b AS b")
            .orderBy("id")
        )
        assert result.dtypes == [("id", "int"), ("a", expected_a_type), ("b", "decimal(20,0)")]
        assert result.collect() == [
            (0, 1.25, Decimal(9007199254740993)),
            (1, float(right_a_value), Decimal(9007199254740993)),
        ]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)
        spark.conf.set("spark.sql.caseSensitive", original_case_sensitive)


@pytest.mark.parametrize("ansi", ["true", "false"])
def test_union_by_name_preserves_nullable_reordered_struct(spark, ansi):
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    try:
        left = spark.sql("SELECT 0 AS id, CAST(NULL AS STRUCT<a:FLOAT,b:BIGINT>) AS s")
        right = spark.sql(
            "SELECT 1 AS id, named_struct('b', CAST(9007199254740993 AS DECIMAL(20,0)), 'a', CAST(2.5 AS FLOAT)) AS s"
        )
        result = left.unionByName(right).selectExpr("id", "s.a", "s.b").orderBy("id")
        assert result.dtypes == [("id", "int"), ("a", "float"), ("b", "decimal(20,0)")]
        assert result.schema["a"].nullable is True
        assert result.schema["b"].nullable is True
        assert result.collect() == [(0, None, None), (1, 2.5, Decimal(9007199254740993))]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)


@pytest.mark.parametrize(
    ("first_name", "second_name", "case_equivalent"),
    [
        ("a", "A", True),
        ("é", "É", True),
        ("\u0131", "İ", True),
        ("ß", "ẞ", True),
        ("ᾀ", "ᾈ", True),
        ("𐐀", "𐐨", True),
        ("ﬅ", "ﬆ", False),
        ("\U00010570", "\U00010597", False),
    ],
)
@pytest.mark.parametrize("case_sensitive", ["false", "true"])
def test_union_by_name_numeric_matching_uses_case_resolution(
    spark, case_sensitive, first_name, second_name, case_equivalent
):
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    original_case_sensitive = spark.conf.get("spark.sql.caseSensitive")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    spark.conf.set("spark.sql.caseSensitive", case_sensitive)
    try:
        left = spark.sql(
            f"SELECT 0 AS id, named_struct('{first_name}', CAST(1.25 AS FLOAT), '{second_name}', 16777217L) AS s"
        )
        right = spark.sql(
            f"SELECT 1 AS id, named_struct('{second_name}', 16777219L, '{first_name}', CAST(2.5 AS FLOAT)) AS s"
        )
        result = left.unionByName(right).where("id = 0")
        if case_sensitive == "false" and case_equivalent:
            expected_type = f"struct<{first_name}:double,{second_name}:double>"
        else:
            expected_type = f"struct<{first_name}:float,{second_name}:bigint>"
        assert result.selectExpr("typeof(s) AS result_type").collect() == [(expected_type,)]
        assert tuple(result.collect()[0]["s"]) == (1.25, 16777217)
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)
        spark.conf.set("spark.sql.caseSensitive", original_case_sensitive)


@pytest.mark.parametrize("string_first", [False, True])
@pytest.mark.parametrize("allow_missing", [False, True])
@pytest.mark.parametrize(("container", "field"), [("{}", "s"), ("array({})", "s[0]")])
def test_union_by_name_coerces_matching_string_numeric_fields(spark, string_first, allow_missing, container, field):
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        numeric = "named_struct('a', CAST(1.25 AS FLOAT), 'b', 9007199254740993L)"
        strings = "named_struct('b', '9007199254740993', 'a', '16777217.25')"
        left = spark.sql(f"SELECT 0 AS id, {container.format(numeric)} AS s")
        right = spark.sql(f"SELECT 1 AS id, {container.format(strings)} AS s")
        if string_first:
            left, right = right, left
        result = (
            left.unionByName(right, allowMissingColumns=allow_missing)
            .selectExpr("id", f"{field}.a AS a", f"{field}.b AS b")
            .orderBy("id")
        )
        assert result.dtypes == [("id", "int"), ("a", "double"), ("b", "bigint")]
        assert result.schema["a"].nullable is True
        assert result.schema["b"].nullable is True
        assert result.collect() == [(0, 1.25, 9007199254740993), (1, 16777217.25, 9007199254740993)]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)


def test_union_by_name_retains_positional_string_numeric_case_collision(spark):
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    original_case_sensitive = spark.conf.get("spark.sql.caseSensitive")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    spark.conf.set("spark.sql.caseSensitive", "false")
    try:
        left = spark.sql("SELECT 0 AS id, named_struct('a', CAST(1.25 AS FLOAT), 'A', '2') AS s")
        right = spark.sql("SELECT 1 AS id, named_struct('A', CAST(2.5 AS FLOAT), 'a', '3') AS s")
        result = left.unionByName(right).where("id = 0")
        assert result.selectExpr("typeof(s)").collect() == [("struct<a:float,A:string>",)]
        assert tuple(result.collect()[0]["s"]) == (1.25, "2")
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)
        spark.conf.set("spark.sql.caseSensitive", original_case_sensitive)


def test_union_conditional_preserves_metadata_and_unselected_cast_laziness(spark):
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        left = spark.sql("SELECT 0 AS id, 'bad' AS v")
        right = spark.sql("SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v")
        left = left.select("id", F.col("v").alias("v", metadata={"note": "kept"}))
        right = right.select("id", F.col("v").alias("v", metadata={"note": "kept"}))
        result = (
            left.union(right)
            .select("id", F.expr("CAST(IF(id = 0, 0, v) AS DOUBLE)").alias("value", metadata={"result": "kept"}))
            .orderBy("id")
        )
        assert result.schema["value"].metadata == {"result": "kept"}
        assert result.collect() == [(0, 0.0), (1, 2.5)]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)


def test_union_conditional_preserves_repeated_constant_producers(spark):
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        result = spark.sql("SELECT 0 AS id, '1.5' AS v").union(
            spark.sql("SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v")
        )
        result = result.selectExpr("id", "CAST(v AS DOUBLE) AS v")
        for _ in range(12):
            result = result.selectExpr("id", "v + v AS v")
        result = result.selectExpr("id", "CAST(IF(id = 0, 0, v) AS DOUBLE) AS value").orderBy("id")
        assert result.dtypes == [("id", "int"), ("value", "double")]
        assert result.collect() == [(0, 0.0), (1, 10240.0)]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)
