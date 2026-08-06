"""Tests for Spark Catalog function APIs."""

import pytest

SPARK_4_2_VERSION_UPDATE_BUILT_INS = {
    "current_path",
    "is_valid_variant",
    "kll_merge_agg_bigint",
    "kll_merge_agg_double",
    "kll_merge_agg_float",
    "measure",
    "time_bucket",
    "time_from_micros",
    "time_from_millis",
    "time_from_seconds",
    "time_to_micros",
    "time_to_millis",
    "time_to_seconds",
    "tuple_difference_double",
    "tuple_difference_integer",
    "tuple_difference_theta_double",
    "tuple_difference_theta_integer",
    "tuple_intersection_agg_double",
    "tuple_intersection_agg_integer",
    "tuple_intersection_double",
    "tuple_intersection_integer",
    "tuple_intersection_theta_double",
    "tuple_intersection_theta_integer",
    "tuple_sketch_agg_double",
    "tuple_sketch_agg_integer",
    "tuple_sketch_estimate_double",
    "tuple_sketch_estimate_integer",
    "tuple_sketch_summary_double",
    "tuple_sketch_summary_integer",
    "tuple_sketch_theta_double",
    "tuple_sketch_theta_integer",
    "tuple_union_agg_double",
    "tuple_union_agg_integer",
    "tuple_union_double",
    "tuple_union_integer",
    "tuple_union_theta_double",
    "tuple_union_theta_integer",
    "vector_avg",
    "vector_cosine_similarity",
    "vector_inner_product",
    "vector_l2_distance",
    "vector_norm",
    "vector_normalize",
    "vector_sum",
}


def _show_function_names(spark, sql):
    return {row.function for row in spark.sql(sql).collect()}


def test_list_functions_returns_spark_function_fields(spark):
    functions = spark.catalog.listFunctions()
    assert functions

    function = next(f for f in functions if f.name == "to_date")
    assert function._fields == (
        "name",
        "catalog",
        "namespace",
        "description",
        "className",
        "isTemporary",
    )
    assert function.description.startswith("\n    to_date(date_str[, fmt]) - ")
    assert "`date_str`" in function.description
    assert function.className == "org.apache.spark.sql.catalyst.expressions.ParseToDate"
    assert function.isTemporary is True


def test_list_functions_includes_built_ins(spark):
    names = {f.name for f in spark.catalog.listFunctions()}
    assert {
        "+",
        "<>",
        "between",
        "current_database",
        "to_date",
        "window",
        "||",
    }.issubset(names)


def test_list_functions_includes_spark_4_2_version_update_built_ins(spark):
    names = {f.name for f in spark.catalog.listFunctions()}
    assert SPARK_4_2_VERSION_UPDATE_BUILT_INS.issubset(names)


def test_list_functions_has_metadata_for_built_ins(spark):
    functions = {f.name: f for f in spark.catalog.listFunctions()}
    expected = {
        "+": "org.apache.spark.sql.catalyst.expressions.Add",
        "concat": "org.apache.spark.sql.catalyst.expressions.Concat",
        "current_path": "org.apache.spark.sql.catalyst.expressions.CurrentPath",
        "time_bucket": "org.apache.spark.sql.catalyst.expressions.TimeBucketExpressionBuilder",
        "to_date": "org.apache.spark.sql.catalyst.expressions.ParseToDate",
        "tuple_difference_double": "org.apache.spark.sql.catalyst.expressions.TupleDifferenceDouble",
        "vector_norm": "org.apache.spark.sql.catalyst.expressions.VectorNorm",
    }
    for name, class_name in expected.items():
        assert functions[name].isTemporary is True
        assert functions[name].className == class_name
        assert functions[name].description


def test_list_functions_includes_user_registered_udfs(spark):
    function_name = "catalog_add_one"
    spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {function_name}")
    try:
        spark.udf.register(
            function_name,
            lambda value: value + 1 if value is not None else None,
            "long",
        )

        functions = {f.name: f for f in spark.catalog.listFunctions()}
        function = functions[function_name]
        assert function.name == function_name
        assert function.isTemporary is True
        assert function_name in {f.name for f in spark.catalog.listFunctions("default", function_name)}
        assert _show_function_names(spark, f"SHOW USER FUNCTIONS LIKE '{function_name}'") == {function_name}
        assert _show_function_names(spark, f"SHOW SYSTEM FUNCTIONS LIKE '{function_name}'") == set()
        assert _show_function_names(spark, f"SHOW ALL FUNCTIONS LIKE '{function_name}'") == {function_name}
        assert spark.sql(f"SELECT {function_name}(1) AS value").collect()[0].value == 2  # noqa PLR2004

        rows = [row.function_desc for row in spark.sql(f"DESCRIBE FUNCTION {function_name}").collect()]
        assert rows[0] == f"Function: {function_name}"
        assert rows[-1] == "Usage: N/A."
    finally:
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {function_name}")


def test_list_functions_respects_database_and_pattern(spark):
    names = {f.name for f in spark.catalog.listFunctions("default", "to*")}
    assert "to_date" in names
    assert "to_timestamp" in names
    assert "+" not in names
    assert "current_database" not in names


def test_show_functions_returns_spark_sql_shape(spark):
    functions = spark.sql("SHOW FUNCTIONS")
    assert functions.columns == ["function"]

    names = {row.function for row in functions.where("function IN ('+', 'to_date')").collect()}
    assert names == {"+", "to_date"}


def test_describe_function_returns_signature_and_description(spark):
    result = spark.sql("DESCRIBE FUNCTION to_date")
    assert result.columns == ["function_desc"]

    rows = [row.function_desc for row in result.collect()]
    expected_usage = (
        "Usage: \n"
        "    to_date(date_str[, fmt]) - Parses the `date_str` expression with the `fmt` expression to\n"
        "      a date. Returns null with invalid input. By default, it follows casting rules to a date if\n"
        "      the `fmt` is omitted.\n"
        "  "
    )
    assert rows == [
        "Function: to_date",
        "Class: org.apache.spark.sql.catalyst.expressions.ParseToDate",
        expected_usage,
    ]


def test_describe_function_extended_adds_extended_usage(spark):
    rows = [row.function_desc for row in spark.sql("DESC FUNCTION EXTENDED to_date").collect()]
    assert rows[-1].startswith("Extended Usage:\n    Arguments:\n")
    assert "* date_str - A string to be parsed to date." in rows[-1]
    assert "\n    Examples:\n" in rows[-1]
    assert "> SELECT to_date('2009-07-30 04:17:52');" in rows[-1]
    assert "    Since: 1.5.0" in rows[-1]


def test_describe_function_supports_string_literal_name(spark):
    rows = [row.function_desc for row in spark.sql("DESC FUNCTION 'concat'").collect()]
    assert rows[0] == "Function: concat"
    assert rows[1] == "Class: org.apache.spark.sql.catalyst.expressions.Concat"
    assert rows[2].startswith("Usage: concat(col1, col2, ..., colN) - ")


def test_describe_function_supports_operator_name(spark):
    rows = [row.function_desc for row in spark.sql("DESCRIBE FUNCTION +").collect()]
    assert rows[0] == "Function: +"
    assert rows[1] == "Class: org.apache.spark.sql.catalyst.expressions.Add"
    assert rows[2].startswith("Usage: expr1 + expr2 - ")


def test_describe_function_reports_unknown_function(spark):
    with pytest.raises(Exception, match=r"(?i)(not found|function)"):
        spark.sql("DESCRIBE FUNCTION no_such_function").collect()
    with pytest.raises(Exception, match=r"(?i)(not found|function)"):
        spark.sql("DESCRIBE FUNCTION default.to_date").collect()


def test_show_functions_respects_scope(spark):
    assert _show_function_names(spark, "SHOW SYSTEM FUNCTIONS LIKE 'to_date'") == {"to_date"}
    assert _show_function_names(spark, "SHOW USER FUNCTIONS LIKE 'to_date'") == set()
    assert _show_function_names(spark, "SHOW ALL FUNCTIONS LIKE 'to_date'") == {"to_date"}


def test_show_functions_respects_namespace_and_pattern(spark):
    assert _show_function_names(spark, "SHOW FUNCTIONS IN default LIKE 'to_date'") == {"to_date"}
    assert _show_function_names(spark, "SHOW FUNCTIONS FROM default LIKE 'to_date'") == {"to_date"}


def test_show_functions_supports_legacy_identifier_pattern(spark):
    assert _show_function_names(spark, "SHOW FUNCTIONS to_date") == {"to_date"}
    assert _show_function_names(spark, "SHOW SYSTEM FUNCTIONS to_date") == {"to_date"}
    assert _show_function_names(spark, "SHOW FUNCTIONS LIKE to_date") == {"to_date"}
    assert _show_function_names(spark, "SHOW FUNCTIONS default.to_date") == {"to_date"}
    assert _show_function_names(spark, "SHOW FUNCTIONS no_such_db.to_date") == {"to_date"}


def test_show_functions_requires_like_after_namespace(spark):
    with pytest.raises(Exception, match=r"(?i)(expected|parse|syntax|extra input)"):
        spark.sql("SHOW FUNCTIONS IN default to_date").collect()
    with pytest.raises(Exception, match=r"(?i)(expected|parse|syntax|extra input)"):
        spark.sql("SHOW FUNCTIONS IN default LIKE to_date").collect()
