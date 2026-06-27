"""Tests for Spark Catalog function APIs."""

import pytest


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


def test_show_functions_respects_scope(spark):
    assert _show_function_names(spark, "SHOW SYSTEM FUNCTIONS LIKE 'to_date'") == {"to_date"}
    assert _show_function_names(spark, "SHOW USER FUNCTIONS LIKE 'to_date'") == set()
    assert _show_function_names(spark, "SHOW ALL FUNCTIONS LIKE 'to_date'") == {"to_date"}


def test_show_functions_respects_namespace_and_pattern(spark):
    assert _show_function_names(spark, "SHOW FUNCTIONS IN default LIKE 'to_date'") == {
        "to_date"
    }
    assert _show_function_names(spark, "SHOW FUNCTIONS FROM default LIKE 'to_date'") == {
        "to_date"
    }


def test_show_functions_supports_legacy_identifier_pattern(spark):
    assert _show_function_names(spark, "SHOW FUNCTIONS to_date") == {"to_date"}
    assert _show_function_names(spark, "SHOW SYSTEM FUNCTIONS to_date") == {"to_date"}
    assert _show_function_names(spark, "SHOW FUNCTIONS LIKE to_date") == {"to_date"}
    assert _show_function_names(spark, "SHOW FUNCTIONS default.to_date") == {"to_date"}


def test_show_functions_requires_like_after_namespace(spark):
    with pytest.raises(Exception, match="(?i)(expected|parse|syntax|extra input)"):
        spark.sql("SHOW FUNCTIONS IN default to_date").collect()
