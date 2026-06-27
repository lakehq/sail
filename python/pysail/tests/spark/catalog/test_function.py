"""Tests for Spark Catalog function APIs."""


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
