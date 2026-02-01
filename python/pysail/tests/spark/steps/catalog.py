from __future__ import annotations

from pytest_bdd import given, parsers, then, when


@given(parsers.parse('a temporary view "{view_name}" from query "{sql}"'))
def create_temp_view(view_name, sql, spark):
    """Creates a temporary view from the given SQL query."""
    spark.sql(sql).createOrReplaceTempView(view_name)
    yield
    spark.catalog.dropTempView(view_name)


@when("I call spark.catalog.listTables()", target_fixture="catalog_result")
def list_tables(spark):
    """Calls spark.catalog.listTables() and returns the result."""
    return spark.catalog.listTables()


@when("I call spark.catalog.listDatabases()", target_fixture="catalog_result")
def list_databases(spark):
    """Calls spark.catalog.listDatabases() and returns the result."""
    return spark.catalog.listDatabases()


@then(parsers.parse('the result contains a table with name "{expected}"'))
def check_table_name(catalog_result, expected):
    """Checks that the result contains a table with the expected name."""
    names = [t.name for t in catalog_result]
    assert expected in names, f"Expected name '{expected}' not found in {names}"


@then(parsers.parse('the result contains a table with tableType "{expected}"'))
def check_table_type(catalog_result, expected):
    """Checks that the result contains a table with the expected tableType."""
    table_types = [t.tableType for t in catalog_result]
    assert expected in table_types, f"Expected tableType '{expected}' not found in {table_types}"


@then(parsers.parse("the result contains a table with isTemporary {expected}"))
def check_is_temporary(catalog_result, expected):
    """Checks that the result contains a table with the expected isTemporary value."""
    expected_bool = expected == "True"
    is_temporary_values = [t.isTemporary for t in catalog_result]
    assert expected_bool in is_temporary_values, f"Expected isTemporary {expected_bool} not found in {is_temporary_values}"


@then(parsers.parse('the result contains a database with name "{expected}"'))
def check_database_name(catalog_result, expected):
    """Checks that the result contains a database with the expected name."""
    names = [d.name for d in catalog_result]
    assert expected in names, f"Expected name '{expected}' not found in {names}"
