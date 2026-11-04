import os

from pytest_bdd import given, scenarios

from pysail.testing.spark.utils.sql import escape_sql_string_literal

scenarios("features")


@given("the configured execution mode", target_fixture="variables")
def configured_execution_mode(variables):
    mode = os.environ.get("SAIL_MODE", "local").replace("-", "_")
    variables["configured_mode"] = escape_sql_string_literal(mode)
    return variables
