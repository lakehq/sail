import pytest
from pyspark.sql import functions as F  # noqa: N812
from pytest_bdd import parsers, scenarios, then, when

scenarios("features/with_column_generators.feature")

pytestmark = [
    pytest.mark.parametrize("local_timezone", ["UTC"], indirect=True),
    pytest.mark.usefixtures("local_timezone"),
]


@pytest.fixture(params=["true", "false"], ids=["ansi", "non-ansi"], autouse=True)
def generator_config(spark, request):
    settings = {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.ansi.enabled": request.param,
        "spark.sql.caseSensitive": "false",
    }
    previous = {key: spark.conf.get(key, None) for key in settings}
    try:
        for key, value in settings.items():
            spark.conf.set(key, value)
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)


@when(parsers.parse("{api} adds column x using {expression}"), target_fixture="column_query")
def add_column(spark, api, expression):
    def build():
        source = spark.sql("SELECT array(1, 2) AS a")
        value = F.expr(expression)
        if api == "withColumn":
            return source.withColumn("x", value)
        assert api == "withColumns"
        return source.withColumns({"x": value})

    return build


@then("the column query rejects a nested generator")
def nested_generator_error(column_query):
    with pytest.raises(Exception, match=r"\[UNSUPPORTED_GENERATOR.NESTED_IN_EXPRESSIONS\].*nested in expressions"):
        column_query()._show_string(truncate=False)  # noqa: SLF001


@then("the column query returns its elements")
def generator_result(column_query):
    assert column_query()._show_string(truncate=False) == (  # noqa: SLF001
        "+------+---+\n|a     |x  |\n+------+---+\n|[1, 2]|1  |\n|[1, 2]|2  |\n+------+---+\n"
    )
