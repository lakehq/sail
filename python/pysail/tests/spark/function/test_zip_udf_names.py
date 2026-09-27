import pytest

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

# Local Spark teardown removes SPARK_REMOTE before later module fixtures run.
_PREFER_REGISTERED_FUNCTIONS = is_jvm_spark() and pyspark_version() >= (4, 2)


@pytest.fixture(scope="module", autouse=True)
def _prefer_registered_functions(spark):
    # Spark 4.2 defaults to resolving built-ins before temporary functions.
    # These tests exercise user functions registered with built-in names.
    if _PREFER_REGISTERED_FUNCTIONS:
        spark.conf.set("spark.sql.path.enabled", "true")
        spark.sql("SET PATH = system.session, system.builtin, current_schema").collect()


@pytest.mark.parametrize("function", ["zip_with", "map_zip_with"])
def test_zip_function_udf_default_column_names(spark, function):
    # The module-scoped session isolates these registrations from built-in tests.
    spark.udf.register(function, lambda x, y, z: x + y + z, "int")
    # Function names are fixed pytest parameters.
    result = spark.sql(f"SELECT {function}(1, 2, 3)")
    expected_name = f"{function}(1, 2, 3)"
    expected_value = 6
    assert result.columns == [expected_name]
    assert result.first()[0] == expected_value
    assert result.select(expected_name).first()[0] == expected_value

    result = spark.sql(f"SELECT {function}((SELECT 1), 2, 3) AS result")
    assert result.first().result == expected_value
