import pytest


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
