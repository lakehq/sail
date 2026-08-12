import datetime

import pytest
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.utils.common import pyspark_version

pytestmark = pytest.mark.skipif(pyspark_version() < (4, 1), reason="current_time requires Spark 4.1+")


@pytest.fixture(autouse=True)
def enable_time_type(spark):
    key = "spark.sql.timeType.enabled"
    original = spark.conf.get(key)
    spark.conf.set(key, True)
    yield
    spark.conf.set(key, original)


@pytest.mark.parametrize(
    ("precision", "expected_name"),
    [
        (None, "current_time(6)"),
        *[(precision, f"current_time({precision})") for precision in range(7)],
    ],
)
def test_current_time_collects_as_python_time(spark, precision, expected_name):
    expression = F.current_time() if precision is None else F.current_time(precision)
    frame = spark.range(1).select(expression)

    value = frame.first()[0]
    assert isinstance(value, datetime.time)
    assert frame.schema.names == [expected_name]

    effective_precision = 6 if precision is None else precision
    assert frame.schema.fields[0].dataType.precision == effective_precision
    assert value.microsecond % (10 ** (6 - effective_precision)) == 0


def test_nested_current_time_values_collect(spark):
    row = spark.sql(
        """
        SELECT
          array(current_time(3)) AS array_value,
          named_struct('value', current_time(0)) AS struct_value,
          map('value', current_time(6)) AS map_value
        """
    ).first()

    assert isinstance(row.array_value[0], datetime.time)
    assert row.array_value[0].microsecond % 1000 == 0
    assert isinstance(row.struct_value.value, datetime.time)
    assert row.struct_value.value.microsecond == 0
    assert isinstance(row.map_value["value"], datetime.time)


@pytest.mark.parametrize("precision", [1, 2, 4, 5])
def test_nested_current_time_schema_preserves_precision(spark, precision):
    frame = spark.sql(
        f"""
        SELECT
          array(current_time({precision})) AS array_value,
          named_struct('value', current_time({precision})) AS named_struct_value,
          struct(current_time({precision})) AS struct_value,
          map('value', current_time({precision})) AS map_value,
          typeof(array(current_time({precision}))) AS array_type,
          typeof(named_struct('value', current_time({precision}))) AS named_struct_type,
          typeof(struct(current_time({precision}))) AS struct_type,
          typeof(map('value', current_time({precision}))) AS map_type
        """
    )

    assert frame.schema["array_value"].dataType.elementType.precision == precision
    assert frame.schema["named_struct_value"].dataType.fields[0].dataType.precision == precision
    assert frame.schema["struct_value"].dataType.fields[0].dataType.precision == precision
    assert frame.schema["map_value"].dataType.valueType.precision == precision
    row = frame.first()
    assert row.array_type == f"array<time({precision})>"
    assert row.named_struct_type == f"struct<value:time({precision})>"
    assert row.struct_type == f"struct<col1:time({precision})>"
    assert row.map_type == f"map<string,time({precision})>"


def test_nested_current_time_allows_non_time_common_type_widening(spark):
    row = spark.sql(
        """
        SELECT
          typeof(array(
            named_struct('t', current_time(1), 'n', 1),
            named_struct('t', current_time(1), 'n', CAST(1 AS BIGINT)))) AS array_type,
          typeof(map(
            1, named_struct('t', current_time(1), 'n', 1),
            2, named_struct('t', current_time(1), 'n', CAST(1 AS BIGINT)))) AS map_type
        """
    ).first()

    assert row.array_type == "array<struct<t:time(1),n:bigint>>"
    assert row.map_type == "map<int,struct<t:time(1),n:bigint>>"


def test_nested_time_precision_is_ignored_when_legacy_common_type_is_string(spark):
    key = "spark.sql.ansi.enabled"
    original = spark.conf.get(key)
    spark.conf.set(key, False)
    try:
        row = spark.sql(
            """
            SELECT typeof(array(
              named_struct('t', current_time(1)),
              named_struct('t', 'value'),
              named_struct('t', current_time(2)))) AS array_type
            """
        ).first()
        assert row.array_type == "array<struct<t:string>>"
    finally:
        spark.conf.set(key, original)


@pytest.mark.parametrize(
    "expression",
    [
        "array(current_time(1), current_time(2))",
        "array(named_struct('t', current_time(1)), named_struct('t', current_time(5)))",
        "map(current_time(1), 1, current_time(4), 2)",
        "map(1, named_struct('t', current_time(1)), 2, named_struct('t', current_time(5)))",
    ],
)
def test_collection_constructors_reject_different_time_precisions(spark, expression):
    with pytest.raises(Exception, match="Spark TIME precisions must match"):
        spark.sql(f"SELECT {expression}").collect()


def test_current_time_alias_preserves_precision_and_user_metadata(spark):
    precision = 2
    frame = spark.range(1).select(F.current_time(precision).alias("value", metadata={"x": "y"}))

    field = frame.schema["value"]
    assert field.dataType.precision == precision
    assert field.metadata == {"x": "y"}
    assert isinstance(frame.first().value, datetime.time)


def test_empty_current_time_result_collects(spark):
    assert spark.sql("SELECT current_time(3) AS value WHERE false").collect() == []
