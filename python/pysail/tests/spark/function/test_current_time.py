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


def test_current_time_alias_preserves_precision_and_user_metadata(spark):
    precision = 2
    frame = spark.range(1).select(F.current_time(precision).alias("value", metadata={"x": "y"}))

    field = frame.schema["value"]
    assert field.dataType.precision == precision
    assert field.metadata == {"x": "y"}
    assert isinstance(frame.first().value, datetime.time)


def test_empty_current_time_result_collects(spark):
    assert spark.sql("SELECT current_time(3) AS value WHERE false").collect() == []
