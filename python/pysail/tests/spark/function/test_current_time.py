import datetime as dt

import pytest
from pyspark.sql import types

pytestmark = pytest.mark.skipif(not hasattr(types, "TimeType"), reason="TimeType requires Spark 4.1+")


def test_current_time_precision_schema_and_collect(spark):
    df = spark.sql(
        """
        SELECT
          current_time(1) AS t1,
          current_time(2) AS t2,
          current_time(4) AS t4,
          current_time(5) AS t5
        """
    )

    assert [field.dataType for field in df.schema.fields] == [
        types.TimeType(1),
        types.TimeType(2),
        types.TimeType(4),
        types.TimeType(5),
    ]

    row = df.collect()[0]
    assert all(isinstance(value, dt.time) for value in row)
    assert row.t1.microsecond % 100_000 == 0
    assert row.t2.microsecond % 10_000 == 0
    assert row.t4.microsecond % 100 == 0
    assert row.t5.microsecond % 10 == 0
