import math

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


@pytest.fixture
def scanned_rows(spark, tmp_path):
    rows = [Row(id=i, name=f"name-{i}") for i in range(1, 25)]
    path = str(tmp_path / "rows")
    spark.createDataFrame(rows).repartition(3).write.parquet(path)
    return spark.read.parquet(path), rows


# Ported from Spark 3 (3.5.9): pyspark/sql/tests/connect/test_connect_basic.py,
# SparkConnectBasicTests.test_tail; explicitly order the scan before taking its tail.
@pytest.mark.parametrize("size", [0, 10, 30])
def test_tail_of_ordered_scan(scanned_rows, size):
    df, rows = scanned_rows
    assert df.orderBy("id").tail(size) == (rows[-size:] if size else [])


# Ported from Spark 3 (3.5.9): pyspark/sql/tests/connect/test_connect_basic.py,
# SparkConnectBasicTests.test_collect; order rows before limiting the result.
def test_collect_limit_preserves_duplicate_and_struct_columns(scanned_rows):
    df, rows = scanned_rows
    selected = df.orderBy("id").limit(10)
    assert selected.collect() == rows[:10]
    result = selected.select(
        F.log("id").alias("log"),
        F.log("id").alias("log"),
        F.struct("id", "name").alias("nested"),
        F.struct("id", "name").alias("nested"),
    )
    nested = StructType([StructField("id", LongType()), StructField("name", StringType())])
    assert result.schema == StructType(
        [
            StructField("log", DoubleType()),
            StructField("log", DoubleType()),
            StructField("nested", nested, False),
            StructField("nested", nested, False),
        ]
    )
    actual = result.collect()
    assert len(actual) == len(rows[:10])
    for row, expected in zip(actual, rows[:10], strict=True):
        assert row.__fields__ == ["log", "log", "nested", "nested"]
        assert row[0] == pytest.approx(math.log(expected.id), rel=1e-14, abs=1e-15)
        assert row[1] == row[0]
        assert row[2] == row[3] == expected
