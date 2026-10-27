"""Exercise repeat on columns so constant folding cannot hide missing UDFs."""

import pytest
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.utils.common import is_jvm_spark


def test_repeat_columns(spark):
    data = [
        (0, "ab", 3, "ababab"),
        (1, "like", 2, "likelike"),
        (2, "", 4, ""),
        (3, "abc", 0, ""),
        (4, "abc", -2, ""),
        (5, None, 2, None),
        (6, "abc", None, None),
    ]
    df = spark.createDataFrame([(i, s, n) for i, s, n, _ in data], "id INT, s STRING, n INT").repartition(2)
    actual = df.selectExpr("id", "repeat(s, n) AS result").orderBy("id").collect()
    assert [row.result for row in actual] == [expected for _, _, _, expected in data]


def test_repeat_function_api(spark):
    df = spark.range(4, numPartitions=2)
    actual = df.select("id", F.repeat(F.col("id").cast("string"), 2).alias("result")).orderBy("id").collect()
    assert [row.result for row in actual] == ["00", "11", "22", "33"]


def test_space_column(spark):
    actual = (
        spark.range(-1, 4, numPartitions=2).selectExpr("id", "space(CAST(id AS INT)) AS result").orderBy("id").collect()
    )
    assert [row.result for row in actual] == ["", "", " ", "  ", "   "]


@pytest.mark.skipif(is_jvm_spark(), reason="Sail system tables only")
def test_repeat_in_system_table_filter(spark):
    # System table filters execute on the driver, whose task context has no
    # default DataFusion UDF registry when the cluster plan is decoded.
    rows = spark.sql("SELECT key FROM system.session.options WHERE repeat(key, 2) = 'modemode'").collect()
    assert [row.key for row in rows] == ["mode"]
