import pytest
from pyspark.sql.types import Row


def test_get_item_ignore_case(spark):
    df = spark.sql("SELECT struct(1 AS b) AS a")
    assert df.select(df.a.getItem("b")).collect() == [Row(**{"a.b": 1})]
    assert df.select(df.a.getItem("B")).collect() == [Row(**{"a.B": 1})]


@pytest.mark.skip(reason="not working")
def test_get_item_nested_map(spark):
    df = spark.sql("SELECT struct(map(1, 2) AS b) AS a")
    assert df.select(df.a.getItem("b").getItem(1)).collect() == [Row(**{"a.b[1]": 2})]
    df = spark.sql("SELECT map('b', map(1, 2)) AS a")
    assert df.select(df.a.getItem("b").getItem(1)).collect() == [Row(**{"a[b][1]": 2})]
