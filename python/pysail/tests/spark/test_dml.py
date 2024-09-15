import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

@pytest.fixture(scope="module", autouse=True)
def person_table(spark):
    df = spark.createDataFrame(
        [(100, "Mary", None), (200, "John", 30), (300, "Mike", 80), (400, "Dan", 50)],
        schema="id INT, name STRING, age INT",
    )
    name = "person"
    df.createOrReplaceTempView(name)
    yield
    spark.catalog.dropTempView(name)


def test_insert(spark):
    print(spark.sql("SELECT * FROM person").toPandas())
    spark.sql("INSERT INTO person VALUES (500, 'Shehab', 99)")
    actual = spark.sql("SELECT * FROM person WHERE id = 500").toPandas()
    expected = pd.DataFrame(
        {"id": [500], "name": ["Shehab"], "age": [99]},
    ).astype({"id": "int32", "name": "string", "age": "int32"})
    assert_frame_equal(actual, expected)