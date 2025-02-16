import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

# The test cases are adapted from the Spark SQL documentation.
# https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-groupby.html


@pytest.fixture(scope="module", autouse=True)
def dealer_table(sail):
    df = sail.createDataFrame(
        [
            (100, "Fremont", "Honda Civic", 10),
            (100, "Fremont", "Honda Accord", 15),
            (100, "Fremont", "Honda CRV", 7),
            (200, "Dublin", "Honda Civic", 20),
            (200, "Dublin", "Honda Accord", 10),
            (200, "Dublin", "Honda CRV", 3),
            (300, "San Jose", "Honda Civic", 5),
            (300, "San Jose", "Honda Accord", 8),
        ],
        schema="id INT, city STRING, car_model STRING, quantity INT",
    )
    name = "dealer"
    df.createOrReplaceTempView(name)
    yield
    sail.catalog.dropTempView(name)


@pytest.fixture(scope="module", autouse=True)
def person_table(sail):
    df = sail.createDataFrame(
        [(100, "Mary", None), (200, "John", 30), (300, "Mike", 80), (400, "Dan", 50)],
        schema="id INT, name STRING, age INT",
    )
    name = "person"
    df.createOrReplaceTempView(name)
    yield
    sail.catalog.dropTempView(name)


def test_group_by(sail):
    actual = sail.sql("SELECT id, sum(quantity) FROM dealer GROUP BY id ORDER BY id").toPandas()
    expected = pd.DataFrame(
        {"id": [100, 200, 300], "sum(quantity)": [32, 33, 13]},
    ).astype({"id": "int32", "sum(quantity)": "int64"})
    assert_frame_equal(actual, expected)


def test_group_by_column_position(sail):
    actual = sail.sql("SELECT id, sum(quantity) FROM dealer GROUP BY 1 ORDER BY 1").toPandas()
    expected = pd.DataFrame(
        {"id": [100, 200, 300], "sum(quantity)": [32, 33, 13]},
    ).astype({"id": "int32", "sum(quantity)": "int64"})
    assert_frame_equal(actual, expected)


def test_multiple_aggregations(sail):
    actual = sail.sql("""
        SELECT id, sum(quantity) AS sum, max(quantity) AS max
        FROM dealer
        GROUP BY id
        ORDER BY id
    """).toPandas()
    expected = pd.DataFrame({"id": [100, 200, 300], "sum": [32, 33, 13], "max": [15, 20, 8]}).astype(
        {"id": "int32", "sum": "int64", "max": "int32"}
    )
    assert_frame_equal(actual, expected)


def test_count_distinct(sail):
    actual = sail.sql("""
        SELECT car_model, count(DISTINCT city) AS count
        FROM dealer
        GROUP BY car_model
    """).toPandas()
    expected = pd.DataFrame({"car_model": ["Honda Civic", "Honda CRV", "Honda Accord"], "count": [3, 2, 3]})

    def sort(df):
        return df.sort_values("car_model", ignore_index=True)

    assert_frame_equal(sort(actual), sort(expected))


@pytest.mark.skip(reason="not implemented")
def test_aggregation_filter(sail):
    actual = sail.sql("""
        SELECT id, sum(quantity) FILTER (
            WHERE car_model IN ('Honda Civic', 'Honda CRV')
        ) AS `sum(quantity)`
        FROM dealer
        GROUP BY id
        ORDER BY id
    """).toPandas()
    expected = pd.DataFrame({"id": [100, 200, 300], "sum(quantity)": [17, 23, 5]}).astype(
        {"id": "int32", "sum(quantity)": "int64"}
    )
    assert_frame_equal(actual, expected)


def test_grouping_sets(sail):
    actual = sail.sql("""
        SELECT city, car_model, sum(quantity) AS sum
        FROM dealer
        GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ())
        ORDER BY city, car_model
    """).toPandas()
    expected = pd.DataFrame(
        {
            "city": [
                None,
                None,
                None,
                None,
                "Dublin",
                "Dublin",
                "Dublin",
                "Dublin",
                "Fremont",
                "Fremont",
                "Fremont",
                "Fremont",
                "San Jose",
                "San Jose",
                "San Jose",
            ],
            "car_model": [
                None,
                "Honda Accord",
                "Honda CRV",
                "Honda Civic",
                None,
                "Honda Accord",
                "Honda CRV",
                "Honda Civic",
                None,
                "Honda Accord",
                "Honda CRV",
                "Honda Civic",
                None,
                "Honda Accord",
                "Honda Civic",
            ],
            "sum": [78, 33, 10, 35, 33, 10, 3, 20, 32, 15, 7, 10, 13, 8, 5],
        }
    )
    assert_frame_equal(actual, expected)


def test_rollup(sail):
    actual = sail.sql("""
        SELECT city, car_model, sum(quantity) AS sum
        FROM dealer
        GROUP BY city, car_model WITH ROLLUP
        ORDER BY city, car_model
    """).toPandas()
    expected = pd.DataFrame(
        {
            "city": [
                None,
                "Dublin",
                "Dublin",
                "Dublin",
                "Dublin",
                "Fremont",
                "Fremont",
                "Fremont",
                "Fremont",
                "San Jose",
                "San Jose",
                "San Jose",
            ],
            "car_model": [
                None,
                None,
                "Honda Accord",
                "Honda CRV",
                "Honda Civic",
                None,
                "Honda Accord",
                "Honda CRV",
                "Honda Civic",
                None,
                "Honda Accord",
                "Honda Civic",
            ],
            "sum": [78, 33, 10, 3, 20, 32, 15, 7, 10, 13, 8, 5],
        }
    )
    assert_frame_equal(actual, expected)


def test_cube(sail):
    actual = sail.sql("""
        SELECT city, car_model, sum(quantity) AS sum
        FROM dealer
        GROUP BY city, car_model WITH CUBE
        ORDER BY city, car_model
    """).toPandas()
    expected = pd.DataFrame(
        {
            "city": [
                None,
                None,
                None,
                None,
                "Dublin",
                "Dublin",
                "Dublin",
                "Dublin",
                "Fremont",
                "Fremont",
                "Fremont",
                "Fremont",
                "San Jose",
                "San Jose",
                "San Jose",
            ],
            "car_model": [
                None,
                "Honda Accord",
                "Honda CRV",
                "Honda Civic",
                None,
                "Honda Accord",
                "Honda CRV",
                "Honda Civic",
                None,
                "Honda Accord",
                "Honda CRV",
                "Honda Civic",
                None,
                "Honda Accord",
                "Honda Civic",
            ],
            "sum": [78, 33, 10, 35, 33, 10, 3, 20, 32, 15, 7, 10, 13, 8, 5],
        }
    )
    assert_frame_equal(actual, expected)


def test_aggregation_with_nulls(sail):
    actual = sail.sql("SELECT FIRST(age) FROM person").toPandas()
    expected = pd.DataFrame({"first(age)": [None]}).astype({"first(age)": "float64"})
    assert_frame_equal(actual, expected)


@pytest.mark.skip(reason="not implemented")
def test_aggregation_ignore_nulls(sail):
    actual = sail.sql("SELECT FIRST(age IGNORE NULLS), LAST(id), SUM(id) FROM person").toPandas()
    expected = pd.DataFrame({"first(age)": [30], "last(id)": [400], "sum(id)": [1000]})
    assert_frame_equal(actual, expected)
