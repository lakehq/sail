import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark import Row


@pytest.fixture(scope="module", autouse=True)
def customer(sail):
    sail.createDataFrame(
        [
            Row(id=1, name="Alice", age=34),
            Row(id=2, name="Bob", age=36),
            Row(id=3, name="Charlie", age=30),
        ]
    ).createOrReplaceTempView("customer")
    yield
    sail.catalog.dropTempView("customer")


@pytest.fixture(scope="module", autouse=True)
def order(sail):
    sail.createDataFrame(
        [
            Row(id=1, customer_id=1, amount=100),
            Row(id=2, customer_id=2, amount=200),
            Row(id=3, customer_id=2, amount=300),
        ]
    ).createOrReplaceTempView("order")
    yield
    sail.catalog.dropTempView("order")


def test_join_group_by(sail):
    # This example is inspired by the Ballista documentation:
    #   https://datafusion.apache.org/ballista/contributors-guide/architecture.html
    actual = sail.sql(
        """
        SELECT customer.name AS name, SUM(order.amount) AS total
        FROM customer
        JOIN order ON customer.id = order.customer_id
        GROUP BY customer.name
        ORDER BY total DESC
        """
    ).toPandas()
    expected = pd.DataFrame({"name": ["Bob", "Alice"], "total": [500, 100]})
    assert_frame_equal(actual, expected)
