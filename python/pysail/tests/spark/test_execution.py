import os

import pandas as pd
import pyspark.sql.functions as F  # noqa: N812
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql.types import Row
from pyspark.sql.window import Window

from pysail.spark import SparkConnectServer
from pysail.tests.spark.utils import is_jvm_spark


@pytest.fixture(scope="session")
def remote():
    """Override the global remote fixture to use local-cluster mode for this module."""
    original_mode = os.environ.get("SAIL_MODE")
    os.environ["SAIL_MODE"] = "local-cluster"

    try:
        server = SparkConnectServer("127.0.0.1", 0)
        server.start(background=True)
        address = server.listening_address
        _, port = address
        yield f"sc://localhost:{port}"
        server.stop()
    finally:
        # Restore original environment
        if original_mode is None:
            os.environ.pop("SAIL_MODE", None)
        else:
            os.environ["SAIL_MODE"] = original_mode


@pytest.fixture(scope="module")
def large_dataset(spark):
    """Create a larger dataset to test distributed execution."""
    total = []
    for i in range(1000):
        data = Row(id=i, group=i % 10, value=i * 2, name=f"item_{i}", category=f"cat_{i % 5}")
        total.append(data)

    df = spark.createDataFrame(total)
    df.createOrReplaceTempView("large_dataset")
    yield df
    spark.catalog.dropTempView("large_dataset")


@pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")
class TestLocalClusterExecution:
    def test_basic_query_execution(self, spark):
        """Test basic query execution in local-cluster mode."""
        result = spark.sql("SELECT 1 + 1 AS result").collect()
        assert result[0]["result"] == 2  # noqa: PLR2004

    def test_dataframe_operations(self, spark):
        """Test DataFrame operations in local-cluster mode."""
        df = spark.createDataFrame([Row(a=1, b="hello"), Row(a=2, b="world"), Row(a=3, b="test")])

        result = df.select("a", "b").filter(F.col("a") > 1).orderBy("a").toPandas()
        expected = pd.DataFrame({"a": [2, 3], "b": ["world", "test"]}).astype(
            {"a": "int64"}
        )  # Spark Connect uses int64

        assert_frame_equal(result, expected)

    def test_aggregation_with_groupby(self, large_dataset):
        result = (
            large_dataset.groupBy("group")
            .agg(
                F.count("*").alias("count"),
                F.sum("value").alias("sum_value"),
                F.avg("value").alias("avg_value"),
                F.max("value").alias("max_value"),
                F.min("value").alias("min_value"),
            )
            .orderBy("group")
            .toPandas()
        )

        assert len(result) == 10  # noqa: PLR2004
        assert result["count"].sum() == 1000  # noqa: PLR2004

        group_0 = result[result["group"] == 0].iloc[0]
        assert group_0["count"] == 100  # noqa: PLR2004
        assert group_0["sum_value"] == sum(i * 2 for i in range(0, 1000, 10))

    def test_join_operations(self, spark):
        """Test join operations in local-cluster mode."""
        customers = spark.createDataFrame(
            [
                Row(id=1, name="Alice", city="NYC"),
                Row(id=2, name="Bob", city="LA"),
                Row(id=3, name="Charlie", city="Chicago"),
            ]
        )

        orders = spark.createDataFrame(
            [
                Row(customer_id=1, order_id=101, amount=100.0),
                Row(customer_id=1, order_id=102, amount=150.0),
                Row(customer_id=2, order_id=103, amount=200.0),
                Row(customer_id=3, order_id=104, amount=75.0),
            ]
        )

        result = (
            customers.join(orders, customers.id == orders.customer_id, "inner")
            .select("name", "city", "order_id", "amount")
            .orderBy("order_id")
            .toPandas()
        )

        expected = pd.DataFrame(
            {
                "name": ["Alice", "Alice", "Bob", "Charlie"],
                "city": ["NYC", "NYC", "LA", "Chicago"],
                "order_id": [101, 102, 103, 104],
                "amount": [100.0, 150.0, 200.0, 75.0],
            }
        ).astype({"order_id": "int64"})

        assert_frame_equal(result, expected)

    def test_window_functions(self, large_dataset):
        """Test window functions in local-cluster mode."""
        window_spec = Window.partitionBy("category").orderBy("value")

        result = (
            large_dataset.select(
                "id",
                "category",
                "value",
                F.row_number().over(window_spec).alias("row_num"),
                F.rank().over(window_spec).alias("rank"),
                F.lag("value", 1).over(window_spec).alias("prev_value"),
            )
            .filter(F.col("category") == "cat_0")
            .orderBy("value")
            .limit(5)
            .toPandas()
        )

        assert len(result) == 5  # noqa: PLR2004
        assert result["row_num"].tolist() == [1, 2, 3, 4, 5]
        assert result["rank"].tolist() == [1, 2, 3, 4, 5]
        assert pd.isna(result["prev_value"].iloc[0])

    def test_complex_sql_query(self, spark):
        """Test complex SQL query execution in local-cluster mode."""
        sales_data = []
        for i in range(500):
            sales_data.append(  # noqa: PERF401
                Row(
                    sale_id=i,
                    product_id=i % 20,
                    region=f"region_{i % 5}",
                    amount=100 + (i % 100),
                    sale_date=f"2024-{(i % 12) + 1:02d}-01",
                )
            )

        df = spark.createDataFrame(sales_data)
        df.createOrReplaceTempView("sales")

        result = spark.sql("""
            WITH regional_sales AS (
                SELECT
                    region,
                    product_id,
                    SUM(amount) as total_amount,
                    COUNT(*) as sale_count,
                    AVG(amount) as avg_amount
                FROM sales
                GROUP BY region, product_id
            ),
            top_products AS (
                SELECT
                    region,
                    product_id,
                    total_amount,
                    ROW_NUMBER() OVER (PARTITION BY region ORDER BY total_amount DESC) as rank
                FROM regional_sales
            )
            SELECT
                region,
                product_id,
                total_amount,
                rank
            FROM top_products
            WHERE rank <= 3
            ORDER BY region, rank
        """).toPandas()

        assert len(result) == 15  # noqa: PLR2004
        regions = result["region"].unique()
        assert len(regions) == 5  # noqa: PLR2004

        for region in regions:
            region_data = result[result["region"] == region]
            assert len(region_data) == 3  # noqa: PLR2004
            assert region_data["rank"].tolist() == [1, 2, 3]

        spark.catalog.dropTempView("sales")

    def test_multiple_operations(self, spark):
        """Test that multiple operations in local-cluster mode."""
        df1 = spark.range(100).select(F.col("id").alias("id1"))
        df2 = spark.range(100).select(F.col("id").alias("id2"))

        result1 = df1.groupBy().sum("id1").collect()[0][0]
        result2 = df2.groupBy().sum("id2").collect()[0][0]
        result3 = df1.join(df2, df1.id1 == df2.id2).count()

        expected_sum = sum(range(100))  # 0 + 1 + ... + 99 = 4950
        assert result1 == expected_sum
        assert result2 == expected_sum
        assert result3 == 100  # noqa: PLR2004

    def test_repartitioning_in_cluster_mode(self, large_dataset):
        """Test data repartitioning in local-cluster mode."""
        repartitioned = large_dataset.repartition(8)

        original_count = large_dataset.count()
        repartitioned_count = repartitioned.count()
        assert original_count == repartitioned_count == 1000  # noqa: PLR2004

        original_sum = large_dataset.agg(F.sum("value")).collect()[0][0]
        repartitioned_sum = repartitioned.agg(F.sum("value")).collect()[0][0]
        assert original_sum == repartitioned_sum

        coalesced = large_dataset.coalesce(4)
        coalesced_count = coalesced.count()
        coalesced_sum = coalesced.agg(F.sum("value")).collect()[0][0]
        assert coalesced_count == 1000  # noqa: PLR2004
        assert coalesced_sum == original_sum
