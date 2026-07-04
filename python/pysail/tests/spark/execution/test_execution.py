import pandas as pd
import pyspark.sql.functions as F  # noqa: N812
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql.types import Row
from pyspark.sql.window import Window

from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")


def _partition_count(df):
    def counter(_iterator):
        yield pd.DataFrame({"n": [1]})

    return df.mapInPandas(counter, schema="n: long").count()


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


def test_basic_query_execution(spark):
    """Test basic query execution in local-cluster mode."""
    result = spark.sql("SELECT 1 + 1 AS result").collect()
    assert result[0]["result"] == 2  # noqa: PLR2004


def test_dataframe_operations(spark):
    """Test DataFrame operations in local-cluster mode."""
    df = spark.createDataFrame([Row(a=1, b="hello"), Row(a=2, b="world"), Row(a=3, b="test")])

    result = df.select("a", "b").filter(F.col("a") > 1).orderBy("a").toPandas()
    expected = pd.DataFrame({"a": [2, 3], "b": ["world", "test"]}).astype({"a": "int64"})  # Spark Connect uses int64

    assert_frame_equal(result, expected)


def test_aggregation_with_groupby(large_dataset):
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


def test_aggregate_expression_with_scalar_subquery_in_cluster_mode(spark):
    result = spark.sql("""
        SELECT
            SUM(CAST(v AS BIGINT) + (
                SELECT MIN(CAST(x AS BIGINT))
                FROM VALUES (10), (20) AS s(x)
            )) AS total
        FROM VALUES (1), (2), (3) AS t(v)
    """).collect()

    assert result == [Row(total=36)]


def test_join_operations(spark):
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


def test_window_functions(large_dataset):
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


def test_complex_sql_query(spark):
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


def test_multiple_operations(spark):
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


def test_repartitioning_in_cluster_mode(large_dataset):
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


def test_parquet_directory_scan_reads_each_file_once_in_cluster_mode(spark, tmp_path):
    path = tmp_path / "parquet_files"
    path.mkdir()
    pd.DataFrame({"id": [1]}).to_parquet(path / "part-0.parquet")
    pd.DataFrame({"id": [2]}).to_parquet(path / "part-1.parquet")

    rows = spark.read.parquet(str(path)).orderBy("id").collect()
    assert rows == [Row(id=1), Row(id=2)]


def test_coalesce_plan_contains_dedicated_exec_in_cluster_mode(spark):
    plan = spark.range(0, 12, 1, 4).coalesce(2)._explain_string()  # noqa: SLF001
    assert "CoalesceExec" in plan


@pytest.mark.parametrize(
    ("row_count", "input_partition_count", "output_partition_count", "expected_partition_count"),
    [
        (48, 4, 6, 4),
        (48, 4, 2, 2),
        (10, 2, 1, 1),
    ],
)
def test_coalesce_partition_count_in_cluster_mode(
    spark,
    row_count,
    input_partition_count,
    output_partition_count,
    expected_partition_count,
):
    df = spark.range(0, row_count, 1, input_partition_count).coalesce(output_partition_count)
    assert _partition_count(df) == expected_partition_count


def test_coalesce_spark_parity_in_cluster_mode(spark):
    row_count = 48
    input_partition_count = 4
    increased_partition_count = 6
    output_partition_count = 2

    def input_partition_groups(df):
        def counter(iterator):
            input_pids = set()
            for pdf in iterator:
                input_pids.update(pdf["input_pid"].tolist())
            yield pd.DataFrame({"input_pids": [",".join(str(pid) for pid in sorted(input_pids))]})

        rows = df.mapInPandas(counter, schema="input_pids: string").collect()
        return [
            set() if row["input_pids"] == "" else {int(pid) for pid in row["input_pids"].split(",")} for row in rows
        ]

    increased_rows = (
        spark.range(0, row_count, 1, input_partition_count)
        .coalesce(increased_partition_count)
        .selectExpr("spark_partition_id() AS pid")
        .distinct()
        .collect()
    )
    assert {row["pid"] for row in increased_rows} == set(range(input_partition_count))

    groups = input_partition_groups(
        spark.range(0, row_count, 1, input_partition_count)
        .selectExpr("spark_partition_id() AS input_pid")
        .coalesce(output_partition_count)
    )

    assert len(groups) == output_partition_count
    assert all(group for group in groups)
    assert {pid for group in groups for pid in group} == set(range(input_partition_count))
    assert sum(len(group) for group in groups) == input_partition_count


def test_coalesce_preserves_data_in_cluster_mode(spark):
    row_count = 48
    input_partition_count = 4

    df = spark.range(0, row_count, 1, input_partition_count).select(
        "id",
        (F.col("id") % 3).alias("group"),
    )

    actual = df.coalesce(2).orderBy("id").toPandas()
    expected = df.orderBy("id").toPandas()

    assert_frame_equal(actual, expected)


def test_coalesce_to_one_partition_in_cluster_mode(spark):
    df = spark.range(0, 20, 1, 4)
    coalesced = df.coalesce(1)

    actual_ids = sorted(row["id"] for row in coalesced.collect())
    assert actual_ids == list(range(20))


def test_coalesce_hint_in_cluster_mode(spark):
    df = spark.range(0, 12, 1, 4).select("id", (F.col("id") % 3).alias("group"))

    actual = df.hint("COALESCE", 2).orderBy("id").toPandas()
    expected = df.orderBy("id").toPandas()

    assert_frame_equal(actual, expected)


@pytest.mark.parametrize(
    ("output_partition_count", "expected_partition_count"),
    [
        (2, 2),
        (6, 4),
    ],
)
def test_coalesce_hint_partition_count_in_cluster_mode(
    spark,
    output_partition_count,
    expected_partition_count,
):
    df = spark.range(0, 48, 1, 4).hint("COALESCE", output_partition_count)
    assert _partition_count(df) == expected_partition_count


@pytest.mark.parametrize("partition_count", [0, -1])
def test_coalesce_hint_rejects_non_positive_partition_count_in_cluster_mode(spark, partition_count):
    df = spark.range(0, 10, 1, 2).hint("COALESCE", partition_count)
    with pytest.raises(Exception, match="COALESCE hint requires at least one partition"):
        df.count()
