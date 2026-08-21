import pandas as pd
import pyarrow as pa
import pyspark.sql.functions as F  # noqa: N812
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql.types import Row, StringType
from pyspark.sql.window import Window

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")

_VIEW_SHUFFLE_PARTITIONS = 4
_VIEW_SHUFFLE_ROWS = 128
_VIEW_BATCH_SIZE = 8
_VIEW_LABEL = f"selected-label-{'l' * 48}"
_VIEW_BINARY = f"selected-binary-{'b' * 48}".encode()


def _partition_count(df):
    def counter(_iterator):
        yield pd.DataFrame({"n": [1]})

    return df.mapInPandas(counter, schema="n: long").count()


def _to_arrow_table(df):
    if to_table := getattr(type(df), "_to_table", None):
        table, _ = to_table(df)
        return table

    query = df._plan.to_proto(df._session.client)  # noqa: SLF001
    table, _ = df._session.client.to_table(query)  # noqa: SLF001
    return table


@pytest.fixture
def cross_worker_view_spark():
    envs = {
        "SAIL_MODE": "local-cluster",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE": "flight",
        "SAIL_CLUSTER__WORKER_INITIAL_COUNT": str(_VIEW_SHUFFLE_PARTITIONS),
        "SAIL_CLUSTER__WORKER_TASK_SLOTS": "1",
        "SAIL_EXECUTION__BATCH_SIZE": str(_VIEW_BATCH_SIZE),
        "SAIL_EXECUTION__DEFAULT_PARALLELISM": str(_VIEW_SHUFFLE_PARTITIONS),
    }
    with spark_connect_server(envs=envs) as server, spark_session_factory(server.remote) as sessions:
        yield sessions.create()


def _assert_cross_worker_flight_topology(spark):
    expected_options = {
        "cluster.shuffle_backend.type": "flight",
        "cluster.worker_initial_count": str(_VIEW_SHUFFLE_PARTITIONS),
        "cluster.worker_task_slots": "1",
        "execution.batch_size": str(_VIEW_BATCH_SIZE),
        "execution.default_parallelism": str(_VIEW_SHUFFLE_PARTITIONS),
        "mode": "local_cluster",
    }
    quoted_option_keys = ", ".join(f"'{key}'" for key in expected_options)
    query = f"SELECT key, value FROM system.session.options WHERE key IN ({quoted_option_keys})"  # noqa: S608
    options = {row.key: row.value for row in spark.sql(query).collect()}
    assert options == expected_options

    running_workers = _to_arrow_table(
        spark.table("system.cluster.workers")
        .where((F.col("session_id") == spark.session_id) & (F.col("status") == "RUNNING"))
        .select("worker_id")
    )
    assert len(set(running_workers.column("worker_id").to_pylist())) >= _VIEW_SHUFFLE_PARTITIONS


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


def test_parquet_utf8_view_across_cluster_shuffle(cross_worker_view_spark, tmp_path):
    spark = cross_worker_view_spark

    path = tmp_path / "utf8_view.parquet"
    path.mkdir()
    rows_per_file = _VIEW_SHUFFLE_ROWS // _VIEW_SHUFFLE_PARTITIONS
    for file_index in range(_VIEW_SHUFFLE_PARTITIONS):
        start = file_index * rows_per_file
        row_values = range(start, start + rows_per_file)
        pd.DataFrame(
            {
                "key": [f"partition-key-{value:03d}-{'k' * 48}" for value in row_values],
                "label": [None if value % 34 == 0 else _VIEW_LABEL for value in row_values],
                "raw": [None if value % 51 == 0 else _VIEW_BINARY for value in row_values],
                "value": list(row_values),
            }
        ).to_parquet(path / f"part-{file_index}.parquet")

    input_dfs = [
        spark.read.parquet(str(path / f"part-{file_index}.parquet")).withColumn("source_file", F.lit(file_index))
        for file_index in range(_VIEW_SHUFFLE_PARTITIONS)
    ]
    df = input_dfs[0]
    for input_df in input_dfs[1:]:
        df = df.unionByName(input_df)

    assert isinstance(df.schema["key"].dataType, StringType)
    selected_values = list(range(0, _VIEW_SHUFFLE_ROWS, 5))
    selected_rows = (
        df.where("value % 5 = 0")
        .select(
            "*",
            F.spark_partition_id().alias("source_partition"),
            F.pmod("value", F.lit(_VIEW_SHUFFLE_PARTITIONS)).alias("shuffle_key"),
        )
        .repartition(_VIEW_SHUFFLE_PARTITIONS, "shuffle_key")
        .selectExpr(
            "*",
            "spark_partition_id() AS shuffle_partition",
            "regexp_extract(label, '(selected)-(label)', 2) AS extracted",
            "regexp_extract_all(label, '(selected)-(label)', 1) AS extracted_all",
            "split(label, '-')[1] AS split_part",
            "hash(label) AS hashed",
            "hash(raw) AS binary_hashed",
        )
        .orderBy("value")
        .collect()
    )
    _assert_cross_worker_flight_topology(spark)
    assert [row.value for row in selected_rows] == selected_values
    source_partition_by_file = {}
    for row in selected_rows:
        source_partition = source_partition_by_file.setdefault(row.source_file, row.source_partition)
        assert row.source_partition == source_partition
    assert set(source_partition_by_file) == set(range(_VIEW_SHUFFLE_PARTITIONS))
    assert len(set(source_partition_by_file.values())) == _VIEW_SHUFFLE_PARTITIONS
    sources_by_shuffle_partition = {}
    for row in selected_rows:
        sources_by_shuffle_partition.setdefault(row.shuffle_partition, set()).add(row.source_file)
    assert sources_by_shuffle_partition
    assert all(
        source_files == set(range(_VIEW_SHUFFLE_PARTITIONS)) for source_files in sources_by_shuffle_partition.values()
    )
    # The one-slot topology and four-way source/destination task sets require multiple workers.
    # Every non-empty destination consumes all four sources, so peer Flight fetch is required.

    label_hashes = set()
    binary_hashes = set()
    for row in selected_rows:
        expected_label = None if row.value % 34 == 0 else _VIEW_LABEL
        expected_raw = None if row.value % 51 == 0 else _VIEW_BINARY
        assert row.source_file == row.value // rows_per_file
        assert row.key == f"partition-key-{row.value:03d}-{'k' * 48}"
        assert row.label == expected_label
        actual_raw = None if row.raw is None else bytes(row.raw)
        assert actual_raw == expected_raw
        assert row.extracted == (None if expected_label is None else "label")
        assert row.extracted_all == (None if expected_label is None else ["selected"])
        assert row.split_part == (None if expected_label is None else "label")
        if expected_label is not None:
            label_hashes.add(row.hashed)
        if expected_raw is not None:
            binary_hashes.add(row.binary_hashed)

    assert len(label_hashes) == 1
    assert len(binary_hashes) == 1

    counts = {
        row.label: (row.row_count, row.raw_count)
        for row in (
            df.repartition(_VIEW_SHUFFLE_PARTITIONS, "label")
            .groupBy("label")
            .agg(F.count("*").alias("row_count"), F.count("raw").alias("raw_count"))
            .collect()
        )
    }
    assert counts == {None: (4, 2), _VIEW_LABEL: (124, 123)}  # noqa: PLR2004


@pytest.mark.parametrize(
    ("use_large_var_types", "expected_string_type", "expected_binary_type"),
    [
        (False, pa.string(), pa.binary()),
        (True, pa.large_string(), pa.large_binary()),
    ],
)
def test_parquet_view_output_honors_arrow_width_config_across_cluster(
    spark,
    tmp_path,
    use_large_var_types,
    expected_string_type,
    expected_binary_type,
):
    path = tmp_path / "view_output_width.parquet"
    alpha = f"alpha-{'a' * 48}"
    beta = f"beta-{'b' * 48}"
    first = f"first-{'f' * 48}".encode()
    second = f"second-{'s' * 48}".encode()
    pd.DataFrame(
        {
            "key": [beta, alpha],
            "raw": [second, first],
        }
    ).to_parquet(path)

    config_key = "spark.sql.execution.arrow.useLargeVarTypes"
    previous_value = spark.conf.get(config_key)
    spark.conf.set(config_key, str(use_large_var_types).lower())
    try:
        df = (
            spark.read.parquet(str(path))
            .repartition(2, "key")
            .select(
                "key",
                "raw",
                F.struct(
                    F.array("key").alias("items"),
                    F.create_map("key", "raw").alias("mapping"),
                ).alias("nested"),
            )
            .orderBy("key")
        )
        table = _to_arrow_table(df)
        empty = _to_arrow_table(df.where("false"))
    finally:
        spark.conf.set(config_key, previous_value)

    assert table.column("key").to_pylist() == [alpha, beta]
    assert table.column("raw").to_pylist() == [first, second]
    assert empty.num_rows == 0
    for output in (table, empty):
        assert output.schema.field("key").type == expected_string_type
        assert output.schema.field("raw").type == expected_binary_type
        nested_type = output.schema.field("nested").type
        assert nested_type.field("items").type.value_type == expected_string_type
        mapping_type = nested_type.field("mapping").type
        assert mapping_type.key_type == expected_string_type
        assert mapping_type.item_type == expected_binary_type


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
