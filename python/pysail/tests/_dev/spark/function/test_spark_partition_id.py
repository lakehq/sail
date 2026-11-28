# SPDX-License-Identifier: Apache-2.0

import pyarrow as pa
import pyarrow.parquet as pq
import pyspark.sql.functions as F  # noqa: N812
import pytest

from pysail.testing.spark.utils.sql import escape_sql_string_literal


def test_spark_partition_id_smoke(spark):
    rows = spark.sql("SELECT spark_partition_id() AS pid").collect()
    assert len(rows) == 1
    value = rows[0]["pid"]
    assert isinstance(value, int)
    assert value >= 0


def test_spark_partition_id_constant_within_single_partition(spark):
    # `range(..., numPartitions=1)` forces one partition; all rows must
    # share the same partition id (which is 0).
    rows = spark.range(0, 10, numPartitions=1).selectExpr("id", "spark_partition_id() AS pid").collect()
    assert len(rows) == 10
    pids = {row["pid"] for row in rows}
    assert pids == {0}


def test_spark_partition_id_distinct_across_partitions(spark):
    # With multiple partitions, `spark_partition_id` should produce
    # all partition indices in `[0, numPartitions)`.
    num_partitions = 4
    rows = spark.range(0, 100, numPartitions=num_partitions).selectExpr("spark_partition_id() AS pid").collect()
    pids = {row["pid"] for row in rows}
    assert pids == set(range(num_partitions))


def test_spark_partition_id_same_when_called_twice_in_select(spark):
    rows = spark.sql(
        """
        SELECT
          spark_partition_id() AS pid1,
          spark_partition_id() AS pid2
        FROM range(0, 10)
        """
    ).collect()
    assert rows, "expected non-empty result"
    for r in rows:
        assert r["pid1"] == r["pid2"]


def test_spark_partition_id_inside_lateral_view_expression(spark):
    rows = spark.sql(
        """
        SELECT id, v
        FROM range(0, 10)
        LATERAL VIEW explode(array(spark_partition_id())) AS v
        ORDER BY id
        """
    ).collect()
    assert len(rows) == 10
    for index, row in enumerate(rows):
        assert row["id"] == index
        assert isinstance(row["v"], int)
        assert row["v"] >= 0


def test_spark_partition_id_return_type_is_int(spark):
    df = spark.sql("SELECT spark_partition_id() AS pid")
    [field] = df.schema.fields
    assert field.name == "pid"
    # Spark's `spark_partition_id` returns IntegerType (32-bit signed int).
    assert field.dataType.typeName() == "integer"


def test_spark_partition_id_in_filter(spark):
    df = spark.range(40, numPartitions=4)
    filtered = df.filter(F.spark_partition_id() == 2)
    assert filtered.columns == ["id"]
    assert sorted(row.id for row in filtered.collect()) == list(range(20, 30))


@pytest.mark.parametrize("sql", [False, True], ids=["dataframe", "sql"])
def test_spark_partition_id_in_grouping(spark, sql):
    if sql:
        result = spark.sql(
            "SELECT spark_partition_id() + 10 AS p, count(*) AS n "
            "FROM range(0, 40, 1, 4) GROUP BY spark_partition_id() + 10"
        )
    else:
        df = spark.range(40, numPartitions=4)
        result = df.groupBy((F.spark_partition_id() + 10).alias("p")).count()
    assert sorted(tuple(row) for row in result.collect()) == [(i + 10, 10) for i in range(4)]


@pytest.mark.parametrize("global_sort", [False, True], ids=["local-sort", "global-sort"])
def test_spark_partition_id_in_sort(spark, global_sort):
    df = spark.range(40, numPartitions=4)
    sort = df.orderBy if global_sort else df.sortWithinPartitions
    result = sort(F.spark_partition_id().desc(), F.col("id").desc())
    assert result.columns == ["id"]
    rows = [row.id for row in result.collect()]
    assert sorted(rows) == list(range(40))
    if global_sort:
        assert rows == list(range(39, -1, -1))


def test_spark_partition_id_in_repartition(spark):
    df = spark.range(40, numPartitions=4).repartition(4, F.spark_partition_id())
    assert df.columns == ["id"]
    assert sorted(row.id for row in df.collect()) == list(range(40))


def test_spark_partition_id_grouping_reuses_value_in_having(spark):
    result = spark.sql(
        "SELECT spark_partition_id() AS p, count(*) AS n "
        "FROM range(0, 40, 1, 4) GROUP BY spark_partition_id() HAVING p >= 2"
    )
    assert sorted(tuple(row) for row in result.collect()) == [(2, 10), (3, 10)]


@pytest.mark.parametrize("function", ["spark_partition_id", "monotonically_increasing_id"])
def test_partition_function_in_having(spark, function):
    result = spark.sql(f"SELECT count(*) FROM range(40) HAVING {function}() = 0")  # noqa: S608
    assert result.collect() == [(40,)]


@pytest.mark.parametrize("function", [F.spark_partition_id, F.monotonically_increasing_id])
def test_partition_function_rejected_inside_aggregate(spark, function):
    with pytest.raises(Exception, match=r"(?i)non.?deterministic"):
        spark.range(40).agg(F.max(function())).collect()


@pytest.mark.spark(skip_if_jvm=True, reason="Sail reads each large Parquet row group in its own partition")
@pytest.mark.parametrize("function", ["spark_partition_id", "monotonically_increasing_id"])
@pytest.mark.parametrize("source", ["table", "path"])
def test_partition_dependent_id_without_other_columns_over_files(spark, tmp_path, function, source):
    # The value depends on the partition of each row, which a scan that reads no column keeps.
    location = tmp_path / "data"
    location.mkdir()
    schema = pa.schema([("id", pa.int64()), ("payload", pa.string())])
    with pq.ParquetWriter(location / "data.parquet", schema, compression="NONE", use_dictionary=False) as writer:
        # Each row group is over 1 MiB, so the scan reads it in its own partition.
        for start in range(0, 30_000, 10_000):
            ids = range(start, start + 10_000)
            writer.write_table(pa.table({"id": ids, "payload": ["x" * 128] * len(ids)}, schema=schema))
    if source == "table":
        relation = "partition_dependent_ids"
        spark.sql(f"CREATE TABLE {relation} USING parquet LOCATION '{escape_sql_string_literal(str(location))}'")
        frame = spark.table(relation)
    else:
        relation = f"parquet.`{location}`"
        frame = spark.read.parquet(str(location))
    try:
        alone = f"SELECT {function}() AS v FROM {relation}"  # noqa: S608
        with_id = f"SELECT id, {function}() AS v FROM {relation}"  # noqa: S608
        expected = sorted(row.v for row in spark.sql(with_id).collect())
        assert len(expected) == 30_000
        assert len(set(expected)) > 1
        assert sorted(row.v for row in spark.sql(alone).collect()) == expected
        assert spark.sql(f"SELECT count(*), max(v) FROM ({alone})").collect() == [(30_000, expected[-1])]  # noqa: S608
        assert sorted(row[0] for row in frame.select(getattr(F, function)()).collect()) == expected
    finally:
        spark.sql("DROP TABLE IF EXISTS partition_dependent_ids")


@pytest.fixture
def union_inputs(tmp_path):
    """Writes the Parquet directories `a`, `b` and `c`, with a file for each of the row counts."""
    paths = {}
    start = 0
    for name, counts in {"a": [100, 500, 2000, 50, 1000, 300], "b": [700, 40, 1500], "c": [250, 900]}.items():
        path = tmp_path / name
        path.mkdir()
        for i, count in enumerate(counts):
            ids = pa.array(range(start, start + count), pa.int64())
            pq.write_table(pa.table({"id": ids}), path / f"{name}{i}.parquet")
            start += count
        paths[name] = f"parquet.`{path}`"
    return paths


def partitions_of_files(spark, query):
    """Returns the partition and the row count of each file that the query reads, by its file name."""
    rows = spark.sql(
        f"SELECT f, p, count(*) AS n FROM ({query}) AS q GROUP BY f, p"  # noqa: S608
    ).collect()
    return sorted((row.f.rsplit("/", 1)[-1], row.p, row.n) for row in rows)


def select_files(relation, where=""):
    return f"SELECT input_file_name() AS f, spark_partition_id() AS p, id FROM {relation}{where}"  # noqa: S608


@pytest.mark.spark(skip_if_jvm=True, reason="Sail groups the files of a scan in its own way")
@pytest.mark.parametrize(
    "inputs", [["a", "b"], ["b", "a"], ["a", ["b", "c"]], [["a", "b"], "c"]], ids=["ab", "ba", "a(bc)", "(ab)c"]
)
@pytest.mark.parametrize("where", ["", " WHERE id % 2 = 0"], ids=["all", "filtered"])
def test_spark_partition_id_over_a_union_follows_the_partitions_of_its_inputs(spark, union_inputs, inputs, where):
    def union(inputs):
        if isinstance(inputs, str):
            return f"SELECT * FROM {union_inputs[inputs]}"  # noqa: S608
        return " UNION ALL ".join(f"SELECT * FROM ({union(x)}) AS u" for x in inputs)  # noqa: S608

    def flatten(inputs):
        return [inputs] if isinstance(inputs, str) else [y for x in inputs for y in flatten(x)]

    # A union returns the partitions of its inputs one input after another.
    expected = []
    offset = 0
    for name in flatten(inputs):
        alone = partitions_of_files(spark, select_files(union_inputs[name], where))
        expected.extend((f, p + offset, n) for f, p, n in alone)
        offset += max(p for _, p, _ in alone) + 1
    expected.sort()
    query = select_files(f"({union(inputs)}) AS u", where)
    for _ in range(5):
        assert partitions_of_files(spark, query) == expected
    projected = f"SELECT f, p, id + 1 AS id FROM ({query}) AS q"  # noqa: S608
    assert partitions_of_files(spark, projected) == expected


@pytest.mark.spark(skip_if_jvm=True, reason="Sail groups the files of a scan in its own way")
def test_spark_partition_id_inside_union_inputs_follows_the_partitions_of_each_input(spark, union_inputs):
    alone = partitions_of_files(spark, select_files(union_inputs["b"]))
    query = f"{select_files(union_inputs['a'])} UNION ALL {select_files(union_inputs['b'])}"
    for _ in range(5):
        rows = partitions_of_files(spark, query)
        assert [x for x in rows if x[0].startswith("b")] == alone
