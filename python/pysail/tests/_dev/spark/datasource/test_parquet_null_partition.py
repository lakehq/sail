# SPDX-License-Identifier: Apache-2.0

import datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__"


def partition_directories(path):
    """Returns the names of the directories under the path that have data files."""
    return {str(p.parent.relative_to(path)) for p in path.rglob("*.parquet") if not p.name.startswith(("_", "."))}


@pytest.mark.parametrize(
    ("data_type", "values", "directories", "default_value"),
    [
        ("INT", [0, None, 1, None, 0], {"k=0", "k=1"}, 0),
        ("BIGINT", [0, None, -3, None, 0], {"k=0", "k=-3"}, 0),
        ("SMALLINT", [0, None, 7, None, 0], {"k=0", "k=7"}, 0),
        ("BOOLEAN", [False, None, True, None, False], {"k=false", "k=true"}, False),
        (
            "DATE",
            [datetime.date(1970, 1, 1), None, datetime.date(2024, 2, 29), None, datetime.date(1970, 1, 1)],
            {"k=1970-01-01", "k=2024-02-29"},
            datetime.date(1970, 1, 1),
        ),
        ("STRING", ["a", None, "b", None, "a"], {"k=a", "k=b"}, ""),
    ],
)
def test_parquet_null_partition_values(spark, tmp_path, data_type, values, directories, default_value):
    path = tmp_path / "table"
    schema = f"id LONG, k {data_type}"
    rows = list(enumerate(values))
    spark.createDataFrame(rows, schema=schema).repartition(2).write.partitionBy("k").parquet(str(path))
    # The rows without a value are not in the directory of the default value of the type.
    assert partition_directories(path) == directories | {f"k={_DEFAULT_PARTITION}"}

    df = spark.read.schema(schema).parquet(str(path))
    assert df.schema["k"].nullable
    assert sorted((r.id, r.k) for r in df.collect()) == rows

    null_ids = [i for i, v in rows if v is None]
    assert sorted(r.id for r in df.filter("k IS NULL").collect()) == null_ids
    assert sorted(r.id for r in df.filter("k IS NOT NULL").collect()) == [i for i, v in rows if v is not None]
    assert sorted(r.id for r in df.filter(df.k == default_value).collect()) == [
        i for i, v in rows if v == default_value
    ]
    assert sorted(r.id for r in df.filter(df.k.eqNullSafe(None)).collect()) == null_ids

    (counts,) = df.selectExpr("count(k) AS values", "count(*) AS rows").collect()
    assert counts.values == len(rows) - len(null_ids)
    assert counts.rows == len(rows)
    groups = {r.k: r["count"] for r in df.groupBy("k").count().collect()}
    assert groups == {v: values.count(v) for v in values}


def test_parquet_empty_string_partition_value_is_null(spark, tmp_path):
    path = tmp_path / "table"
    schema = "id LONG, k STRING"
    rows = [(0, "a"), (1, ""), (2, None), (3, ""), (4, "a")]
    spark.createDataFrame(rows, schema=schema).coalesce(1).write.partitionBy("k").parquet(str(path))
    # The empty string and the missing value share the directory of the default partition,
    # and there is no directory with an empty value.
    assert partition_directories(path) == {"k=a", f"k={_DEFAULT_PARTITION}"}
    assert len(list((path / f"k={_DEFAULT_PARTITION}").glob("*.parquet"))) == 1

    df = spark.read.schema(schema).parquet(str(path))
    assert sorted((r.id, r.k) for r in df.collect()) == [(0, "a"), (1, None), (2, None), (3, None), (4, "a")]
    assert df.filter("k = ''").count() == 0
    assert df.filter(f"k = '{_DEFAULT_PARTITION}'").count() == 0
    assert sorted(r.id for r in df.filter("k IS NULL").collect()) == [1, 2, 3]


def test_parquet_null_partition_values_without_schema(spark, tmp_path):
    path = tmp_path / "table"
    rows = [(0, 0), (1, None), (2, 1), (3, None)]
    spark.createDataFrame(rows, schema="id LONG, k INT").write.partitionBy("k").parquet(str(path))

    df = spark.read.parquet(str(path))
    assert df.schema["k"].nullable
    # The type of the partition column depends on the type inference of the reader.
    actual = sorted((r.id, None if r.k is None else str(r.k)) for r in df.collect())
    assert actual == [(0, "0"), (1, None), (2, "1"), (3, None)]
    assert sorted(r.id for r in df.filter("k IS NULL").collect()) == [1, 3]
    assert sorted(r.id for r in df.filter("k IS NOT NULL").collect()) == [0, 2]


def test_parquet_null_partition_values_of_several_columns(spark, tmp_path):
    path = tmp_path / "table"
    schema = "id LONG, a INT, b STRING"
    rows = [(0, 1, "x"), (1, None, "x"), (2, 1, None), (3, None, None), (4, 0, ""), (5, None, "x")]
    spark.createDataFrame(rows, schema=schema).write.partitionBy("a", "b").parquet(str(path))
    assert partition_directories(path) == {
        "a=1/b=x",
        f"a={_DEFAULT_PARTITION}/b=x",
        f"a=1/b={_DEFAULT_PARTITION}",
        f"a={_DEFAULT_PARTITION}/b={_DEFAULT_PARTITION}",
        f"a=0/b={_DEFAULT_PARTITION}",
    }

    df = spark.read.schema(schema).parquet(str(path))
    expected = [(0, 1, "x"), (1, None, "x"), (2, 1, None), (3, None, None), (4, 0, None), (5, None, "x")]
    assert sorted(((r.id, r.a, r.b) for r in df.collect()), key=lambda r: r[0]) == expected
    assert sorted(r.id for r in df.filter("a IS NULL AND b = 'x'").collect()) == [1, 5]
    assert sorted(r.id for r in df.filter("a = 1 AND b IS NULL").collect()) == [2]
    assert sorted(r.id for r in df.filter("a = 0").collect()) == [4]
    assert sorted(r.id for r in df.filter("a IS NULL AND b IS NULL").collect()) == [3]


@pytest.mark.parametrize(
    ("data_type", "directory", "value"),
    [
        ("INT", "1", 1),
        ("BIGINT", "0", 0),
        ("DOUBLE", "1.5", 1.5),
        ("BOOLEAN", "true", True),
        ("STRING", "a", "a"),
        ("DATE", "2024-01-02", datetime.date(2024, 1, 2)),
        ("DECIMAL(10, 2)", "1.50", Decimal("1.50")),
    ],
)
def test_parquet_read_default_partition_directory(spark, tmp_path, data_type, directory, value):
    # The directories are written without Sail, in the way that Hive and Spark write them.
    path = tmp_path / "table"
    for name, ids in [(f"k={directory}", [0, 1]), (f"k={_DEFAULT_PARTITION}", [2, 3, 4])]:
        (path / name).mkdir(parents=True)
        pq.write_table(pa.table({"id": pa.array(ids, type=pa.int64())}), path / name / "part-0.parquet")

    df = spark.read.schema(f"id LONG, k {data_type}").parquet(str(path))
    assert df.schema["k"].nullable
    assert sorted((r.id, r.k) for r in df.collect()) == [(0, value), (1, value), (2, None), (3, None), (4, None)]
    assert sorted(r.id for r in df.filter("k IS NULL").collect()) == [2, 3, 4]
    assert sorted(r.id for r in df.filter("k IS NOT NULL").collect()) == [0, 1]
    assert sorted(r.id for r in df.filter(df.k == value).collect()) == [0, 1]
    (counts,) = df.selectExpr("count(k) AS values", "count(*) AS rows").collect()
    assert (counts.values, counts.rows) == (2, 5)


def test_csv_read_default_partition_directory(spark, tmp_path):
    path = tmp_path / "table"
    for name, ids in [("k=1", [0, 1]), (f"k={_DEFAULT_PARTITION}", [2, 3])]:
        (path / name).mkdir(parents=True)
        (path / name / "part-0.csv").write_text("".join(f"{i}\n" for i in ids))

    df = spark.read.schema("id LONG, k INT").csv(str(path))
    assert sorted((r.id, r.k) for r in df.collect()) == [(0, 1), (1, 1), (2, None), (3, None)]
    assert sorted(r.id for r in df.filter("k IS NULL").collect()) == [2, 3]
    assert sorted(r.id for r in df.filter("k = 1").collect()) == [0, 1]
