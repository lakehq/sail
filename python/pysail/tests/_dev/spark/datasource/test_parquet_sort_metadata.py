# ruff: noqa: S608
from collections import Counter
from uuid import uuid4

import pyarrow.parquet as pq
import pytest


@pytest.mark.spark(skip_if_jvm=True, reason="Standalone sortBy is a Sail extension")
@pytest.mark.parametrize("mode", ["append", "overwrite"], ids=["append", "dynamic-overwrite"])
@pytest.mark.parametrize("nested", [False, True], ids=["flat", "nested"])
def test_partitioned_sort_metadata_uses_parquet_leaf_indices(spark, tmp_path, mode, nested):
    path = tmp_path / "data"
    payload_type = "STRUCT<x:INT,y:INT>" if nested else "INT"
    # The legal #1 identifier also matches the resolver's internal field name.
    schema = f"payload {payload_type}, `#1` INT, p INT"
    seed = [((8, 80), 8, 1), ((9, 90), 9, 2)]
    incoming = [((1, 90), 1, 1), ((2, 10), 3, 1), ((3, 50), 2, 1)]
    if not nested:
        seed = [(payload[1], value, partition) for payload, value, partition in seed]
        incoming = [(payload[1], value, partition) for payload, value, partition in incoming]

    def write(rows, write_mode):
        (
            spark.createDataFrame(rows, schema)
            .coalesce(1)
            .write.partitionBy("p")
            .sortBy("#1")
            .mode(write_mode)
            .option("partitionOverwriteMode", "dynamic")
            .parquet(str(path))
        )

    def files(partition):
        return {file.name: file.read_bytes() for file in (path / partition).glob("*.parquet")}

    write(seed, "append")
    untouched = files("p=2")
    replaced = files("p=1")
    assert untouched
    assert replaced
    write(incoming, mode)
    assert files("p=2") == untouched
    if mode == "overwrite":
        assert not set(files("p=1")) & set(replaced)
    else:
        assert files("p=1").items() >= replaced.items()

    expected = (seed if mode == "append" else [seed[1]]) + incoming
    actual = spark.read.schema(schema).parquet(str(path)).collect()
    assert Counter((tuple(row.payload) if nested else row.payload, row["#1"], row.p) for row in actual) == Counter(
        expected
    )

    physical_rows = []
    leaf_index = 2 if nested else 1
    parquet_files = list(path.rglob("*.parquet"))
    assert parquet_files
    for file in parquet_files:
        parquet = pq.ParquetFile(file)
        assert parquet.schema.names == (["x", "y", "#1"] if nested else ["payload", "#1"])
        partition = int(file.parent.name.removeprefix("p="))
        assert parquet.metadata.num_row_groups > 0
        for index in range(parquet.metadata.num_row_groups):
            group = parquet.metadata.row_group(index)
            assert group.column(leaf_index).path_in_schema == "#1"
            assert group.sorting_columns == (pq.SortingColumn(leaf_index, descending=False, nulls_first=True),)
            rows = parquet.read_row_group(index).to_pylist()
            values = [row["#1"] for row in rows]
            assert values == sorted(values)
            physical_rows.extend(
                (
                    (row["payload"]["x"], row["payload"]["y"]) if nested else row["payload"],
                    row["#1"],
                    partition,
                )
                for row in rows
            )
    assert Counter(physical_rows) == Counter(expected)


@pytest.mark.spark(
    skip_if_jvm=True, reason="Standalone sortBy is a Sail extension; validate footer order independently"
)
@pytest.mark.parametrize("mode", ["append", "overwrite"], ids=["append", "dynamic-overwrite"])
@pytest.mark.parametrize(
    ("partitions", "values", "keys", "sql", "row_group_size"),
    [
        (("", "__HIVE_DEFAULT_PARTITION__"), (2, 1), ("#0", "#2"), False, 3),
        ((None, ""), (2, 1), ("#0", "#2"), False, 3),
        ((None, "__HIVE_DEFAULT_PARTITION__"), (2, 1), ("#0", "#2"), False, 3),
        (("", "__HIVE_DEFAULT_PARTITION__"), (2, None), ("#0", "#2"), False, 3),
        (("", "__HIVE_DEFAULT_PARTITION__"), (2, 1), ("#1", "#0", "#2"), False, 3),
        (("", "__HIVE_DEFAULT_PARTITION__"), (2, 1), ("#2", "#0"), False, 3),
        (("", "", "__HIVE_DEFAULT_PARTITION__", "__HIVE_DEFAULT_PARTITION__"), (4, 5, 1, 2), ("#0", "#2"), False, 3),
        (("", "__HIVE_DEFAULT_PARTITION__"), (None, 1), ("#0", "#2"), True, None),
        (("", "__HIVE_DEFAULT_PARTITION__"), (2, 1), ("#1", "#0", "#2"), True, None),
    ],
    ids=[
        "empty-marker",
        "null-empty",
        "null-marker",
        "nullable-value",
        "middle-key",
        "trailing-key",
        "multiple-row-groups",
        "sql-nullable-value",
        "sql-middle-key",
    ],
)
def test_partitioned_sort_metadata_stops_at_removed_keys(
    spark, tmp_path, mode, partitions, values, keys, sql, row_group_size
):
    path = tmp_path / "data"
    schema = "`#0` STRING, `#1` INT, `#2` INT, origin STRING"
    seed = [("", 7, 90, "old"), ("untouched", 7, 80, "untouched")]
    incoming = [
        (partition, 7, value, f"incoming-{index}")
        for index, (partition, value) in enumerate(zip(partitions, values, strict=True))
    ]
    table = f"sort_metadata_{uuid4().hex}"
    view = f"sort_input_{uuid4().hex}"
    config = "spark.sql.sources.partitionOverwriteMode"
    previous = spark.conf.get(config, None)

    def write(rows, write_mode):
        frame = spark.createDataFrame(rows, schema).coalesce(1)
        if sql:
            frame.createOrReplaceTempView(view)
            operation = "OVERWRITE" if write_mode == "overwrite" else "INTO"
            spark.sql(f"INSERT {operation} {table} PARTITION(`#0`) SELECT `#1`, `#2`, origin, `#0` FROM {view}")
        else:
            writer = (
                frame.write.partitionBy("#0").sortBy(*keys).mode(write_mode).option("partitionOverwriteMode", "dynamic")
            )
            if row_group_size is not None:
                writer = writer.option("max_row_group_size", row_group_size)
            writer.parquet(str(path))

    def files():
        return {file.relative_to(path): file.read_bytes() for file in path.rglob("*.parquet")}

    try:
        spark.conf.set(config, "dynamic")
        if sql:
            (
                spark.createDataFrame([], schema)
                .write.format("parquet")
                .partitionBy("#0")
                .sortBy(*keys)
                .option("path", str(path))
                .saveAsTable(table)
            )
        write(seed, "append")
        before = files()
        assert len(before) == len(seed)
        untouched = {name: data for name, data in before.items() if name.parts[0] == "%230=untouched"}
        assert untouched
        write(incoming, mode)
        after = files()
        assert len(after.keys() - before.keys()) == 1
        assert after.items() >= untouched.items()
        if mode == "append":
            assert after.items() >= before.items()
        else:
            assert not (before.keys() - untouched.keys()) & after.keys()
        expected = (seed if mode == "append" else [seed[1]]) + incoming
        logical = [
            (None if partition in (None, "", "__HIVE_DEFAULT_PARTITION__") else partition, a, b, origin)
            for partition, a, b, origin in expected
        ]
        actual = spark.read.schema(schema).parquet(str(path)).select("#0", "#1", "#2", "origin").collect()
        assert Counter(tuple(row) for row in actual) == Counter(logical)

        prefix = keys[: keys.index("#0")]
        sorting = tuple(
            pq.SortingColumn(["#1", "#2"].index(key), descending=False, nulls_first=not sql) for key in prefix
        )
        physical = []
        incoming_groups = []
        for name in after:
            parquet = pq.ParquetFile(path / name)
            assert parquet.schema.names == ["#1", "#2", "origin"]
            assert parquet.metadata.num_row_groups > 0
            for index in range(parquet.metadata.num_row_groups):
                group = parquet.metadata.row_group(index)
                assert group.sorting_columns == sorting
                rows = parquet.read_row_group(index).to_pylist()
                physical.extend((row["#1"], row["#2"], row["origin"]) for row in rows)
                for key in prefix:
                    ordered = [row[key] for row in rows]
                    assert ordered == sorted(ordered)
                if name not in before:
                    incoming_groups.append(rows)
        assert Counter(physical) == Counter((a, b, origin) for _, a, b, origin in expected)
        assert sum(len(group) for group in incoming_groups) == len(incoming)
        if row_group_size is not None and len(incoming) > row_group_size:
            assert sorted(len(group) for group in incoming_groups) == [len(incoming) - row_group_size, row_group_size]
    finally:
        if sql:
            spark.catalog.dropTempView(view)
            spark.sql(f"DROP TABLE IF EXISTS {table}")
        if previous is None:
            spark.conf.unset(config)
        else:
            spark.conf.set(config, previous)
