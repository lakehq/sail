from collections import Counter

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
