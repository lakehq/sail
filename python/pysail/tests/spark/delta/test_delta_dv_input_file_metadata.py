# ruff: noqa: PLR2004

import json
from collections import Counter
from itertools import pairwise

import pytest

from pysail.testing.spark.session import spark_connect_server
from pysail.tests.spark.delta.utils import write_delta_with_row_groups


@pytest.fixture(scope="module")
def remote():
    with spark_connect_server(envs={"SAIL_EXECUTION__DEFAULT_PARALLELISM": "4"}) as server:
        yield server.remote


@pytest.mark.parametrize("column_mapping", ["none", "name", "id"])
def test_dv_input_file_metadata_preserves_scan_splits(spark, tmp_path, column_mapping):
    path = tmp_path / "dv_metadata_splits"
    write_delta_with_row_groups(path, column_mapping, include_stats=True, value_padding=512)
    assert (path / "part-0.parquet").stat().st_size > 1024 * 1024

    spark.sql(f"DELETE FROM delta.`{path}` WHERE id < 4096 AND id % 7 = 0")  # noqa: S608
    actions = [
        json.loads(line) for line in (path / "_delta_log" / "00000000000000000001.json").read_text().splitlines()
    ]
    assert any(action.get("add", {}).get("deletionVector", {}).get("cardinality", 0) > 0 for action in actions)
    frame = spark.read.format("delta").option("metadataAsDataRead", "false").load(str(path))
    metadata_frame = frame.selectExpr(
        "id",
        "input_file_name() AS file_name",
        "input_file_block_start() AS block_start",
        "input_file_block_length() AS block_length",
        "length(value) + input_file_block_start() AS value_with_offset",
    )
    rows = metadata_frame.orderBy("id").collect()
    file_names = [row.file_name for row in frame.selectExpr("input_file_name() AS file_name").collect()]

    live = [i for i in range(4352) if i >= 4096 or i % 7 != 0]
    assert [row.id for row in rows] == live
    assert Counter(file_names) == Counter(row.file_name for row in rows)
    ranges_by_file = {index: set() for index in range(2)}
    for row in rows:
        file_index = 0 if row.id < 4096 else 1
        assert row.file_name == (path / f"part-{file_index}.parquet").as_uri()
        assert row.value_with_offset == len(f"value-{row.id}") + 512 + row.block_start
        ranges_by_file[file_index].add((row.block_start, row.block_length))
    limited = metadata_frame.limit(3).collect()
    assert len(limited) == len({row.id for row in limited}) == 3
    for row in limited:
        assert row.id in live
        data_file = path / f"part-{0 if row.id < 4096 else 1}.parquet"
        assert row.file_name == data_file.as_uri()
        assert row.block_start >= 0
        assert row.block_length > 0
        assert row.block_start + row.block_length <= data_file.stat().st_size
        assert row.value_with_offset == len(f"value-{row.id}") + 512 + row.block_start
    assert len(ranges_by_file[0]) > 1
    for file_index, file_ranges in ranges_by_file.items():
        ranges = sorted(file_ranges)
        assert ranges[0][0] == 0
        assert all(length > 0 for _, length in ranges)
        assert all(start + length == next_start for (start, length), (next_start, _) in pairwise(ranges))
        assert sum(length for _, length in ranges) == (path / f"part-{file_index}.parquet").stat().st_size
