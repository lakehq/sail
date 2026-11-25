# SPDX-License-Identifier: Apache-2.0

import pyarrow as pa
import pytest


@pytest.mark.spark(skip_if_jvm=True, reason="Uses the Sail Arrow IPC data source")
def test_rlike_run_end_encoded_strings(spark, tmp_path):
    values = pa.RunEndEncodedArray.from_arrays([2, 3, 4], ["foo", "bar", None])
    patterns = pa.RunEndEncodedArray.from_arrays([1, 3, 4], ["oo", "ar", ".*"])
    table = pa.table({"id": range(4), "value": values, "pattern": patterns})
    path = tmp_path / "strings.arrow"
    with path.open("wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)

    frame = spark.read.format("arrow").load(str(path))
    rows = frame.selectExpr("id", "value RLIKE 'oo' AS literal_match", "regexp_like(value, pattern) AS column_match")
    assert [(row.literal_match, row.column_match) for row in rows.orderBy("id").collect()] == [
        (True, True),
        (True, False),
        (False, True),
        (None, None),
    ]
