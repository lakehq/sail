"""Spark-parity behavior when reading a directory that contains a mix of
file types and hidden marker files.

The fixture directory (`mixed_files_dir` in `conftest.py`) contains:
  - lower.csv          (data)
  - upper.CSV          (data, uppercase extension)
  - lower.json         (data, foreign extension — Spark reads it as CSV)
  - test.txt           (data, foreign extension)
  - _SUCCESS           (hidden marker, must be skipped)
  - _random            (hidden, must be skipped)
  - .something         (hidden, must be skipped)

Spark's `HiddenFileFilter` excludes anything starting with `_` or `.`,
and otherwise reads every file in the directory regardless of extension.
These tests pin that behavior.
"""

from pyspark.sql import Row


def _safe_sort_key(row):
    return tuple((v is not None, v) for v in row)


# Expected rows (header=false): every line of every non-hidden file becomes a row.
# CSV column count is determined by the file with the most fields (here, all
# files have at most 2 fields, so the schema is `_c0, _c1`).
_HEADER_FALSE_EXPECTED = [
    Row(_c0="name", _c1="age"),  # lower.csv header line
    Row(_c0="Lower", _c1="30"),
    Row(_c0="name", _c1="age"),  # upper.CSV header line
    Row(_c0="Upper", _c1="50"),
    Row(_c0='{"id":1', _c1='"value":"a"}'),  # lower.json line 1, comma-split
    Row(_c0='{"id":2', _c1='"value":"b"}'),  # lower.json line 2
    Row(_c0="name", _c1="age"),  # test.txt header line
    Row(_c0="Test", _c1="50"),
]


# Expected rows (header=true): one header line is consumed per file, so each
# of the four data files contributes one data row.
_HEADER_TRUE_EXPECTED_VALUES = sorted(
    [
        ("Lower", "30"),
        ("Upper", "50"),
        ('{"id":2', '"value":"b"}'),  # lower.json data line; first line was treated as header
        ("Test", "50"),
    ]
)


def test_csv_no_schema_no_header_show(spark, mixed_files_dir):
    df = spark.read.format("csv").option("header", "false").load(str(mixed_files_dir))
    df.show()  # smoke test


def test_csv_no_schema_no_header_collect(spark, mixed_files_dir):
    df = spark.read.format("csv").option("header", "false").load(str(mixed_files_dir))
    rows = df.collect()
    assert sorted(rows, key=_safe_sort_key) == sorted(_HEADER_FALSE_EXPECTED, key=_safe_sort_key)


def test_csv_no_schema_with_header_show(spark, mixed_files_dir):
    df = spark.read.format("csv").option("header", "true").load(str(mixed_files_dir))
    df.show()  # smoke test


def test_csv_no_schema_with_header_collect(spark, mixed_files_dir):
    df = spark.read.format("csv").option("header", "true").load(str(mixed_files_dir))
    # Column names come from whichever file was listed first; order is
    # not deterministic. Compare the row VALUES instead, sorted.
    rows = df.collect()
    actual = sorted([tuple(r) for r in rows])
    assert actual == _HEADER_TRUE_EXPECTED_VALUES


def test_csv_schema_no_header_show(spark, mixed_files_dir):
    df = spark.read.format("csv").schema("k STRING, v INT").option("header", "false").load(str(mixed_files_dir))
    df.show()  # smoke test


def test_csv_schema_no_header_collect(spark, mixed_files_dir):
    df = spark.read.format("csv").schema("k STRING, v INT").option("header", "false").load(str(mixed_files_dir))
    rows = df.collect()
    # `v` casts to INT — every header line and the JSON content fail to cast
    # and become NULL. Numeric data lines parse cleanly.
    expected = sorted(
        [
            Row(k="name", v=None),
            Row(k="Lower", v=30),
            Row(k="name", v=None),
            Row(k="Upper", v=50),
            Row(k='{"id":1', v=None),
            Row(k='{"id":2', v=None),
            Row(k="name", v=None),
            Row(k="Test", v=50),
        ],
        key=_safe_sort_key,
    )
    assert sorted(rows, key=_safe_sort_key) == expected


def test_csv_schema_with_header_show(spark, mixed_files_dir):
    df = spark.read.format("csv").schema("k STRING, v INT").option("header", "true").load(str(mixed_files_dir))
    df.show()  # smoke test


def test_csv_schema_with_header_collect(spark, mixed_files_dir):
    df = spark.read.format("csv").schema("k STRING, v INT").option("header", "true").load(str(mixed_files_dir))
    rows = df.collect()
    # Header line is consumed in every file. The JSON file's first line is
    # treated as a "header" and discarded; its second line `{"id":2,"value":"b"}`
    # parses to ('{"id":2', None) under the `k STRING, v INT` schema since
    # `"value":"b"}` cannot cast to INT.
    expected = sorted(
        [
            Row(k="Lower", v=30),
            Row(k="Upper", v=50),
            Row(k='{"id":2', v=None),
            Row(k="Test", v=50),
        ],
        key=_safe_sort_key,
    )
    assert sorted(rows, key=_safe_sort_key) == expected


def test_hidden_files_are_excluded(spark, mixed_files_dir):
    # Even though `_SUCCESS`, `_random`, and `.something` exist in the
    # directory, the listing must skip them. `_random` and `.something`
    # contain the literal string "garbage" — assert that string never
    # appears in any read row.
    df = spark.read.format("csv").option("header", "false").load(str(mixed_files_dir))
    rows = df.collect()
    flat = {value for row in rows for value in row if value is not None}
    assert "garbage" not in flat


# -----------------------------------------------------------------------------
# JSON reader on the same heterogeneous directory.
#
# Spark's JSON reader parses each line of every non-hidden file as a JSON
# document. Lines from `lower.json` are valid; lines from CSV / TXT files
# fail to parse and surface in the inferred `_corrupt_record` column.
# -----------------------------------------------------------------------------


def _row_as_sorted_items(row):
    return tuple(sorted(row.asDict().items()))


def test_json_no_schema_show(spark, mixed_files_dir):
    df = spark.read.format("json").load(str(mixed_files_dir))
    df.show()  # smoke test


def test_json_no_schema_collect(spark, mixed_files_dir):
    df = spark.read.format("json").load(str(mixed_files_dir))
    rows = df.collect()
    expected = sorted(
        [
            Row(_corrupt_record=None, id=1, value="a"),
            Row(_corrupt_record=None, id=2, value="b"),
            Row(_corrupt_record="name,age", id=None, value=None),
            Row(_corrupt_record="Lower,30", id=None, value=None),
            Row(_corrupt_record="name,age", id=None, value=None),
            Row(_corrupt_record="Upper,50", id=None, value=None),
            Row(_corrupt_record="name,age", id=None, value=None),
            Row(_corrupt_record="Test,50", id=None, value=None),
        ],
        key=_row_as_sorted_items,
    )
    actual = sorted(rows, key=_row_as_sorted_items)
    assert [_row_as_sorted_items(r) for r in actual] == [_row_as_sorted_items(r) for r in expected]


def test_json_schema_show(spark, mixed_files_dir):
    df = spark.read.format("json").schema("k STRING, v INT").load(str(mixed_files_dir))
    df.show()  # smoke test


def test_json_schema_collect(spark, mixed_files_dir):
    # An explicit schema with field names not present in any document yields
    # NULLs everywhere — but every line of every non-hidden file must still
    # contribute a row.
    df = spark.read.format("json").schema("k STRING, v INT").load(str(mixed_files_dir))
    rows = df.collect()
    expected = [Row(k=None, v=None) for _ in range(8)]
    assert rows == expected
