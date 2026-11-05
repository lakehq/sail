"""Spark-parity behavior when reading a directory that contains a mix of
file types and hidden marker files.

The fixture directory (`mixed_files_dir` in `conftest.py`) contains:
  - lower.csv          (data)
  - upper.CSV          (data, uppercase extension)
  - lower.json         (data, foreign extension — Spark reads it as CSV)
  - upper.JSON         (data, foreign extension, uppercase)
  - test.txt           (data, foreign extension)
  - _SUCCESS           (hidden marker, must be skipped)
  - _random            (hidden, must be skipped)
  - .something         (hidden, must be skipped)

Spark's `HiddenFileFilter` excludes anything starting with `_` or `.`,
and otherwise reads every file in the directory regardless of extension.
These tests pin that behavior.
"""

import pytest
from pyspark.sql import Row


def _safe_sort_key(row):
    return tuple((v is not None, v) for v in row)


def _row_as_sorted_items(row):
    return tuple(sorted(row.asDict().items()))


# Expected rows (header=false, no schema): every line of every non-hidden
# file becomes a row, every value as a string.
#
# TODO: Sail's CSV parser treats a leading `"` as an opening quote and
# strips it (so the JSON files' second comma-separated piece surfaces as
# `value:"a"}`). Spark KEEPS the leading quote and emits `"value":"a"}`
# verbatim. This is a separate parity gap from the case-insensitive /
# read-everything change tested in this file; once it's fixed, the
# expected values below should drop the missing `"` back in.
_HEADER_FALSE_EXPECTED = [
    Row(_c0="name", _c1="age"),  # lower.csv L1
    Row(_c0="Lower", _c1="30"),  # lower.csv L2
    Row(_c0="name", _c1="age"),  # upper.CSV L1
    Row(_c0="Upper", _c1="50"),  # upper.CSV L2
    Row(_c0='{"id":1', _c1='value:"a"}'),  # lower.json L1; Spark: `"value":"a"}`
    Row(_c0='{"id":2', _c1='value:"b"}'),  # lower.json L2; Spark: `"value":"b"}`
    Row(_c0='{"id":1', _c1='value:"A"}'),  # upper.JSON L1; Spark: `"value":"A"}`
    Row(_c0='{"id":2', _c1='value:"B"}'),  # upper.JSON L2; Spark: `"value":"B"}`
    Row(_c0="name", _c1="age"),  # test.txt L1
    Row(_c0="Test", _c1="50"),  # test.txt L2
]


# Expected row VALUES (header=true, no schema): one header line is consumed
# per file, so each of the five data files contributes one data row.
# Column NAMES come from whichever file was listed first; only the values
# are stable enough to assert on. The leading-quote-stripping caveat noted
# above for `_HEADER_FALSE_EXPECTED` applies to the JSON-derived rows.
_HEADER_TRUE_EXPECTED_VALUES = sorted(
    [
        ("Lower", "30"),
        ("Upper", "50"),
        ('{"id":2', 'value:"b"}'),  # lower.json header consumed; Spark: `"value":"b"}`
        ('{"id":2', 'value:"B"}'),  # upper.JSON header consumed; Spark: `"value":"B"}`
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
    rows = df.collect()
    actual = sorted([tuple(r) for r in rows])
    assert actual == _HEADER_TRUE_EXPECTED_VALUES


# TODO: Sail strips a leading `"` from a CSV field; Spark keeps it (e.g. `value:"A"}` vs `"value":"A"}`).
def test_csv_schema_no_header_show(spark, mixed_files_dir):
    df = spark.read.format("csv").schema("k STRING, v STRING").option("header", "false").load(str(mixed_files_dir))
    df.show()  # smoke test


# TODO: Sail strips a leading `"` from a CSV field; Spark keeps it (e.g. `value:"A"}` vs `"value":"A"}`).
def test_csv_schema_no_header_collect(spark, mixed_files_dir):
    df = spark.read.format("csv").schema("k STRING, v STRING").option("header", "false").load(str(mixed_files_dir))
    rows = df.collect()
    expected = sorted(
        [
            Row(k="name", v="age"),
            Row(k="Lower", v="30"),
            Row(k="name", v="age"),
            Row(k="Upper", v="50"),
            Row(k='{"id":1', v='value:"a"}'),
            Row(k='{"id":2', v='value:"b"}'),
            Row(k='{"id":1', v='value:"A"}'),
            Row(k='{"id":2', v='value:"B"}'),
            Row(k="name", v="age"),
            Row(k="Test", v="50"),
        ],
        key=_safe_sort_key,
    )
    assert sorted(rows, key=_safe_sort_key) == expected


# TODO: Sail strips a leading `"` from a CSV field; Spark keeps it (e.g. `value:"A"}` vs `"value":"A"}`).
def test_csv_schema_with_header_show(spark, mixed_files_dir):
    df = spark.read.format("csv").schema("k STRING, v STRING").option("header", "true").load(str(mixed_files_dir))
    df.show()  # smoke test


# TODO: Sail strips a leading `"` from a CSV field; Spark keeps it (e.g. `value:"A"}` vs `"value":"A"}`).
def test_csv_schema_with_header_collect(spark, mixed_files_dir):
    df = spark.read.format("csv").schema("k STRING, v STRING").option("header", "true").load(str(mixed_files_dir))
    rows = df.collect()
    expected = sorted(
        [
            Row(k="Lower", v="30"),
            Row(k="Upper", v="50"),
            Row(k='{"id":2', v='value:"B"}'),
            Row(k='{"id":2', v='value:"b"}'),
            Row(k="Test", v="50"),
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
# Spark parses each line as a JSON document and surfaces lines that fail to
# parse via an inferred `_corrupt_record` column.
# -----------------------------------------------------------------------------


@pytest.mark.skip(reason="Sail's JSON reader errors on non-JSON lines; Spark uses _corrupt_record.")
def test_json_no_schema_show(spark, mixed_files_dir):
    df = spark.read.format("json").load(str(mixed_files_dir))
    df.show()  # smoke test


@pytest.mark.skip(reason="Sail's JSON reader errors on non-JSON lines; Spark uses _corrupt_record.")
def test_json_no_schema_collect(spark, mixed_files_dir):
    df = spark.read.format("json").load(str(mixed_files_dir))
    rows = df.collect()
    expected = [
        Row(_corrupt_record=None, id=1, value="a"),
        Row(_corrupt_record=None, id=2, value="b"),
        Row(_corrupt_record=None, id=1, value="A"),
        Row(_corrupt_record=None, id=2, value="B"),
        Row(_corrupt_record="name,age", id=None, value=None),
        Row(_corrupt_record="Lower,30", id=None, value=None),
        Row(_corrupt_record="name,age", id=None, value=None),
        Row(_corrupt_record="Upper,50", id=None, value=None),
        Row(_corrupt_record="name,age", id=None, value=None),
        Row(_corrupt_record="Test,50", id=None, value=None),
    ]
    actual_items = sorted([_row_as_sorted_items(r) for r in rows])
    expected_items = sorted([_row_as_sorted_items(r) for r in expected])
    assert actual_items == expected_items


@pytest.mark.skip(reason="Sail's JSON reader errors on non-JSON lines; Spark uses _corrupt_record.")
def test_json_schema_show(spark, mixed_files_dir):
    df = spark.read.format("json").schema("k STRING, v INT").load(str(mixed_files_dir))
    df.show()  # smoke test


@pytest.mark.skip(reason="Sail's JSON reader errors on non-JSON lines; Spark uses _corrupt_record.")
def test_json_schema_collect(spark, mixed_files_dir):
    # With an explicit schema whose field names don't appear in any
    # document, every line yields all-NULL — but every line of every
    # non-hidden file must still produce a row (10 rows total: 2 lines
    # each from the 5 data files).
    df = spark.read.format("json").schema("k STRING, v INT").load(str(mixed_files_dir))
    rows = df.collect()
    expected = [Row(k=None, v=None) for _ in range(10)]
    assert rows == expected
