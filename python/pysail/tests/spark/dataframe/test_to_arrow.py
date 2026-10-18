"""Arrow conversion parity for `DataFrame.toArrow()`.

These assertions cannot live in a `.feature` file: `Then query schema` compares Spark's
`treeString`, which says nothing about the Arrow type a column is materialised as. The
Spark type and the Arrow type are two separate mappings, and they can drift apart
independently — a column can have the right `treeString` and the wrong Arrow type.

Every expected value here was measured against Spark JVM 4.2.0 with
`spark.sql.session.timeZone = UTC` and `spark.sql.timeType.enabled = true`.
"""

import contextlib

import pytest

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

pytest.importorskip("pyarrow")

pytestmark = pytest.mark.skipif(pyspark_version() < (4,), reason="DataFrame.toArrow requires PySpark 4.0+")


@pytest.fixture(scope="module")
def arrow_spark(spark):
    """The session with the confs the expected values below were captured under."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    with contextlib.suppress(Exception):
        # Internal gate; its default is `Utils.isTesting`, so TIME is unreachable without it.
        spark.conf.set("spark.sql.timeType.enabled", "true")
    return spark


# (id, query, expected pyarrow type string)
#
# The temporal rows are the load-bearing ones. TIME is `time(6)` on the Spark side but
# `time64[ns]` on the Arrow side — the unit does NOT follow the declared precision, so an
# implementation that derives the Arrow unit from the Spark precision produces time64[us]
# and diverges. TIMESTAMP carries `tz=UTC` while TIMESTAMP_NTZ carries no zone at all;
# swapping those two is the single most likely mistake and both are asserted so the pair
# discriminates. INTERVAL DAY TO SECOND becomes `duration[us]`, not an Arrow interval.
ARROW_TYPES = [
    ("timestamp", "SELECT TIMESTAMP '2024-01-15 10:30:45.123456' AS c", "timestamp[us, tz=UTC]"),
    ("timestamp_ntz", "SELECT TIMESTAMP_NTZ '2024-01-15 10:30:45.123456' AS c", "timestamp[us]"),
    ("date", "SELECT DATE '2024-01-15' AS c", "date32[day]"),
    ("time", "SELECT TIME '10:30:45.123456' AS c", "time64[ns]"),
    ("to_time", "SELECT to_time('10:30:45.123456') AS c", "time64[ns]"),
    ("to_date", "SELECT to_date('2024-01-15') AS c", "date32[day]"),
    ("to_timestamp", "SELECT to_timestamp('2024-01-15 10:30:45') AS c", "timestamp[us, tz=UTC]"),
    ("to_timestamp_ntz", "SELECT to_timestamp_ntz('2024-01-15 10:30:45') AS c", "timestamp[us]"),
    ("unix_timestamp", "SELECT unix_timestamp(TIMESTAMP '2024-01-15 10:30:45') AS c", "int64"),
    ("day_time_interval", "SELECT INTERVAL '1 02:03:04' DAY TO SECOND AS c", "duration[us]"),
    ("decimal", "SELECT CAST(1.10 AS DECIMAL(3,2)) AS c", "decimal128(3, 2)"),
    ("double", "SELECT CAST('Infinity' AS DOUBLE) AS c", "double"),
    ("binary", "SELECT X'537061726B' AS c", "binary"),
    ("array_with_null", "SELECT array(1, CAST(NULL AS INT), 3) AS c", "list<element: int32>"),
    ("map", "SELECT map('k','v') AS c", "map<string, string>"),
]


@pytest.mark.parametrize(("name", "query", "expected"), ARROW_TYPES, ids=[c[0] for c in ARROW_TYPES])
def test_to_arrow_type(arrow_spark, name, query, expected):
    if name in {"time", "to_time"} and pyspark_version() < (4, 1):
        pytest.skip("TIME type requires PySpark 4.1+")
    table = arrow_spark.sql(query).toArrow()
    assert str(table.schema.field(0).type) == expected


def test_to_arrow_pre_epoch_fraction_keeps_its_sign(arrow_spark):
    """A pre-epoch fractional timestamp must survive as -500000us, not be rounded.

    This is the Arrow-side counterpart of the unix_timestamp truncation rule: the value
    itself is stored exactly, and only the *seconds* conversions round it.
    """
    table = arrow_spark.sql("SELECT TIMESTAMP '1969-12-31 23:59:59.5' AS c").toArrow()
    assert str(table.schema.field(0).type) == "timestamp[us, tz=UTC]"
    assert str(table.column(0)[0]) == "1969-12-31 23:59:59.500000+00:00"


def test_to_arrow_rejects_year_month_interval(arrow_spark):
    """INTERVAL YEAR TO MONTH has no Arrow representation and must be refused.

    The contrasting half of `day_time_interval` above, which DOES convert: a blanket
    "intervals are durations" mapping would silently produce a wrong type here instead of
    raising, so asserting only the day-time case would not discriminate.
    """
    df = arrow_spark.sql("SELECT INTERVAL '3-1' YEAR TO MONTH AS c")
    with pytest.raises(Exception, match="UNSUPPORTED_DATA_TYPE_FOR_ARROW_CONVERSION"):
        df.toArrow()


@pytest.mark.xfail(
    not is_jvm_spark(),
    reason="Sail loses the non-nullable flag on a struct field built by named_struct: "
    "Spark emits struct<a: int32 not null, b: int32>, Sail emits struct<a: int32, b: int32>. "
    "The root cause is upstream of the Arrow mapping — the literal itself is already typed "
    "nullable in Sail — so this is a field-nullability gap, not a conversion bug.",
    strict=True,
)
def test_to_arrow_struct_field_nullability(arrow_spark):
    table = arrow_spark.sql("SELECT named_struct('a',1,'b',CAST(NULL AS INT)) AS c").toArrow()
    assert str(table.schema.field(0).type) == "struct<a: int32 not null, b: int32>"


@pytest.mark.parametrize("large", [False, True], ids=["list_view", "large_list_view"])
@pytest.mark.parametrize("sliced", [False, True], ids=["full", "sliced"])
@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("array_min(items)", [30, 10, None, None, None, 10]),
        ("array_max(items)", [40, 30, None, None, None, 10]),
        ("transform(items, (x, i) -> x + i + id)", [[30, 41], [11, None, 33], None, [], [None], [15, None]]),
    ],
    ids=["min", "max", "transform"],
)
def test_array_functions_on_arrow_list_views(spark, remote, tmp_path, large, sliced, expression, expected):
    import pyarrow as pa

    array_type = pa.LargeListViewArray if large else pa.ListViewArray
    items = array_type.from_arrays(
        [2, 0, 1, 0, 4, 0],
        [2, 3, 2, 0, 1, 2],
        pa.array([10, None, 30, 40, None], type=pa.int32()),
        mask=pa.array([False, False, True, False, False, False]),
    )
    table = pa.table({"id": range(6), "items": items})
    logical_rows = [(0, [30, 40]), (1, [10, None, 30]), (2, None), (3, []), (4, [None]), (5, [10, None])]
    expected_rows = [{"id": row_id, "result": value} for row_id, value in enumerate(expected)]
    if sliced:
        table = table.slice(1, 4)
        logical_rows = logical_rows[1:5]
        expected_rows = expected_rows[1:5]

    # SQL cannot express Arrow's overlapping views or unused values behind a NULL list.
    # Spark checks equivalent logical arrays; Sail reads the original physical layout.
    if remote.startswith("local"):
        frame = spark.createDataFrame(logical_rows, "id long, items array<int>")
    else:
        path = tmp_path / "list_views.arrow"
        with path.open("wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
        frame = spark.read.format("arrow").load(str(path))

    result = frame.selectExpr("id", f"{expression} AS result").orderBy("id").toArrow()
    assert result.to_pylist() == expected_rows
