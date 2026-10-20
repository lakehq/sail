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


# Keep deferred view coverage active on Spark; only confirmed Sail failures are marked.
LIST_VIEW_LAMBDA_BUG = pytest.mark.xfail(
    not is_jvm_spark(), reason="Sail cannot bind these lambdas to list-view inputs", strict=True
)
LIST_VIEW_RESULT_BUG = pytest.mark.xfail(
    not is_jvm_spark(), reason="Sail returns nested list views that PySpark cannot convert", strict=True
)
NESTED_LIST_VIEW_BUG = pytest.mark.xfail(
    not is_jvm_spark(), reason="Sail cannot evaluate or return these nested list-view results", strict=True
)


@pytest.mark.parametrize("large", [False, True], ids=["list_view", "large_list_view"])
@pytest.mark.parametrize("sliced", [False, True], ids=["full", "sliced"])
@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("array_min(items)", [30, 10, None, None, None, 10]),
        ("array_max(items)", [40, 30, None, None, None, 10]),
        pytest.param(
            "transform(items, (x, i) -> x + i + id)",
            [[30, 41], [11, None, 33], None, [], [None], [15, None]],
            marks=LIST_VIEW_LAMBDA_BUG,
        ),
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

    result = frame.selectExpr("id", f"{expression} AS result").orderBy("id")
    assert [row.asDict(recursive=True) for row in result.collect()] == expected_rows
    assert result.toArrow().to_pylist() == expected_rows


@pytest.mark.parametrize("large", [False, True], ids=["list_view", "large_list_view"])
@pytest.mark.parametrize("sliced", [False, True], ids=["full", "sliced"])
@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("array_compact(items)", [[30, 40], [10, 30], None, [], [], [10]]),
        pytest.param(
            "filter(items, (x, i) -> x > 20 OR i = 1)",
            [[30, 40], [None, 30], None, [], [], [None]],
            marks=LIST_VIEW_LAMBDA_BUG,
        ),
        pytest.param("exists(items, x -> x > 25)", [True, True, None, False, None, None], marks=LIST_VIEW_LAMBDA_BUG),
        pytest.param("forall(items, x -> x > 0)", [True, None, None, True, None, None], marks=LIST_VIEW_LAMBDA_BUG),
        (
            "aggregate(items, CAST(id AS INT), (acc, x) -> acc + coalesce(x, 0), acc -> acc + 100)",
            [170, 141, None, 103, 104, 115],
        ),
        pytest.param(
            "array_sort(items, (l, r) -> CASE WHEN l IS NULL THEN IF(r IS NULL, 0, 1) "
            "WHEN r IS NULL THEN -1 ELSE r - l END)",
            [[40, 30], [30, 10, None], None, [], [None], [10, None]],
            marks=LIST_VIEW_LAMBDA_BUG,
        ),
        ("array_position(items, 30)", [1, 3, None, 0, 0, 0]),
        ("array_intersect(items, array(30, 10))", [[30], [10, 30], None, [], [], [10]]),
        pytest.param(
            "array(items)",
            [[[30, 40]], [[10, None, 30]], [None], [[]], [[None]], [[10, None]]],
            marks=LIST_VIEW_RESULT_BUG,
        ),
        (
            "arrays_zip(items, array(30, 10))",
            [
                [{"items": 30, "1": 30}, {"items": 40, "1": 10}],
                [{"items": 10, "1": 30}, {"items": None, "1": 10}, {"items": 30, "1": None}],
                None,
                [{"items": None, "1": 30}, {"items": None, "1": 10}],
                [{"items": None, "1": 30}, {"items": None, "1": 10}],
                [{"items": 10, "1": 30}, {"items": None, "1": 10}],
            ],
        ),
    ],
    ids=["compact", "filter", "exists", "forall", "aggregate", "sort", "position", "intersect", "array", "zip"],
)
def test_additional_array_functions_on_arrow_list_views(spark, remote, tmp_path, large, sliced, expression, expected):
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

    # Spark checks logical values; Sail also exercises overlapping and sliced Arrow views.
    if remote.startswith("local"):
        frame = spark.createDataFrame(logical_rows, "id long, items array<int>")
    else:
        path = tmp_path / "array_views.arrow"
        with path.open("wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
        frame = spark.read.format("arrow").load(str(path))

    # View inputs must produce ordinary lists, including nested results, for Spark clients.
    result = frame.selectExpr("id", f"{expression} AS result").orderBy("id")
    assert [row.asDict(recursive=True) for row in result.collect()] == expected_rows
    table = result.toArrow()
    table.validate(full=True)
    assert table.to_pylist() == expected_rows


@pytest.mark.parametrize("large", [False, True], ids=["list", "large_list"])
def test_array_compact_on_sliced_arrow_lists(spark, remote, tmp_path, large):
    import pyarrow as pa

    array_type = pa.LargeListArray if large else pa.ListArray
    items = array_type.from_arrays(
        [0, 1, 4, 6, 6, 7],
        pa.array([99, 10, None, 30, 40, None, 99], type=pa.int32()),
        mask=pa.array([False, False, True, False, False]),
    )
    table = pa.table({"id": range(5), "items": items})
    if remote.startswith("local"):
        frame = spark.createDataFrame(
            [(0, [99]), (1, [10, None, 30]), (2, None), (3, []), (4, [99])], "id long, items array<int>"
        )
    else:
        path = tmp_path / "sliced_lists.arrow"
        with path.open("wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
        frame = spark.read.format("arrow").load(str(path))

    query = frame.offset(1).limit(3).selectExpr("id", "array_compact(items) AS result").orderBy("id")
    result = query.toArrow()
    assert result.to_pylist() == [{"id": 1, "result": [10, 30]}, {"id": 2, "result": None}, {"id": 3, "result": []}]
    assert [row.asDict(recursive=True) for row in query.collect()] == result.to_pylist()
    assert not result.schema.field("result").type.value_field.nullable


@pytest.mark.parametrize("large", [False, True], ids=["list_view", "large_list_view"])
@pytest.mark.parametrize("outer_view", [False, True], ids=["list", "view"])
@pytest.mark.parametrize("sliced", [False, True], ids=["full", "sliced"])
@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        pytest.param(
            "array_min(items)", [[10, None, 30], [10, None, 30], None, None, [], [50]], marks=NESTED_LIST_VIEW_BUG
        ),
        pytest.param(
            "array_max(items)", [[10, None, 30], [30, 40], None, None, [50], [50]], marks=NESTED_LIST_VIEW_BUG
        ),
        pytest.param(
            "array_compact(items)",
            [[[10, None, 30]], [[30, 40], [10, None, 30]], [], None, [[], [50]], [[50]]],
            marks=NESTED_LIST_VIEW_BUG,
        ),
        pytest.param(
            "filter(items, x -> x IS NOT NULL)",
            [[[10, None, 30]], [[30, 40], [10, None, 30]], [], None, [[], [50]], [[50]]],
            marks=NESTED_LIST_VIEW_BUG,
        ),
        pytest.param(
            "filter(items, x -> true)",
            [[[10, None, 30], None], [[30, 40], [10, None, 30]], [], None, [[], [50]], [[50]]],
            marks=NESTED_LIST_VIEW_BUG,
        ),
        pytest.param("filter(items, x -> false)", [[], [], [], None, [], []], marks=NESTED_LIST_VIEW_BUG),
        pytest.param(
            "transform(items, x -> x)",
            [[[10, None, 30], None], [[30, 40], [10, None, 30]], [], None, [[], [50]], [[50]]],
            marks=NESTED_LIST_VIEW_BUG,
        ),
        pytest.param(
            "array_intersect(items, items)",
            [[[10, None, 30], None], [[30, 40], [10, None, 30]], [], None, [[], [50]], [[50]]],
            marks=NESTED_LIST_VIEW_BUG,
        ),
        ("array_position(items, seed)", [0, 2, None, None, 2, 0]),
        pytest.param(
            "aggregate(items, seed, (acc, x) -> x)",
            [None, [10, None, 30], None, None, [50], [50]],
            marks=NESTED_LIST_VIEW_BUG,
        ),
        pytest.param(
            "aggregate(items, seed, (acc, x) -> acc)",
            [[30, 40], [10, None, 30], None, None, [50], [30, 40]],
            marks=NESTED_LIST_VIEW_BUG,
        ),
        pytest.param(
            "aggregate(items, 0, (acc, x) -> acc + 1, acc -> seed)",
            [[30, 40], [10, None, 30], None, None, [50], [30, 40]],
            marks=NESTED_LIST_VIEW_BUG,
        ),
        pytest.param(
            "array_sort(items, (l, r) -> CASE WHEN l IS NULL THEN IF(r IS NULL, 0, 1) "
            "WHEN r IS NULL THEN -1 ELSE size(r) - size(l) END)",
            [[[10, None, 30], None], [[10, None, 30], [30, 40]], [], None, [[50], []], [[50]]],
            marks=NESTED_LIST_VIEW_BUG,
        ),
        pytest.param(
            "arrays_zip(items)",
            [
                [{"items": [10, None, 30]}, {"items": None}],
                [{"items": [30, 40]}, {"items": [10, None, 30]}],
                [],
                None,
                [{"items": []}, {"items": [50]}],
                [{"items": [50]}],
            ],
            marks=NESTED_LIST_VIEW_BUG,
        ),
        pytest.param(
            "array(items)",
            [[[[10, None, 30], None]], [[[30, 40], [10, None, 30]]], [[]], [None], [[[], [50]]], [[[50]]]],
            marks=NESTED_LIST_VIEW_BUG,
        ),
    ],
    ids=[
        "min",
        "max",
        "compact",
        "filter",
        "filter_true",
        "filter_false",
        "transform",
        "intersect",
        "position",
        "merge",
        "zero",
        "finish",
        "sort",
        "zip",
        "array",
    ],
)
def test_array_functions_return_ordinary_nested_lists(
    spark, remote, tmp_path, large, outer_view, sliced, expression, expected
):
    import pyarrow as pa

    view_type = pa.LargeListViewArray if large else pa.ListViewArray
    inner = view_type.from_arrays(
        [2, 0, 1, 0, 4],
        [2, 3, 2, 0, 1],
        pa.array([10, None, 30, 40, 50], type=pa.int32()),
        mask=pa.array([False, False, True, False, False]),
    )
    mask = pa.array([False, False, False, True, False, False])
    if outer_view:
        items = view_type.from_arrays([1, 0, 0, 2, 3, 4], [2, 2, 0, 2, 2, 1], inner, mask=mask)
    else:
        list_type = pa.LargeListArray if large else pa.ListArray
        items = list_type.from_arrays([0, 2, 4, 4, 6, 8, 9], inner.take([1, 2, 0, 1, 2, 3, 3, 4, 4]), mask=mask)
    seed = inner.take([0, 1, 2, 3, 4, 0])
    table = pa.table({"id": range(6), "items": items, "seed": seed})
    logical_rows = [
        (0, [[10, None, 30], None], [30, 40]),
        (1, [[30, 40], [10, None, 30]], [10, None, 30]),
        (2, [], None),
        (3, None, []),
        (4, [[], [50]], [50]),
        (5, [[50]], [30, 40]),
    ]
    expected_rows = [{"id": row_id, "result": value} for row_id, value in enumerate(expected)]
    if sliced:
        table = table.slice(1, 4)
        logical_rows = logical_rows[1:5]
        expected_rows = expected_rows[1:5]
    if remote.startswith("local"):
        frame = spark.createDataFrame(logical_rows, "id long, items array<array<int>>, seed array<int>")
    else:
        path = tmp_path / "nested_views.arrow"
        with path.open("wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
        frame = spark.read.format("arrow").load(str(path))

    # An ordinary outer list is insufficient: nested views must also be materialized.
    result = frame.selectExpr("id", f"{expression} AS result").orderBy("id")
    assert [row.asDict(recursive=True) for row in result.collect()] == expected_rows
    table = result.toArrow()
    table.validate(full=True)
    assert table.to_pylist() == expected_rows


@pytest.mark.parametrize("large", [False, True], ids=["list_view", "large_list_view"])
@LIST_VIEW_RESULT_BUG
def test_array_results_normalize_views_inside_structs_and_maps(spark, remote, tmp_path, large):
    import pyarrow as pa

    view_type = pa.LargeListViewArray if large else pa.ListViewArray
    seed = view_type.from_arrays(
        [1, 0, 0], [2, 0, 1], pa.array([10, 20, None], type=pa.int32()), mask=pa.array([False, False, True])
    )
    if remote.startswith("local"):
        frame = spark.createDataFrame([(0, [20, None]), (1, []), (2, None)], "id long, seed array<int>")
    else:
        table = pa.table({"id": range(3), "seed": seed})
        path = tmp_path / "nested_fields.arrow"
        with path.open("wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
        frame = spark.read.format("arrow").load(str(path))
    result = frame.selectExpr(
        "id",
        "array(named_struct('values', seed)) AS structs",
        "aggregate(array(1), 0, (acc, x) -> acc, acc -> map('k', seed)) AS maps",
    ).orderBy("id")
    expected = [
        {"id": i, "structs": [{"values": value}], "maps": {"k": value}}
        for i, value in enumerate([[20, None], [], None])
    ]
    assert [row.asDict(recursive=True) for row in result.collect()] == expected
    table = result.toArrow()
    table.validate(full=True)
    assert table.to_pylist() == [dict(row, maps=list(row["maps"].items())) for row in expected]


@pytest.mark.parametrize("large", [False, True], ids=["list_view", "large_list_view"])
def test_array_view_conversion_preserves_duplicate_struct_fields(spark, remote, tmp_path, large):
    import pyarrow as pa

    rows = [(0, [1, 2], [10]), (1, [], [20, 30])]
    if remote.startswith("local"):
        frame = spark.createDataFrame(rows, "id long, a array<int>, b array<int>")
    else:
        view_type = pa.large_list_view(pa.int32()) if large else pa.list_view(pa.int32())
        table = pa.table(
            {
                "id": [row[0] for row in rows],
                "a": pa.array([row[1] for row in rows], type=view_type),
                "b": pa.array([row[2] for row in rows], type=view_type),
            }
        )
        path = tmp_path / "duplicate_fields.arrow"
        with path.open("wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
        frame = spark.read.format("arrow").load(str(path))

    result = frame.selectExpr("id", "array(named_struct('x', a, 'x', b)) AS result").orderBy("id")
    if remote.startswith("local"):
        # Spark's toArrow() rejects duplicate names; its Row output preserves them.
        assert [tuple(row.result[0]) for row in result.collect()] == [(row[1], row[2]) for row in rows]
        return

    # toArrow() rejects duplicate names, and collect() cannot convert Sail's raw
    # duplicate names through dictionaries. Inspect the transport table by position.
    table, _ = result._to_table()  # noqa: SLF001
    table.validate(full=True)
    structs = table.column("result").combine_chunks().values
    # A dict would collapse duplicate names and could hide a name-based cast
    # copying the first 'x' into both fields. Check each position separately.
    assert structs.field(0).to_pylist() == [row[1] for row in rows]
    assert structs.field(1).to_pylist() == [row[2] for row in rows]
