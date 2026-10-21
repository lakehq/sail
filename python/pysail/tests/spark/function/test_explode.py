import pytest

from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.mark.skip(reason="Temporarily skipped until we have a fix for the issue")
def test_explode_in_aggregate_with_group_by_id(spark):
    rows = spark.sql(
        """
        SELECT id, max(explode(sequence(id)))
        FROM range(10)
        GROUP BY id
        ORDER BY id
        """
    ).collect()
    assert len(rows) == 10  # noqa: PLR2004

    for index, row in enumerate(rows):
        assert row[0] == index
        assert row[1] is None or isinstance(row[1], int)


@pytest.mark.skip(reason="Temporarily skipped until we have a fix for the issue")
def test_explode_in_grouping_expression_projection(spark):
    rows = spark.sql(
        """
        SELECT max(id), explode(sequence(id))
        FROM range(10)
        GROUP BY explode(sequence(id))
        ORDER BY explode(sequence(id))
        """
    ).collect()
    assert rows

    group_values = [row[1] for row in rows]
    assert group_values == sorted(group_values)

    for row in rows:
        assert isinstance(row[0], int)
        assert isinstance(row[1], int)


@pytest.mark.parametrize("large", [False, True], ids=["list_view", "large_list_view"])
@pytest.mark.parametrize("sliced", [False, True], ids=["full", "sliced"])
@pytest.mark.parametrize(
    "function", ["explode", "explode_outer", "posexplode", "posexplode_outer", "inline", "inline_outer"]
)
@pytest.mark.xfail(not is_jvm_spark(), reason="Sail generators do not accept list-view inputs", strict=True)
def test_generators_on_arrow_list_views(spark, remote, tmp_path, large, sliced, function):
    import pyarrow as pa

    values = [{"a": 10, "b": "ten"}, {"a": 20, "b": None}, None, {"a": 40, "b": "forty"}, {"a": 50, "b": "fifty"}]
    struct_type = pa.struct([("a", pa.int32()), ("b", pa.string())])
    array_type = pa.LargeListViewArray if large else pa.ListViewArray
    items = array_type.from_arrays(
        [4, 1, 0, 2, 1, 4, 0],
        [1, 3, 0, 2, 2, 1, 0],
        pa.array(values, type=struct_type),
        mask=pa.array([False, False, False, True, False, False, True]),
    )
    table = pa.table({"id": list(range(7)), "items": items})
    logical_rows = [
        (0, [values[4]]),
        (1, [values[1], None, values[3]]),
        (2, []),
        (3, None),
        (4, [values[1], None]),
        (5, [values[4]]),
        (6, None),
    ]
    if sliced:
        table = table.slice(1, 5)
        logical_rows = logical_rows[1:6]

    # Spark uses equivalent logical arrays; Sail reads the original Arrow view layout.
    if remote.startswith("local"):
        frame = spark.createDataFrame(logical_rows, "id long, items array<struct<a:int,b:string>>")
    else:
        path = tmp_path / "list_views.arrow"
        with path.open("wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
        frame = spark.read.format("arrow").load(str(path))

    result = frame.selectExpr("id", f"{function}(items)")
    rows = result.collect()
    expected = []
    for row_id, elements in logical_rows:
        for position, element in enumerate(elements or []):
            row = {"id": row_id}
            if function.startswith("inline"):
                row.update(element or {"a": None, "b": None})
            else:
                if function.startswith("posexplode"):
                    row["pos"] = position
                row["col"] = element
            expected.append(row)
        if not elements and function.endswith("outer"):
            row = {"id": row_id}
            if function.startswith("inline"):
                row.update(a=None, b=None)
            else:
                if function.startswith("posexplode"):
                    row["pos"] = None
                row["col"] = None
            expected.append(row)
    assert sorted((row.asDict(recursive=True) for row in rows), key=repr) == sorted(expected, key=repr)
    assert sorted(result.toArrow().to_pylist(), key=repr) == sorted(expected, key=repr)


@pytest.mark.parametrize("large", [False, True], ids=["list_view", "large_list_view"])
@pytest.mark.xfail(not is_jvm_spark(), reason="Sail posexplode does not accept list-view inputs", strict=True)
def test_posexplode_shared_view_values_have_positions_per_row(spark, remote, tmp_path, large):
    import pyarrow as pa

    array_type = pa.LargeListViewArray if large else pa.ListViewArray
    items = array_type.from_arrays([0, 1], [3, 2], pa.array([10, 20, 30], type=pa.int32()))
    table = pa.table({"id": [0, 1], "items": items})
    if remote.startswith("local"):
        frame = spark.createDataFrame([(0, [10, 20, 30]), (1, [20, 30])], "id long, items array<int>")
    else:
        path = tmp_path / "overlapping_views.arrow"
        with path.open("wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
        frame = spark.read.format("arrow").load(str(path))

    result = frame.selectExpr("id", "posexplode(items)").orderBy("id", "pos")
    rows = result.collect()
    assert [tuple(row) for row in rows] == [(0, 0, 10), (0, 1, 20), (0, 2, 30), (1, 0, 20), (1, 1, 30)]
    assert [tuple(row.values()) for row in result.toArrow().to_pylist()] == [tuple(row) for row in rows]


@pytest.mark.parametrize("large", [False, True], ids=["list_view", "large_list_view"])
@pytest.mark.parametrize("outer_view", [False, True], ids=["list", "view"])
@pytest.mark.parametrize(
    "function", ["explode", "explode_outer", "posexplode", "posexplode_outer", "inline", "inline_outer"]
)
@pytest.mark.xfail(
    not is_jvm_spark(), reason="Sail generators do not accept or normalize nested list views", strict=True
)
def test_generators_return_ordinary_nested_lists(spark, remote, tmp_path, large, outer_view, function):
    import pyarrow as pa

    view_type = pa.LargeListViewArray if large else pa.ListViewArray
    inner = view_type.from_arrays(
        [1, 0, 0], [2, 0, 1], pa.array([10, 20, None], type=pa.int32()), mask=pa.array([False, False, True])
    )
    structs = pa.StructArray.from_arrays([inner], names=["values"], mask=pa.array([False, False, True]))
    mask = pa.array([False, False, False, True])
    if outer_view:
        items = view_type.from_arrays([0, 1, 0, 0], [3, 2, 0, 1], structs, mask=mask)
    else:
        list_type = pa.LargeListArray if large else pa.ListArray
        items = list_type.from_arrays([0, 3, 5, 5, 6], structs.take([0, 1, 2, 1, 2, 0]), mask=mask)
    logical_rows = [
        (0, [{"values": [20, None]}, {"values": []}, None]),
        (1, [{"values": []}, None]),
        (2, []),
        (3, None),
    ]
    if remote.startswith("local"):
        frame = spark.createDataFrame(logical_rows, "id long, items array<struct<values:array<int>>>")
    else:
        table = pa.table({"id": range(4), "items": items})
        path = tmp_path / "nested_generator_views.arrow"
        with path.open("wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
        frame = spark.read.format("arrow").load(str(path))

    expected = []
    for row_id, elements in logical_rows:
        entries = list(enumerate(elements or []))
        if not entries and function.endswith("outer"):
            entries = [(None, None)]
        for position, value in entries:
            row = {"id": row_id}
            if function.startswith("inline"):
                row.update(value or {"values": None})
            else:
                if function.startswith("posexplode"):
                    row["pos"] = position
                row["col"] = value
            expected.append(row)
    result = frame.selectExpr("id", f"{function}(items)")
    assert sorted([row.asDict(recursive=True) for row in result.collect()], key=repr) == sorted(expected, key=repr)
    table = result.toArrow()
    table.validate(full=True)
    assert sorted(table.to_pylist(), key=repr) == sorted(expected, key=repr)


@pytest.mark.parametrize("large", [False, True], ids=["list_view", "large_list_view"])
@pytest.mark.parametrize("function", ["explode", "explode_outer", "posexplode", "posexplode_outer"])
@pytest.mark.xfail(
    not is_jvm_spark(), reason="Sail returns map values as list views that PySpark cannot convert", strict=True
)
def test_map_generators_return_ordinary_list_values(spark, remote, tmp_path, large, function):
    import pyarrow as pa

    view_type = pa.LargeListViewArray if large else pa.ListViewArray
    values = view_type.from_arrays(
        [1, 0, 0], [2, 0, 1], pa.array([10, 20, None], type=pa.int32()), mask=pa.array([False, False, True])
    )
    items = pa.MapArray.from_arrays(
        [0, 3, 3, 3], pa.array(["a", "b", "c"]), values, mask=pa.array([False, False, True])
    )
    if remote.startswith("local"):
        frame = spark.createDataFrame(
            [(0, {"a": [20, None], "b": [], "c": None}), (1, {}), (2, None)], "id long, items map<string,array<int>>"
        )
    else:
        table = pa.table({"id": range(3), "items": items})
        path = tmp_path / "map_views.arrow"
        with path.open("wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
        frame = spark.read.format("arrow").load(str(path))
    expected = [{"id": 0, "key": key, "value": value} for key, value in [("a", [20, None]), ("b", []), ("c", None)]]
    if function.startswith("posexplode"):
        expected = [dict(row, pos=i) for i, row in enumerate(expected)]
    if function.endswith("outer"):
        for i in [1, 2]:
            row = {"id": i, "key": None, "value": None}
            if function.startswith("posexplode"):
                row["pos"] = None
            expected.append(row)
    result = frame.selectExpr("id", f"{function}(items)").orderBy("id", "key")
    assert [row.asDict(recursive=True) for row in result.collect()] == expected
    table = result.toArrow()
    table.validate(full=True)
    assert table.to_pylist() == expected


@pytest.mark.parametrize("function", ["explode", "explode_outer", "posexplode", "posexplode_outer"])
@pytest.mark.parametrize("dynamic", [False, True])
def test_map_generators_preserve_interval_fields(spark, function, dynamic):
    from datetime import timedelta

    key = "id" if dynamic else "CAST(1 AS BIGINT)"
    source = spark.range(2).selectExpr(
        f"map_from_entries(CAST(array(struct({key}, INTERVAL '1' DAY)) AS ARRAY<STRUCT<k:BIGINT,v:INTERVAL DAY>>)) AS m"
    )
    result = source.selectExpr(f"{function}(m)").orderBy("key")
    expected = [(i if dynamic else 1, timedelta(days=1)) for i in range(2)]
    assert [(row.key, row.value) for row in result.collect()] == expected
    assert result.schema["value"].dataType.simpleString() == "interval day"
