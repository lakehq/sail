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
def test_generators_on_arrow_list_views(spark, tmp_path, large, sliced, function):
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
    if is_jvm_spark():
        frame = spark.createDataFrame(logical_rows, "id long, items array<struct<a:int,b:string>>")
    else:
        path = tmp_path / "list_views.arrow"
        with path.open("wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
        frame = spark.read.format("arrow").load(str(path))

    rows = frame.selectExpr("id", f"{function}(items)").collect()
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
