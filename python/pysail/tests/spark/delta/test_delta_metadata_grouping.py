import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from pysail.testing.spark.steps.plan import normalize_plan_text


def _write_partitioned_table(path, files, mapping_mode="none", p_type=None):
    path.mkdir()
    names = ["p", "q", "v"]
    physical_names = names if mapping_mode == "none" else [f"col-{name}" for name in names]
    fields = []
    for index, (name, dtype) in enumerate(zip(names, [p_type or StringType(), IntegerType(), LongType()], strict=True)):
        metadata = (
            {}
            if mapping_mode == "none"
            else {"delta.columnMapping.id": index + 1, "delta.columnMapping.physicalName": physical_names[index]}
        )
        fields.append(StructField(name, dtype, metadata=metadata))
    configuration = (
        {}
        if mapping_mode == "none"
        else {"delta.columnMapping.mode": mapping_mode, "delta.columnMapping.maxColumnId": "3"}
    )
    actions = [
        {"protocol": {"minReaderVersion": 1 if mapping_mode == "none" else 2, "minWriterVersion": 5}},
        {
            "metaData": {
                "id": path.name,
                "format": {"provider": "parquet", "options": {}},
                "schemaString": StructType(fields).json(),
                "partitionColumns": ["p", "q"],
                "configuration": configuration,
                "createdTime": 0,
            }
        },
    ]
    known_files = []
    arrow_field = pa.field(
        physical_names[2], pa.int64(), metadata=None if mapping_mode == "none" else {"PARQUET:field_id": "3"}
    )
    for index, (p, q, values, known_stats) in enumerate(files):
        data_file = path / f"part-{index}.parquet"
        table = pa.Table.from_arrays([pa.array(values, pa.int64())], schema=pa.schema([arrow_field]))
        pq.write_table(table, data_file)
        add = {
            "path": data_file.name,
            "partitionValues": {
                physical_names[0]: None if p is None else str(p),
                physical_names[1]: None if q is None else str(q),
            },
            "size": data_file.stat().st_size,
            "modificationTime": 0,
            "dataChange": True,
        }
        if known_stats:
            non_null = [value for value in values if value is not None]
            add["stats"] = json.dumps(
                {
                    "numRecords": len(values),
                    "minValues": {physical_names[2]: min(non_null)} if non_null else {},
                    "maxValues": {physical_names[2]: max(non_null)} if non_null else {},
                    "nullCount": {physical_names[2]: len(values) - len(non_null)},
                }
            )
            known_files.append(data_file)
        actions.append({"add": add})
    log_dir = path / "_delta_log"
    log_dir.mkdir()
    (log_dir / "00000000000000000000.json").write_text("".join(json.dumps(action) + "\n" for action in actions))
    return known_files


@pytest.mark.parametrize("metadata_as_data", ["false", "true"])
@pytest.mark.parametrize("mapping_mode", ["none", "name", "id"])
def test_metadata_grouping_reads_only_residual_files(spark, tmp_path, metadata_as_data, mapping_mode):
    path = tmp_path / "metadata_groups"
    hive_marker = "__HIVE_DEFAULT_PARTITION__"
    known_files = _write_partitioned_table(
        path,
        [
            ("a", 1, [7] * 10_000, True),
            ("a", 1, [8, 9], False),
            (None, 2, [7] * 3, True),
            ("empty", 3, [], True),
            ("empty-unknown", 3, [], False),
            ("a", None, [5] * 2, True),
            (hive_marker, 4, [7] * 3, True),
            (hive_marker, 4, [8, 9], False),
        ],
        mapping_mode,
    )
    # Metadata queries must succeed without opening any eligible data file.
    for data_file in known_files:
        data_file.unlink()
    frame = spark.read.format("delta").option("metadataAsDataRead", metadata_as_data).load(str(path))
    assert frame.groupBy("p", "q").count().orderBy("p", "q").collect() == [
        Row(p=None, q=2, count=3),
        Row(p=hive_marker, q=4, count=5),
        Row(p="a", q=None, count=2),
        Row(p="a", q=1, count=10_002),
    ]
    assert frame.select("q", "p").distinct().orderBy("p", "q").collect() == [
        Row(q=2, p=None),
        Row(q=4, p=hive_marker),
        Row(q=None, p="a"),
        Row(q=1, p="a"),
    ]
    assert frame.groupBy("v").count().orderBy("v").collect() == [Row(5, 2), Row(7, 10_006), Row(8, 2), Row(9, 2)]
    assert frame.count() == 10_012  # noqa: PLR2004
    frame.selectExpr("q AS second", "p AS first").createOrReplaceTempView("metadata_group_aliases")
    try:
        assert spark.sql(
            "SELECT first, second, COUNT(*) AS n FROM metadata_group_aliases GROUP BY first, second ORDER BY first, second"
        ).collect() == [Row(None, 2, 3), Row(hive_marker, 4, 5), Row("a", None, 2), Row("a", 1, 10_002)]
    finally:
        spark.catalog.dropTempView("metadata_group_aliases")


@pytest.mark.yamlsnapshot(group="plan")
@pytest.mark.parametrize("metadata_as_data", ["false", "true"])
@pytest.mark.parametrize("files", [[], [("a", 1, [], True)], [("a", 1, [], False)]])
def test_metadata_grouping_empty_inputs(spark, tmp_path, snapshot, metadata_as_data, files):
    path = tmp_path / "empty_metadata_groups"
    for data_file in _write_partitioned_table(path, files):
        data_file.unlink()
    frame = spark.read.format("delta").option("metadataAsDataRead", metadata_as_data).load(str(path))
    assert frame.count() == 0
    assert frame.groupBy("p").count().collect() == []
    assert frame.select("p").distinct().collect() == []
    assert {
        "count": normalize_plan_text(frame.selectExpr("COUNT(*)")._explain_string()),  # noqa: SLF001
        "group": normalize_plan_text(frame.groupBy("p").count()._explain_string()),  # noqa: SLF001
        "distinct": normalize_plan_text(frame.select("p").distinct()._explain_string()),  # noqa: SLF001
    } == snapshot


@pytest.mark.yamlsnapshot(group="plan")
@pytest.mark.parametrize("metadata_as_data", ["false", "true"])
def test_metadata_grouping_single_compressed_file(spark, tmp_path, snapshot, metadata_as_data):
    path = tmp_path / "single_metadata_group"
    rows = 100_000
    for data_file in _write_partitioned_table(path, [("x" * 50, 1, [7] * rows, True)]):
        data_file.unlink()
    frame = spark.read.format("delta").option("metadataAsDataRead", metadata_as_data).load(str(path))
    assert frame.groupBy("p").count().collect() == [Row("x" * 50, rows)]
    assert frame.select("p").distinct().collect() == [Row("x" * 50)]
    assert {
        "group": normalize_plan_text(frame.groupBy("p").count()._explain_string()),  # noqa: SLF001
        "distinct": normalize_plan_text(frame.select("p").distinct()._explain_string()),  # noqa: SLF001
    } == snapshot


@pytest.mark.yamlsnapshot(group="plan")
@pytest.mark.parametrize("metadata_as_data", ["false", "true"])
def test_metadata_grouping_residual_and_fallback_plans(spark, tmp_path, snapshot, metadata_as_data):
    path = tmp_path / "metadata_plan_inputs"
    _write_partitioned_table(
        path,
        [("a", 1, [7] * 10_000, True), ("a", 1, [None, 8, 9], False), ("b", 2, [7], True), (None, 3, [5], True)],
    )
    frame = spark.read.format("delta").option("metadataAsDataRead", metadata_as_data).load(str(path))
    frame.createOrReplaceTempView("metadata_plan_inputs")
    queries = {
        "global_count": ("SELECT COUNT(*) FROM metadata_plan_inputs", [(10_005,)]),
        "partition_group": (
            "SELECT p, q, COUNT(*) FROM metadata_plan_inputs GROUP BY p, q",
            [(None, 3, 1), ("a", 1, 10_003), ("b", 2, 1)],
        ),
        "partition_distinct": ("SELECT DISTINCT q, p FROM metadata_plan_inputs", [(3, None), (1, "a"), (2, "b")]),
        "data_group": (
            "SELECT v, COUNT(*) FROM metadata_plan_inputs GROUP BY v",
            [(None, 1), (5, 1), (7, 10_001), (8, 1), (9, 1)],
        ),
        "filtered_count": ("SELECT COUNT(*) FROM metadata_plan_inputs WHERE p = 'a' OR p IS NULL", [(10_004,)]),
        "filtered_group": (
            "SELECT p, COUNT(*) FROM metadata_plan_inputs WHERE p = 'a' OR p IS NULL GROUP BY p",
            [(None, 1), ("a", 10_003)],
        ),
        "filtered_distinct": (
            "SELECT DISTINCT p FROM metadata_plan_inputs WHERE p = 'a' OR p IS NULL",
            [(None,), ("a",)],
        ),
        "data_filter_group": ("SELECT p, COUNT(*) FROM metadata_plan_inputs WHERE v > 7 GROUP BY p", [("a", 2)]),
        "data_filter_distinct": ("SELECT DISTINCT p FROM metadata_plan_inputs WHERE v > 7", [("a",)]),
        "nullable_count": ("SELECT COUNT(v) FROM metadata_plan_inputs", [(10_004,)]),
        "limited_group": (
            "SELECT p, COUNT(*) FROM (SELECT p FROM metadata_plan_inputs WHERE p = 'a' LIMIT 2) GROUP BY p",
            [("a", 2)],
        ),
        "data_distinct": ("SELECT DISTINCT v FROM metadata_plan_inputs", [(None,), (5,), (7,), (8,), (9,)]),
    }
    plans = {}
    try:
        for name, (query, expected) in queries.items():
            result = spark.sql(query)
            assert sorted(map(tuple, result.collect()), key=repr) == sorted(expected, key=repr), name
            plans[name] = normalize_plan_text(result._explain_string())  # noqa: SLF001
        assert plans == snapshot
    finally:
        spark.catalog.dropTempView("metadata_plan_inputs")


@pytest.mark.yamlsnapshot(group="plan")
@pytest.mark.parametrize("metadata_as_data", ["false", "true"])
def test_metadata_grouping_missing_row_count_plans(spark, tmp_path, snapshot, metadata_as_data):
    path = tmp_path / "missing_row_count_plans"
    _write_partitioned_table(path, [("a", 1, [1, 2], False)])
    log = path / "_delta_log" / "00000000000000000000.json"
    actions = [json.loads(line) for line in log.read_text().splitlines()]
    actions[-1]["add"]["stats"] = json.dumps({"minValues": {"v": 1}, "maxValues": {"v": 2}})
    log.write_text("".join(json.dumps(action) + "\n" for action in actions))
    frame = spark.read.format("delta").option("metadataAsDataRead", metadata_as_data).load(str(path))
    queries = {
        "count": (frame.selectExpr("COUNT(*)"), [Row(2)]),
        "group": (frame.groupBy("p").count(), [Row("a", 2)]),
        "filtered_group": (frame.where("v > 1").groupBy("p").count(), [Row("a", 1)]),
        "extrema": (frame.selectExpr("MIN(v)", "MAX(v)"), [Row(1, 2)]),
    }
    plans = {}
    for name, (query, expected) in queries.items():
        assert query.collect() == expected, name
        plans[name] = normalize_plan_text(query._explain_string())  # noqa: SLF001
    assert plans == snapshot


@pytest.mark.parametrize("metadata_as_data", ["false", "true"])
def test_metadata_grouping_preserves_filters_limits_and_nullable_counts(spark, tmp_path, metadata_as_data):
    path = tmp_path / "filtered_metadata_groups"
    _write_partitioned_table(path, [("a", 1, [None, 1, 2], True), ("b", 2, [3, 4], True)])
    frame = spark.read.format("delta").option("metadataAsDataRead", metadata_as_data).load(str(path))
    assert frame.where("v > 1").groupBy("p").count().orderBy("p").collect() == [Row("a", 1), Row("b", 2)]
    assert frame.where("v > 1").select("p").distinct().orderBy("p").collect() == [Row("a"), Row("b")]
    assert frame.where("p = 'a'").count() == 3  # noqa: PLR2004
    assert sum(row[1] for row in frame.limit(2).groupBy("p").count().collect()) == 2  # noqa: PLR2004
    assert frame.selectExpr("COUNT(v)").collect() == [Row(4)]
    assert frame.select("v").distinct().orderBy("v").collect() == [Row(None), Row(1), Row(2), Row(3), Row(4)]


@pytest.mark.yamlsnapshot(group="plan")
@pytest.mark.parametrize("mapping_mode", ["none", "name", "id"])
def test_partition_filtered_exact_aggregates_do_not_read_files(spark, tmp_path, snapshot, mapping_mode):
    path = tmp_path / "partition_filtered_aggregates"
    for data_file in _write_partitioned_table(
        path, [("a", 1, [None, 2, 4], True), ("b", 2, [7, 8], True), (None, 3, [9], True)], mapping_mode
    ):
        data_file.unlink()
    frame = spark.read.format("delta").load(str(path))
    frame.selectExpr("p AS part", "q AS key", "v AS value").createOrReplaceTempView("filtered_metadata_alias")
    queries = {
        "filtered": (
            "SELECT COUNT(*), COUNT(value), MIN(value), MAX(value) "
            "FROM filtered_metadata_alias WHERE part = 'a' OR part IS NULL",
            [Row(4, 3, 2, 9)],
        ),
        "contradiction": (
            "SELECT COUNT(*), MIN(value), MAX(value) FROM filtered_metadata_alias WHERE key > key",
            [Row(0, None, None)],
        ),
        "absent_partition": (
            "SELECT COUNT(*), MIN(value), MAX(value) FROM filtered_metadata_alias WHERE part = 'absent'",
            [Row(0, None, None)],
        ),
    }
    plans = {}
    try:
        for name, (query, expected) in queries.items():
            result = spark.sql(query)
            assert result.collect() == expected, name
            plans[name] = normalize_plan_text(result._explain_string())  # noqa: SLF001
    finally:
        spark.catalog.dropTempView("filtered_metadata_alias")
    # Spark Connect 3.5's isEmpty() retains columns unless explicitly projected away.
    assert frame.where("p = 'a'").select().isEmpty() is False
    assert frame.where("p = 'absent'").select().isEmpty() is True
    assert frame.where("q IN (1, 3)").select().limit(2).collect() == [(), ()]
    assert plans == snapshot


@pytest.mark.parametrize("metadata_as_data", ["false", "true"])
@pytest.mark.parametrize("mapping_mode", ["none", "name", "id"])
def test_partition_filtered_grouping_reads_only_residual_files(spark, tmp_path, metadata_as_data, mapping_mode):
    path = tmp_path / "partition_filtered_groups"
    for data_file in _write_partitioned_table(
        path,
        [("a", 1, [2] * 10_000, True), ("a", 1, [3, 4], False), ("b", 2, [5], True), (None, 3, [6], True)],
        mapping_mode,
    ):
        data_file.unlink()
    frame = spark.read.format("delta").option("metadataAsDataRead", metadata_as_data).load(str(path))
    selected = frame.where("p = 'a' OR p IS NULL")
    assert selected.groupBy("p").count().orderBy("p").collect() == [Row(None, 1), Row("a", 10_002)]
    assert selected.select("p").distinct().orderBy("p").collect() == [Row(None), Row("a")]
    assert selected.count() == 10_003  # noqa: PLR2004
    assert frame.where("q > q").groupBy("p").count().collect() == []
    assert frame.where("q > q").count() == 0


@pytest.mark.parametrize("metadata_as_data", ["false", "true"])
def test_metadata_partition_filters_compare_columns_exactly(spark, tmp_path, metadata_as_data):
    path = tmp_path / "partition_column_comparison"
    (
        spark.createDataFrame([(2, 10, 10), (10, 2, 20), (3, 2, 21), (None, 2, 30)], "p INT, q INT, v INT")
        .repartition(1)
        .write.format("delta")
        .partitionBy("p", "q")
        .save(str(path))
    )
    frame = spark.read.format("delta").option("metadataAsDataRead", metadata_as_data).load(str(path))
    assert sorted(row.v for row in frame.where("p > q").limit(2).collect()) == [20, 21]
    for data_file in path.rglob("*.parquet"):
        data_file.unlink()
    assert frame.where("p > q").count() == 2  # noqa: PLR2004
    assert frame.where("NOT (p <= q)").groupBy("p").count().orderBy("p").collect() == [Row(3, 1), Row(10, 1)]


@pytest.mark.yamlsnapshot(group="plan")
@pytest.mark.parametrize("metadata_as_data", ["false", "true"])
@pytest.mark.parametrize("mapping_mode", ["none", "name", "id"])
def test_partition_filters_use_requested_schema(spark, tmp_path, snapshot, metadata_as_data, mapping_mode):
    path = tmp_path / "partition_schema_override"
    _write_partitioned_table(
        path,
        [(1, 2, [1], True), (2, 10, [2], True), (3, 2, [3], True), (10, 2, [10], True)],
        mapping_mode,
        p_type=IntegerType(),
    )
    frame = (
        spark.read.format("delta")
        .option("metadataAsDataRead", metadata_as_data)
        .schema("p STRING, q INT, v BIGINT")
        .load(str(path))
    )
    for predicate, expected in [("p < '2'", [1, 10]), ("p < '20'", [1, 2, 10]), ("p > '20'", [3])]:
        selected = frame.where(predicate)
        assert [row.v for row in selected.orderBy("v").collect()] == expected
        assert selected.count() == len(expected)
    assert frame.where("p < 'abc'").count() == 4  # noqa: PLR2004
    selected = frame.where("p < '2'")
    assert {
        "rows": normalize_plan_text(selected._explain_string()),  # noqa: SLF001
        "count": normalize_plan_text(selected.selectExpr("COUNT(*)")._explain_string()),  # noqa: SLF001
    } == snapshot


@pytest.mark.parametrize("metadata_as_data", ["false", "true"])
@pytest.mark.parametrize("mapping_mode", ["none", "name", "id"])
def test_partition_filters_preserve_unknown_with_residual_files(spark, tmp_path, metadata_as_data, mapping_mode):
    path = tmp_path / "unknown_partition_predicate"
    _write_partitioned_table(
        path,
        [(1, 2, [1], True), (2, 10, [2], False), (10, 2, [10], True), (None, 2, [3], False)],
        mapping_mode,
        p_type=IntegerType(),
    )
    frame = spark.read.format("delta").option("metadataAsDataRead", metadata_as_data).load(str(path))
    for predicate in ["p NOT IN (1, NULL)", "p > q AND CAST(NULL AS BOOLEAN)"]:
        selected = frame.where(predicate)
        assert selected.collect() == []
        assert selected.select().limit(1).collect() == []
        assert selected.count() == 0
        assert selected.groupBy("p").count().collect() == []
    assert frame.where("p > q AND v > 0").collect() == [Row(p=10, q=2, v=10)]
    assert frame.where("p > q OR v = 2").orderBy("v").collect() == [Row(p=2, q=10, v=2), Row(p=10, q=2, v=10)]


@pytest.mark.yamlsnapshot(group="plan")
@pytest.mark.parametrize("metadata_as_data", ["false", "true"])
@pytest.mark.parametrize("mapping_mode", ["none", "name", "id"])
def test_empty_partition_encoding_is_null(spark, tmp_path, snapshot, metadata_as_data, mapping_mode):
    path = tmp_path / "empty_partition_encoding"
    hive_marker = "__HIVE_DEFAULT_PARTITION__"
    _write_partitioned_table(
        path,
        [
            ("", 1, [1, -1], True),
            (None, 2, [2], False),
            ("a", 3, [3], True),
            (hive_marker, 4, [4, -4], True),
            (hive_marker, 5, [5], False),
        ],
        mapping_mode,
    )
    frame = spark.read.format("delta").option("metadataAsDataRead", metadata_as_data).load(str(path))
    assert frame.select("p", "v").orderBy("v").collect() == [
        Row(hive_marker, -4),
        Row(None, -1),
        Row(None, 1),
        Row(None, 2),
        Row("a", 3),
        Row(hive_marker, 4),
        Row(hive_marker, 5),
    ]
    assert [row.v for row in frame.where("p IS NULL").orderBy("v").collect()] == [-1, 1, 2]
    assert [row.v for row in frame.where("p IS NULL AND v > 0").orderBy("v").collect()] == [1, 2]
    assert [row.v for row in frame.where("p = ''").collect()] == []
    assert frame.where("p IS NULL").groupBy("p").count().collect() == [Row(None, 3)]
    assert frame.where(frame.p == hive_marker).groupBy("p").count().collect() == [Row(hive_marker, 3)]
    assert [row.v for row in frame.where((frame.p == hive_marker) & (frame.v > 0)).orderBy("v").collect()] == [4, 5]
    assert [
        row.v
        for row in frame.where("(p IS NULL AND v > 0) OR (p = '__HIVE_DEFAULT_PARTITION__' AND v < 0)")
        .orderBy("v")
        .collect()
    ] == [-4, 1, 2]
    assert {
        "null_group": normalize_plan_text(frame.where("p IS NULL").groupBy("p").count()._explain_string()),  # noqa: SLF001
        "mixed_predicate": normalize_plan_text(
            frame.where("(p IS NULL AND v > 0) OR (p = '__HIVE_DEFAULT_PARTITION__' AND v < 0)")._explain_string()  # noqa: SLF001
        ),
    } == snapshot


@pytest.mark.yamlsnapshot(group="plan")
@pytest.mark.parametrize("mapping_mode", ["none", "name", "id"])
@pytest.mark.parametrize(
    ("predicate", "removed_files", "remaining_values"),
    [
        ("p IS NULL", {"part-1.parquet", "part-2.parquet"}, [1, 4]),
        ("p = '__HIVE_DEFAULT_PARTITION__'", {"part-0.parquet"}, [2, 3, 4]),
    ],
    ids=["null-partitions", "literal-hive-marker"],
)
def test_partition_delete_distinguishes_null_from_hive_marker(
    spark, tmp_path, snapshot, mapping_mode, predicate, removed_files, remaining_values
):
    path = tmp_path / "delete_partition_encodings"
    _write_partitioned_table(
        path,
        [("__HIVE_DEFAULT_PARTITION__", 1, [1], True), (None, 2, [2], True), ("", 3, [3], True), ("a", 4, [4], True)],
        mapping_mode,
    )
    statement = f"DELETE FROM delta.`{path}` WHERE {predicate}"  # noqa: S608
    plan = spark.sql(f"EXPLAIN {statement}").collect()[0][0]
    spark.sql(statement).collect()
    latest = sorted((path / "_delta_log").glob("*.json"))[-1]
    actions = [json.loads(line) for line in latest.read_text().splitlines()]
    assert {action["remove"]["path"] for action in actions if "remove" in action} == removed_files
    assert [row.v for row in spark.read.format("delta").load(str(path)).orderBy("v").collect()] == remaining_values
    assert normalize_plan_text(plan) == snapshot


@pytest.mark.parametrize("metadata_as_data", ["false", "true"])
@pytest.mark.parametrize("stats", [{}, {"minValues": {"v": 1}, "maxValues": {"v": 2}}, {"numRecords": None}])
def test_metadata_grouping_without_num_records_scans_file(spark, tmp_path, metadata_as_data, stats):
    path = tmp_path / "partial_file_stats"
    _write_partitioned_table(path, [("a", 1, [1, 2], False)])
    log = path / "_delta_log" / "00000000000000000000.json"
    actions = [json.loads(line) for line in log.read_text().splitlines()]
    actions[-1]["add"]["stats"] = json.dumps(stats)
    log.write_text("".join(json.dumps(action) + "\n" for action in actions))
    frame = spark.read.format("delta").option("metadataAsDataRead", metadata_as_data).load(str(path))
    assert frame.count() == 2  # noqa: PLR2004
    assert frame.groupBy("p").count().collect() == [Row("a", 2)]
    assert frame.where("v > 1").groupBy("p").count().collect() == [Row("a", 1)]
    assert frame.selectExpr("MIN(v)", "MAX(v)").collect() == [Row(1, 2)]


@pytest.mark.yamlsnapshot(group="plan")
@pytest.mark.parametrize("metadata_as_data", ["false", "true"])
def test_metadata_grouping_uses_deletion_vector_cardinality(spark, tmp_path, snapshot, metadata_as_data):
    path = tmp_path / "deleted_metadata_groups"
    (
        spark.createDataFrame([(1, "a"), (2, "a"), (3, "a"), (4, "b"), (5, "b"), (6, None)], "id INT, p STRING")
        .repartition(1)
        .write.format("delta")
        .partitionBy("p")
        .option("delta.enableDeletionVectors", "true")
        .save(str(path))
    )
    spark.sql(f"DELETE FROM delta.`{path}` WHERE id IN (1, 2, 4)")  # noqa: S608
    latest = sorted((path / "_delta_log").glob("*.json"))[-1]
    actions = [json.loads(line) for line in latest.read_text().splitlines()]
    vectors = [action["add"]["deletionVector"] for action in actions if action.get("add", {}).get("deletionVector")]
    assert sorted(vector["cardinality"] for vector in vectors) == [1, 2]
    assert spark.read.format("delta").load(str(path)).orderBy("id").collect() == [
        Row(id=3, p="a"),
        Row(id=5, p="b"),
        Row(id=6, p=None),
    ]
    limited = spark.read.format("delta").load(str(path)).limit(3)
    assert sorted(row.id for row in limited.collect()) == [3, 5, 6]
    frame = spark.read.format("delta").option("metadataAsDataRead", metadata_as_data).load(str(path))
    assert frame.groupBy("id").count().orderBy("id").collect() == [Row(3, 1), Row(5, 1), Row(6, 1)]
    plans = {
        "data_group": normalize_plan_text(frame.groupBy("id").count()._explain_string()),  # noqa: SLF001
        "limited_rows": normalize_plan_text(limited._explain_string()),  # noqa: SLF001
    }
    for data_file in path.rglob("*.parquet"):
        data_file.unlink()
    for vector_file in path.rglob("deletion_vector_*.bin"):
        vector_file.unlink()
    frame = spark.read.format("delta").option("metadataAsDataRead", metadata_as_data).load(str(path))
    assert frame.count() == 3  # noqa: PLR2004
    assert frame.groupBy("p").count().orderBy("p").collect() == [Row(None, 1), Row("a", 1), Row("b", 1)]
    assert frame.select("p").distinct().orderBy("p").collect() == [Row(None), Row("a"), Row("b")]
    assert frame.where("p = 'a'").count() == 1
    assert frame.where("p = 'a'").groupBy("p").count().collect() == [Row("a", 1)]
    plans.update(
        {
            "count": normalize_plan_text(frame.selectExpr("COUNT(*)")._explain_string()),  # noqa: SLF001
            "partition_group": normalize_plan_text(frame.groupBy("p").count()._explain_string()),  # noqa: SLF001
            "partition_distinct": normalize_plan_text(frame.select("p").distinct()._explain_string()),  # noqa: SLF001
            "filtered_count": normalize_plan_text(frame.where("p = 'a'").selectExpr("COUNT(*)")._explain_string()),  # noqa: SLF001
        }
    )
    if metadata_as_data == "false":
        assert frame.where("p = 'a'").select().isEmpty() is False
        assert frame.select().limit(4).collect() == [(), (), ()]
    assert plans == snapshot
