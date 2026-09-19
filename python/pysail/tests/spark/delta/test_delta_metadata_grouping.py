import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType


def _write_partitioned_table(path, files, mapping_mode="none"):
    path.mkdir()
    names = ["p", "q", "v"]
    physical_names = names if mapping_mode == "none" else [f"col-{name}" for name in names]
    fields = []
    for index, (name, dtype) in enumerate(zip(names, [StringType(), IntegerType(), LongType()], strict=True)):
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
            "partitionValues": {physical_names[0]: p, physical_names[1]: None if q is None else str(q)},
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
    known_files = _write_partitioned_table(
        path,
        [
            ("a", 1, [7] * 10_000, True),
            ("a", 1, [8, 9], False),
            (None, 2, [7] * 3, True),
            ("empty", 3, [], True),
            ("empty-unknown", 3, [], False),
            ("a", None, [5] * 2, True),
        ],
        mapping_mode,
    )
    # Metadata queries must succeed without opening any eligible data file.
    for data_file in known_files:
        data_file.unlink()
    frame = spark.read.format("delta").option("metadataAsDataRead", metadata_as_data).load(str(path))
    assert frame.groupBy("p", "q").count().orderBy("p", "q").collect() == [
        Row(p=None, q=2, count=3),
        Row(p="a", q=None, count=2),
        Row(p="a", q=1, count=10_002),
    ]
    assert frame.select("q", "p").distinct().orderBy("p", "q").collect() == [
        Row(q=2, p=None),
        Row(q=None, p="a"),
        Row(q=1, p="a"),
    ]
    assert frame.groupBy("v").count().orderBy("v").collect() == [Row(5, 2), Row(7, 10_003), Row(8, 1), Row(9, 1)]
    assert frame.count() == 10_007  # noqa: PLR2004
    frame.selectExpr("q AS second", "p AS first").createOrReplaceTempView("metadata_group_aliases")
    try:
        assert spark.sql(
            "SELECT first, second, COUNT(*) AS n FROM metadata_group_aliases GROUP BY first, second ORDER BY first, second"
        ).collect() == [Row(None, 2, 3), Row("a", None, 2), Row("a", 1, 10_002)]
    finally:
        spark.catalog.dropTempView("metadata_group_aliases")


@pytest.mark.parametrize("metadata_as_data", ["false", "true"])
@pytest.mark.parametrize("files", [[], [("a", 1, [], True)], [("a", 1, [], False)]])
def test_metadata_grouping_empty_inputs(spark, tmp_path, metadata_as_data, files):
    path = tmp_path / "empty_metadata_groups"
    for data_file in _write_partitioned_table(path, files):
        data_file.unlink()
    frame = spark.read.format("delta").option("metadataAsDataRead", metadata_as_data).load(str(path))
    assert frame.count() == 0
    assert frame.groupBy("p").count().collect() == []
    assert frame.select("p").distinct().collect() == []


@pytest.mark.parametrize("metadata_as_data", ["false", "true"])
def test_metadata_grouping_single_compressed_file(spark, tmp_path, metadata_as_data):
    path = tmp_path / "single_metadata_group"
    rows = 100_000
    for data_file in _write_partitioned_table(path, [("x" * 50, 1, [7] * rows, True)]):
        data_file.unlink()
    frame = spark.read.format("delta").option("metadataAsDataRead", metadata_as_data).load(str(path))
    assert frame.groupBy("p").count().collect() == [Row("x" * 50, rows)]
    assert frame.select("p").distinct().collect() == [Row("x" * 50)]


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


@pytest.mark.parametrize("mapping_mode", ["none", "name", "id"])
def test_partition_filtered_exact_aggregates_do_not_read_files(spark, tmp_path, mapping_mode):
    path = tmp_path / "partition_filtered_aggregates"
    for data_file in _write_partitioned_table(
        path, [("a", 1, [None, 2, 4], True), ("b", 2, [7, 8], True), (None, 3, [9], True)], mapping_mode
    ):
        data_file.unlink()
    frame = spark.read.format("delta").load(str(path))
    frame.selectExpr("p AS part", "q AS key", "v AS value").createOrReplaceTempView("filtered_metadata_alias")
    try:
        assert spark.sql(
            "SELECT COUNT(*), COUNT(value), MIN(value), MAX(value) "
            "FROM filtered_metadata_alias WHERE part = 'a' OR part IS NULL"
        ).collect() == [Row(4, 3, 2, 9)]
        assert spark.sql(
            "SELECT COUNT(*), MIN(value), MAX(value) FROM filtered_metadata_alias WHERE key > key"
        ).collect() == [Row(0, None, None)]
        assert spark.sql(
            "SELECT COUNT(*), MIN(value), MAX(value) FROM filtered_metadata_alias WHERE part = 'absent'"
        ).collect() == [Row(0, None, None)]
    finally:
        spark.catalog.dropTempView("filtered_metadata_alias")
    # Spark Connect 3.5's isEmpty() retains columns unless explicitly projected away.
    assert frame.where("p = 'a'").select().isEmpty() is False
    assert frame.where("p = 'absent'").select().isEmpty() is True
    assert frame.where("q IN (1, 3)").select().limit(2).collect() == [(), ()]


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
        spark.createDataFrame([(1, 2, 10), (3, 2, 20), (3, 2, 21), (None, 2, 30)], "p INT, q INT, v INT")
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
    assert frame.where("NOT (p <= q)").groupBy("p").count().collect() == [Row(3, 2)]


@pytest.mark.parametrize("metadata_as_data", ["false", "true"])
def test_metadata_grouping_uses_deletion_vector_cardinality(spark, tmp_path, metadata_as_data):
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
    limited = spark.read.format("delta").load(str(path)).limit(3).collect()
    assert sorted(row.id for row in limited) == [3, 5, 6]
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
    if metadata_as_data == "false":
        assert frame.where("p = 'a'").select().isEmpty() is False
        assert frame.select().limit(4).collect() == [(), (), ()]
