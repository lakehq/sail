from __future__ import annotations

import json
from typing import TYPE_CHECKING
from urllib.parse import unquote

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F  # noqa: N812

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import SparkSession


def _latest_metadata(base: Path) -> dict:
    for log_file in sorted((base / "_delta_log").glob("*.json"), reverse=True):
        with log_file.open("r", encoding="utf-8") as f:
            for line in f:
                obj = json.loads(line)
                if "metaData" in obj:
                    return obj["metaData"]
    message = f"metadata action not found in {base / '_delta_log'}"
    raise AssertionError(message)


def _physical_name_for_column(metadata: dict, column_name: str) -> str:
    schema = json.loads(metadata["schemaString"])
    for field in schema["fields"]:
        if field["name"] == column_name:
            return field.get("metadata", {}).get("delta.columnMapping.physicalName", column_name)
    message = f"column {column_name!r} not found in schema"
    raise AssertionError(message)


def _latest_added_parquet_files(base: Path) -> list[Path]:
    for log_file in sorted((base / "_delta_log").glob("*.json"), reverse=True):
        added = []
        with log_file.open("r", encoding="utf-8") as f:
            for line in f:
                action = json.loads(line)
                if "add" in action and action["add"]["path"].endswith(".parquet"):
                    added.append(base / unquote(action["add"]["path"]))
        if added:
            return added
    return []


def _assert_parquet_struct_matches_delta(
    arrow_type: pa.Schema | pa.StructType,
    delta_type: dict,
) -> None:
    for delta_field in delta_type["fields"]:
        metadata = delta_field["metadata"]
        physical_name = metadata["delta.columnMapping.physicalName"]
        arrow_field = arrow_type.field(physical_name)
        arrow_metadata = {key.decode(): value.decode() for key, value in (arrow_field.metadata or {}).items()}
        assert arrow_metadata["PARQUET:field_id"] == str(metadata["delta.columnMapping.id"])

        _assert_parquet_type_matches_delta(arrow_field.type, delta_field["type"])


def _assert_parquet_type_matches_delta(
    arrow_type: pa.DataType,
    delta_type: dict | str,
) -> None:
    if not isinstance(delta_type, dict):
        return

    type_name = delta_type["type"]
    if type_name == "struct":
        _assert_parquet_struct_matches_delta(arrow_type, delta_type)
    elif type_name == "array":
        _assert_parquet_type_matches_delta(arrow_type.value_type, delta_type["elementType"])
    elif type_name == "map":
        _assert_parquet_type_matches_delta(arrow_type.key_type, delta_type["keyType"])
        _assert_parquet_type_matches_delta(arrow_type.item_type, delta_type["valueType"])


def _assert_parquet_files_match_delta_schema(
    base: Path,
    *,
    latest_only: bool = False,
) -> None:
    delta_schema = json.loads(_latest_metadata(base)["schemaString"])
    parquet_files = _latest_added_parquet_files(base) if latest_only else list(base.rglob("*.parquet"))
    assert parquet_files
    for parquet_file in parquet_files:
        _assert_parquet_struct_matches_delta(
            pq.ParquetFile(parquet_file).schema_arrow,
            delta_schema,
        )


def test_create_table_with_column_mapping_name(spark, tmp_path: Path):
    base = tmp_path / "delta_cm_name"
    df = spark.createDataFrame(
        [
            Row(id=1, name="a"),
            Row(id=2, name="b"),
        ]
    )

    # Write new table with the official Delta table property name.
    (df.write.format("delta").mode("overwrite").option("delta.columnMapping.mode", "name").save(str(base)))

    # Basic read should succeed
    out = spark.read.format("delta").load(str(base)).orderBy("id").collect()
    assert [r.asDict() for r in out] == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
    ]

    # Inspect first commit log to validate protocol and metadata
    log_file = base / "_delta_log" / "00000000000000000000.json"
    assert log_file.exists(), f"missing delta log file: {log_file}"
    protocol = None
    metadata = None
    with log_file.open("r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            if "protocol" in obj:
                protocol = obj["protocol"]
            if "metaData" in obj:
                metadata = obj["metaData"]

    assert protocol is not None, "protocol action not found in first commit"
    assert metadata is not None, "metadata action not found in first commit"

    assert protocol.get("minReaderVersion", 0) >= 2  # noqa: PLR2004
    assert protocol.get("minWriterVersion", 0) >= 5  # noqa: PLR2004
    config = metadata.get("configuration", {})
    assert config.get("delta.columnMapping.mode") == "name"
    assert "delta.columnMapping.maxColumnId" in config
    assert int(config["delta.columnMapping.maxColumnId"]) >= 2  # noqa: PLR2004


def test_create_and_append_with_column_mapping_id(spark, tmp_path: Path):
    base = tmp_path / "delta_cm_id"
    df = spark.createDataFrame(
        [
            Row(i=1, s="x"),
            Row(i=2, s="y"),
        ]
    )

    # Create table with id mode
    df.write.format("delta").mode("overwrite").option("column_mapping_mode", "id").save(str(base))

    # Append without option
    df2 = spark.createDataFrame([Row(i=3, s="z")])
    df2.write.format("delta").mode("append").save(str(base))

    out = spark.read.format("delta").load(str(base)).orderBy("i").collect()
    assert [r.asDict() for r in out] == [
        {"i": 1, "s": "x"},
        {"i": 2, "s": "y"},
        {"i": 3, "s": "z"},
    ]

    # Validate protocol and configuration reflect id mode
    log_file = base / "_delta_log" / "00000000000000000000.json"
    assert log_file.exists(), f"missing delta log file: {log_file}"
    protocol = None
    metadata = None
    with log_file.open("r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            if "protocol" in obj:
                protocol = obj["protocol"]
            if "metaData" in obj:
                metadata = obj["metaData"]

    assert protocol is not None, "protocol action not found in first commit"
    assert metadata is not None, "metadata action not found in first commit"
    assert protocol.get("minReaderVersion", 0) >= 2  # noqa: PLR2004
    assert protocol.get("minWriterVersion", 0) >= 5  # noqa: PLR2004
    config = metadata.get("configuration", {})
    assert config.get("delta.columnMapping.mode") == "id"
    assert "delta.columnMapping.maxColumnId" in config
    assert int(config["delta.columnMapping.maxColumnId"]) >= 2  # noqa: PLR2004


@pytest.mark.parametrize("mapping_mode", ["name", "id"])
def test_scalar_column_mapping_read_does_not_expose_parquet_field_ids(
    spark: SparkSession,
    tmp_path: Path,
    mapping_mode: str,
):
    source_path = tmp_path / f"delta_cm_scalar_source_{mapping_mode}"
    source = spark.createDataFrame([Row(id=1, label="a")])
    (
        source.write.format("delta")
        .mode("overwrite")
        .option("delta.columnMapping.mode", mapping_mode)
        .save(str(source_path))
    )

    loaded_schema = spark.read.format("delta").load(str(source_path)).schema
    for field in loaded_schema.fields:
        assert "PARQUET:field_id" not in field.metadata
        assert "parquet.field.id" not in field.metadata


def test_merge_schema_with_column_mapping_name(spark, tmp_path: Path):
    base = tmp_path / "delta_cm_merge_name"

    # Create initial table with name mode
    df = spark.createDataFrame(
        [
            Row(id=1, name="a"),
            Row(id=2, name="b"),
        ]
    )
    df.write.format("delta").mode("overwrite").option("column_mapping_mode", "name").save(str(base))

    # Append with a new column using mergeSchema
    df2 = spark.createDataFrame(
        [
            Row(id=3, name="c", age=10),
            Row(id=4, name="d", age=20),
        ]
    )
    df2.write.format("delta").mode("append").option("mergeSchema", "true").save(str(base))

    # Read should include new column, with nulls for old rows
    out = spark.read.format("delta").load(str(base)).orderBy("id").collect()
    assert [r.asDict() for r in out] == [
        {"id": 1, "name": "a", "age": None},
        {"id": 2, "name": "b", "age": None},
        {"id": 3, "name": "c", "age": 10},
        {"id": 4, "name": "d", "age": 20},
    ]

    # Validate that maxColumnId exists and is non-decreasing across commits with metadata
    log_dir = base / "_delta_log"
    logs = sorted(log_dir.glob("*.json"))
    assert logs, f"no delta logs in {log_dir}"

    def extract_metadata_config(p: Path) -> dict | None:
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                obj = json.loads(line)
                if "metaData" in obj:
                    return obj["metaData"].get("configuration", {})
        return None

    cfgs = [c for c in (extract_metadata_config(p) for p in logs) if c is not None]
    assert cfgs, "no metadata actions found in commit logs"
    assert cfgs[0].get("delta.columnMapping.mode") == "name"
    assert "delta.columnMapping.maxColumnId" in cfgs[0]
    # If there is a later metadata action, ensure maxColumnId is non-decreasing
    if len(cfgs) > 1:
        assert int(cfgs[-1]["delta.columnMapping.maxColumnId"]) >= int(cfgs[0]["delta.columnMapping.maxColumnId"]) + 1


def test_merge_nested_struct_in_name_mode(spark, tmp_path: Path):
    base = tmp_path / "delta_cm_nested_struct"
    df = spark.createDataFrame([Row(user=Row(id=1, name="a"))])
    df.write.format("delta").mode("overwrite").option("column_mapping_mode", "name").save(str(base))

    df2 = spark.createDataFrame([Row(user=Row(id=2, name="b", age=30))])
    df2.write.format("delta").mode("append").option("mergeSchema", "true").save(str(base))

    out = spark.read.format("delta").load(str(base)).orderBy("user.id").collect()
    assert [r.asDict(recursive=True) for r in out] == [
        {"user": {"id": 1, "name": "a", "age": None}},
        {"user": {"id": 2, "name": "b", "age": 30}},
    ]


def test_column_mapping_nested_struct_round_trip(spark, tmp_path: Path):
    source_path = tmp_path / "delta_cm_nested_round_trip_source"
    output_path = tmp_path / "delta_cm_nested_round_trip_output"
    source_rows = [
        Row(
            id=1,
            code="USD",
            original_details=Row(amount=10, active=True),
        )
    ]

    (
        spark.createDataFrame(source_rows)
        .write.format("delta")
        .mode("overwrite")
        .option("delta.columnMapping.mode", "name")
        .save(str(source_path))
    )

    loaded = spark.read.format("delta").load(str(source_path))
    assert [row.asDict(recursive=True) for row in loaded.collect()] == [
        {
            "id": 1,
            "code": "USD",
            "original_details": {"amount": 10, "active": True},
        }
    ]
    assert loaded.select("original_details.amount").collect() == [Row(amount=10)]
    _assert_parquet_files_match_delta_schema(source_path)

    rebuilt = loaded.select(F.struct(F.col("id"), F.col("code")).alias("details"))
    (rebuilt.write.format("delta").mode("overwrite").option("delta.columnMapping.mode", "name").save(str(output_path)))

    result = spark.read.format("delta").load(str(output_path))
    assert [row.asDict(recursive=True) for row in result.collect()] == [{"details": {"id": 1, "code": "USD"}}]
    _assert_parquet_files_match_delta_schema(output_path)
    target_schema = _latest_metadata(output_path)["schemaString"]
    assert "PARQUET:field_id" not in target_schema
    assert "parquet.field.id" not in target_schema


def test_deep_nested_predicate_with_explicit_schema(spark, tmp_path: Path):
    base = tmp_path / "delta_cm_deep_predicate"
    rows = [
        Row(id=1, payload=Row(level=Row(value=10))),
        Row(id=2, payload=Row(level=Row(value=20))),
    ]
    source = spark.createDataFrame(rows)
    (source.write.format("delta").mode("overwrite").option("delta.columnMapping.mode", "name").save(str(base)))

    loaded = spark.read.schema(source.schema).format("delta").load(str(base))
    filtered = loaded.where(F.col("payload.level.value") > F.lit(10)).collect()

    assert [row.asDict(recursive=True) for row in filtered] == [{"id": 2, "payload": {"level": {"value": 20}}}]


def test_nested_mapping_with_metadata_as_data_read(spark, tmp_path: Path):
    base = tmp_path / "delta_cm_metadata_as_data"
    rows = [
        Row(id=1, payload=Row(level=Row(value=10))),
        Row(id=2, payload=Row(level=Row(value=20))),
    ]
    source = spark.createDataFrame(rows)
    (source.write.format("delta").mode("overwrite").option("delta.columnMapping.mode", "name").save(str(base)))

    loaded = spark.read.format("delta").option("metadataAsDataRead", "true").load(str(base)).orderBy("id")

    assert [row.asDict(recursive=True) for row in loaded.collect()] == [
        {"id": 1, "payload": {"level": {"value": 10}}},
        {"id": 2, "payload": {"level": {"value": 20}}},
    ]
    filtered = (
        spark.read.format("delta")
        .option("metadataAsDataRead", "true")
        .load(str(base))
        .where(F.col("payload.level.value") > F.lit(10))
        .collect()
    )
    assert [row.asDict(recursive=True) for row in filtered] == [{"id": 2, "payload": {"level": {"value": 20}}}]


def test_dotted_column_mapping_with_metadata_as_data_filter(spark, tmp_path: Path):
    base = tmp_path / "delta_cm_metadata_as_data_dotted"
    source = spark.createDataFrame([(1, "a"), (2, "b")], ["event.id", "label"])
    (source.write.format("delta").mode("overwrite").option("delta.columnMapping.mode", "name").save(str(base)))

    rows = (
        spark.read.format("delta")
        .option("metadataAsDataRead", "true")
        .load(str(base))
        .where(F.col("`event.id`") == F.lit(2))
        .collect()
    )

    assert [row.asDict() for row in rows] == [{"event.id": 2, "label": "b"}]


def test_merge_array_of_struct_in_name_mode(spark, tmp_path: Path):
    base = tmp_path / "delta_cm_array_struct"
    df = spark.createDataFrame([Row(events=[Row(ts=1)])])
    (df.write.format("delta").mode("overwrite").option("delta.columnMapping.mode", "name").save(str(base)))

    df2 = spark.createDataFrame([Row(events=[Row(ts=2, kind="x")])])
    df2.write.format("delta").mode("append").option("mergeSchema", "true").save(str(base))

    rows = [r.asDict(recursive=True) for r in spark.read.format("delta").load(str(base)).collect()]
    assert {"ts": 2, "kind": "x"} in [event for row in rows for event in row["events"]]
    _assert_parquet_files_match_delta_schema(base, latest_only=True)


def test_merge_schema_after_consecutive_arrays_writes_mapped_parquet_fields(
    spark: SparkSession,
    tmp_path: Path,
):
    base = tmp_path / "delta_cm_consecutive_arrays"
    initial = spark.createDataFrame([Row(matrix=[[Row(value=1)]])])
    (initial.write.format("delta").mode("overwrite").option("delta.columnMapping.mode", "name").save(str(base)))

    appended = spark.createDataFrame([Row(matrix=[[Row(value=2)]], label="new")])
    appended.write.format("delta").mode("append").option("mergeSchema", "true").save(str(base))

    _assert_parquet_files_match_delta_schema(base, latest_only=True)


def test_add_new_array_struct_field(spark, tmp_path: Path):
    base = tmp_path / "delta_cm_new_array_struct"
    df = spark.createDataFrame([Row(id=1)])
    df.write.format("delta").mode("overwrite").option("column_mapping_mode", "name").save(str(base))

    df2 = spark.createDataFrame([Row(id=2, items=[Row(a=10)])])
    df2.write.format("delta").mode("append").option("mergeSchema", "true").save(str(base))

    out = spark.read.format("delta").load(str(base)).orderBy("id").collect()
    rows = [r.asDict(recursive=True) for r in out]
    assert rows[0]["id"] == 1
    assert rows[0]["items"] is None
    assert rows[1]["id"] == 2  # noqa: PLR2004
    assert isinstance(rows[1]["items"], list)
    assert len(rows[1]["items"]) == 1
    assert rows[1]["items"][0] == {"a": 10}
    _assert_parquet_files_match_delta_schema(base, latest_only=True)


def test_merge_map_value_struct(spark, tmp_path: Path):
    base = tmp_path / "delta_cm_map_value_struct"
    df = spark.createDataFrame([Row(attrs={"k": Row(a=1)})])
    df.write.format("delta").mode("overwrite").option("column_mapping_mode", "name").save(str(base))

    df2 = spark.createDataFrame([Row(attrs={"k": Row(a=2, b=3)})])
    df2.write.format("delta").mode("append").option("mergeSchema", "true").save(str(base))

    rows = [row.asDict(recursive=True) for row in spark.read.format("delta").load(str(base)).collect()]
    assert {"k": {"a": 2, "b": 3}} in [row["attrs"] for row in rows]
    _assert_parquet_files_match_delta_schema(base, latest_only=True)


def test_partitioned_table_with_column_mapping_name(spark, tmp_path: Path):
    """Ensure partition columns are resolved when column mapping is enabled."""

    base = tmp_path / "delta_partitioned_cm_name"

    # Create initial partitioned table with column mapping
    df = spark.createDataFrame(
        [
            Row(id=1, region="us", data="a"),
            Row(id=2, region="eu", data="b"),
        ]
    )

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("column_mapping_mode", "name")
        .partitionBy("region")
        .save(str(base))
    )

    # Verify initial write produced data files under the partitioned directory
    parquet_files = list(base.glob("**/*.parquet"))
    assert parquet_files

    # Append new data
    df2 = spark.createDataFrame(
        [
            Row(id=3, region="us", data="c"),
            Row(id=4, region="asia", data="d"),
        ]
    )

    # This would fail previously because the physical partition column was "col-<uuid>"
    df2.write.format("delta").mode("append").save(str(base))

    # Verify read
    out = spark.read.format("delta").load(str(base)).orderBy("id").collect()
    assert [r.asDict() for r in out] == [
        {"id": 1, "region": "us", "data": "a"},
        {"id": 2, "region": "eu", "data": "b"},
        {"id": 3, "region": "us", "data": "c"},
        {"id": 4, "region": "asia", "data": "d"},
    ]

    for metadata_as_data in ("false", "true"):
        filtered = (
            spark.read.format("delta")
            .option("metadataAsDataRead", metadata_as_data)
            .load(str(base))
            .where(F.col("region") == "us")
            .orderBy("id")
            .collect()
        )
        assert [row.asDict() for row in filtered] == [
            {"id": 1, "region": "us", "data": "a"},
            {"id": 3, "region": "us", "data": "c"},
        ]


def test_remove_actions_for_partitioned_column_mapping_table_use_physical_keys(spark, tmp_path: Path):
    base = tmp_path / "delta_cm_partition_remove_keys"

    df = spark.createDataFrame(
        [
            Row(id=1, region="us", data="a"),
            Row(id=2, region="eu", data="b"),
        ]
    )
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("column_mapping_mode", "name")
        .partitionBy("region")
        .save(str(base))
    )

    physical_region = _physical_name_for_column(_latest_metadata(base), "region")
    assert physical_region != "region"

    df2 = spark.createDataFrame(
        [
            Row(id=3, region="apac", data="c"),
        ]
    )
    df2.write.format("delta").mode("overwrite").save(str(base))

    latest_log = sorted((base / "_delta_log").glob("*.json"))[-1]
    remove_partition_values = []
    with latest_log.open("r", encoding="utf-8") as f:
        for line in f:
            action = json.loads(line)
            if "remove" in action:
                remove_partition_values.append(action["remove"].get("partitionValues", {}))

    assert remove_partition_values
    assert all(physical_region in values for values in remove_partition_values)
    assert all("region" not in values for values in remove_partition_values)


def test_partitioned_table_with_column_mapping_id(spark, tmp_path: Path):
    """Partitioned table append/read should work in column mapping id mode."""

    base = tmp_path / "delta_partitioned_cm_id"

    df = spark.createDataFrame(
        [
            Row(id=1, region="us", data="a"),
            Row(id=2, region="eu", data="b"),
        ]
    )

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("column_mapping_mode", "id")
        .partitionBy("region")
        .save(str(base))
    )

    parquet_files = list(base.glob("**/*.parquet"))
    assert parquet_files

    df2 = spark.createDataFrame(
        [
            Row(id=3, region="us", data="c"),
            Row(id=4, region="asia", data="d"),
        ]
    )

    df2.write.format("delta").mode("append").save(str(base))

    out = spark.read.format("delta").load(str(base)).orderBy("id").collect()
    assert [r.asDict() for r in out] == [
        {"id": 1, "region": "us", "data": "a"},
        {"id": 2, "region": "eu", "data": "b"},
        {"id": 3, "region": "us", "data": "c"},
        {"id": 4, "region": "asia", "data": "d"},
    ]


def test_column_mapping_supports_special_characters_in_column_names(spark, tmp_path: Path):
    """Column mapping should preserve Delta-supported special characters in column names."""

    base = tmp_path / "delta_cm_special_names"
    df = spark.createDataFrame(
        [
            Row(**{"first.name": "alice", "name with space": 1, "a,b": "x=y"}),
            Row(**{"first.name": "bob", "name with space": 2, "a,b": "p=q"}),
        ]
    )

    df.write.format("delta").mode("overwrite").option("column_mapping_mode", "name").save(str(base))

    out = spark.read.format("delta").load(str(base)).orderBy(F.col("`name with space`"))
    rows = [row.asDict() for row in out.collect()]
    assert rows == [
        {"first.name": "alice", "name with space": 1, "a,b": "x=y"},
        {"first.name": "bob", "name with space": 2, "a,b": "p=q"},
    ]

    projected = out.selectExpr("`first.name`", "`name with space`", "`a,b`").collect()
    assert [row.asDict() for row in projected] == rows


RENAMED_EXPECTED_ROWS = [
    {"id": 1, "code": "alpha", "details": {"total": 10, "active": True}},
    {"id": 2, "code": "beta", "details": {"total": 20, "active": False}},
    {"id": 3, "code": "gamma", "details": {"total": 30, "active": True}},
]


def _collect_rows(df) -> list[dict]:
    return sorted(
        (row.asDict(recursive=True) for row in df.collect()),
        key=lambda row: row["id"],
    )


def _rename_schema(schema: dict) -> dict:
    renamed = json.loads(json.dumps(schema))
    for field in renamed["fields"]:
        if field["name"] == "label":
            field["name"] = "code"
        if field["name"] == "details":
            for child in field["type"]["fields"]:
                if child["name"] == "amount":
                    child["name"] = "total"
    return renamed


def _append_metadata_only_rename(table_path: Path) -> None:
    metadata = _latest_metadata(table_path)
    original_schema = json.loads(metadata["schemaString"])
    renamed_schema = _rename_schema(original_schema)

    original_fields = {field["name"]: field["metadata"] for field in original_schema["fields"]}
    renamed_fields = {field["name"]: field["metadata"] for field in renamed_schema["fields"]}
    assert renamed_fields["code"] == original_fields["label"]
    original_details = {field["name"]: field["metadata"] for field in original_schema["fields"][2]["type"]["fields"]}
    renamed_details = {field["name"]: field["metadata"] for field in renamed_schema["fields"][2]["type"]["fields"]}
    assert renamed_details["total"] == original_details["amount"]

    log_dir = table_path / "_delta_log"
    version = max(int(path.stem) for path in log_dir.glob("*.json")) + 1
    renamed_metadata = {
        **metadata,
        "schemaString": json.dumps(renamed_schema, separators=(",", ":")),
    }
    actions = [
        {
            "commitInfo": {
                "operation": "RENAME COLUMN",
                "operationParameters": {
                    "oldColumnPath": "label,details.amount",
                    "newColumnPath": "code,details.total",
                },
                "readVersion": version - 1,
                "isBlindAppend": True,
            }
        },
        {"metaData": renamed_metadata},
    ]
    log_file = log_dir / f"{version:020}.json"
    log_file.write_text(
        "".join(f"{json.dumps(action, separators=(',', ':'))}\n" for action in actions),
        encoding="utf-8",
    )


def _change_nested_physical_name(table_path: Path) -> None:
    metadata = _latest_metadata(table_path)
    schema = json.loads(metadata["schemaString"])
    details = next(field for field in schema["fields"] if field["name"] == "details")
    amount = next(field for field in details["type"]["fields"] if field["name"] == "amount")
    amount["metadata"]["delta.columnMapping.physicalName"] = "missing-physical-name"

    log_dir = table_path / "_delta_log"
    version = max(int(path.stem) for path in log_dir.glob("*.json")) + 1
    changed_metadata = {
        **metadata,
        "schemaString": json.dumps(schema, separators=(",", ":")),
    }
    actions = [
        {
            "commitInfo": {
                "operation": "SET TBLPROPERTIES",
                "readVersion": version - 1,
                "isBlindAppend": True,
            }
        },
        {"metaData": changed_metadata},
    ]
    (log_dir / f"{version:020}.json").write_text(
        "".join(f"{json.dumps(action, separators=(',', ':'))}\n" for action in actions),
        encoding="utf-8",
    )


@pytest.fixture
def renamed_table(spark: SparkSession, tmp_path: Path) -> Path:
    table_path = tmp_path / "renamed_column_mapping"
    initial_rows = [
        Row(id=1, label="alpha", details=Row(amount=10, active=True)),
        Row(id=2, label="beta", details=Row(amount=20, active=False)),
    ]
    (
        spark.createDataFrame(initial_rows)
        .write.format("delta")
        .mode("overwrite")
        .option("delta.columnMapping.mode", "name")
        .save(str(table_path))
    )
    _append_metadata_only_rename(table_path)

    appended = [Row(id=3, code="gamma", details=Row(total=30, active=True))]
    spark.createDataFrame(appended).write.format("delta").mode("append").save(str(table_path))
    return table_path


def test_reads_files_written_before_and_after_rename(
    spark: SparkSession,
    renamed_table: Path,
):
    df = spark.read.format("delta").load(str(renamed_table))

    assert _collect_rows(df) == RENAMED_EXPECTED_ROWS
    assert sorted(row.total for row in df.select("details.total").collect()) == [
        10,
        20,
        30,
    ]


def test_append_preserves_renamed_nested_mapping(
    spark: SparkSession,
    renamed_table: Path,
):
    row = [Row(id=4, code="delta", details=Row(total=40, active=False))]
    spark.createDataFrame(row).write.format("delta").mode("append").save(str(renamed_table))

    assert _collect_rows(spark.read.format("delta").load(str(renamed_table))) == [
        *RENAMED_EXPECTED_ROWS,
        {"id": 4, "code": "delta", "details": {"total": 40, "active": False}},
    ]
    schema_string = _latest_metadata(renamed_table)["schemaString"]
    assert "PARQUET:field_id" not in schema_string
    assert "parquet.field.id" not in schema_string


def test_overwrite_preserves_renamed_nested_mapping(
    spark: SparkSession,
    renamed_table: Path,
):
    original_schema = _latest_metadata(renamed_table)["schemaString"]
    loaded = spark.read.format("delta").load(str(renamed_table))

    loaded.where("id < 3").write.format("delta").mode("overwrite").save(str(renamed_table))

    assert _collect_rows(spark.read.format("delta").load(str(renamed_table))) == RENAMED_EXPECTED_ROWS[:2]
    assert _latest_metadata(renamed_table)["schemaString"] == original_schema


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        ("name", None),
        ("id", 10),
    ],
)
def test_nested_resolution_uses_only_the_configured_identity(
    spark: SparkSession,
    tmp_path: Path,
    mode: str,
    expected: int | None,
):
    table_path = tmp_path / f"nested_resolution_{mode}"
    rows = [Row(id=1, details=Row(amount=10))]
    (
        spark.createDataFrame(rows)
        .write.format("delta")
        .mode("overwrite")
        .option("delta.columnMapping.mode", mode)
        .save(str(table_path))
    )
    _change_nested_physical_name(table_path)

    row = spark.read.format("delta").load(str(table_path)).collect()[0]

    assert row.details.amount == expected
