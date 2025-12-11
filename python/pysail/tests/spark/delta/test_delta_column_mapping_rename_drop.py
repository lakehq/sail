from __future__ import annotations

import json
from pathlib import Path  # noqa: TC003

from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def _latest_metadata(base: Path) -> dict:
    logs = sorted((base / "_delta_log").glob("*.json"))
    assert logs, f"no delta logs found in {base}"
    with logs[-1].open("r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            if "metaData" in obj:
                return obj["metaData"]
    msg = "metadata action not found in latest delta log"
    raise AssertionError(msg)


def _field_metadata(schema_json: dict, name: str) -> dict:
    for field in schema_json.get("fields", []):
        if field.get("name") == name:
            return field.get("metadata", {})
    msg = f"field {name} not found in schema"
    raise AssertionError(msg)


def _mapping_physical_metadata(field_meta: dict) -> dict:
    phys = field_meta.get("delta.columnMapping.physicalName")
    assert phys, "missing physicalName in field metadata"
    return {"delta.columnMapping.physicalName": str(phys)}


def _schema_json(metadata: dict) -> dict:
    return json.loads(metadata["schemaString"])


class TestDeltaColumnMappingRenameDrop:
    def test_column_mapping_rename_preserves_physical(self, spark, tmp_path: Path):
        base = tmp_path / "delta_cm_rename"
        df = spark.createDataFrame([Row(id=1, name="a"), Row(id=2, name="b")])
        (df.write.format("delta").mode("overwrite").option("column_mapping_mode", "name").save(str(base)))

        metadata0 = _latest_metadata(base)
        schema0 = _schema_json(metadata0)
        id_meta = _mapping_physical_metadata(_field_metadata(schema0, "id"))
        name_meta = _mapping_physical_metadata(_field_metadata(schema0, "name"))

        renamed_schema = StructType(
            [
                StructField("id", IntegerType(), True, metadata=id_meta),
                StructField("name_new", StringType(), True, metadata=name_meta),
            ]
        )
        df2 = spark.createDataFrame([(3, "c"), (4, "d")], schema=renamed_schema)
        (df2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(base)))

        out = spark.read.format("delta").load(str(base)).orderBy("id").collect()
        assert [r.asDict() for r in out] == [
            {"id": 3, "name_new": "c"},
            {"id": 4, "name_new": "d"},
        ]

        metadata1 = _latest_metadata(base)
        schema1 = _schema_json(metadata1)
        field_names = [f.get("name") for f in schema1.get("fields", [])]
        assert "name" not in field_names
        assert "name_new" in field_names
        cfg1 = metadata1["configuration"]
        cfg0 = metadata0["configuration"]
        assert int(cfg1.get("delta.columnMapping.maxColumnId", "0")) >= int(
            cfg0.get("delta.columnMapping.maxColumnId", "0")
        )

    def test_column_mapping_drop_column_retains_ids(self, spark, tmp_path: Path):
        base = tmp_path / "delta_cm_drop"
        df = spark.createDataFrame([Row(id=1, name="a", age=10), Row(id=2, name="b", age=20)])
        (df.write.format("delta").mode("overwrite").option("column_mapping_mode", "name").save(str(base)))

        metadata0 = _latest_metadata(base)
        schema0 = _schema_json(metadata0)
        cfg0 = metadata0["configuration"]
        id_meta = _mapping_physical_metadata(_field_metadata(schema0, "id"))
        age_meta = _mapping_physical_metadata(_field_metadata(schema0, "age"))

        drop_schema = StructType(
            [
                StructField("id", IntegerType(), True, metadata=id_meta),
                StructField("age", IntegerType(), True, metadata=age_meta),
            ]
        )
        df2 = spark.createDataFrame([(3, 30), (4, 40)], schema=drop_schema)
        (df2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(base)))

        out = spark.read.format("delta").load(str(base)).orderBy("id").collect()
        assert [r.asDict() for r in out] == [
            {"id": 3, "age": 30},
            {"id": 4, "age": 40},
        ]

        metadata1 = _latest_metadata(base)
        schema1 = _schema_json(metadata1)
        cfg1 = metadata1["configuration"]
        id_new = _field_metadata(schema1, "id")
        age_new = _field_metadata(schema1, "age")
        field_names = [f.get("name") for f in schema1.get("fields", [])]

        assert "name" not in field_names
        assert id_new.get("delta.columnMapping.physicalName") == id_meta.get("delta.columnMapping.physicalName")
        assert age_new.get("delta.columnMapping.physicalName") == age_meta.get("delta.columnMapping.physicalName")
        assert cfg1.get("delta.columnMapping.maxColumnId") == cfg0.get("delta.columnMapping.maxColumnId")

    def test_column_mapping_overwrite_preserves_custom_metadata(self, spark, tmp_path: Path):
        """Ensure non-columnMapping metadata survives overwriteSchema with column mapping."""

        base = tmp_path / "delta_cm_metadata_preserve"
        df = spark.createDataFrame([Row(id=1)])
        (df.write.format("delta").mode("overwrite").option("column_mapping_mode", "name").save(str(base)))

        metadata0 = _latest_metadata(base)
        schema0 = _schema_json(metadata0)
        id_meta_full = _field_metadata(schema0, "id")
        id_meta = _mapping_physical_metadata(id_meta_full)
        id_before = id_meta_full.get("delta.columnMapping.id")
        phys_before = id_meta.get("delta.columnMapping.physicalName")

        # Add a custom metadata key on the same field and overwrite schema
        # Only add the new user metadata; existing columnMapping metadata should be preserved
        # by the engine.
        custom_meta = {"user.comment": "keep-me"}
        new_schema = StructType([StructField("id", IntegerType(), True, metadata=custom_meta)])
        df2 = spark.createDataFrame([Row(id=2)], schema=new_schema)
        (df2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(base)))

        metadata1 = _latest_metadata(base)
        schema1 = _schema_json(metadata1)
        id_meta_new = _field_metadata(schema1, "id")

        # Column mapping metadata should remain, and custom metadata should be preserved
        assert id_meta_new.get("delta.columnMapping.physicalName") == phys_before
        if id_before is not None:
            assert id_meta_new.get("delta.columnMapping.id") == id_before
        user_comment = id_meta_new.get("user.comment") or id_meta_new.get("metadata.user.comment")
        assert user_comment == "keep-me"
