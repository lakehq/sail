from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing._dev.spark.utils.jvm import classic_spark_mode
from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests._dev.spark.delta.utils import _effective, _tree

if TYPE_CHECKING:
    from pathlib import Path


pytestmark = pytest.mark.spark(version=">=4", reason="Uses VARIANT SQL functions")


def _write_shredded_variant_delta_table(table_location: Path) -> None:
    table_location.mkdir(parents=True, exist_ok=True)
    log_dir = table_location / "_delta_log"
    log_dir.mkdir(parents=True, exist_ok=True)

    data_file = table_location / "part-00000-shredded-variant.parquet"
    payload_type = pa.struct(
        [
            pa.field("metadata", pa.binary(), nullable=False),
            pa.field("typed_value", pa.int64(), nullable=True),
        ]
    )
    payload = pa.array(
        [
            {"metadata": b"\x01\x00\x00", "typed_value": 42},
            {"metadata": b"\x01\x00\x00", "typed_value": 99},
        ],
        type=payload_type,
    )
    table = pa.Table.from_arrays(
        [
            pa.array([1, 2], type=pa.int64()),
            payload,
        ],
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=True),
                pa.field("payload", payload_type, nullable=True),
            ]
        ),
    )
    pq.write_table(table, data_file)

    schema = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": True, "metadata": {}},
            {"name": "payload", "type": "variant", "nullable": True, "metadata": {}},
        ],
    }
    created_time = 1_700_000_000_000
    actions = [
        {
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["variantType", "variantShredding-preview"],
                "writerFeatures": ["variantType", "variantShredding-preview"],
            }
        },
        {
            "metaData": {
                "id": "00000000-0000-0000-0000-000000000001",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps(schema, separators=(",", ":")),
                "partitionColumns": [],
                "configuration": {"delta.enableVariantShredding": "true"},
                "createdTime": created_time,
            }
        },
        {
            "add": {
                "path": data_file.name,
                "partitionValues": {},
                "size": data_file.stat().st_size,
                "modificationTime": created_time,
                "dataChange": True,
                "stats": json.dumps(
                    {
                        "numRecords": 2,
                        "minValues": {"id": 1},
                        "maxValues": {"id": 2},
                        "nullCount": {"id": 0, "payload": 0},
                    },
                    separators=(",", ":"),
                ),
            }
        },
    ]
    with (log_dir / "00000000000000000000.json").open("w", encoding="utf-8") as f:
        for action in actions:
            f.write(json.dumps(action, separators=(",", ":")))
            f.write("\n")


def test_delta_dataframe_read_shredded_variant_physical_data(spark, tmp_path: Path):
    table_path = tmp_path / "delta_variant_shredded_read"
    _write_shredded_variant_delta_table(table_path)

    rows = (
        spark.read.format("delta")
        .load(str(table_path))
        .select(
            "id",
            F.expr("variant_get(payload, '$', 'int')").alias("payload_value"),
            F.expr("to_json(payload)").alias("payload_json"),
        )
        .orderBy("id")
        .collect()
    )

    assert [(row.id, row.payload_value, row.payload_json) for row in rows] == [
        (1, 42, "42"),
        (2, 99, "99"),
    ]


def _data_parquet_files(table_location: Path) -> list[Path]:
    return sorted(path for path in table_location.rglob("*.parquet") if "_delta_log" not in path.parts)


def _first_add_stats(table_location: Path) -> dict:
    log_files = sorted((table_location / "_delta_log").glob("*.json"))
    for log_file in log_files:
        with log_file.open("r", encoding="utf-8") as f:
            for line in f:
                action = json.loads(line)
                if "add" in action:
                    return json.loads(action["add"]["stats"])
    msg = f"add action not found in {table_location / '_delta_log'}"
    raise AssertionError(msg)


def test_delta_writer_shreds_variant_physical_data(spark, tmp_path: Path):
    table_path = tmp_path / "delta_variant_shredded_write"
    spark.sql("DROP TABLE IF EXISTS delta_variant_shredded_write_table")
    spark.sql(
        f"""
        CREATE TABLE delta_variant_shredded_write_table (
          id INT,
          payload VARIANT
        )
        USING DELTA
        LOCATION '{table_path.as_posix()}'
        TBLPROPERTIES ('delta.enableVariantShredding' = 'true')
        """
    )
    spark.sql(
        """
        INSERT INTO delta_variant_shredded_write_table
        SELECT * FROM VALUES
          (1, parse_json('{"a":1,"b":"delta"}')),
          (2, parse_json('{"a":2,"b":"lake"}'))
        """
    )

    data_files = _data_parquet_files(table_path)
    assert len(data_files) == 1

    payload_field = pq.read_schema(data_files[0]).field("payload")
    payload_type = payload_field.type
    assert pa.types.is_struct(payload_type)
    payload_fields = {payload_type.field(i).name for i in range(payload_type.num_fields)}
    assert "metadata" in payload_fields
    assert "typed_value" in payload_fields
    typed_value = payload_type.field("typed_value").type
    assert pa.types.is_struct(typed_value)
    typed_value_fields = {typed_value.field(i).name for i in range(typed_value.num_fields)}
    assert "a" in typed_value_fields
    assert "b" in typed_value_fields

    rows = (
        spark.read.format("delta")
        .load(str(table_path))
        .select(
            "id",
            F.expr("variant_get(payload, '$.a', 'int')").alias("a"),
            F.expr("variant_get(payload, '$.b', 'string')").alias("b"),
        )
        .orderBy("id")
        .collect()
    )
    assert [(row.id, row.a, row.b) for row in rows] == [(1, 1, "delta"), (2, 2, "lake")]

    stats = _first_add_stats(table_path)
    assert isinstance(stats.get("minValues", {}).get("payload"), str)
    assert isinstance(stats.get("maxValues", {}).get("payload"), str)
    assert stats.get("nullCount", {}).get("payload") == 0
    assert "typed_value" not in json.dumps(stats, separators=(",", ":"))


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/main/scala/org/apache/spark/sql/delta/TableFeature.scala#L817-L875
@pytest.mark.spark(version=">=4", reason="Uses VARIANT SQL type")
@pytest.mark.parametrize(
    ("variant_feature", "shredding_feature"),
    [
        ("variantType-preview", None),
        ("variantType-preview", "variantShredding-preview"),
        # Stable shredding is a newer supported Sail feature, not a Delta 4.0.1 parity case.
        ("variantType", "variantShredding"),
    ],
)
def test_unrelated_alter_preserves_variant_feature_choice(spark, tmp_path, variant_feature, shredding_feature):
    name = "variant_property_lifecycle"
    path = tmp_path / "variant"
    properties = f"'delta.feature.{variant_feature}'='supported'"
    readers = {variant_feature}
    if shredding_feature:
        properties += f", 'delta.feature.{shredding_feature}'='supported', 'delta.enableVariantShredding'='true'"
        readers.add(shredding_feature)
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, v VARIANT) USING delta "
        f"LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES ({properties})"
    ).collect()
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1, NULL)").collect()  # noqa: S608
        initial = _effective(path, "protocol")
        assert set(initial["readerFeatures"]) == readers
        assert set(initial["writerFeatures"]) == readers | {"appendOnly", "invariants"}
        for marker in ("first", "second"):
            spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('custom.marker'='{marker}')").collect()
            assert _effective(path, "protocol") == initial
            assert not any(key.startswith("delta.feature.") for key in _effective(path, "metaData")["configuration"])
        spark.sql(f"INSERT INTO {name} SELECT 2, parse_json('{{\"x\":2}}')").collect()  # noqa: S608
        assert _effective(path, "protocol") == initial
        assert spark.sql(f"SELECT id, CAST(v AS STRING) AS payload FROM {name} ORDER BY id").collect() == [  # noqa: S608
            Row(id=1, payload=None),
            Row(id=2, payload='{"x":2}'),
        ]
        # Deliberate feature requests still upgrade, including stable shredding's Variant dependency.
        if shredding_feature == "variantShredding-preview":
            requested, added = "variantShredding", {"variantShredding", "variantType"}
        elif shredding_feature == "variantShredding":
            requested, added = "variantShredding-preview", {"variantShredding-preview"}
        else:
            requested, added = "variantType", {"variantType"}
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.feature.{requested}'='supported')").collect()
        upgraded = _effective(path, "protocol")
        assert set(upgraded["readerFeatures"]) == readers | added
        assert set(upgraded["writerFeatures"]) == readers | added | {"appendOnly", "invariants"}
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


@pytest.mark.integration
@pytest.mark.spark(version=">=4", reason="Uses VARIANT SQL type")
def test_external_preview_variant_survives_alter_and_cross_engine_writes(spark, delta_jvm_spark, tmp_path):
    path = tmp_path / "external_preview"
    target = f"delta.`{path}`"
    with classic_spark_mode():
        delta_jvm_spark.sql(
            f"CREATE TABLE {target} (id BIGINT, v VARIANT) USING delta "
            "TBLPROPERTIES ('delta.feature.variantType-preview'='supported')"
        ).collect()
        delta_jvm_spark.sql(f"INSERT INTO {target} VALUES (1, NULL)").collect()  # noqa: S608
        # Separate writes retain an all-null file alongside a non-null Variant file.
        delta_jvm_spark.sql(f"INSERT INTO {target} SELECT 0, parse_json('{{\"x\":0}}')").collect()  # noqa: S608
    initial = _effective(path, "protocol")
    assert set(initial["readerFeatures"]) == {"variantType-preview"}
    assert not any(key.startswith("delta.feature.") for key in _effective(path, "metaData")["configuration"])
    # Sail ALTER/INSERT use catalog targets; the JVM retains its native Delta path target.
    name = "external_preview_property_lifecycle"
    before_registration = _tree(path)
    initial_metadata = _effective(path, "metaData")
    spark.sql(f"CREATE TABLE {name} USING delta LOCATION '{escape_sql_string_literal(str(path))}'").collect()
    try:
        assert _tree(path) == before_registration
        assert _effective(path, "metaData") == initial_metadata
        assert _effective(path, "protocol") == initial
        initial_rows = [Row(id=0, payload='{"x":0}'), Row(id=1, payload=None)]
        sail_query = f"SELECT id, CAST(v AS STRING) AS payload FROM {name} ORDER BY id"  # noqa: S608
        jvm_query = f"SELECT id, CAST(v AS STRING) AS payload FROM {target} ORDER BY id"  # noqa: S608
        with classic_spark_mode():
            assert delta_jvm_spark.sql(jvm_query).collect() == initial_rows
        # Exercise source-schema conversion independently of any ALTER or Sail write.
        assert spark.sql(sail_query).collect() == initial_rows
        for marker in ("first", "second"):
            spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('custom.marker'='{marker}')").collect()
            assert _effective(path, "protocol") == initial
            assert _effective(path, "metaData")["schemaString"] == initial_metadata["schemaString"]
            assert spark.sql(sail_query).collect() == initial_rows
            with classic_spark_mode():
                assert delta_jvm_spark.sql(jvm_query).collect() == initial_rows
        spark.sql(f"INSERT INTO {name} SELECT 2, parse_json('{{\"x\":2}}')").collect()  # noqa: S608
        with classic_spark_mode():
            delta_jvm_spark.sql(f"INSERT INTO {target} VALUES (3, NULL)").collect()  # noqa: S608
            expected = [
                Row(id=0, payload='{"x":0}'),
                Row(id=1, payload=None),
                Row(id=2, payload='{"x":2}'),
                Row(id=3, payload=None),
            ]
            query = f"SELECT id, CAST(v AS STRING) AS payload FROM {target} ORDER BY id"  # noqa: S608
            assert delta_jvm_spark.sql(query).collect() == expected
        assert spark.sql(f"SELECT id, CAST(v AS STRING) AS payload FROM {name} ORDER BY id").collect() == expected  # noqa: S608
        assert _effective(path, "protocol") == initial
        assert _effective(path, "metaData")["schemaString"] == initial_metadata["schemaString"]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


@pytest.mark.integration
@pytest.mark.parametrize("shape", ["array", "map", "struct", "binary"])
def test_nested_variant_append_and_reopen(spark, delta_jvm_spark, tmp_path, shape):
    import shutil

    types = {
        "array": "ARRAY<VARIANT>",
        "map": "MAP<STRING, VARIANT>",
        "struct": "STRUCT<inner:STRUCT<payload:VARIANT>>",
        "binary": "STRUCT<metadata:BINARY,value:BINARY>",
    }
    wrappers = {
        "array": "array({value})",
        "map": "map('key', {value})",
        "struct": "named_struct('inner', named_struct('payload', {value}))",
    }
    access = {"array": "v[0]", "map": "v['key']", "struct": "v.inner.payload"}
    data_type = types[shape]
    expressions = [f"CAST(NULL AS {data_type})"]
    if shape == "binary":
        expressions += [
            "named_struct('metadata', CAST(NULL AS BINARY), 'value', CAST(NULL AS BINARY))",
            "named_struct('metadata', X'01', 'value', X'02')",
        ]
        projection = "v IS NULL AS container_null, hex(v.metadata) AS metadata, hex(v.value) AS value"
    else:
        expressions += [
            wrappers[shape].format(value=value)
            for value in [
                "CAST(NULL AS VARIANT)",
                "parse_json('null')",
                "parse_json('\"hello\"')",
                "parse_json('{\"x\":2}')",
            ]
        ]
        value = access[shape]
        projection = (
            f"v IS NULL AS container_null, {value} IS NULL AS sql_null, "
            f"is_variant_null({value}) AS json_null, CAST({value} AS STRING) AS text, "
            f"to_json({value}) AS json"
        )
    path = tmp_path / "nested"
    target = f"delta.`{path}`"
    properties = "" if shape == "binary" else " TBLPROPERTIES ('delta.feature.variantType-preview'='supported')"
    with classic_spark_mode():
        delta_jvm_spark.sql(f"CREATE TABLE {target} (id BIGINT, v {data_type}) USING delta{properties}").collect()
        for index, expression in enumerate(expressions):
            delta_jvm_spark.sql(f"INSERT INTO {target} SELECT CAST({index} AS BIGINT), {expression}").collect()  # noqa: S608
        reference_schema = delta_jvm_spark.read.format("delta").load(str(path)).schema
        expected = delta_jvm_spark.sql(f"SELECT id, {projection} FROM {target} ORDER BY id").collect()  # noqa: S608
    if shape != "binary":
        assert [row.text for row in expected] == [None, None, None, "hello", '{"x":2}']
        assert [row.json for row in expected] == [None, None, "null", '"hello"', '{"x":2}']
        assert [row.sql_null for row in expected] == [True, True, False, False, False]
        assert [row.json_null for row in expected] == [False, False, True, False, False]
    initial_protocol = _effective(path, "protocol")
    assert spark.read.format("delta").load(str(path)).schema == reference_schema
    assert spark.sql(f"SELECT id, {projection} FROM {target} ORDER BY id").collect() == expected  # noqa: S608
    for index, expression in enumerate(expressions, start=len(expressions)):
        spark.sql(f"SELECT CAST({index} AS BIGINT) AS id, {expression} AS v").write.format("delta").mode("append").save(  # noqa: S608
            str(path)
        )
    expected += [Row(**{**row.asDict(), "id": row.id + len(expressions)}) for row in expected]
    reopened = tmp_path / "reopened"
    shutil.copytree(path, reopened)
    for location in [path, reopened]:
        assert _effective(location, "protocol") == initial_protocol
        assert spark.read.format("delta").load(str(location)).schema == reference_schema
        assert spark.sql(f"SELECT id, {projection} FROM delta.`{location}` ORDER BY id").collect() == expected  # noqa: S608
        with classic_spark_mode():
            assert delta_jvm_spark.read.format("delta").load(str(location)).schema == reference_schema
            assert (
                delta_jvm_spark.sql(
                    f"SELECT id, {projection} FROM delta.`{location}` ORDER BY id"  # noqa: S608
                ).collect()
                == expected
            )
