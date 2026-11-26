# SPDX-License-Identifier: Apache-2.0
# ruff: noqa: S608

import json
import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql.types import StringType, StructField, StructType

from pysail.testing._dev.spark.steps.iceberg import _find_latest_metadata
from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests._dev.spark.iceberg.utils import _local_file_path


def _blobs(file):
    content = _local_file_path(file["statistics-path"]).read_bytes()
    assert len(content) == file["file-size-in-bytes"]
    start = len(content) - file["file-footer-size-in-bytes"]
    assert content[:4] == content[start : start + 4] == content[-4:] == b"PFA1"
    assert struct.unpack("<ii", content[-12:-4]) == (len(content) - start - 16, 0)
    blobs = json.loads(content[start + 4 : -12])["blobs"]
    assert [{k: v for k, v in b.items() if k not in {"length", "offset", "compression-codec"}} for b in blobs] == file[
        "blob-metadata"
    ]
    sketches = {}
    for blob in blobs:
        data = content[blob["offset"] : blob["offset"] + blob["length"]]
        with pa.CompressedInputStream(pa.BufferReader(data), blob["compression-codec"]) as stream:
            sketches[blob["fields"][0]] = (int(blob["properties"]["ndv"]), stream.read())
    return sketches


def test_table_stats_snapshot_schema_and_sketches(spark, tmp_path):
    # Apache Iceberg 1.12.0 TestComputeTableStatsProcedure and TestComputeTableStatsAction.
    name = "ndv_stats"
    path = tmp_path / name
    spark.sql(f"""CREATE TABLE {name} (id INT, p STRING, s STRUCT<x:INT>)
        USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'""")
    call = f"CALL system.compute_table_stats('{name}')"
    try:
        empty = spark.sql(call)
        assert empty.schema == StructType([StructField("statistics_file", StringType(), True)])
        assert empty.collect() == []
        spark.sql(f"""INSERT INTO {name} VALUES (1,'a',NULL), (1,'a',named_struct('x',4)),
            (2,'b',named_struct('x',4)), (3,'',NULL), (4,NULL,named_struct('x',5))""")
        old = _find_latest_metadata(path)["current-snapshot-id"]
        result = spark.sql(call).first()
        file = _find_latest_metadata(path)["statistics"][0]
        assert file["statistics-path"] == result.statistics_file
        assert file["snapshot-id"] == old
        blobs = _blobs(file)
        assert {k: v[0] for k, v in blobs.items()} == {1: 4, 2: 2}
        # Retained hashes of the ordered compact sketches from Spark 4.1.3 / Iceberg 1.12.0.
        assert blobs[1][1][16:].hex() == "ab129f62d10914095d9caa2282e0f9241bc99e1a19099d7987c967fd15c6547f"
        assert blobs[2][1][16:].hex() == "857f40b689c7e53417c11d528507017b"
        spark.sql(f"ALTER TABLE {name} RENAME COLUMN id TO renamed")
        spark.sql(f"INSERT INTO {name} VALUES (9,'c',named_struct('x',6))")
        spark.sql(call).collect()
        before = _find_latest_metadata(path)
        latest = before["current-snapshot-id"]
        assert latest != old
        result = spark.sql(f"""CALL system.compute_table_stats(table => '{name}', snapshot_id => {old},
            columns => array('s.x','id','id'))""").first()
        after = _find_latest_metadata(path)
        assert after["snapshots"] == before["snapshots"]
        files = {f["snapshot-id"]: f for f in after["statistics"]}
        assert files[old]["statistics-path"] == result.statistics_file
        assert {k: v[0] for k, v in _blobs(files[old]).items()} == {4: 2, 1: 4}
        assert _blobs(files[latest])[1][0] == 5
        for args, error in [
            ("snapshot_id => -42", "Snapshot not found"),
            ("columns => array('id')", "Can't find column"),
            ("columns => array('s')", "non-primitive"),
            ("columns => array()", "null/empty"),
        ]:
            with pytest.raises(Exception, match=error):
                spark.sql(f"CALL system.compute_table_stats(table => '{name}', {args})").collect()
        assert _find_latest_metadata(path) == after
        with pytest.raises(Exception, match="Table must be partitioned"):
            spark.sql(f"CALL system.compute_partition_stats('{name}')").collect()
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("version", [2, 3])
def test_partition_stats_full_incremental_and_reuse(spark, tmp_path, version):
    name = "partition_stats"
    path = tmp_path / name
    spark.sql(f"""CREATE TABLE {name} (id BIGINT, p INT) USING iceberg PARTITIONED BY (p)
        LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES ('format-version'='{version}', 'write.delete.mode'='merge-on-read')""")
    call = f"CALL system.compute_partition_stats('{name}')"
    try:
        empty = spark.sql(call)
        assert empty.schema == StructType([StructField("partition_statistics_file", StringType(), True)])
        assert empty.collect() == []
        spark.sql(f"INSERT INTO {name} VALUES (1, 1), (2, 1), (3, NULL)")
        old = _find_latest_metadata(path)["current-snapshot-id"]
        spark.sql(f"INSERT INTO {name} VALUES (4, 1), (5, 2)")
        before = _find_latest_metadata(path)
        old_path = spark.sql(f"CALL system.compute_partition_stats('{name}', {old})").first()[0]
        current_path = spark.sql(call).first()[0]
        assert spark.sql(call).first()[0] == current_path
        after = _find_latest_metadata(path)
        assert after["snapshots"] == before["snapshots"]
        latest = after["current-snapshot-id"]
        assert {f["snapshot-id"]: f["statistics-path"] for f in after["partition-statistics"]} == {
            old: old_path,
            latest: current_path,
        }
        for file in after["partition-statistics"]:
            local = _local_file_path(file["statistics-path"])
            assert local.stat().st_size == file["file-size-in-bytes"]
            table = pq.read_table(local)
            fields = list(table.schema)
            assert [int(f.metadata[b"PARQUET:field_id"]) for f in fields] == list(range(1, len(fields) + 1))
            assert int(fields[0].type[0].metadata[b"PARQUET:field_id"]) == 1000
            rows = table.to_pylist()
            expected = (
                [(None, 1, old), (1, 2, old)]
                if file["snapshot-id"] == old
                else [(None, 1, old), (1, 3, latest), (2, 1, latest)]
            )
            assert [
                (r["partition"]["p"], r["data_record_count"], r["last_updated_snapshot_id"]) for r in rows
            ] == expected
            assert all(r["total_record_count"] is None and r["total_data_file_size_in_bytes"] > 0 for r in rows)
            assert ("dv_count" in table.column_names) == (version == 3)
        spark.sql(f"DELETE FROM {name} WHERE id = 1")
        deleted = pq.read_table(_local_file_path(spark.sql(call).first()[0])).to_pylist()
        one = next(row for row in deleted if row["partition"]["p"] == 1)
        assert one["data_record_count"] == 3
        assert one["position_delete_record_count"] == 1
        assert one["position_delete_file_count"] == (1 if version == 2 else 0)
        assert one.get("dv_count", 0) == (1 if version == 3 else 0)
        spark.sql(f"CALL system.compute_table_stats('{name}')").collect()
        assert _blobs(_find_latest_metadata(path)["statistics"][0])[1][0] == 4
        with pytest.raises(Exception, match="Snapshot not found"):
            spark.sql(f"CALL system.compute_partition_stats('{name}', -42)").collect()
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_partition_stats_across_overwrite_then_append(spark, tmp_path):
    name = "replaced_partition_stats"
    path = tmp_path / name
    spark.sql(f"""CREATE TABLE {name} (id BIGINT, p INT) USING iceberg PARTITIONED BY (p)
        LOCATION '{escape_sql_string_literal(str(path))}'""")
    call = f"CALL system.compute_partition_stats('{name}')"
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1, 1), (2, 2)")
        spark.sql(call).collect()
        spark.sql(f"DELETE FROM {name} WHERE p = 1")
        removed = _find_latest_metadata(path)["current-snapshot-id"]
        spark.sql(f"INSERT INTO {name} VALUES (3, 2)")
        current_path = _local_file_path(spark.sql(call).first()[0])
        rows = pq.read_table(current_path).to_pylist()
        assert [(r["partition"]["p"], r["data_record_count"]) for r in rows] == [(1, 0), (2, 2)]
        assert rows[0]["data_file_count"] == rows[0]["total_data_file_size_in_bytes"] == 0
        assert rows[0]["last_updated_snapshot_id"] == removed
        # Iceberg falls back to the target snapshot's manifests when the ancestor file is unavailable.
        current_path.unlink()
        spark.sql(f"INSERT INTO {name} VALUES (4, 3)")
        rows = pq.read_table(_local_file_path(spark.sql(call).first()[0])).to_pylist()
        assert [(r["partition"]["p"], r["data_record_count"]) for r in rows if r["data_file_count"]] == [(2, 2), (3, 1)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_partition_stats_project_old_spec_by_field_id(spark, tmp_path):
    name = "evolved_partition_stats"
    path = tmp_path / name
    spark.sql(f"""CREATE TABLE {name} (id BIGINT, p INT) USING iceberg PARTITIONED BY (p)
        LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES ('format-version'='1')""")
    call = f"CALL system.compute_partition_stats('{name}')"
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1, 10)")
        spark.sql(call).collect()
        spark.sql(f"ALTER TABLE {name} REPLACE PARTITION FIELD p WITH p AS renamed")
        spark.sql(f"ALTER TABLE {name} ADD PARTITION FIELD id")
        spark.sql(f"INSERT INTO {name} VALUES (2, 10)")
        spec = _find_latest_metadata(path)["default-spec-id"]
        table = pq.read_table(_local_file_path(spark.sql(call).first()[0]))
        assert [(r["partition"], r["spec_id"], r["data_record_count"]) for r in table.to_pylist()] == [
            ({"renamed": 10, "id": None}, 0, 1),
            ({"renamed": 10, "id": 2}, spec, 1),
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_stats_empty_branch_and_invalid_table(spark, tmp_path):
    name = "empty_branch_stats"
    spark.sql(
        f"CREATE TABLE {name} (p INT) USING iceberg PARTITIONED BY (p) LOCATION '{escape_sql_string_literal(str(tmp_path))}'"
    )
    try:
        spark.sql(f"ALTER TABLE {name} CREATE BRANCH audit")
        metadata = _find_latest_metadata(tmp_path)
        branch = metadata["refs"]["audit"]["snapshot-id"]
        for procedure in ("compute_table_stats", "compute_partition_stats"):
            assert spark.sql(f"CALL system.{procedure}('{name}')").collect() == []
            with pytest.raises(Exception, match=r"(?i)(not found|does not exist|unknown table)"):
                spark.sql(f"CALL system.{procedure}('no_such_stats_table')").collect()
        assert spark.sql(f"CALL system.compute_partition_stats('{name}', {branch})").collect() == []
        assert _find_latest_metadata(tmp_path) == metadata
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_table_stats_primitive_types(spark, tmp_path):
    # TestComputeTableStatsAction exercises each type with random values.
    # Repeated and null inputs give every generated blob an independently known NDV.
    name = "typed_ndv_stats"
    types = [
        "TINYINT",
        "SMALLINT",
        "INT",
        "BIGINT",
        "TIMESTAMP",
        "TIMESTAMP_NTZ",
        "DATE",
        "DECIMAL(18,3)",
        "BINARY",
        "BOOLEAN",
        "FLOAT",
        "DOUBLE",
    ]
    fields = ", ".join(f"c{i} {t}" for i, t in enumerate(types))
    spark.sql(f"CREATE TABLE {name} ({fields}) USING iceberg LOCATION '{escape_sql_string_literal(str(tmp_path))}'")
    try:
        values = []
        for n in (1, 2, 2):
            row = [str(n)] * 4 + [
                f"TIMESTAMP '2020-01-0{n} 01:02:03'",
                f"TIMESTAMP_NTZ '2020-01-0{n} 01:02:03'",
                f"DATE '2020-01-0{n}'",
                f"{n}.123",
                f"X'0{n}'",
                str(n == 1).upper(),
                str(n),
                str(n),
            ]
            values.append("(" + ",".join(row) + ")")
        values.append("(" + ",".join(["NULL"] * len(types)) + ")")
        spark.sql(f"INSERT INTO {name} VALUES {', '.join(values)}")
        spark.sql(f"CALL system.compute_table_stats('{name}')").collect()
        assert {i: v[0] for i, v in _blobs(_find_latest_metadata(tmp_path)["statistics"][0]).items()} == dict.fromkeys(
            range(1, len(types) + 1), 2
        )
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_table_stats_variant_and_no_primitive_columns(spark, tmp_path):
    for name, fields, data in [
        ("variant_stats", "id INT, v VARIANT", "2, parse_json('123')"),
        ("complex_stats", "s STRUCT<x:INT>", "named_struct('x',1)"),
    ]:
        directory = tmp_path / name
        spark.sql(
            f"CREATE TABLE {name} ({fields}) USING iceberg LOCATION '{escape_sql_string_literal(str(directory))}' TBLPROPERTIES ('format-version'='3')"
        )
        try:
            spark.sql(f"INSERT INTO {name} SELECT {data}")
            if name == "variant_stats":
                spark.sql(f"CALL system.compute_table_stats('{name}')").collect()
                assert list(_blobs(_find_latest_metadata(directory)["statistics"][0])) == [1]
                with pytest.raises(Exception, match="non-primitive"):
                    spark.sql(f"CALL system.compute_table_stats('{name}', columns => array('v'))").collect()
            else:
                with pytest.raises(Exception, match="No columns found"):
                    spark.sql(f"CALL system.compute_table_stats('{name}')").collect()
        finally:
            spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("directory", ["stats files", "stats%20files"])
def test_stats_custom_metadata_path_and_avro_ancestor(spark, tmp_path, directory):
    name = "avro_partition_stats"
    path, metadata = tmp_path / "table", tmp_path / directory
    spark.sql(
        f"CREATE TABLE {name} (id INT, p INT) USING iceberg PARTITIONED BY (p) LOCATION '{escape_sql_string_literal(str(path))}'"
    )
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1,10),(2,10)")
        spark.sql(
            f"ALTER TABLE {name} SET TBLPROPERTIES ('write.format.default'='avro', 'write.metadata.path'='{escape_sql_string_literal(str(metadata))}')"
        )
        ndv = _local_file_path(spark.sql(f"CALL system.compute_table_stats('{name}')").first()[0])
        file = _local_file_path(spark.sql(f"CALL system.compute_partition_stats('{name}')").first()[0])
        assert ndv.parent == file.parent == metadata
        assert file.suffix == ".avro"
        assert file.read_bytes()[:4] == b"Obj\x01"
        first = spark.read.format("avro").load(str(file)).first()
        assert first.partition.p == 10
        assert first.data_record_count == 2
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('write.format.default'='parquet')")
        spark.sql(f"INSERT INTO {name} VALUES (3,20)")
        second = pq.read_table(
            _local_file_path(spark.sql(f"CALL system.compute_partition_stats('{name}')").first()[0])
        ).to_pylist()
        assert [(row["partition"]["p"], row["data_record_count"]) for row in second] == [(10, 2), (20, 1)]
        assert second[0]["last_updated_snapshot_id"] == first.last_updated_snapshot_id
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_external_partition_stats_field_ids_and_optional_counts(spark, tmp_path):
    name = "external_partition_stats"
    spark.sql(
        f"CREATE TABLE {name} (id INT, p INT) USING iceberg PARTITIONED BY (p) LOCATION '{escape_sql_string_literal(str(tmp_path))}'"
    )
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1,10),(2,10),(3,20)")
        procedure = f"CALL system.compute_partition_stats('{name}')"
        file = _local_file_path(spark.sql(procedure).first()[0])
        original = pq.read_table(file)
        columns, arrays = [], []
        for field, array in zip(original.schema, original.columns, strict=True):
            if field.name in {"last_updated_at", "last_updated_snapshot_id"}:
                continue
            names = {
                "data_record_count": "total_data_file_size_in_bytes",
                "total_data_file_size_in_bytes": "data_record_count",
            }
            columns.append(field.with_name(names.get(field.name, field.name)))
            arrays.append(original["data_record_count"] if field.name == "total_record_count" else array)
        pq.write_table(pa.Table.from_arrays(arrays, schema=pa.schema(columns)), file)
        spark.sql(f"INSERT INTO {name} VALUES (4,10)")
        result = pq.read_table(_local_file_path(spark.sql(procedure).first()[0])).to_pylist()
        assert [(r["partition"]["p"], r["data_record_count"], r["total_record_count"]) for r in result] == [
            (10, 3, None),
            (20, 1, 1),
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize(
    ("size", "expected_ndv", "digest"),
    [
        (1, 1, "e81d5c85bb25b760d465b1bb9b3447003816889278347d08c35d4cdaea3f271f"),
        (4097, 4095, "c9c45f834675a4ef17463c429f5d2208ef999586d71691a690eb49595ac40b49"),
        (65536, 64631, "2167f54c1cb4d437dd639d6474e435ec6ec0c19fa2b29be0a9d1808ea233dc85"),
    ],
)
def test_alpha_sketch_matches_java(spark, tmp_path, size, expected_ndv, digest):
    import hashlib

    # Generated with the DataSketches Java Alpha builder bundled in Iceberg 1.12.0.
    spark.sql(
        f"CREATE TABLE alpha_stats (id BIGINT) USING iceberg LOCATION '{escape_sql_string_literal(str(tmp_path))}'"
    )
    try:
        spark.sql(f"SELECT id FROM range(0, {size}, 1, 1)").write.insertInto("alpha_stats")
        spark.sql("CALL system.compute_table_stats('alpha_stats')").collect()
        ndv, payload = _blobs(_find_latest_metadata(tmp_path)["statistics"][0])[1]
        assert ndv == expected_ndv
        assert hashlib.sha256(payload).hexdigest() == digest
    finally:
        spark.sql("DROP TABLE IF EXISTS alpha_stats")


def test_stats_with_equality_deletes(spark, sql_catalog):
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.typedef import Record
    from pyiceberg.types import LongType, NestedField

    from pysail.tests._dev.spark.iceberg.test_iceberg_equality_delete import _append_equality_delete_snapshot

    identifier = ("default", "equality_stats")
    spec = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="p"))
    table = sql_catalog.create_table(
        identifier,
        Schema(NestedField(1, "id", LongType(), required=False), NestedField(2, "p", LongType(), required=False)),
        partition_spec=spec,
    )
    path = _local_file_path(table.location())
    try:
        table.append(pa.table({"id": [1, 2, 3], "p": [1, 1, 2]}))
        _append_equality_delete_snapshot(table, pa.table({"id": [1]}), [1], partition=Record(1), partition_spec=spec)
        spark.sql(f"CREATE TABLE eq_stats USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'")
        stats = spark.sql("CALL system.compute_partition_stats('eq_stats')").first()[0]
        rows = pq.read_table(_local_file_path(stats)).to_pylist()
        assert [
            (
                r["partition"]["p"],
                r["data_record_count"],
                r["equality_delete_file_count"],
                r["equality_delete_record_count"],
            )
            for r in rows
        ] == [(1, 2, 1, 1), (2, 1, 0, 0)]
        spark.sql("CALL system.compute_table_stats('eq_stats')").collect()
        assert _blobs(_find_latest_metadata(path)["statistics"][0])[1][0] == 2
    finally:
        spark.sql("DROP TABLE IF EXISTS eq_stats")
        sql_catalog.drop_table(identifier)


@pytest.mark.parametrize("procedure", ["compute_table_stats", "compute_partition_stats"])
def test_stats_system_catalog(spark, procedure):
    with pytest.raises(Exception, match=r"(?i)(system|iceberg|unsupported)"):
        spark.sql(f"CALL system.{procedure}('system.session.sessions')").collect()


def test_partition_stats_on_diverging_branches(spark, tmp_path):
    # PartitionStatsHandlerTestBase.testLatestStatsFileWithBranch
    name = "branch_partition_stats"
    call = f"CALL system.compute_partition_stats('{name}')"
    spark.sql(
        f"CREATE TABLE {name} (id INT, p INT) USING iceberg PARTITIONED BY (p) LOCATION '{escape_sql_string_literal(str(tmp_path))}'"
    )
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1, 10)")
        base = _find_latest_metadata(tmp_path)["current-snapshot-id"]
        spark.sql(call).collect()
        spark.sql(f"INSERT INTO {name} VALUES (2, 20)")
        other = _find_latest_metadata(tmp_path)["current-snapshot-id"]
        other_path = spark.sql(call).first()[0]
        spark.sql(f"ALTER TABLE {name} CREATE BRANCH audit AS OF VERSION {other}")
        spark.sql(f"CALL system.set_current_snapshot('{name}', {base})").collect()
        spark.sql(f"INSERT INTO {name} VALUES (3, 30)")
        selected = _find_latest_metadata(tmp_path)["current-snapshot-id"]
        result = spark.sql(
            f"CALL system.compute_partition_stats(table => '{name}', snapshot_id => {selected})"
        ).first()[0]
        rows = pq.read_table(_local_file_path(result)).to_pylist()
        assert [(row["partition"]["p"], row["data_record_count"], row["last_updated_snapshot_id"]) for row in rows] == [
            (10, 1, base),
            (30, 1, selected),
        ]
        assert spark.sql(f"CALL system.compute_partition_stats('{name}', {other})").first()[0] == other_path
        assert [r["partition"]["p"] for r in pq.read_table(_local_file_path(other_path)).to_pylist()] == [10, 20]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
