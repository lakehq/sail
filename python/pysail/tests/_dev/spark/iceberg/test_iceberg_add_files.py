# [CREDIT]: https://github.com/apache/iceberg/blob/6976e020b894f6a6777704df2b8c4458cb291ae9/spark/v4.0/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestAddFilesProcedure.java#L84
# ruff: noqa: S608
import hashlib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql.types import LongType, StructField, StructType

from pysail.testing._dev.spark.steps.iceberg import _find_latest_metadata
from pysail.testing.spark.utils.sql import escape_sql_string_literal


def _add(spark, name, source, arguments=""):
    frame = spark.sql(
        f"CALL system.add_files(table => '{name}', source_table => '{escape_sql_string_literal(source)}'{arguments})"
    )
    assert frame.schema == StructType(
        [StructField("added_files_count", LongType(), False), StructField("changed_partition_count", LongType(), True)]
    )
    return tuple(frame.first())


@pytest.mark.parametrize("version", [1, 2, 3])
@pytest.mark.parametrize("partitioned", [False, True])
def test_add_files_import_duplicates_and_reopen(spark, tmp_path, version, partitioned):
    source = tmp_path / "source"
    source.mkdir()
    for i, p in enumerate(["a", "b"]):
        directory = source / f"p={p}" if partitioned else source
        directory.mkdir(exist_ok=True)
        values = {"id": [i], "value": [f"row-{i}"]}
        if not partitioned:
            values["p"] = [p]
        pq.write_table(pa.table(values), directory / f"{i}.parquet")
    before = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in source.rglob("*.parquet")}
    name = "add_files_target"
    path = tmp_path / "target"
    partition = "PARTITIONED BY (p)" if partitioned else ""
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, value STRING, p STRING) USING iceberg {partition} "
        f"LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES ('format-version'='{version}')"
    )
    try:
        source_name = f"parquet.`{source}`"
        changed = (2 if partitioned else 1) if version == 1 else None
        assert _add(spark, name, source_name) == (2, changed)
        assert [tuple(row) for row in spark.table(name).orderBy("id").collect()] == [
            (0, "row-0", "a"),
            (1, "row-1", "b"),
        ]
        metadata = _find_latest_metadata(path)
        assert "schema.name-mapping.default" in metadata["properties"]
        with pytest.raises(Exception, match="already referenced"):
            _add(spark, name, source_name)
        assert _find_latest_metadata(path) == metadata
        assert _add(spark, name, source_name, ", check_duplicate_files => false, parallelism => 2") == (
            2,
            changed,
        )
        assert spark.read.format("iceberg").load(str(path)).count() == 2 * len(before)
        assert {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in source.rglob("*.parquet")} == before
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_add_files_empty_and_invalid_parallelism(spark, tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    path = tmp_path / "target"
    spark.sql(
        f"CREATE TABLE add_files_empty (id BIGINT) USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'"
    )
    try:
        assert _add(spark, "add_files_empty", f"parquet.`{source}`") == (0, 0)
        assert spark.table("add_files_empty").count() == 0
        before = _find_latest_metadata(path)
        for parallelism in [0, -1]:
            with pytest.raises(Exception, match="Parallelism should be larger than 0"):
                _add(spark, "add_files_empty", f"parquet.`{source}`", f", parallelism => {parallelism}")
            assert _find_latest_metadata(path) == before
    finally:
        spark.sql("DROP TABLE IF EXISTS add_files_empty")


@pytest.mark.parametrize("catalog_source", [False, True])
@pytest.mark.parametrize("filter_value", [None, "a", "__HIVE_DEFAULT_PARTITION__"])
def test_add_files_partition_filter(spark, tmp_path, catalog_source, filter_value):
    source = tmp_path / "source"
    for i, value in enumerate(["a", "b", "__HIVE_DEFAULT_PARTITION__"]):
        directory = source / f"p={value}"
        directory.mkdir(parents=True)
        pq.write_table(pa.table({"id": [i]}), directory / "data.parquet")
    name = "add_files_filtered"
    path = tmp_path / "target"
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, p STRING) USING iceberg PARTITIONED BY (p) "
        f"LOCATION '{escape_sql_string_literal(str(path))}'"
    )
    if catalog_source:
        spark.sql(
            "CREATE TABLE add_files_source (id BIGINT, p STRING) USING parquet PARTITIONED BY (p) "
            f"LOCATION '{escape_sql_string_literal(str(source))}'"
        )
    try:
        source_name = "add_files_source" if catalog_source else f"parquet.`{source}`"
        arguments = "" if filter_value is None else f", partition_filter => map('p', '{filter_value}')"
        assert _add(spark, name, source_name, arguments) == ((3 if filter_value is None else 1), None)
        expected = [(0, "a"), (1, "b"), (2, None)]
        if filter_value is not None:
            expected = [expected[0 if filter_value == "a" else 2]]
        assert [tuple(row) for row in spark.table(name).orderBy("id").collect()] == expected
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        spark.sql("DROP TABLE IF EXISTS add_files_source")


@pytest.mark.parametrize("columns", [{"id": [1], "extra": [7]}, {"value": ["x"]}])
def test_add_files_schema_missing_and_extra_columns(spark, tmp_path, columns):
    source = tmp_path / "source"
    source.mkdir()
    pq.write_table(pa.table(columns), source / "data.parquet")
    name = "add_files_schema"
    spark.sql(
        f"CREATE TABLE {name} (value STRING, id BIGINT) USING iceberg "
        f"LOCATION '{escape_sql_string_literal(str(tmp_path / 'target'))}'"
    )
    try:
        assert _add(spark, name, f"parquet.`{source}`") == (1, None)
        assert tuple(spark.table(name).first()) == (columns.get("value", [None])[0], columns.get("id", [None])[0])
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_add_files_historical_partition_spec(spark, tmp_path):
    source = tmp_path / "source"
    (source / "p=a").mkdir(parents=True)
    pq.write_table(pa.table({"id": [1], "value": ["x"]}), source / "p=a" / "data.parquet")
    name = "add_files_evolved"
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, value STRING, p STRING) USING iceberg PARTITIONED BY (p) "
        f"LOCATION '{escape_sql_string_literal(str(tmp_path / 'target'))}'"
    )
    try:
        spark.sql(f"ALTER TABLE {name} ADD PARTITION FIELD value")
        assert _add(spark, name, f"parquet.`{source}`") == (1, None)
        assert spark.table(name).first().asDict() == {"id": 1, "value": "x", "p": "a"}
        spark.sql(f"INSERT INTO {name} (id, value, p) VALUES (2, 'y', 'b')")
        assert [tuple(row) for row in spark.table(name).select("id", "value", "p").orderBy("id").collect()] == [
            (1, "x", "a"),
            (2, "y", "b"),
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def _write_source_file(path, file_format, rows):
    from pyarrow import orc
    from pyiceberg.avro.file import AvroOutputFile
    from pyiceberg.io.pyarrow import PyArrowFileIO
    from pyiceberg.schema import Schema
    from pyiceberg.typedef import Record
    from pyiceberg.types import LongType as IcebergLongType
    from pyiceberg.types import NestedField, StringType

    path.parent.mkdir(parents=True, exist_ok=True)
    names = ["id", "value", "p"][: len(rows[0])]
    if file_format == "avro":
        fields = [
            NestedField(i, name, IcebergLongType() if name == "id" else StringType()) for i, name in enumerate(names, 1)
        ]
        with AvroOutputFile(PyArrowFileIO().new_output(str(path)), Schema(*fields), "source") as writer:
            writer.write_block([Record(*row) for row in rows])
    else:
        table = pa.Table.from_pylist([dict(zip(names, row, strict=True)) for row in rows])
        if file_format == "orc":
            orc.write_table(table, path)
        else:
            pq.write_table(table, path)


@pytest.mark.parametrize("file_format", ["avro", "orc"])
@pytest.mark.parametrize("partitioned", [False, True])
def test_add_files_source_formats(spark, tmp_path, file_format, partitioned):
    source = tmp_path / "source"
    if partitioned:
        for part, row in [("a", (1, "x")), ("__HIVE_DEFAULT_PARTITION__", (2, "y"))]:
            _write_source_file(source / f"p={part}" / f"data.{file_format}", file_format, [row])
    else:
        _write_source_file(source / f"data.{file_format}", file_format, [(1, "x", "a"), (2, "y", None)])
    name = "add_files_format"
    partition = "PARTITIONED BY (p)" if partitioned else ""
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, value STRING, p STRING) USING iceberg {partition} "
        f"LOCATION '{escape_sql_string_literal(str(tmp_path / 'target'))}'"
    )
    try:
        count = 2 if partitioned else 1
        assert _add(spark, name, f"{file_format}.`{source}`") == (count, None)
        expected = [(1, "x", "a"), (2, "y", None)]
        assert [tuple(row) for row in spark.table(name).select("id", "value", "p").orderBy("id").collect()] == expected
        assert [row.value for row in spark.table(name).where("id = 2").select("value").collect()] == ["y"]
        spark.sql(f"INSERT INTO {name} (id, value, p) VALUES (3, 'z', 'b')")
        assert [tuple(row) for row in spark.table(name).select("id", "value", "p").orderBy("id").collect()] == [
            *expected,
            (3, "z", "b"),
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize(
    ("partition_type", "partition_value", "expected"),
    [
        ("INT", "7", 7),
        ("STRING", "01", "01"),
        ("STRING", "%E4%B8%AD%E6%96%87%2F%25", "中文/%"),
        ("INT", "__HIVE_DEFAULT_PARTITION__", None),
    ],
)
def test_add_files_partition_value_types(spark, tmp_path, partition_type, partition_value, expected):
    source = tmp_path / "source"
    directory = source / f"p={partition_value}"
    directory.mkdir(parents=True)
    pq.write_table(pa.table({"id": [1]}), directory / "data.parquet")
    name = "add_files_partition_type"
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, p {partition_type}) USING iceberg PARTITIONED BY (p) "
        f"LOCATION '{escape_sql_string_literal(str(tmp_path / 'target'))}'"
    )
    try:
        assert _add(spark, name, f"parquet.`{source}`") == (1, None)
        assert spark.table(name).first().asDict() == {"id": 1, "p": expected}
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("partitioned", [False, True])
def test_add_files_delete_then_import_again(spark, tmp_path, partitioned):
    source = tmp_path / "source"
    directory = source / "p=a" if partitioned else source
    directory.mkdir(parents=True)
    values = {"id": [1]}
    if not partitioned:
        values["p"] = ["a"]
    pq.write_table(pa.table(values), directory / "data.parquet")
    name = "add_files_reimport"
    partition = "PARTITIONED BY (p)" if partitioned else ""
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, p STRING) USING iceberg {partition} "
        f"LOCATION '{escape_sql_string_literal(str(tmp_path / 'target'))}'"
    )
    try:
        assert _add(spark, name, f"parquet.`{source}`") == (1, None)
        spark.sql(f"DELETE FROM {name} WHERE id = 1")
        assert spark.table(name).count() == 0
        assert (directory / "data.parquet").exists()
        assert _add(spark, name, f"parquet.`{source}`") == (1, None)
        assert spark.table(name).first().asDict() == {"id": 1, "p": "a"}
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize(
    ("source_partitioned", "target_partitioned", "arguments", "error"),
    [
        (True, False, "", "partition spec"),
        (False, True, "", "partition spec"),
        (False, False, ", partition_filter => map('p', 'a')", "partition"),
        (True, True, ", partition_filter => map('wrong', 'a')", "not partitioned"),
        (True, True, ", partition_filter => map('p', 'missing')", "matching partitions"),
    ],
)
def test_add_files_invalid_partition_import_preserves_state(
    spark, tmp_path, source_partitioned, target_partitioned, arguments, error
):
    source = tmp_path / "source"
    directory = source / "p=a" if source_partitioned else source
    directory.mkdir(parents=True)
    pq.write_table(pa.table({"id": [1]}), directory / "data.parquet")
    name = "add_files_invalid"
    path = tmp_path / "target"
    partition = "PARTITIONED BY (p)" if target_partitioned else ""
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, p STRING) USING iceberg {partition} LOCATION '{escape_sql_string_literal(str(path))}'"
    )
    try:
        before = _find_latest_metadata(path)
        with pytest.raises(Exception, match=error):
            _add(spark, name, f"parquet.`{source}`", arguments)
        assert _find_latest_metadata(path) == before
        assert spark.table(name).count() == 0
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("file_format", ["avro", "orc", "parquet"])
def test_add_files_single_file_source(spark, tmp_path, file_format):
    source = tmp_path / f"input.{file_format}"
    _write_source_file(source, file_format, [(1, "x", "a"), (2, "y", None)])
    name = "add_files_single"
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, value STRING, p STRING) USING iceberg LOCATION '{escape_sql_string_literal(str(tmp_path / 'target'))}'"
    )
    try:
        assert _add(spark, name, f"{file_format}.`{source}`") == (1, None)
        assert [tuple(row) for row in spark.table(name).orderBy("id").collect()] == [(1, "x", "a"), (2, "y", None)]
        assert [row.record_count for row in spark.table(f"{name}.files").collect()] == [2]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("filter_expression", ["map('day', '2021-01-01')", "map('day', '2021-01-01', 'dept', '01')"])
def test_add_files_date_and_string_composite_partition(spark, tmp_path, filter_expression):
    import datetime

    source = tmp_path / "source"
    for day, value in [("2021-01-01", 1), ("2021-01-02", 2)]:
        directory = source / f"day={day}" / "dept=01"
        directory.mkdir(parents=True)
        pq.write_table(pa.table({"id": [value]}), directory / "data.parquet")
    name = "add_files_composite"
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, day DATE, dept STRING) USING iceberg PARTITIONED BY (day, dept) LOCATION '{escape_sql_string_literal(str(tmp_path / 'target'))}'"
    )
    try:
        assert _add(spark, name, f"parquet.`{source}`", f", partition_filter => {filter_expression}") == (1, None)
        assert spark.table(name).first().asDict() == {"id": 1, "day": datetime.date(2021, 1, 1), "dept": "01"}
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_add_files_empty_partitioned_catalog_source(spark, tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    spark.sql(
        f"CREATE TABLE add_files_empty_source (id BIGINT, p STRING) USING parquet PARTITIONED BY (p) LOCATION '{escape_sql_string_literal(str(source))}'"
    )
    spark.sql(
        f"CREATE TABLE add_files_empty_target (id BIGINT, p STRING) USING iceberg PARTITIONED BY (p) LOCATION '{escape_sql_string_literal(str(tmp_path / 'target'))}'"
    )
    try:
        assert _add(
            spark, "add_files_empty_target", "add_files_empty_source", ", partition_filter => map('p', 'missing')"
        ) == (0, 0)
        assert spark.table("add_files_empty_target").count() == 0
    finally:
        spark.sql("DROP TABLE IF EXISTS add_files_empty_source")
        spark.sql("DROP TABLE IF EXISTS add_files_empty_target")


def test_add_files_mixed_case_partition_filter(spark, tmp_path):
    source = tmp_path / "source"
    directory = source / "name=John%20Doe"
    directory.mkdir(parents=True)
    pq.write_table(pa.table({"id": [1]}), directory / "data.parquet")
    spark.sql(
        f"CREATE TABLE add_files_case_source (id BIGINT, name STRING) USING parquet PARTITIONED BY (name) LOCATION '{escape_sql_string_literal(str(source))}'"
    )
    spark.sql(
        f"CREATE TABLE add_files_case_target (id BIGINT, `naMe` STRING) USING iceberg PARTITIONED BY (`naMe`) LOCATION '{escape_sql_string_literal(str(tmp_path / 'target'))}'"
    )
    try:
        assert _add(
            spark, "add_files_case_target", "add_files_case_source", ", partition_filter => map('naMe', 'John Doe')"
        ) == (1, None)
        assert spark.table("add_files_case_target").first().asDict() == {"id": 1, "naMe": "John Doe"}
    finally:
        spark.sql("DROP TABLE IF EXISTS add_files_case_source")
        spark.sql("DROP TABLE IF EXISTS add_files_case_target")


@pytest.mark.parametrize("version", [1, 2, 3])
def test_add_files_dropped_partition_spec(spark, tmp_path, version):
    source = tmp_path / "source"
    (source / "p=a").mkdir(parents=True)
    pq.write_table(pa.table({"id": [1], "q": ["x"]}), source / "p=a" / "data.parquet")
    name = "add_files_dropped_spec"
    path = tmp_path / "target"
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, p STRING, q STRING) USING iceberg PARTITIONED BY (p, q) LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES ('format-version'='{version}')"
    )
    try:
        spark.sql(f"ALTER TABLE {name} DROP PARTITION FIELD q")
        spark.sql(f"ALTER TABLE {name} ADD PARTITION FIELD q")
        before = _find_latest_metadata(path)
        if version == 1:
            with pytest.raises(Exception, match="partition spec"):
                _add(spark, name, f"parquet.`{source}`")
            assert _find_latest_metadata(path) == before
        else:
            assert _add(spark, name, f"parquet.`{source}`") == (1, None)
            assert spark.table(name).first().asDict() == {"id": 1, "p": "a", "q": "x"}
            compatible = next(spec["spec-id"] for spec in before["partition-specs"] if len(spec["fields"]) == 1)
            assert {row.spec_id for row in spark.table(f"{name}.files").collect()} == {compatible}
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("inheritance", [False, True])
def test_add_files_repeated_partition_import_with_snapshot_inheritance(spark, tmp_path, inheritance):
    import uuid

    from pysail.testing._dev.spark.steps.iceberg import _current_manifest_list

    source = tmp_path / "source"
    for value in [1, 2]:
        directory = source / f"p={value}"
        directory.mkdir(parents=True)
        pq.write_table(pa.table({"id": [value]}), directory / "data.parquet")
    name = "add_files_inheritance"
    path = tmp_path / "target"
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, p INT) USING iceberg PARTITIONED BY (p) LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES ('format-version'='1', 'compatibility.snapshot-id-inheritance.enabled'='{str(inheritance).lower()}')"
    )
    try:
        for value in [1, 2]:
            assert _add(
                spark, name, f"parquet.`{source}`", f", partition_filter => map('p', '{value}'), parallelism => 2"
            ) == (1, None if inheritance else 1)
        assert [tuple(row) for row in spark.table(name).select("id", "p").orderBy("id").collect()] == [(1, 1), (2, 2)]
        for manifest in _current_manifest_list(_find_latest_metadata(path))["manifests"]:
            assert manifest["partition-spec-id"] == 0
            uuid.UUID(manifest["manifest-path"].rsplit("/", 1)[-1].removeprefix("add-files-").removesuffix(".avro"))
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
