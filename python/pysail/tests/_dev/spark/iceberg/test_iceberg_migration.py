# [CREDIT]: https://github.com/apache/iceberg/blob/6976e020b894f6a6777704df2b8c4458cb291ae9/spark/v4.1/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestMigrateTableProcedure.java
# [CREDIT]: https://github.com/apache/iceberg/blob/6976e020b894f6a6777704df2b8c4458cb291ae9/spark/v4.1/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestSnapshotTableProcedure.java

from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.avro.file import AvroFile
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyspark.errors import PySparkException
from pyspark.sql.types import LongType, StructField, StructType

from pysail.testing._dev.spark.steps.iceberg import _current_manifest_list, _find_latest_metadata
from pysail.testing.spark.utils.sql import escape_sql_string_literal


@pytest.mark.parametrize("procedure", ["migrate", "snapshot"])
@pytest.mark.parametrize("partitioned", [True, False])
def test_table_import(spark, tmp_path, procedure, partitioned):
    source = tmp_path / "source"
    data = source / "p=a" if partitioned else source
    data.mkdir(parents=True)
    file = data / "part.parquet"
    pq.write_table(pa.table({"id": [42]}), file)
    original = file.read_bytes()
    ddl = "(id BIGINT, p STRING) USING parquet PARTITIONED BY (p)" if partitioned else "(id BIGINT) USING parquet"
    spark.sql(f"CREATE TABLE src {ddl} LOCATION '{escape_sql_string_literal(str(source))}' TBLPROPERTIES ('x'='old')")
    try:
        path = tmp_path / "target" if procedure == "snapshot" else source
        args = (
            f"source_table => 'src', table => 'dest', location => '{escape_sql_string_literal(str(path))}'"
            if procedure == "snapshot"
            else "table => 'src'"
        )
        props = (
            "'x','new','snapshot','false','gc.enabled','true'"
            if procedure == "snapshot"
            else "'x','new','migrated','false'"
        )
        result = spark.sql(f"CALL system.{procedure}({args}, properties => map({props}), parallelism => 2)")
        field = "imported_files_count" if procedure == "snapshot" else "migrated_files_count"
        assert result.schema == StructType([StructField(field, LongType(), False)])
        assert tuple(result.first()) == (1,)
        target, other = ("dest", "src") if procedure == "snapshot" else ("src", "src_BACKUP_")
        expected = (42, "a") if partitioned else (42,)
        assert tuple(spark.table(target).first()) == expected
        assert tuple(spark.table(other).first()) == expected
        metadata = _find_latest_metadata(path)
        assert metadata["properties"]["x"] == "new"
        assert "schema.name-mapping.default" in metadata["properties"]
        if procedure == "snapshot":
            assert metadata["properties"]["gc.enabled"] == "false"
            assert metadata["properties"]["snapshot"] == "true"
        else:
            assert metadata["properties"]["migrated"] == "true"
        assert tuple(spark.read.format("iceberg").load(str(path)).first()) == expected
        new_row = "(99, 'b')" if partitioned else "(99)"
        spark.sql(f"INSERT INTO {target} VALUES {new_row}")  # noqa: S608
        assert spark.table(target).count() == 2
        assert spark.table(other).count() == 1
        if procedure == "snapshot":
            spark.sql("DROP TABLE dest")
            assert tuple(spark.table("src").first()) == expected
        assert file.read_bytes() == original
    finally:
        for table in ["dest", "src", "src_BACKUP_"]:
            spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_migration_collisions_and_empty_source(spark, tmp_path):
    source = tmp_path / "empty"
    source.mkdir()
    spark.sql(f"CREATE TABLE src (id INT) USING parquet LOCATION '{escape_sql_string_literal(str(source))}'")
    spark.sql("CREATE TABLE taken (v STRING) USING parquet")
    try:
        for overlap in [source, source / "sub", source.parent]:
            with pytest.raises(Exception, match="overlap"):
                spark.sql(f"CALL system.snapshot('src','dest','{escape_sql_string_literal(str(overlap))}')").collect()
        with pytest.raises(Exception, match="already exists"):
            spark.sql("CALL system.migrate(table => 'src', backup_table_name => 'taken')").collect()
        assert not (source / "metadata").exists()
        assert spark.table("src").columns == ["id"]
        assert spark.table("taken").columns == ["v"]
        assert tuple(spark.sql("CALL system.snapshot('src', 'dest')").first()) == (0,)
        assert tuple(spark.sql("CALL system.migrate(table => 'src', drop_backup => true)").first()) == (0,)
        assert not spark.catalog.tableExists("src_BACKUP_")
    finally:
        for table in ["src", "taken", "dest"]:
            spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("procedure", ["migrate", "snapshot"])
def test_import_failure_preserves_catalog_and_files(spark, tmp_path, procedure):
    source = tmp_path / "source"
    source.mkdir()
    bad = source / "part.parquet"
    bad.write_bytes(b"not a parquet file")
    spark.sql(f"CREATE TABLE src (id BIGINT) USING parquet LOCATION '{escape_sql_string_literal(str(source))}'")
    target = source if procedure == "migrate" else tmp_path / "dest"
    args = (
        "table => 'src', backup_table_name => 'original'"
        if procedure == "migrate"
        else f"source_table => 'src', table => 'dest', location => '{escape_sql_string_literal(str(target))}'"
    )
    try:
        for suffix, message in [
            (", parallelism => -1", "Parallelism"),
            (", parallelism => 0", "Parallelism"),
            (", properties => map('write.metadata.metrics.column.missing','full')", "Invalid metrics config"),
            (", ignore_missing_files => true", "[Pp]arquet|footer"),
        ]:
            with pytest.raises(Exception, match=message):
                spark.sql(f"CALL system.{procedure}({args}{suffix})").collect()
            assert spark.catalog.tableExists("src")
            assert not spark.catalog.tableExists("original")
            assert not spark.catalog.tableExists("dest")
            assert not (target / "metadata").exists()
        pq.write_table(pa.table({"id": [1]}), bad)
        assert tuple(spark.sql(f"CALL system.{procedure}({args})").first()) == (1,)
        if procedure == "migrate":
            assert spark.table("original").first().id == 1
    finally:
        for table in ["src", "original", "dest"]:
            spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("populated", [True, False])
def test_managed_source(spark, populated):
    spark.sql("CREATE TABLE managed_src (id BIGINT) USING parquet")
    try:
        if populated:
            spark.sql("INSERT INTO managed_src VALUES (42)")
        expected = int(populated)
        assert spark.sql("CALL system.snapshot('managed_src', 'managed_dest')").first()[0] == expected
        result = spark.sql(f"CALL system.migrate('managed_src', map('foo','bar'), {str(populated).lower()})")
        assert result.first()[0] == expected
        assert spark.catalog.tableExists("managed_src_BACKUP_") == (not populated)
        for table in ["managed_dest", "managed_src"]:
            assert spark.table(table).count() == expected
    finally:
        for table in ["managed_dest", "managed_src", "managed_src_BACKUP_"]:
            spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize(("procedure", "version"), [("snapshot", 1), ("snapshot", 2), ("migrate", 2)])
@pytest.mark.parametrize("partitioned", [True, False])
def test_multi_file_import(spark, tmp_path, procedure, version, partitioned):
    source = tmp_path / "source"
    partition = "PARTITIONED BY (id)" if partitioned else ""
    spark.sql(
        f"CREATE TABLE src (id BIGINT NOT NULL, data STRING) USING parquet {partition} LOCATION '{escape_sql_string_literal(str(source))}'"
    )
    try:
        for i, value in [(1, "a"), (2, "b")]:
            spark.sql(f"INSERT INTO src (id, data) VALUES ({i}, '{value}')")  # noqa: S608
        path = source if procedure == "migrate" else tmp_path / "snapshot"
        target = "src" if procedure == "migrate" else "dest"
        args = (
            "table => 'src'"
            if procedure == "migrate"
            else f"source_table => 'src', table => 'dest', location => '{escape_sql_string_literal(str(path))}'"
        )
        assert (
            spark.sql(
                f"CALL system.{procedure}({args}, properties => map('format-version','{version}'), parallelism => 2)"
            ).first()[0]
            == 2
        )
        expected = [("a", 1), ("b", 2)] if partitioned else [(1, "a"), (2, "b")]
        assert [tuple(row) for row in spark.table(target).orderBy("id").collect()] == expected
        metadata = _find_latest_metadata(path)
        assert metadata["format-version"] == version
        ids = []
        for manifest in _current_manifest_list(metadata)["manifests"]:
            with AvroFile(PyArrowFileIO().new_input(manifest["manifest-path"])) as reader:
                assert reader.header.meta["format-version"] == str(version)
                index = next(i for i, field in enumerate(reader.schema.fields) if field.field_id == 1)
                ids.extend(row[index] for row in reader)
        expected_id = None if partitioned and version == 2 else metadata["current-snapshot-id"]
        assert ids == [expected_id] * 2
    finally:
        for name in ["src", "dest", "src_BACKUP_"]:
            spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("partitioned", [False, True])
def test_snapshot_variant(spark, tmp_path, partitioned):
    partition = "PARTITIONED BY (id)" if partitioned else ""
    spark.sql(
        f"CREATE TABLE src (id BIGINT, data VARIANT) USING parquet {partition} LOCATION '{escape_sql_string_literal(str(tmp_path / 'src'))}'"
    )
    try:
        values = [(1, 123), (2, 456)] if partitioned else [(1, 123)]
        for i, value in values:
            spark.sql(f"""INSERT INTO src (id, data) VALUES ({i}, parse_json('{{"key":{value}}}'))""")  # noqa: S608
        result = spark.sql("CALL system.snapshot('src', 'dest', properties => map('format-version','3'))")
        assert result.first()[0] == len(values)
        assert [
            tuple(row)
            for row in spark.sql("SELECT id, variant_get(data, '$.key', 'int') FROM dest ORDER BY id").collect()
        ] == values
    finally:
        for name in ["src", "dest"]:
            spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_migrate_special_partition(spark, tmp_path):
    spark.sql(
        f"CREATE TABLE src (id BIGINT, data STRING, dt DATE) USING parquet PARTITIONED BY (data, dt) LOCATION '{escape_sql_string_literal(str(tmp_path / 'src'))}'"
    )
    try:
        spark.sql("INSERT INTO src VALUES (1, '2023/05/30', DATE '2023-05-30')")
        assert spark.sql("CALL system.migrate('src')").first()[0] == 1
        assert tuple(spark.table("src").first()) == (1, "2023/05/30", date(2023, 5, 30))
    finally:
        for name in ["src", "src_BACKUP_"]:
            spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize(
    ("call", "reason"),
    [
        ("migrate()", "(?i)required.*table"),
        ("migrate(map('foo', 'bar'))", "(?i)map.*(string|utf8)"),
        ("migrate('')", "empty identifier for argument table"),
        ("snapshot('foo')", "(?i)required.*table"),
        ("snapshot('n', 't', map('foo', 'bar'))", "(?i)map.*(string|utf8)"),
        ("snapshot('src', 'dest', 'loc', map(2, 1, 1))", "(?i)map.*(even|pairs|2n|multiple of 2)"),
        ("snapshot('', 'dest')", "empty identifier for argument source_table"),
        ("snapshot('src', '')", "empty identifier for argument table"),
    ],
)
def test_invalid_procedure_arguments(spark, call, reason):
    with pytest.raises(PySparkException, match=reason):
        spark.sql(f"CALL system.{call}").collect()


def test_migrate_empty_partitioned(spark, tmp_path):
    spark.sql(
        f"CREATE TABLE src (id BIGINT, data STRING) USING parquet PARTITIONED BY (id) LOCATION '{escape_sql_string_literal(str(tmp_path))}'"
    )
    try:
        assert spark.sql("CALL system.migrate(table => 'src', drop_backup => true)").first()[0] == 0
        assert spark.table("src").count() == 0
        assert not spark.catalog.tableExists("src_BACKUP_")
    finally:
        spark.sql("DROP TABLE IF EXISTS src")


@pytest.mark.parametrize("procedure", ["migrate", "snapshot"])
@pytest.mark.parametrize(
    ("case", "error"),
    [
        ("transactional", "transactional source"),
        ("inferred", "explicit source schema"),
        ("glob", "pathglobfilter"),
        ("recursive", "recursivefilelookup"),
        ("swapped_ids", "Parquet field ID"),
        ("nested_ids", "Parquet field ID"),
        ("matching_ids", None),
    ],
)
def test_source_import_safety(spark, tmp_path, procedure, case, error):
    source = tmp_path / "src"
    source.mkdir()
    columns = "" if case == "inferred" else "(a BIGINT, b BIGINT)"
    if case == "nested_ids":
        columns = "(a STRUCT<x: BIGINT>, b BIGINT)"
    options = {"glob": "OPTIONS (pathGlobFilter 'keep*')", "recursive": "OPTIONS (recursiveFileLookup 'true')"}
    properties = (
        "TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')"
        if case == "transactional"
        else ""
    )
    schema = pa.schema([("a", pa.int64()), ("b", pa.int64())])
    values = [10]
    if case.endswith("_ids"):
        a, b = (2, 1) if case == "swapped_ids" else (1, 2)
        dtype = pa.int64()
        if case == "nested_ids":
            dtype = pa.struct([pa.field("x", pa.int64(), metadata={"PARQUET:field_id": "99"})])
            values = [{"x": 10}]
        schema = pa.schema(
            [
                pa.field("a", dtype, metadata={"PARQUET:field_id": str(a)}),
                pa.field("b", pa.int64(), metadata={"PARQUET:field_id": str(b)}),
            ]
        )
    data = pa.Table.from_arrays([pa.array(values, type=schema.field(0).type), pa.array([20])], schema=schema)
    directory = source / {"transactional": "delta_0000001_0000001_0000", "recursive": "nested"}.get(case, "")
    directory.mkdir(exist_ok=True)
    pq.write_table(data, directory / "keep.parquet")
    if case == "glob":
        pq.write_table(data, directory / "excluded.parquet")
    spark.sql(
        f"CREATE TABLE src {columns} USING parquet {options.get(case, '')} LOCATION '{escape_sql_string_literal(str(source))}' {properties}"
    )
    dest = tmp_path / "target"
    call = (
        "CALL system.migrate('src')"
        if procedure == "migrate"
        else f"CALL system.snapshot('src','dest','{escape_sql_string_literal(str(dest))}')"
    )
    try:
        before = spark.table("src").collect()
        assert len(before) == 1
        if error:
            with pytest.raises(PySparkException, match=error):
                spark.sql(call).collect()
            assert spark.table("src").collect() == before
            assert not spark.catalog.tableExists("src_BACKUP_")
            assert not spark.catalog.tableExists("dest")
            assert not (source / "metadata").exists()
            assert not (dest / "metadata").exists()
        else:
            assert spark.sql(call).first()[0] == 1
            assert tuple(spark.table("src" if procedure == "migrate" else "dest").first()) == (10, 20)
    finally:
        for name in ["src", "src_BACKUP_", "dest"]:
            spark.sql(f"DROP TABLE IF EXISTS {name}")
