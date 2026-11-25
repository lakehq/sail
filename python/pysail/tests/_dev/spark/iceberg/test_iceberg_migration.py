import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql.types import LongType, StructField, StructType

from pysail.testing._dev.spark.steps.iceberg import _find_latest_metadata
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
        result = spark.sql(f"CALL system.{procedure}({args}, properties => map('x','new'), parallelism => 2)")
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
        assert tuple(spark.read.format("iceberg").load(str(path)).first()) == expected
        new_row = "(99, 'b')" if partitioned else "(99)"
        spark.sql(f"INSERT INTO {target} VALUES {new_row}")  # noqa: S608
        assert spark.table(target).count() == 2
        assert spark.table(other).count() == 1
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
        with pytest.raises(Exception, match="overlap"):
            spark.sql(
                f"CALL system.snapshot('src','dest','{escape_sql_string_literal(str(source / 'sub'))}')"
            ).collect()
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
        assert spark.sql("CALL system.migrate('managed_src')").first()[0] == expected
        for table in ["managed_dest", "managed_src", "managed_src_BACKUP_"]:
            assert spark.table(table).count() == expected
    finally:
        for table in ["managed_dest", "managed_src", "managed_src_BACKUP_"]:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
