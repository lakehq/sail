# [CREDIT]: https://github.com/apache/iceberg/blob/6976e020b894f6a6777704df2b8c4458cb291ae9/spark/v4.0/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestAddFilesProcedure.java

import uuid
from collections import Counter
from contextlib import contextmanager
from functools import cache
from typing import NamedTuple

import pytest
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from pysail.testing.spark.session import spark_connect_server

SCHEMA = "id INT, name STRING, dept STRING, subdept STRING"
ROWS = [
    (1, "John Doe", "hr", "communications"),
    (2, "Jane Doe", "hr", "salary"),
    (3, "Matt Doe", "hr", "communications"),
    (4, "Will Doe", "facilities", "all"),
]
NULL_ROW = (None, None, None, None)
SOURCE_WRITES = 2


@pytest.fixture(scope="module")
def remote(hms_s3_metastore_endpoint, hms_s3_env):
    catalogs = (
        f'[{{name="sail",type="hms",uris=["{hms_s3_metastore_endpoint}"]}},'
        '{name="paths",type="memory",initial_database=["default"]}]'
    )
    with spark_connect_server(
        envs={"SAIL_CATALOG__LIST": catalogs, "SAIL_CATALOG__DEFAULT_CATALOG": "sail", **hms_s3_env}
    ) as server:
        yield server.remote


@pytest.fixture(scope="module")
def database(jvm_spark):
    name = f"add_files_{uuid.uuid4().hex}"
    jvm_spark.sql(f"CREATE DATABASE {name} LOCATION 's3://hms-warehouse/{name}'")
    yield name
    jvm_spark.sql(f"DROP DATABASE {name} CASCADE")


@pytest.fixture(scope="module")
def source(jvm_spark, database):
    @cache
    def create(partitions=(), *, include_null=False, empty=False):
        name = f"{database}.s_{uuid.uuid4().hex}"
        location = f"s3://hms-warehouse/{database}/{name.split('.')[-1]}"
        frame = jvm_spark.createDataFrame(ROWS + ([NULL_ROW] if include_null else []), SCHEMA).coalesce(1)
        columns = [c for c in frame.columns if c not in partitions]
        types = dict(frame.dtypes)
        ddl = ", ".join(f"{c} {types[c]}" for c in columns)
        partition_ddl = ", ".join(f"{c} {types[c]}" for c in partitions)
        partition_by = f"PARTITIONED BY ({partition_ddl})" if partitions else ""
        jvm_spark.sql(f"CREATE TABLE {name} ({ddl}) {partition_by} STORED AS parquet LOCATION '{location}'")
        if not empty:
            for _ in range(SOURCE_WRITES):
                frame.select(*columns, *partitions).write.insertInto(name)
        return name, f"parquet.`{location}`"

    return create


class Catalog(NamedTuple):
    profile: str
    reference: SparkSession
    name: str
    version: int

    def manifest_counts(self, files, partitions):
        return files, partitions if self.version == 1 else None


@pytest.fixture(scope="module", params=["hive-v1", "hadoop-v2", "session-v2"])
def catalog(request, jvm_spark, database):
    session = jvm_spark.newSession()
    profile = request.param
    if profile == "hadoop-v2":
        session.conf.set("spark.sql.catalog.hadoop", "org.apache.iceberg.spark.SparkCatalog")
        session.conf.set("spark.sql.catalog.hadoop.type", "hadoop")
        session.conf.set("spark.sql.catalog.hadoop.cache-enabled", "false")
        session.conf.set("spark.sql.catalog.hadoop.warehouse", f"s3://hms-warehouse/{database}/hadoop")
        session.sql(f"CREATE NAMESPACE hadoop.{database}")
        return Catalog(profile, session, "hadoop", 2)
    if profile == "session-v2":
        session.conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        session.conf.set("spark.sql.catalog.spark_catalog.type", "hive")
        session.conf.set("spark.sql.catalog.spark_catalog.default-namespace", "default")
        session.conf.set("spark.sql.catalog.spark_catalog.parquet-enabled", "true")
        session.conf.set("spark.sql.catalog.spark_catalog.cache-enabled", "false")
        return Catalog(profile, session, "spark_catalog", 2)
    session.conf.set("spark.sql.catalog.iceberg.default-namespace", "default")
    return Catalog(profile, session, "iceberg", 1)


@pytest.fixture
def target(catalog, spark, database):
    profile, reference, catalog_name, version = catalog

    @contextmanager
    def create(partitions=(), *, schema=SCHEMA, inherit=False):
        table = f"t_{uuid.uuid4().hex}"
        upstream = f"{catalog_name}.{database}.{table}"
        partition_by = f"PARTITIONED BY ({', '.join(partitions)})" if partitions else ""
        reference.sql(
            f"CREATE TABLE {upstream} ({schema}) USING iceberg {partition_by} "
            f"TBLPROPERTIES ('format-version'='{version}',"
            f"'compatibility.snapshot-id-inheritance.enabled'='{str(inherit).lower()}')"
        )
        if profile == "hadoop-v2":
            name = f"paths.default.{table}"
            location = f"s3://hms-warehouse/{database}/hadoop/{database}/{table}"
            spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{location}'")
        else:
            name = f"sail.{database}.{table}"
        try:
            yield name, upstream
        finally:
            reference.sql(f"DROP TABLE IF EXISTS {upstream}")
            if profile == "hadoop-v2":
                spark.sql(f"DROP TABLE IF EXISTS {name}")

    return create


def call(spark, target, source, args=""):
    return tuple(spark.sql(f"CALL {target.split('.')[0]}.system.add_files('{target}', '{source}'{args})").first())


def metadata_table(catalog, tables, suffix):
    reference, upstream = catalog.reference, tables[1]
    if upstream.startswith("spark_catalog."):
        upstream = "iceberg." + upstream.split(".", 1)[1]
    reference.catalog.refreshTable(upstream)
    return reference.table(f"{upstream}.{suffix}")


def assert_manifest_specs(catalog, tables, spec_id):
    reference = catalog.reference
    iceberg = reference._jvm.org.apache.iceberg  # noqa: SLF001
    table = iceberg.spark.Spark3Util.loadIcebergTable(reference._jsparkSession, tables[1])  # noqa: SLF001
    table.refresh()
    expected = table.specs().get(spec_id)
    manifests = table.currentSnapshot().allManifests(table.io())
    assert manifests.size() > 0
    for manifest in manifests:
        reader = iceberg.ManifestFiles.read(manifest, table.io(), None)
        try:
            assert reader.spec().equals(expected)
        finally:
            reader.close()
        uuid.UUID(manifest.path().rsplit("/", 1)[-1].removeprefix("add-files-").removesuffix(".avro"))


def assert_rows(catalog, spark, tables, expected, columns="id, name, dept, subdept"):
    name, upstream = tables
    reference = catalog.reference
    reference.catalog.refreshTable(upstream)
    for session, table in [(spark, name), (reference, upstream)]:
        actual = session.sql(f"SELECT {columns} FROM {table}").collect()  # noqa: S608
        assert Counter(map(tuple, actual)) == Counter(expected)


@pytest.mark.parametrize("by_name", [False, True], ids=["path", "hive-source"])
@pytest.mark.parametrize("partitioned", [False, True])
def test_import_and_duplicates(catalog, source, target, spark, by_name, partitioned):
    parts = ("id",) if partitioned else ()
    source_name, path = source(parts)
    input_table = source_name if by_name else path
    with target(parts) as tables:
        args = ", partition_filter => map('id', 1)" if partitioned else ""
        expected = ROWS[:1] if partitioned else ROWS
        changed = 1 if catalog.version == 1 or (by_name and not partitioned) else None
        assert call(spark, tables[0], input_table, args + ", parallelism => 2") == (2, changed)
        assert_rows(catalog, spark, tables, expected * 2)
        with pytest.raises(PySparkException, match="already referenced"):
            call(spark, tables[0], input_table, args)
        assert call(spark, tables[0], input_table, args + ", check_duplicate_files => false") == (2, changed)
        assert_rows(catalog, spark, tables, expected * 4)


@pytest.mark.parametrize(
    ("parts", "filter_sql", "include_null", "indexes"),
    [
        (("id",), "", False, [0, 1, 2, 3]),
        (("id", "dept"), "map('id', 1)", False, [0]),
        (("id", "dept"), "map('dept', 'hr')", False, [0, 1, 2]),
        (("id", "dept"), "map('id', 1)", True, [0]),
        (("id", "dept"), "map('dept', 'hr')", True, [0, 1, 2]),
        (("id", "dept"), "", True, [0, 1, 2, 3, 4]),
        (("id",), "", True, [0, 1, 2, 3, 4]),
        (("dept",), "", True, [0, 1, 2, 3, 4]),
    ],
)
def test_partition_discovery(catalog, source, target, spark, parts, filter_sql, include_null, indexes):
    _, path = source(parts, include_null=include_null)
    with target(parts) as tables:
        args = f", {filter_sql}" if filter_sql else ""
        rows = [*ROWS, NULL_ROW]
        expected = [rows[i] for i in indexes]
        partition_count = len(
            {tuple(row[("id", "name", "dept", "subdept").index(c)] for c in parts) for row in expected}
        )
        assert call(spark, tables[0], path, args) == (
            2 * partition_count,
            partition_count if catalog.version == 1 else None,
        )
        assert_rows(catalog, spark, tables, expected * 2)
        if parts == ("dept",):
            partition_rows = metadata_table(catalog, tables, "partitions").collect()
            assert {row.partition.dept for row in partition_rows} == {None, "hr", "facilities"}


@pytest.mark.parametrize("parts", [(), ("id",)])
def test_delete_then_add(catalog, source, target, spark, parts):
    _, path = source(parts)
    with target(parts) as tables:
        argument = ", map('id', 1)" if parts else ""
        rows = (ROWS[:1] if parts else ROWS) * 2
        call(spark, tables[0], path, argument)
        predicate = " WHERE id = 1" if parts else ""
        spark.sql(f"DELETE FROM {tables[0]}{predicate}")  # noqa: S608
        assert_rows(catalog, spark, tables, [])
        assert call(spark, tables[0], path, argument) == catalog.manifest_counts(2, 1)
        assert_rows(catalog, spark, tables, rows)


@pytest.mark.parametrize(("parts", "extra"), [((), True), ((), False), (("id",), False)])
def test_import_projection(catalog, source, target, spark, parts, extra):
    schema = SCHEMA + ", foo STRING" if extra else "id INT, name STRING, dept STRING"
    _, path = source(parts)
    with target(parts, schema=schema) as tables:
        assert call(spark, tables[0], path) == catalog.manifest_counts(8 if parts else 2, 4 if parts else 1)
        columns = "id, name, dept, subdept, foo" if extra else "id, name, dept"
        expected = [(*row, None) if extra else row[:3] for row in ROWS]
        assert_rows(catalog, spark, tables, expected * 2, columns)


@pytest.mark.parametrize("by_name", [False, True], ids=["path-two-appends", "hive-import"])
def test_snapshot_inheritance(catalog, source, target, spark, by_name):
    source_name, path = source(("id",))
    with target(("id",), inherit=True) as tables:
        if by_name:
            assert call(spark, tables[0], source_name, ", parallelism => 2")[0] == len(ROWS) * SOURCE_WRITES
            expected = ROWS
        else:
            for value in [1, 2]:
                assert call(spark, tables[0], path, f", map('id', {value})")[0] == SOURCE_WRITES
            expected = ROWS[:2]
        assert_rows(catalog, spark, tables, expected * 2)
        assert_manifest_specs(catalog, tables, 0)


@pytest.mark.parametrize("many_specs", [False, True])
def test_historical_spec(catalog, source, target, spark, many_specs):
    source_name, path = source(("id",))
    initial = () if many_specs else ("id", "dept", "subdept")
    with target(initial) as tables:
        name = tables[0]
        if many_specs:
            for field in ["id", "name", "subdept"]:
                spark.sql(f"ALTER TABLE {name} ADD PARTITION FIELD {field}")
            assert call(spark, name, source_name, ", map('id', 1)") == catalog.manifest_counts(2, 1)
            assert_rows(catalog, spark, tables, ROWS[:1] * 2)
            assert {row.spec_id for row in metadata_table(catalog, tables, "files").collect()} == {1}
        else:
            for operation in [
                "DROP PARTITION FIELD dept",
                "DROP PARTITION FIELD subdept",
                "ADD PARTITION FIELD subdept",
            ]:
                spark.sql(f"ALTER TABLE {name} {operation}")
            if catalog.version == 1:
                with pytest.raises(PySparkException, match="partition spec"):
                    call(spark, name, path)
                assert_rows(catalog, spark, tables, [])
            else:
                assert call(spark, name, path) == (8, None)
                assert_rows(catalog, spark, tables, ROWS * 2)
                assert_manifest_specs(catalog, tables, 2)


def test_invalid_filters_and_incompatible_specs(catalog, source, target, spark):
    partitioned, partitioned_path = source(("id",))
    _, plain_path = source()
    with target(("id",)) as tables:
        for argument in ["map('id', '0', 'dept', '1')", "map('dept', '1')"]:
            with pytest.raises(PySparkException, match="not partitioned"):
                call(spark, tables[0], partitioned, f", {argument}")
        for args in [", map('x', '1', 'y', '2')", ", map('dept', '2')"]:
            with pytest.raises(PySparkException, match="partition spec"):
                call(spark, tables[0], plain_path, args)
        with pytest.raises(PySparkException, match="Parallelism should be larger than 0"):
            call(spark, tables[0], partitioned, ", parallelism => 0")
        assert_rows(catalog, spark, tables, [])
    with target() as tables:
        for args in ["", ", map('id', 1)"]:
            with pytest.raises(PySparkException, match="partition spec"):
                call(spark, tables[0], partitioned_path, args)
        assert_rows(catalog, spark, tables, [])
    with target(("dept",)) as tables:
        spark.sql(f"ALTER TABLE {tables[0]} ADD PARTITION FIELD subdept")
        with pytest.raises(PySparkException, match="partition spec"):
            call(spark, tables[0], partitioned_path)
        assert_rows(catalog, spark, tables, [])


def test_import_empty_sources(catalog, source, target, spark, jvm_spark, database):
    empty, empty_path = source(empty=True)
    with target() as tables:
        for value in [empty_path, empty]:
            assert call(spark, tables[0], value) == (0, 0)
        assert_rows(catalog, spark, tables, [])
    registered, _ = source(("id",))
    jvm_spark.sql(
        f"ALTER TABLE {registered} ADD IF NOT EXISTS PARTITION (id=999) LOCATION 's3://hms-warehouse/{database}/empty'"
    )
    with target(("id",)) as tables:
        assert call(spark, tables[0], registered, ", map('id', 999)") == (0, 0)
        assert_rows(catalog, spark, tables, [])


def test_incremental_hive_partitions(catalog, source, target, spark):
    registered, _ = source(("id",))
    with target(("id",)) as tables:
        for value in [1, 2]:
            assert call(spark, tables[0], registered, f", map('id', {value})") == catalog.manifest_counts(2, 1)
            assert_rows(catalog, spark, tables, ROWS[:value] * 2)


@pytest.mark.parametrize("parts", [(), ("id",)])
@pytest.mark.parametrize("file_format", ["orc", "avro"])
def test_external_file_formats(catalog, jvm_spark, database, target, spark, parts, file_format):
    uri = f"s3://hms-warehouse/{database}/data_{uuid.uuid4().hex}"
    writer = jvm_spark.createDataFrame(ROWS, SCHEMA).coalesce(1).write.format(file_format).mode("append")
    if parts:
        writer = writer.partitionBy(*parts)
    for _ in range(SOURCE_WRITES):
        writer.save(uri)
    path = f"{file_format}.`{uri}`"
    with target(parts) as tables:
        assert call(spark, tables[0], path) == catalog.manifest_counts(8 if parts else 2, 4 if parts else 1)
        assert_rows(catalog, spark, tables, ROWS * 2)


def test_single_avro_file(catalog, jvm_spark, database, target, spark):
    uri = f"s3://hms-warehouse/{database}/avro_{uuid.uuid4().hex}"
    jvm_spark.sql("SELECT * FROM VALUES (1,'a'), (2,'b') AS data(id, data)").coalesce(1).write.format("avro").save(uri)
    file = jvm_spark.read.format("avro").load(uri).inputFiles()[0]
    with target(schema="id BIGINT, data STRING") as tables:
        assert call(spark, tables[0], f"avro.`{file}`") == catalog.manifest_counts(1, 1)
        assert_rows(catalog, spark, tables, [(1, "a"), (2, "b")], "id, data")
        assert [row.record_count for row in metadata_table(catalog, tables, "files").collect()] == [2]


@pytest.mark.parametrize("composite", [True, False])
def test_date_partitions(catalog, jvm_spark, database, target, spark, composite):
    uri = f"s3://hms-warehouse/{database}/dates_{uuid.uuid4().hex}"
    df = jvm_spark.sql(
        "SELECT * FROM VALUES (1,DATE '2021-01-01','01'), (2,DATE '2021-01-01','01'), "
        "(3,DATE '2021-01-02','02'), (4,DATE '2021-01-02','02') AS data(id, day, dept)"
    )
    parts = ("day", "dept") if composite else ("day",)
    for predicate in ["id IN (1, 3)", "id IN (2, 4)"]:
        df.where(predicate).coalesce(1).write.mode("append").partitionBy(*parts).parquet(uri)
    with target(parts, schema="id INT, day DATE, dept STRING") as tables:
        assert call(spark, tables[0], f"parquet.`{uri}`", ", map('day', '2021-01-01')") == catalog.manifest_counts(2, 1)
        assert_rows(
            catalog, spark, tables, [(1, "2021-01-01", "01"), (2, "2021-01-01", "01")], "id, CAST(day AS STRING), dept"
        )


def test_mixed_case_hive_filter(catalog, jvm_spark, database, target, spark):
    name = f"{database}.mixed_{uuid.uuid4().hex}"
    jvm_spark.sql(
        f"CREATE TABLE {name} (id INT, subdept STRING, dept STRING) PARTITIONED BY (naMe STRING) STORED AS parquet"
    )
    df = jvm_spark.createDataFrame(ROWS, SCHEMA).select("id", "subdept", "dept", "name").coalesce(1)
    for _ in range(SOURCE_WRITES):
        df.write.insertInto(name)
    with target(("naMe",), schema="id INT, naMe STRING, dept STRING, subdept STRING") as tables:
        assert call(spark, tables[0], name, ", map('naMe','John Doe')") == catalog.manifest_counts(2, 1)
        assert_rows(catalog, spark, tables, ROWS[:1] * 2, "id, naMe, dept, subdept")
        assert spark.table(tables[0]).where("naMe = 'John Doe'").count() == SOURCE_WRITES


def test_all_registered_partitions_with_parallelism(catalog, source, target, spark):
    registered, _ = source(("id",))
    with target(("id",)) as tables:
        assert call(spark, tables[0], registered, ", parallelism => 2") == catalog.manifest_counts(8, 4)
        assert_rows(catalog, spark, tables, ROWS * 2)
