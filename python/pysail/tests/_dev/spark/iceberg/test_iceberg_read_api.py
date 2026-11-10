import json

import pytest
from pyiceberg.table import StaticTable
from pyspark.errors import PySparkException

from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests._dev.spark.iceberg.utils import pyiceberg_file_io_properties


def _quoted(name):
    return "`" + name.replace("`", "``") + "`"


def _assert_table(dataframe, expected):
    assert [(field.name, field.dataType.simpleString(), field.nullable) for field in dataframe.schema] == [
        ("id", "int", True),
        ("value", "string", True),
    ]
    assert [tuple(row) for row in dataframe.orderBy("id").collect()] == expected


@pytest.fixture
def read_table(spark, tmp_path, request):
    name = getattr(request, "param", "iceberg_read_api")
    identifier = _quoted(name)
    path = tmp_path / "table"
    spark.sql(
        f"CREATE TABLE {identifier} (id INT, value STRING) USING iceberg "
        f"LOCATION '{escape_sql_string_literal(str(path))}'"
    )
    try:
        spark.sql(f"INSERT INTO {identifier} VALUES (1, 'one')")  # noqa: S608
        spark.sql(f"INSERT INTO {identifier} VALUES (2, 'two')")  # noqa: S608
        metadata_path = max(
            (path / "metadata").glob("v*.metadata.json"),
            key=lambda file: int(file.name[1:].split(".")[0]),
        )
        yield name, path, metadata_path
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {identifier}")


@pytest.mark.parametrize(
    ("read_table", "identifier_template"),
    [
        pytest.param("iceberg_read_api", "{name}", id="bare"),
        pytest.param("iceberg_read_api", "default.{name}", id="namespace"),
        pytest.param("iceberg_read_api", "sail.default.{name}", id="catalog"),
        pytest.param("iceberg.read`api", "`sail`.`default`.{quoted_name}", id="quoted"),
    ],
    indirect=["read_table"],
)
def test_iceberg_load_identifier_matches_table_and_path(spark, read_table, identifier_template):
    name, path, _ = read_table
    identifier = identifier_template.format(name=name, quoted_name=_quoted(name))
    expected = [(1, "one"), (2, "two")]
    loaded = spark.read.format("iceberg").load(identifier)
    for dataframe in [loaded, spark.table(identifier), spark.read.format("iceberg").load(str(path))]:
        _assert_table(dataframe, expected)
    assert [tuple(row) for row in loaded.where("id > 1").select("value").collect()] == [("two",)]


@pytest.mark.parametrize("directory", ["ordinary", "literal#files"])
def test_iceberg_load_preserves_paths_and_uris(spark, tmp_path, directory):
    path = tmp_path / directory
    expected = [(1, "one"), (2, "two")]
    spark.createDataFrame(expected, "id INT, value STRING").write.format("iceberg").save(str(path))
    # A literal '#' in a filesystem path is not a metadata-table selector.
    for dataframe in [
        spark.read.format("iceberg").load(str(path)),
        spark.read.format("iceberg").load(path.as_uri()),
        spark.table(f"iceberg.{_quoted(str(path))}"),
    ]:
        _assert_table(dataframe, expected)


@pytest.mark.parametrize("target", ["identifier", "path"])
def test_iceberg_load_forwards_snapshot_options(spark, read_table, target):
    name, path, metadata_path = read_table
    metadata = json.loads(metadata_path.read_text())
    snapshot_id = metadata["snapshot-log"][0]["snapshot-id"]
    location = f"sail.default.{name}" if target == "identifier" else str(path)
    # snapshotId is a retained Sail extension; this checks forwarding, not selector aliases.
    historical = spark.read.format("iceberg").option("snapshotId", str(snapshot_id)).load(location)
    _assert_table(historical, [(1, "one")])
    _assert_table(spark.read.format("iceberg").load(location), [(1, "one"), (2, "two")])


@pytest.mark.parametrize("read_table", ["iceberg_read_api", "iceberg.read`api"], indirect=True)
@pytest.mark.parametrize("relation", ["files", "snapshots"])
def test_iceberg_load_metadata_relations(spark, read_table, relation):
    name, _, metadata_path = read_table
    dataframe = spark.read.format("iceberg").load(f"`sail`.`default`.{_quoted(name)}.{_quoted(relation)}")
    if relation == "files":
        reference = StaticTable.from_metadata(metadata_path.as_uri(), properties=pyiceberg_file_io_properties())
        expected = sorted(
            (task.file.file_path, 0, "PARQUET", task.file.record_count) for task in reference.scan().plan_files()
        )
        rows = dataframe.select("file_path", "content", "file_format", "record_count").orderBy("file_path").collect()
    else:
        metadata = json.loads(metadata_path.read_text())
        expected = sorted(
            (snapshot["snapshot-id"], snapshot.get("parent-snapshot-id"), snapshot["summary"]["operation"])
            for snapshot in metadata["snapshots"]
        )
        rows = dataframe.select("snapshot_id", "parent_id", "operation").orderBy("snapshot_id").collect()
    assert [tuple(row) for row in rows] == expected
    assert expected


@pytest.mark.parametrize(
    "identifier",
    [
        "missing_iceberg_read_api",
        "default.missing_iceberg_read_api",
        "sail.default.missing_iceberg_read_api",
        "iceberg.missing_iceberg_read_api",
    ],
)
def test_iceberg_load_missing_identifier_does_not_fall_back_to_path(spark, identifier):
    with pytest.raises(PySparkException, match=r"(?i)(not found|table_or_view_not_found)"):
        spark.read.format("iceberg").load(identifier).collect()


def test_iceberg_load_invalid_identifier_does_not_fall_back_to_path(spark):
    with pytest.raises(PySparkException, match="expected identifier"):
        spark.read.format("iceberg").load("invalid..identifier").collect()


@pytest.mark.parametrize(
    ("relation", "message"),
    [
        ("entries", "Iceberg metadata table 'entries' is recognized but not implemented"),
        ("all_files", "Iceberg metadata table 'all_files' is recognized but not implemented"),
        ("unknown_relation", r"(?i)(not found|table_or_view_not_found)"),
    ],
)
def test_iceberg_load_rejects_unsupported_metadata_relations(spark, read_table, relation, message):
    name, _, _ = read_table
    with pytest.raises(PySparkException, match=message):
        spark.read.format("iceberg").load(f"sail.default.{name}.{relation}").collect()


@pytest.mark.parametrize("kind", ["parquet", "view", "temporary_view", "global_temporary_view"])
def test_iceberg_load_rejects_non_iceberg_targets(spark, tmp_path, kind):
    name = "iceberg_read_wrong_format"
    if kind == "parquet":
        path = escape_sql_string_literal(str(tmp_path / "parquet"))
        spark.sql(f"CREATE TABLE {name} USING parquet LOCATION '{path}' AS SELECT 1 AS id")
        identifier = name
        drop = f"DROP TABLE IF EXISTS {identifier}"
    else:
        qualifier = {"view": "", "temporary_view": "TEMPORARY ", "global_temporary_view": "GLOBAL TEMPORARY "}[kind]
        spark.sql(f"CREATE {qualifier}VIEW {name} AS SELECT 1 AS id")
        identifier = f"global_temp.{name}" if kind == "global_temporary_view" else name
        drop = f"DROP VIEW IF EXISTS {identifier}"
    try:
        message = "not an iceberg table" if kind == "parquet" else r"(?i)(not found|table_or_view_not_found)"
        with pytest.raises(PySparkException, match=message):
            spark.read.format("iceberg").load(identifier).collect()
        assert [tuple(row) for row in spark.table(identifier).collect()] == [(1,)]
    finally:
        spark.sql(drop)


def test_iceberg_load_identifier_ignores_temporary_view_shadowing(spark, read_table):
    name, _, metadata_path = read_table
    expected = [(1, "one"), (2, "two")]
    snapshot_ids = sorted(snapshot["snapshot-id"] for snapshot in json.loads(metadata_path.read_text())["snapshots"])
    identifiers = [name, f"sail.default.{name}"]
    spark.sql(f"CREATE TEMPORARY VIEW {name} AS SELECT 99 AS id, 'temporary' AS value")
    try:
        assert [tuple(row) for row in spark.table(name).collect()] == [(99, "temporary")]
        # Ordinary SQL still resolves CTEs before temporary views and catalog tables.
        cte = spark.sql(f"WITH {name} AS (SELECT 77 AS id, 'cte' AS value) SELECT * FROM {name}")  # noqa: S608
        assert [tuple(row) for row in cte.collect()] == [(77, "cte")]
        for identifier in identifiers:
            _assert_table(spark.read.format("iceberg").load(identifier), expected)
            snapshots = spark.read.format("iceberg").load(f"{identifier}.snapshots")
            assert [row.snapshot_id for row in snapshots.orderBy("snapshot_id").collect()] == snapshot_ids
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {name}")

    _assert_table(spark.table(name), expected)
    for identifier in identifiers:
        _assert_table(spark.read.format("iceberg").load(identifier), expected)
        snapshots = spark.read.format("iceberg").load(f"{identifier}.snapshots")
        assert [row.snapshot_id for row in snapshots.orderBy("snapshot_id").collect()] == snapshot_ids


@pytest.mark.parametrize("file_format", ["iceberg", "parquet"])
def test_iceberg_load_exact_table_precedes_metadata_suffix(spark, read_table, file_format, tmp_path):
    name, _, _ = read_table
    spark.sql(f"CREATE DATABASE {name}")
    try:
        location = escape_sql_string_literal(str(tmp_path / "exact_table"))
        spark.sql(f"CREATE TABLE {name}.files USING {file_format} LOCATION '{location}' AS SELECT 99 AS marker")
        if file_format == "iceberg":
            assert [tuple(row) for row in spark.read.format("iceberg").load(f"{name}.files").collect()] == [(99,)]
        else:
            with pytest.raises(PySparkException, match="not an iceberg table"):
                spark.read.format("iceberg").load(f"{name}.files").collect()
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS {name} CASCADE")


@pytest.mark.parametrize("target", ["identifier", "path", "metadata"])
def test_iceberg_load_rejects_user_schema(spark, read_table, target):
    name, path, _ = read_table
    location = {"identifier": name, "path": str(path), "metadata": f"{name}.snapshots"}[target]
    with pytest.raises(PySparkException, match="does not support user specified schema"):
        spark.read.schema("id INT, value STRING").format("iceberg").load(location).collect()


@pytest.mark.parametrize("key", ["path", "PATH"])
@pytest.mark.parametrize("target", ["identifier", "path", "metadata"])
def test_iceberg_load_option_only_path(spark, read_table, key, target):
    name, path, _ = read_table
    location = {"identifier": f"sail.default.{name}", "path": str(path), "metadata": f"sail.default.{name}.snapshots"}[
        target
    ]
    actual = spark.read.format("iceberg").option(key, location).load()
    expected = spark.read.format("iceberg").load(location)
    assert actual.schema == expected.schema
    assert sorted(actual.collect()) == sorted(expected.collect())


@pytest.mark.parametrize("target", ["identifier", "path"])
def test_iceberg_load_rejects_duplicate_path_inputs(spark, read_table, target):
    name, path, _ = read_table
    location = f"sail.default.{name}" if target == "identifier" else str(path)
    with pytest.raises(PySparkException, match="both a path option and load paths"):
        spark.read.format("iceberg").option("PATH", location).load(location).collect()


def test_iceberg_sql_path_is_not_reinterpreted_as_identifier(spark, read_table):
    name, _, _ = read_table
    with pytest.raises(PySparkException, match="must be an absolute path or URL"):
        spark.table(f"iceberg.{_quoted(name)}").collect()
