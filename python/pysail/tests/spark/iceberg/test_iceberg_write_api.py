# ruff: noqa: S608

import pyarrow.parquet as pq
import pytest
from pyiceberg.manifest import DataFileContent, FileFormat, ManifestContent
from pyspark.errors import PySparkException
from pyspark.sql.types import IntegerType, StructField, StructType

from pysail.testing.spark.steps.iceberg import _current_snapshot, _find_latest_metadata
from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests.spark.iceberg.utils import _current_manifest_entries, _local_file_path

APPEND_APIS = ["sql_position", "sql_name", "sql_columns", "insert_into", "save_table", "v2"]
NAME_APIS = ["sql_name", "sql_columns", "save_table", "v2"]
POSITION_APIS = ["sql_position", "insert_into"]
SOURCE_VIEW = "iceberg_write_api_source"
SEED = [(1, 2)]


def _schema(names=("a", "b"), *, required=False):
    return StructType([StructField(name, IntegerType(), not (required and name == "b")) for name in names])


@pytest.fixture
def write_target(spark, tmp_path):
    name = "iceberg_write_api"
    path = tmp_path / "table"
    try:
        yield name, path
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {SOURCE_VIEW}")
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def _create_table(spark, target, kind="nullable", *, seed=True):
    name, path = target
    required = " NOT NULL" if kind == "required" else ""
    properties = "'format-version'='3'" if kind == "default" else "'format-version'='2'"
    spark.sql(
        f"CREATE TABLE {name} (a INT, b INT{required}) USING iceberg "
        f"LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES ({properties})"
    )
    if kind == "default":
        spark.sql(f"ALTER TABLE {name} ALTER COLUMN b SET DEFAULT 42")
    if seed:
        spark.sql(f"INSERT INTO {name} VALUES (1, 2)")


def _files(path):
    return {file.relative_to(path): file.read_bytes() for file in path.rglob("*") if file.is_file()}


def _assert_rows_and_schema(frame, expected, schema=None):
    schema = schema or _schema()
    assert [(f.name, f.dataType, f.nullable) for f in frame.schema] == [
        (f.name, f.dataType, f.nullable) for f in schema
    ]
    assert [tuple(row) for row in frame.orderBy(*schema.fieldNames()).collect()] == sorted(expected)


def _append(spark, target, source, api, *, columns=None):
    name, path = target
    if api.startswith("sql_"):
        source.createOrReplaceTempView(SOURCE_VIEW)
        clause = ""
        if api == "sql_name":
            clause = " BY NAME"
        elif api == "sql_columns":
            clause = " (" + ", ".join(columns or source.columns) + ")"
        spark.sql(f"INSERT INTO {name}{clause} SELECT * FROM {SOURCE_VIEW}").collect()
    elif api == "insert_into":
        source.write.mode("append").insertInto(name)
    elif api == "save_table":
        source.write.format("iceberg").mode("append").option("path", str(path)).saveAsTable(name)
    else:
        assert api == "v2"
        source.writeTo(name).append()


def _assert_iceberg_data(path, expected, schema=None):
    schema = schema or _schema()
    entries = _current_manifest_entries(path, ManifestContent.DATA)
    assert _current_manifest_entries(path, ManifestContent.DELETES) == []
    assert sum(entry.data_file.record_count for entry in entries) == len(expected)
    actual = []
    for entry in entries:
        data_file = entry.data_file
        assert data_file.content == DataFileContent.DATA
        assert data_file.file_format == FileFormat.PARQUET
        parquet = pq.ParquetFile(_local_file_path(data_file.file_path))
        assert parquet.metadata.num_rows == data_file.record_count
        actual.extend(
            tuple(row[name] for name in schema.fieldNames())
            for row in parquet.read(columns=schema.fieldNames()).to_pylist()
        )
    assert sorted(actual) == sorted(expected)


def _assert_append(spark, target, before, expected, *, required=False):
    name, path = target
    schema = _schema(required=required)
    _assert_rows_and_schema(spark.table(name), expected, schema)
    _assert_rows_and_schema(spark.read.format("iceberg").load(str(path)), expected, schema)
    after = _find_latest_metadata(path)
    for key in (
        "table-uuid",
        "schemas",
        "current-schema-id",
        "partition-specs",
        "default-spec-id",
        "sort-orders",
        "default-sort-order-id",
    ):
        assert after[key] == before[key]
    assert len(after["snapshots"]) == len(before.get("snapshots", [])) + 1
    assert after["snapshots"][:-1] == before.get("snapshots", [])
    snapshot = _current_snapshot(after)
    parent = before.get("current-snapshot-id") if before.get("snapshots") else None
    assert snapshot.get("parent-snapshot-id") == parent
    assert snapshot["summary"]["operation"] == "append"
    _assert_iceberg_data(path, expected, schema)


@pytest.mark.parametrize("api", APPEND_APIS)
@pytest.mark.parametrize(
    ("kind", "shape"),
    [
        ("nullable", "reordered"),
        ("default", "reordered"),
        ("nullable", "missing"),
        ("required", "missing"),
        ("default", "missing"),
        ("default", "null"),
        ("default", "extra"),
        ("default", "unknown"),
    ],
)
def test_iceberg_append_column_matching(spark, write_target, api, kind, shape):
    _create_table(spark, write_target, kind)
    name, path = write_target
    before = _find_latest_metadata(path)
    before_files = _files(path)
    inputs = {
        "reordered": ([(20, 10)], "b INT, a INT"),
        "missing": ([(10,)], "a INT"),
        "null": ([(10, None)], "a INT, b INT"),
        "extra": ([(10, 20, 30)], "a INT, b INT, c INT"),
        "unknown": ([(10, 30)], "a INT, c INT"),
    }
    rows, schema = inputs[shape]
    source = spark.createDataFrame(rows, schema)
    positional = api in POSITION_APIS
    error = None
    if shape == "extra":
        error = "input schema for INSERT has 3 fields" if positional else "column not found in target table: c"
    elif shape == "unknown" and not positional:
        error = "column not found in target table: c"
    elif shape == "missing" and (positional or kind == "required"):
        error = "input schema for INSERT has 1 fields" if positional else r"(column not found|missing value).*b"

    if error:
        with pytest.raises(PySparkException, match=error):
            _append(spark, write_target, source, api)
        assert _files(path) == before_files
        assert _find_latest_metadata(path) == before
        _assert_rows_and_schema(spark.table(name), SEED, _schema(required=kind == "required"))
    else:
        _append(spark, write_target, source, api)
        inserted = {
            "reordered": (20, 10) if positional else (10, 20),
            "missing": (10, 42 if kind == "default" else None),
            "null": (10, None),
            "unknown": (10, 30),
        }[shape]
        _assert_append(spark, write_target, before, [*SEED, inserted])


@pytest.mark.parametrize("api", NAME_APIS)
@pytest.mark.parametrize("kind", ["nullable", "default"])
def test_iceberg_append_rejects_duplicate_columns(spark, write_target, api, kind):
    _create_table(spark, write_target, kind)
    name, path = write_target
    before = _files(path)
    source = spark.createDataFrame([(10, 20)], "a INT, b INT")
    columns = None
    if api == "sql_columns":
        columns = ["a", "a"]
    else:
        source = source.selectExpr("a", "b AS A")
    with pytest.raises(PySparkException, match=r"(?i)(ambiguous|duplicate|more than once|unique)"):
        _append(spark, write_target, source, api, columns=columns)
    assert _files(path) == before
    _assert_rows_and_schema(spark.table(name), SEED)


@pytest.mark.parametrize("api", APPEND_APIS)
def test_iceberg_typed_empty_append_commits_snapshot(spark, write_target, api):
    _create_table(spark, write_target, seed=False)
    _, path = write_target
    before = _find_latest_metadata(path)
    assert before.get("snapshots", []) == []
    source = spark.createDataFrame([], "b INT, a INT")
    _append(spark, write_target, source, api)
    _assert_append(spark, write_target, before, [])
    assert list(path.rglob("*.parquet")) == []


@pytest.mark.parametrize("api", NAME_APIS)
def test_iceberg_typed_empty_omission_does_not_generate_rows(spark, write_target, api):
    _create_table(spark, write_target, "default", seed=False)
    before = _find_latest_metadata(write_target[1])
    _append(spark, write_target, spark.createDataFrame([], "a INT"), api)
    _assert_append(spark, write_target, before, [])
    assert list(write_target[1].rglob("*.parquet")) == []


def test_iceberg_explicit_default_and_null_with_reordered_columns(spark, write_target):
    _create_table(spark, write_target, "default")
    name, path = write_target
    before = _find_latest_metadata(path)
    schema = next(s for s in before["schemas"] if s["schema-id"] == before["current-schema-id"])
    assert schema["fields"][1]["write-default"] == 42  # noqa: PLR2004
    spark.sql(f"INSERT INTO {name} (b, a) VALUES (DEFAULT, 10), (CAST(NULL AS INT), 11)")
    _assert_append(spark, write_target, before, [*SEED, (10, 42), (11, None)])


@pytest.mark.parametrize("api", APPEND_APIS)
def test_iceberg_append_absent_target(spark, write_target, api):
    name, path = write_target
    source = spark.createDataFrame([(20, 10)], "b INT, a INT")
    before = _files(path)
    if api == "save_table":
        _append(spark, write_target, source, api)
        _assert_rows_and_schema(spark.table(name), [(20, 10)], _schema(("b", "a")))
        _assert_iceberg_data(path, [(20, 10)], _schema(("b", "a")))
    else:
        with pytest.raises(PySparkException, match=r"(?i)(table_or_view_not_found|not found|cannot be found)"):
            _append(spark, write_target, source, api)
        assert not spark.catalog.tableExists(name)
        assert _files(path) == before


@pytest.mark.parametrize(
    ("api", "mode", "exists"),
    [
        ("insert_into", "error", True),
        ("insert_into", "ignore", True),
        ("insert_into", "overwrite", True),
        ("insert_into", "overwrite", False),
        ("save_table", "error", True),
        ("save_table", "ignore", True),
        ("save_table", "overwrite", True),
        ("save_table", "error", False),
        ("save_table", "ignore", False),
    ],
)
def test_iceberg_v1_save_modes(spark, write_target, api, mode, exists):
    name, path = write_target
    if exists:
        _create_table(spark, write_target)
    before = _files(path)
    source = spark.createDataFrame([(20, 10)], "b INT, a INT")

    def write():
        if api == "insert_into":
            source.write.mode(mode).insertInto(name)
        else:
            source.write.format("iceberg").mode(mode).option("path", str(path)).saveAsTable(name)

    rejects = (api == "insert_into" and not exists) or (api == "save_table" and exists and mode == "error")
    if rejects:
        with pytest.raises(PySparkException, match=r"(?i)(already exists|not found|cannot be found)"):
            write()
        assert _files(path) == before
        if exists:
            _assert_rows_and_schema(spark.table(name), SEED)
        else:
            assert not spark.catalog.tableExists(name)
        return

    write()
    if api == "save_table" and exists and mode == "ignore":
        assert _files(path) == before
        _assert_rows_and_schema(spark.table(name), SEED)
    elif api == "insert_into":
        # insertInto's error/ignore modes append, unlike saveAsTable.
        expected = [(20, 10)] if mode == "overwrite" else [*SEED, (20, 10)]
        _assert_rows_and_schema(spark.table(name), expected)
        _assert_iceberg_data(path, expected)
    else:
        _assert_rows_and_schema(spark.table(name), [(20, 10)], _schema(("b", "a")))
        _assert_iceberg_data(path, [(20, 10)], _schema(("b", "a")))


@pytest.mark.parametrize("operation", ["create", "replace", "createOrReplace"])
@pytest.mark.parametrize("exists", [False, True])
def test_iceberg_v2_create_and_replace(spark, write_target, operation, exists):
    name, path = write_target
    if exists:
        _create_table(spark, write_target)
    before = _files(path)
    source = spark.createDataFrame([(20, 10)], "b INT, a INT")
    writer = source.writeTo(name).using("iceberg").option("path", str(path))
    rejects = (operation == "create" and exists) or (operation == "replace" and not exists)
    if rejects:
        with pytest.raises(PySparkException, match=r"(?i)(already exists|not found|cannot be found)"):
            getattr(writer, operation)()
        assert _files(path) == before
        if exists:
            _assert_rows_and_schema(spark.table(name), SEED)
        else:
            assert not spark.catalog.tableExists(name)
    else:
        getattr(writer, operation)()
        _assert_rows_and_schema(spark.table(name), [(20, 10)], _schema(("b", "a")))
        _assert_iceberg_data(path, [(20, 10)], _schema(("b", "a")))
