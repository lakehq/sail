"""Exercise Flight SQL through the real ADBC driver, including its strict API.

DBAPI execute() tolerates unsupported preparation, so prepared-statement tests
must call the low-level prepare() API explicitly.
"""

import json

import adbc_driver_manager
import pyarrow as pa
import pytest
from adbc_driver_flightsql import ConnectionOptions

from pysail.tests.flight.conftest import connect, local_flight_uri


def read_result(statement):
    handle, _ = statement.execute_query()
    with pa.RecordBatchReader._import_from_c(handle.address) as reader:  # noqa: SLF001
        return reader.read_all()


def query(connection, sql):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def tables(connection, **filters):
    with connection.adbc_get_objects(depth="all", **filters) as reader:
        catalogs = reader.read_all().to_pylist()
    return [
        table
        for catalog in catalogs
        for schema in catalog["catalog_db_schemas"] or []
        for table in schema["db_schema_tables"] or []
    ]


def test_server_information(flight_connection):
    info = flight_connection.adbc_get_info()
    assert info["vendor_name"] == "Sail"
    assert info["vendor_version"]
    assert set(flight_connection.adbc_get_table_types()) == {"TABLE", "VIEW"}
    with pytest.raises(adbc_driver_manager.NotSupportedError):
        flight_connection.adbc_connection.set_autocommit(False)


def test_catalog_discovery(flight_connection):
    query(flight_connection, "CREATE TEMP VIEW flight_metadata AS SELECT 1 AS id, 'a' AS label")
    matches = tables(flight_connection, table_name_filter="flight_meta%")
    assert [table["table_name"] for table in matches] == ["flight_metadata"]
    assert [column["column_name"] for column in matches[0]["table_columns"]] == ["id", "label"]
    assert matches[0]["table_type"] == "VIEW"
    # ADBC Python currently passes NULL for table_types, so its wrapper cannot
    # exercise CommandGetTables.table_types filtering.
    schema = flight_connection.adbc_get_table_schema(
        "flight_metadata", catalog_filter="sail", db_schema_filter="default"
    )
    assert schema.names == ["id", "label"]


def test_discovery_patterns(flight_connection):
    for name in ("flight_pattern_a", "flight_pattern_ab"):
        query(flight_connection, f"CREATE TEMP VIEW {name} AS SELECT 1 AS id")
    assert [t["table_name"] for t in tables(flight_connection, table_name_filter="flight_pattern__")] == [
        "flight_pattern_a"
    ]
    assert tables(flight_connection, catalog_filter="missing_catalog") == []
    assert tables(flight_connection, db_schema_filter="missing_schema") == []


def test_nested_schema_discovery(flight_connection, tmp_path):
    for schema in ("flight_parent", "flight_parent.child", "flight_parent.child.grandchild"):
        query(flight_connection, f"CREATE DATABASE {schema}")
    schema = "flight_parent.child.grandchild"
    query(
        flight_connection,
        f"CREATE TABLE {schema}.nested_table (value BIGINT) USING parquet LOCATION '{tmp_path.as_posix()}'",
    )
    # Explicit catalog discovery must work independently of the session default.
    flight_connection.adbc_current_catalog = "system"
    with flight_connection.adbc_get_objects(
        depth="db_schemas", catalog_filter="sail", db_schema_filter="flight_parent%"
    ) as reader:
        catalogs = reader.read_all().to_pylist()
    assert [db["db_schema_name"] for catalog in catalogs for db in catalog["catalog_db_schemas"]] == [
        "flight_parent",
        "flight_parent.child",
        schema,
    ]
    matches = tables(flight_connection, catalog_filter="sail", db_schema_filter=schema)
    assert [table["table_name"] for table in matches] == ["nested_table"]
    assert [column["column_name"] for column in matches[0]["table_columns"]] == ["value"]
    assert flight_connection.adbc_get_table_schema(
        "nested_table", catalog_filter="sail", db_schema_filter=schema
    ).names == ["value"]


@pytest.mark.parametrize("initial", [False, True])
def test_schema_discovery_creates_intermediate_databases(monkeypatch, tmp_path, initial):
    schema = "flight_orphan.child.grandchild"
    namespace = '["flight_orphan", "child", "grandchild"]' if initial else '["default"]'
    monkeypatch.setenv("SAIL_CATALOG__LIST", f'[{{type="memory", name="sail", initial_database={namespace}}}]')
    monkeypatch.setenv("SAIL_CATALOG__DEFAULT_CATALOG", "sail")
    if initial:
        monkeypatch.setenv("SAIL_CATALOG__DEFAULT_DATABASE", namespace)
    with local_flight_uri() as uri, connect(uri) as connection:
        if not initial:
            query(connection, f"CREATE DATABASE {schema}")
        query(
            connection,
            f"CREATE TABLE {schema}.orphan_table (value BIGINT) USING parquet LOCATION '{tmp_path.as_posix()}'",
        )
        connection.adbc_current_db_schema = schema
        assert query(connection, "SELECT count(*) FROM orphan_table") == [(0,)]
        with connection.adbc_get_objects(
            depth="db_schemas", catalog_filter="sail", db_schema_filter="flight_orphan%"
        ) as reader:
            catalogs = reader.read_all().to_pylist()
        assert [db["db_schema_name"] for catalog in catalogs for db in catalog["catalog_db_schemas"]] == [
            "flight_orphan",
            "flight_orphan.child",
            schema,
        ]
        matches = tables(connection, catalog_filter="sail", db_schema_filter=schema)
        assert [table["table_name"] for table in matches] == ["orphan_table"]
        assert [column["column_name"] for column in matches[0]["table_columns"]] == ["value"]


@pytest.mark.parametrize("option", ["catalog", "schema"])
@pytest.mark.parametrize("operation", ["query", "update"])
def test_prepared_statement_preserves_namespace(monkeypatch, tmp_path, option, operation):
    monkeypatch.setenv(
        "SAIL_CATALOG__LIST",
        '[{type="memory", name="sail", initial_database=["default"]}, '
        '{type="memory", name="other", initial_database=["default"]}]',
    )
    monkeypatch.setenv("SAIL_CATALOG__DEFAULT_CATALOG", "sail")
    with local_flight_uri() as uri, connect(uri) as connection:
        query(connection, "CREATE DATABASE second")
        original = "sail.default.namespace_table"
        alternate = "other.default.namespace_table" if option == "catalog" else "sail.second.namespace_table"
        for name, location, value in ((original, "original", 1), (alternate, "alternate", 99)):
            query(
                connection,
                f"CREATE TABLE {name} (value BIGINT) USING delta LOCATION '{(tmp_path / location).as_posix()}'",
            )
            query(connection, f"INSERT INTO {name} VALUES ({value})")  # noqa: S608
        with adbc_driver_manager.AdbcStatement(connection.adbc_connection) as statement:
            sql = (
                "SELECT value FROM namespace_table WHERE value > CAST(? AS BIGINT) ORDER BY value"
                if operation == "query"
                else "INSERT INTO namespace_table VALUES (CAST(? AS BIGINT))"
            )
            statement.set_sql_query(sql)
            statement.prepare()
            if option == "catalog":
                connection.adbc_current_catalog = "other"
            else:
                connection.adbc_current_db_schema = "second"
            statement.bind(pa.record_batch([pa.array([0 if operation == "query" else 2])], names=["value"]))
            if operation == "query":
                assert read_result(statement).column("value").to_pylist() == [1]
                query(connection, "INSERT INTO sail.default.namespace_table VALUES (2)")
                assert read_result(statement).column("value").to_pylist() == [1, 2]
            else:
                assert statement.execute_update() == -1
                statement.bind(pa.record_batch([pa.array([3])], names=["value"]))
                assert statement.execute_update() == -1
            assert connection.adbc_current_catalog == ("other" if option == "catalog" else "sail")
            assert connection.adbc_current_db_schema == ("default" if option == "catalog" else "second")
        expected = [(1,), (2,)] if operation == "query" else [(1,), (2,), (3,)]
        assert query(connection, "SELECT value FROM sail.default.namespace_table ORDER BY value") == expected
        assert query(connection, f"SELECT value FROM {alternate}") == [(99,)]  # noqa: S608


@pytest.mark.parametrize("file_format", ["delta", "iceberg"])
@pytest.mark.parametrize("parameterized", [False, True])
def test_prepared_query_refreshes_table_snapshot(flight_connection, tmp_path, file_format, parameterized):
    query(
        flight_connection,
        f"CREATE TABLE flight_fresh (value BIGINT) USING {file_format} LOCATION '{tmp_path.as_posix()}'",
    )
    query(flight_connection, "INSERT INTO flight_fresh VALUES (1)")
    with adbc_driver_manager.AdbcStatement(flight_connection.adbc_connection) as statement:
        sql = (
            "SELECT value FROM flight_fresh WHERE value > CAST(? AS BIGINT) ORDER BY value"
            if parameterized
            else "SELECT value FROM flight_fresh ORDER BY value"
        )
        statement.set_sql_query(sql)
        statement.prepare()
        if parameterized:
            statement.bind(pa.record_batch([pa.array([0], type=pa.int64())], names=["minimum"]))
        assert read_result(statement).column("value").to_pylist() == [1]
        query(flight_connection, "INSERT INTO flight_fresh VALUES (2)")
        assert read_result(statement).column("value").to_pylist() == [1, 2]


@pytest.mark.parametrize("sql", ["SELECT CAST(? AS BIGINT) AS value", "SELECT ? AS value"])
def test_strict_prepared_query(flight_connection, sql):
    with adbc_driver_manager.AdbcStatement(flight_connection.adbc_connection) as statement:
        statement.set_sql_query(sql)
        statement.prepare()
        for value in (41, 72, None):
            statement.bind(pa.record_batch([pa.array([value], type=pa.int64())], names=["value"]))
            assert read_result(statement).to_pylist() == [{"value": value}]


def test_bound_query_batches(flight_connection):
    with adbc_driver_manager.AdbcStatement(flight_connection.adbc_connection) as statement:
        statement.set_sql_query("SELECT CAST(? AS BIGINT) AS value")
        statement.prepare()
        batch = pa.record_batch([pa.array([1, 2, 3], type=pa.int64())], names=["value"])
        statement.bind(batch)
        # SQL without ORDER BY does not order results across parameter sets.
        assert sorted(read_result(statement).column("value").to_pylist()) == [1, 2, 3]


def test_dbapi_parameter_binding(flight_connection):
    with flight_connection.cursor() as cursor:
        cursor.execute("SELECT CAST(? AS BIGINT) + 1 AS value, CAST(? AS STRING) AS label", (41, "answer"))
        assert cursor.fetchall() == [(42, "answer")]


def test_preparation_and_schema_have_no_side_effects(flight_connection):
    sql = "CREATE TEMP VIEW flight_prepared_view AS SELECT 9 AS value"
    with adbc_driver_manager.AdbcStatement(flight_connection.adbc_connection) as statement:
        statement.set_sql_query(sql)
        statement.prepare()
        assert tables(flight_connection, table_name_filter="flight_prepared_view") == []
        schema = statement.execute_schema()
        assert pa.Schema._import_from_c(schema.address).names == []  # noqa: SLF001
        assert tables(flight_connection, table_name_filter="flight_prepared_view") == []
        assert statement.execute_update() == -1
        assert query(flight_connection, "SELECT * FROM flight_prepared_view") == [(9,)]


def test_command_query_executes_without_fetch(flight_connection):
    with adbc_driver_manager.AdbcStatement(flight_connection.adbc_connection) as statement:
        statement.set_sql_query("CREATE TEMP VIEW flight_eager AS SELECT 7 AS value")
        handle, _ = statement.execute_query()
        # The command has finished before anyone consumes its DoGet stream.
        assert query(flight_connection, "SELECT * FROM flight_eager") == [(7,)]
        with pa.RecordBatchReader._import_from_c(handle.address) as reader:  # noqa: SLF001
            reader.read_all()
        assert query(flight_connection, "SELECT * FROM flight_eager") == [(7,)]


def test_schema_only_query(flight_connection):
    with adbc_driver_manager.AdbcStatement(flight_connection.adbc_connection) as statement:
        statement.set_sql_query("SELECT CAST(1 AS BIGINT) AS value, 'hello' AS label WHERE false")
        handle = statement.execute_schema()
        schema = pa.Schema._import_from_c(handle.address)  # noqa: SLF001
        assert schema.names == ["value", "label"]
        assert schema.field("value").type == pa.int64()


@pytest.mark.parametrize("file_format", ["parquet", "delta", "iceberg"])
def test_prepared_update_and_reuse(flight_connection, tmp_path, file_format):
    location = tmp_path.as_posix()
    query(flight_connection, f"CREATE TABLE flight_updates (value BIGINT) USING {file_format} LOCATION '{location}'")
    with adbc_driver_manager.AdbcStatement(flight_connection.adbc_connection) as statement:
        statement.set_sql_query("INSERT INTO flight_updates VALUES (CAST(? AS BIGINT))")
        statement.prepare()
        assert query(flight_connection, "SELECT count(*) FROM flight_updates") == [(0,)]
        for index, value in enumerate((11, 22)):
            statement.bind(pa.record_batch([pa.array([value], type=pa.int64())], names=["value"]))
            assert query(flight_connection, "SELECT count(*) FROM flight_updates") == [(index,)]
            assert statement.execute_update() == -1
    assert query(flight_connection, "SELECT * FROM flight_updates ORDER BY value") == [(11,), (22,)]


@pytest.mark.parametrize("file_format", ["delta", "iceberg"])
@pytest.mark.parametrize("placeholder", ["?", "$1"])
def test_prepared_delete_and_reuse(flight_connection, tmp_path, file_format, placeholder):
    query(
        flight_connection,
        f"CREATE TABLE flight_deletes (value BIGINT) USING {file_format} LOCATION '{tmp_path.as_posix()}'",
    )
    query(flight_connection, "INSERT INTO flight_deletes VALUES (1), (2), (3)")
    with adbc_driver_manager.AdbcStatement(flight_connection.adbc_connection) as statement:
        statement.set_sql_query(f"DELETE FROM flight_deletes WHERE value = {placeholder}")  # noqa: S608
        statement.prepare()
        assert query(flight_connection, "SELECT value FROM flight_deletes ORDER BY value") == [(1,), (2,), (3,)]
        for value, expected in ((2, [(1,), (3,)]), (1, [(3,)])):
            statement.bind(pa.record_batch([pa.array([value], type=pa.int64())], names=["value"]))
            assert statement.execute_update() == -1
            assert query(flight_connection, "SELECT value FROM flight_deletes ORDER BY value") == expected


@pytest.mark.parametrize("placeholder", ["?", "$1"])
def test_prepared_temporary_view_binds_definition(flight_connection, placeholder):
    with adbc_driver_manager.AdbcStatement(flight_connection.adbc_connection) as statement:
        statement.set_sql_query(
            f"CREATE OR REPLACE TEMP VIEW flight_bound_view AS SELECT CAST({placeholder} AS BIGINT) AS value"
        )
        statement.prepare()
        assert tables(flight_connection, table_name_filter="flight_bound_view") == []
        for value in (17, None):
            statement.bind(pa.record_batch([pa.array([value], type=pa.int64())], names=["value"]))
            assert statement.execute_update() == -1
            assert query(flight_connection, "SELECT value FROM flight_bound_view") == [(value,)]


def test_prepared_persistent_view_rejects_parameters(flight_connection):
    with adbc_driver_manager.AdbcStatement(flight_connection.adbc_connection) as statement:
        statement.set_sql_query("CREATE VIEW flight_unbound_view AS SELECT CAST(? AS BIGINT) AS value")
        with pytest.raises(adbc_driver_manager.NotSupportedError, match="persistent view"):
            statement.prepare()
    assert tables(flight_connection, table_name_filter="flight_unbound_view") == []


def test_direct_update(flight_connection):
    with adbc_driver_manager.AdbcStatement(flight_connection.adbc_connection) as statement:
        statement.set_sql_query("CREATE TEMP VIEW flight_direct_update AS SELECT 1 AS value")
        assert statement.execute_update() == -1
    assert query(flight_connection, "SELECT * FROM flight_direct_update") == [(1,)]


def test_connections_are_isolated(flight_uri):
    with connect(flight_uri) as first, connect(flight_uri) as second:
        query(first, "CREATE TEMP VIEW flight_isolated AS SELECT 1 AS value")
        query(second, "CREATE TEMP VIEW flight_isolated AS SELECT 2 AS value")
        assert query(first, "SELECT * FROM flight_isolated") == [(1,)]
        assert query(second, "SELECT * FROM flight_isolated") == [(2,)]


def test_session_options(flight_connection):
    options = flight_connection.adbc_connection
    prefix = ConnectionOptions.OPTION_SESSION_OPTION_PREFIX.value
    options.set_options(**{prefix + "catalog": "sail", prefix + "schema": "default"})
    assert options.get_option(prefix + "catalog") == "sail"
    assert options.get_option(prefix + "schema") == "default"
    values = json.loads(options.get_option(ConnectionOptions.OPTION_SESSION_OPTIONS.value))
    assert values["catalog"] == "sail"
    with pytest.raises(adbc_driver_manager.Error):
        options.set_options(**{prefix + "schema": "no_such_schema"})


def test_prepared_session_commands_update_live_session(flight_connection):
    query(flight_connection, "CREATE DATABASE flight_session_command")
    with adbc_driver_manager.AdbcStatement(flight_connection.adbc_connection) as statement:
        statement.set_sql_query("USE DATABASE flight_session_command")
        statement.prepare()
        assert flight_connection.adbc_current_db_schema == "default"
        assert statement.execute_update() == -1
    assert flight_connection.adbc_current_db_schema == "flight_session_command"
    batch_size = 64
    row_count = 1000
    with adbc_driver_manager.AdbcStatement(flight_connection.adbc_connection) as statement:
        statement.set_sql_query(f"SET datafusion.execution.batch_size = {batch_size}")
        statement.prepare()
        assert statement.execute_update() == -1
    with flight_connection.cursor() as cursor:
        cursor.execute("SELECT id FROM range(1000) ORDER BY id DESC")
        table = cursor.fetch_arrow_table()
    assert table.num_rows == row_count
    assert all(batch.num_rows <= batch_size for batch in table.to_batches())


def test_bad_binding_does_not_execute(flight_connection):
    with adbc_driver_manager.AdbcStatement(flight_connection.adbc_connection) as statement:
        statement.set_sql_query("SELECT CAST(? AS BIGINT) AS value")
        statement.prepare()
        statement.bind(pa.record_batch([pa.array([1]), pa.array([2])], names=["a", "b"]))
        with pytest.raises(adbc_driver_manager.Error):
            read_result(statement)
    assert query(flight_connection, "SELECT 42") == [(42,)]


def test_empty_parameter_batch_is_not_executed(flight_connection):
    with adbc_driver_manager.AdbcStatement(flight_connection.adbc_connection) as statement:
        statement.set_sql_query("SELECT CAST(? AS BIGINT) AS value")
        statement.prepare()
        statement.bind(pa.record_batch([pa.array([], type=pa.int64())], names=["value"]))
        # The ADBC driver omits DoPut for a zero-row batch, leaving this
        # statement unbound. It must not execute with fabricated values.
        with pytest.raises(adbc_driver_manager.Error):
            read_result(statement)


def test_unbound_parameters(flight_connection):
    with adbc_driver_manager.AdbcStatement(flight_connection.adbc_connection) as statement:
        statement.set_sql_query("SELECT CAST(? AS BIGINT) AS value")
        statement.prepare()
        with pytest.raises(adbc_driver_manager.Error):
            read_result(statement)


def test_parameter_marker_in_literal(flight_connection):
    with flight_connection.cursor() as cursor:
        cursor.execute("SELECT '?' AS literal, CAST(? AS BIGINT) AS value -- ?", (42,))
        assert cursor.fetchall() == [("?", 42)]


def test_catalog_and_schema_options_are_isolated(flight_uri):
    with connect(flight_uri) as first, connect(flight_uri) as second:
        query(first, "CREATE DATABASE flight_session_schema")
        first.adbc_current_db_schema = "flight_session_schema"
        assert first.adbc_current_db_schema == "flight_session_schema"
        assert second.adbc_current_db_schema == "default"
        assert query(first, "SELECT current_database()") == [("flight_session_schema",)]
        assert query(second, "SELECT current_database()") == [("default",)]


def test_anonymous_client_without_cookies(flight_uri):
    from adbc_driver_flightsql import dbapi

    with dbapi.connect(flight_uri, autocommit=True) as connection:
        query(connection, "CREATE TEMP VIEW flight_transport AS SELECT 5 AS value")
        assert query(connection, "SELECT * FROM flight_transport") == [(5,)]


def test_empty_query_retains_schema(flight_connection):
    with flight_connection.cursor() as cursor:
        cursor.execute("SELECT CAST(1 AS BIGINT) AS value WHERE false")
        table = cursor.fetch_arrow_table()
        assert table.num_rows == 0
        assert table.schema.field("value").type == pa.int64()


def test_multiple_result_batches(flight_connection):
    with flight_connection.cursor() as cursor:
        cursor.execute("SELECT id FROM range(20000)")
        table = cursor.fetch_arrow_table()
        assert table.num_rows == 20000  # noqa: PLR2004
        assert table.column("id")[0].as_py() == 0
        assert table.column("id")[-1].as_py() == 19999  # noqa: PLR2004


def test_show_and_describe_return_rows(flight_connection):
    query(flight_connection, "CREATE TEMP VIEW flight_describe AS SELECT 1 AS value")
    rows = query(flight_connection, "SHOW TABLES LIKE 'flight_describe'")
    assert any(row[1] == "flight_describe" for row in rows)
    rows = query(flight_connection, "DESCRIBE flight_describe")
    assert rows[0][0] == "value"


@pytest.mark.parametrize("values", [["1", "bad"], ["1", "2"]])
def test_update_batch_is_rejected_before_side_effects(flight_connection, tmp_path, values):
    query(flight_connection, f"CREATE TABLE batch_update (value INT) USING parquet LOCATION '{tmp_path.as_posix()}'")
    with adbc_driver_manager.AdbcStatement(flight_connection.adbc_connection) as statement:
        statement.set_sql_query("INSERT INTO batch_update VALUES (CAST(? AS INT))")
        statement.prepare()
        statement.bind(pa.record_batch([pa.array(values)], names=["value"]))
        with pytest.raises(adbc_driver_manager.NotSupportedError, match="multiple parameter sets"):
            statement.execute_update()
        assert query(flight_connection, "SELECT count(*) FROM batch_update") == [(0,)]
        statement.bind(pa.record_batch([pa.array(["3"])], names=["value"]))
        assert statement.execute_update() == -1
    assert query(flight_connection, "SELECT value FROM batch_update") == [(3,)]


def test_procedure_returns_schema_and_rows(flight_connection, tmp_path):
    query(flight_connection, f"CREATE TABLE procedure_table (id BIGINT) USING iceberg LOCATION '{tmp_path.as_posix()}'")
    query(flight_connection, "INSERT INTO procedure_table VALUES (1)")
    snapshots = query(flight_connection, "SELECT snapshot_id FROM procedure_table.snapshots")
    with adbc_driver_manager.AdbcStatement(flight_connection.adbc_connection) as statement:
        statement.set_sql_query("CALL system.ancestors_of('procedure_table')")
        statement.prepare()
        schema_handle = statement.execute_schema()
        schema = pa.Schema._import_from_c(schema_handle.address)  # noqa: SLF001
        assert schema.names == ["snapshot_id", "timestamp"]
        with pytest.raises(adbc_driver_manager.Error, match="returns rows"):
            statement.execute_update()
        table = read_result(statement)
        assert table.schema == schema
        assert table.column("snapshot_id").to_pylist() == [row[0] for row in snapshots]
