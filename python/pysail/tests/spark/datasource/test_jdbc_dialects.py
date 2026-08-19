"""Integration tests for the SQLAlchemy write fallback (MySQL, SQL Server).

Each dialect starts its own testcontainer.  Writes go through the Sail Spark
Connect server (``df.write.format("jdbc")``); results are verified with a direct
SQLAlchemy query so the assertions do not depend on the reader.
"""

from __future__ import annotations

import pytest

from pysail.testing.spark.jdbc_oracle import (
    SPARK_TYPE_MATRIX_SELECT_EXPRS,
    native_spark_4_1_2_python,
    run_native_jdbc_write,
)

pytestmark = pytest.mark.integration

try:
    from pyspark.sql.datasource import DataSourceArrowWriter  # noqa: F401  (Spark 4.0+)
except ImportError:
    pytest.skip("JDBC data source requires the PySpark Python DataSource API (4.0+)", allow_module_level=True)


@pytest.fixture(scope="module", autouse=True)
def register_jdbc(spark):
    from pysail.spark.datasource.jdbc import JdbcDataSource

    spark.dataSource.register(JdbcDataSource)


def _schema():
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    return StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("score", DoubleType()),
        ]
    )


def _count_names(sa_url, table):
    import sqlalchemy as sa

    engine = sa.create_engine(sa_url)
    try:
        with engine.connect() as conn:
            rows = conn.execute(sa.text(f"SELECT name FROM {table}")).fetchall()  # noqa: S608
    finally:
        engine.dispose()
    return len(rows), {r[0] for r in rows}


def _drop_table(sa_url, table):
    import sqlalchemy as sa

    engine = sa.create_engine(sa_url)
    try:
        with engine.begin() as conn:
            conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
    finally:
        engine.dispose()


def _column_types(sa_url, table):
    import sqlalchemy as sa

    engine = sa.create_engine(sa_url)
    try:
        return {column["name"]: str(column["type"]).lower() for column in sa.inspect(engine).get_columns(table)}
    finally:
        engine.dispose()


def _type_snapshot(sql_type):
    """Normalize SQLAlchemy's driver-specific type objects to behavioral fields."""
    return (
        type(sql_type).__name__.lower(),
        getattr(sql_type, "length", None),
        getattr(sql_type, "precision", None),
        getattr(sql_type, "scale", None),
    )


def _identity_snapshot(identity):
    if not identity:
        return None
    return identity.get("start"), identity.get("increment")


def _table_snapshot(sa_url, table):
    import sqlalchemy as sa

    engine = sa.create_engine(sa_url)
    try:
        inspector = sa.inspect(engine)
        columns = [
            (
                column["name"],
                _type_snapshot(column["type"]),
                column["nullable"],
                bool(column.get("autoincrement")),
                _identity_snapshot(column.get("identity")),
            )
            for column in inspector.get_columns(table)
        ]
        indexes = sorted(
            (tuple(index["column_names"]), bool(index.get("unique"))) for index in inspector.get_indexes(table)
        )
        with engine.connect() as conn:
            rows = conn.execute(sa.text(f"SELECT * FROM {table}")).fetchall()  # noqa: S608
        return columns, indexes, sorted((tuple(row) for row in rows), key=repr)
    finally:
        engine.dispose()


def _created_types_differential(spark, ctx, *, dialect, oracle_options):
    """Auto-create a target from Spark's full write type matrix on both engines and
    compare the created column types, nullability, and round-tripped values.
    """
    opts, sa_url = ctx
    native_table = f"wt_{dialect}_native_type_matrix"
    sail_table = f"wt_{dialect}_sail_type_matrix"
    for table in (native_table, sail_table):
        _drop_table(sa_url, table)
    try:
        run_native_jdbc_write(
            dialect=dialect,
            jdbc_url=opts["url"],
            dbtable=native_table,
            user=opts["user"],
            password=opts["password"],
            schema_json=None,
            rows=[],
            mode="append",
            options=oracle_options,
            select_exprs=SPARK_TYPE_MATRIX_SELECT_EXPRS,
        )
        spark.range(1).selectExpr(*SPARK_TYPE_MATRIX_SELECT_EXPRS).write.format("jdbc").option(
            "dbtable", sail_table
        ).options(**opts).mode("append").save()
        native = _table_snapshot(sa_url, native_table)
        sail = _table_snapshot(sa_url, sail_table)
        assert native == sail
        assert len(native[0]) == len(SPARK_TYPE_MATRIX_SELECT_EXPRS)
    finally:
        for table in (native_table, sail_table):
            _drop_table(sa_url, table)


@pytest.mark.parametrize(
    ("fixture_name", "dialect", "oracle_options"),
    [
        ("mysql_ctx", "mysql", {}),
        ("mssql_ctx", "sqlserver", {"trustServerCertificate": "true"}),
    ],
)
@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_reordered_case_varied_append_matches_native_spark(
    request,
    spark,
    fixture_name,
    dialect,
    oracle_options,
):
    import sqlalchemy as sa
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    opts, sa_url = request.getfixturevalue(fixture_name)
    native_table = f"wt_{dialect}_native_columns"
    sail_table = f"wt_{dialect}_sail_columns"
    engine = sa.create_engine(sa_url)
    try:
        with engine.begin() as conn:
            for table in (native_table, sail_table):
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
                conn.execute(sa.text(f"CREATE TABLE {table} (ID INT, DisplayName VARCHAR(64))"))

        schema = StructType([StructField("displayname", StringType()), StructField("id", IntegerType())])
        rows = [["Alice", 1], ["Bob", 2]]
        run_native_jdbc_write(
            dialect=dialect,
            jdbc_url=opts["url"],
            dbtable=native_table,
            user=opts["user"],
            password=opts["password"],
            schema_json=schema.jsonValue(),
            rows=rows,
            mode="append",
            options=oracle_options,
        )
        spark.createDataFrame([tuple(row) for row in rows], schema).write.format("jdbc").option(
            "dbtable", sail_table
        ).options(**opts).mode("append").save()

        assert _table_snapshot(sa_url, native_table) == _table_snapshot(sa_url, sail_table)
    finally:
        with engine.begin() as conn:
            for table in (native_table, sail_table):
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
        engine.dispose()


@pytest.mark.parametrize(
    ("fixture_name", "dialect", "oracle_options"),
    [
        ("mysql_ctx", "mysql", {}),
        ("mssql_ctx", "sqlserver", {"trustServerCertificate": "true"}),
    ],
)
@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_default_overwrite_schema_change_matches_native_spark(
    request,
    spark,
    fixture_name,
    dialect,
    oracle_options,
):
    import sqlalchemy as sa
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    opts, sa_url = request.getfixturevalue(fixture_name)
    native_table = f"wt_{dialect}_native_schema"
    sail_table = f"wt_{dialect}_sail_schema"
    engine = sa.create_engine(sa_url)
    try:
        with engine.begin() as conn:
            for table in (native_table, sail_table):
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
                conn.execute(sa.text(f"CREATE TABLE {table} (old_id INT, old_value VARCHAR(64))"))

        schema = StructType([StructField("new_id", IntegerType()), StructField("label", StringType())])
        rows = [[1, "new"]]
        run_native_jdbc_write(
            dialect=dialect,
            jdbc_url=opts["url"],
            dbtable=native_table,
            user=opts["user"],
            password=opts["password"],
            schema_json=schema.jsonValue(),
            rows=rows,
            mode="overwrite",
            options=oracle_options,
        )
        spark.createDataFrame([tuple(row) for row in rows], schema).write.format("jdbc").option(
            "dbtable", sail_table
        ).options(**opts).mode("overwrite").save()

        assert _table_snapshot(sa_url, native_table) == _table_snapshot(sa_url, sail_table)
    finally:
        with engine.begin() as conn:
            for table in (native_table, sail_table):
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
        engine.dispose()


@pytest.mark.parametrize(
    ("fixture_name", "dialect", "oracle_options"),
    [
        ("mysql_ctx", "mysql", {}),
        ("mssql_ctx", "sqlserver", {"trustServerCertificate": "true"}),
    ],
)
@pytest.mark.parametrize("mode", [None, "error", "ignore"])
@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_existing_table_nonwriting_modes_match_native_spark(
    request,
    spark,
    fixture_name,
    dialect,
    oracle_options,
    mode,
):
    import subprocess

    import sqlalchemy as sa
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    opts, sa_url = request.getfixturevalue(fixture_name)
    suffix = mode or "default"
    native_table = f"wt_{dialect}_native_{suffix}"
    sail_table = f"wt_{dialect}_sail_{suffix}"
    engine = sa.create_engine(sa_url)
    try:
        with engine.begin() as conn:
            for table in (native_table, sail_table):
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
                conn.execute(sa.text(f"CREATE TABLE {table} (id INT, name VARCHAR(64))"))
                conn.execute(sa.text(f"INSERT INTO {table} VALUES (1, 'old')"))

        schema = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
        rows = [[2, "new"]]

        def native_call():
            run_native_jdbc_write(
                dialect=dialect,
                jdbc_url=opts["url"],
                dbtable=native_table,
                user=opts["user"],
                password=opts["password"],
                schema_json=schema.jsonValue(),
                rows=rows,
                mode=mode,
                options=oracle_options,
                select_exprs=["raise_error('ignore evaluated input') AS id"] if mode == "ignore" else None,
            )

        def sail_call():
            df = (
                spark.range(1).selectExpr("raise_error('ignore evaluated input') AS id")
                if mode == "ignore"
                else spark.createDataFrame([tuple(row) for row in rows], schema)
            )
            (df.write.format("jdbc").option("dbtable", sail_table).options(**opts).mode(mode).save())

        if mode in (None, "error"):
            with pytest.raises(subprocess.CalledProcessError):
                native_call()
            with pytest.raises(Exception, match="already exists"):
                sail_call()
        else:
            native_call()
            sail_call()

        assert _table_snapshot(sa_url, native_table) == _table_snapshot(sa_url, sail_table)
    finally:
        with engine.begin() as conn:
            for table in (native_table, sail_table):
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
        engine.dispose()


@pytest.mark.parametrize(
    ("fixture_name", "dialect", "oracle_options"),
    [
        ("mysql_ctx", "mysql", {}),
        ("mssql_ctx", "sqlserver", {"trustServerCertificate": "true"}),
    ],
)
@pytest.mark.parametrize("mode", [None, "append", "overwrite", "ignore"])
@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_missing_table_save_modes_match_native_spark(
    request,
    spark,
    fixture_name,
    dialect,
    oracle_options,
    mode,
):
    opts, sa_url = request.getfixturevalue(fixture_name)
    suffix = mode or "default"
    native_table = f"wt_{dialect}_native_missing_{suffix}"
    sail_table = f"wt_{dialect}_sail_missing_{suffix}"
    for table in (native_table, sail_table):
        _drop_table(sa_url, table)
    try:
        schema = _schema()
        rows = [[1, "new", 2.0]]
        run_native_jdbc_write(
            dialect=dialect,
            jdbc_url=opts["url"],
            dbtable=native_table,
            user=opts["user"],
            password=opts["password"],
            schema_json=schema.jsonValue(),
            rows=rows,
            mode=mode,
            options=oracle_options,
        )
        writer = (
            spark.createDataFrame([tuple(row) for row in rows], schema)
            .write.format("jdbc")
            .option("dbtable", sail_table)
            .options(**opts)
        )
        if mode is not None:
            writer = writer.mode(mode)
        writer.save()

        assert _table_snapshot(sa_url, native_table) == _table_snapshot(sa_url, sail_table)
    finally:
        for table in (native_table, sail_table):
            _drop_table(sa_url, table)


@pytest.mark.parametrize(
    ("fixture_name", "dialect", "oracle_options"),
    [
        ("mysql_ctx", "mysql", {}),
        ("mssql_ctx", "sqlserver", {"trustServerCertificate": "true"}),
    ],
)
@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_truncate_schema_mismatch_failure_matches_native_spark(
    request,
    spark,
    fixture_name,
    dialect,
    oracle_options,
):
    import subprocess

    import sqlalchemy as sa
    from pyspark.sql.types import IntegerType, StructField, StructType

    opts, sa_url = request.getfixturevalue(fixture_name)
    native_table = f"wt_{dialect}_native_truncate_mismatch"
    sail_table = f"wt_{dialect}_sail_truncate_mismatch"
    engine = sa.create_engine(sa_url)
    try:
        with engine.begin() as conn:
            for table in (native_table, sail_table):
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
                conn.execute(sa.text(f"CREATE TABLE {table} (old_id INT)"))
                conn.execute(sa.text(f"INSERT INTO {table} VALUES (1)"))

        schema = StructType([StructField("new_id", IntegerType())])
        rows = [[2]]
        with pytest.raises(subprocess.CalledProcessError):
            run_native_jdbc_write(
                dialect=dialect,
                jdbc_url=opts["url"],
                dbtable=native_table,
                user=opts["user"],
                password=opts["password"],
                schema_json=schema.jsonValue(),
                rows=rows,
                mode="overwrite",
                options={**oracle_options, "truncate": "true"},
            )
        with pytest.raises(Exception, match=r"new_id|schema"):
            (
                spark.createDataFrame([tuple(row) for row in rows], schema)
                .write.format("jdbc")
                .option("dbtable", sail_table)
                .option("truncate", "true")
                .options(**opts)
                .mode("overwrite")
                .save()
            )

        assert _table_snapshot(sa_url, native_table) == _table_snapshot(sa_url, sail_table)
    finally:
        with engine.begin() as conn:
            for table in (native_table, sail_table):
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
        engine.dispose()


@pytest.mark.parametrize("fixture_name", ["mysql_ctx", "mssql_ctx"])
def test_sqlalchemy_partition_failure_rolls_back_only_failed_partition(request, fixture_name):
    import pyarrow as pa
    import sqlalchemy as sa

    from pysail.spark.datasource.jdbc import SqlAlchemyWriteEngine

    _, sa_url = request.getfixturevalue(fixture_name)
    table = f"wt_{fixture_name}_partition_rollback"
    engine = sa.create_engine(sa_url)
    try:
        with engine.begin() as conn:
            conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
            conn.execute(sa.text(f"CREATE TABLE {table} (id INT PRIMARY KEY)"))
        writer = SqlAlchemyWriteEngine(
            url=sa_url,
            dbtable=table,
            batch_size=1000,
        )
        writer.write_partition(0, [pa.RecordBatch.from_pydict({"id": [1]})])
        with pytest.raises(RuntimeError):
            writer.write_partition(1, [pa.RecordBatch.from_pydict({"id": [2, 1]})])

        with engine.connect() as conn:
            assert conn.execute(sa.text(f"SELECT id FROM {table} ORDER BY id")).fetchall() == [(1,)]
    finally:
        with engine.begin() as conn:
            conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
        engine.dispose()


@pytest.mark.parametrize("fixture_name", ["mysql_ctx", "mssql_ctx"])
def test_sqlalchemy_retry_after_commit_is_at_least_once(request, fixture_name):
    import pyarrow as pa
    import sqlalchemy as sa

    from pysail.spark.datasource.jdbc import SqlAlchemyWriteEngine

    _, sa_url = request.getfixturevalue(fixture_name)
    table = f"wt_{fixture_name}_partition_retry"
    engine = sa.create_engine(sa_url)
    try:
        with engine.begin() as conn:
            conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
            conn.execute(sa.text(f"CREATE TABLE {table} (id INT)"))
        writer = SqlAlchemyWriteEngine(
            url=sa_url,
            dbtable=table,
            batch_size=1000,
        )
        batch = pa.RecordBatch.from_pydict({"id": [1]})
        writer.write_partition(0, [batch])
        writer.write_partition(0, [batch])

        with engine.connect() as conn:
            assert conn.execute(sa.text(f"SELECT id FROM {table} ORDER BY id")).fetchall() == [(1,), (1,)]
    finally:
        with engine.begin() as conn:
            conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
        engine.dispose()


# ---------------------------------------------------------------------------
# MySQL
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def mysql_ctx():
    from testcontainers.community.mysql import MySqlContainer

    with MySqlContainer("mysql:8.4") as c:
        host = c.get_container_host_ip()
        port = c.get_exposed_port(3306)
        jdbc_url = f"jdbc:mysql://{host}:{port}/{c.dbname}"
        sa_url = f"mysql+pymysql://{c.username}:{c.password}@{host}:{port}/{c.dbname}"
        opts = {"url": jdbc_url, "user": c.username, "password": c.password}
        yield opts, sa_url


@pytest.fixture
def mysql_table(mysql_ctx):
    import sqlalchemy as sa

    opts, sa_url = mysql_ctx
    table = "wt_mysql"
    engine = sa.create_engine(sa_url)
    with engine.begin() as conn:
        conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
        conn.execute(sa.text(f"CREATE TABLE {table} (id INT, name VARCHAR(64), score DOUBLE)"))
    engine.dispose()
    yield table, opts, sa_url
    engine = sa.create_engine(sa_url)
    with engine.begin() as conn:
        conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
    engine.dispose()


def test_mysql_server_version_is_pinned(mysql_ctx):
    import sqlalchemy as sa

    _, sa_url = mysql_ctx
    engine = sa.create_engine(sa_url)
    try:
        with engine.connect() as conn:
            assert conn.execute(sa.text("SELECT VERSION()")).scalar_one().startswith("8.4.")
    finally:
        engine.dispose()


def test_mysql_write_append(spark, mysql_table):
    table, opts, sa_url = mysql_table
    df = spark.createDataFrame([(1, "Alice", 9.5), (2, "Bob", 7.2)], _schema())
    df.write.format("jdbc").option("dbtable", table).options(**opts).mode("append").save()

    count, names = _count_names(sa_url, table)
    assert count == 2  # noqa: PLR2004
    assert names == {"Alice", "Bob"}


def test_mysql_write_overwrite(spark, mysql_table):
    table, opts, sa_url = mysql_table
    first = spark.createDataFrame([(1, "Alice", 9.5), (2, "Bob", 7.2)], _schema())
    second = spark.createDataFrame([(3, "Charlie", 8.8)], _schema())

    first.write.format("jdbc").option("dbtable", table).options(**opts).mode("append").save()
    second.write.format("jdbc").option("dbtable", table).options(**opts).mode("overwrite").save()

    count, names = _count_names(sa_url, table)
    assert count == 1
    assert names == {"Charlie"}


@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_mysql_created_types_match_native_spark_4_1_2(spark, mysql_ctx):
    """Create-table type mapping parity across Spark's MySQL write matrix."""
    _created_types_differential(spark, mysql_ctx, dialect="mysql", oracle_options={})


def test_mysql_create_options_and_comment(spark, mysql_ctx):
    import sqlalchemy as sa
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    opts, sa_url = mysql_ctx
    table = "wt_mysql_create_options"
    _drop_table(sa_url, table)
    try:
        schema = StructType([StructField("id", IntegerType()), StructField("label", StringType())])
        spark.createDataFrame([(1, "one")], schema).write.format("jdbc").option("dbtable", table).options(
            **opts,
            createTableColumnTypes="LABEL VARCHAR(32)",
            createTableOptions="ENGINE=InnoDB",
            tableComment="spark parity evidence",
            isolationLevel="SERIALIZABLE",
            queryTimeout="10",
        ).mode("append").save()
        engine = sa.create_engine(sa_url)
        with engine.connect() as conn:
            row = conn.execute(
                sa.text(
                    "SELECT ENGINE, TABLE_COMMENT FROM information_schema.tables "
                    "WHERE table_schema = DATABASE() AND table_name = :table"
                ),
                {"table": table},
            ).one()
            length = conn.execute(
                sa.text(
                    "SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() AND table_name = :table AND column_name = 'label'"
                ),
                {"table": table},
            ).scalar_one()
        engine.dispose()
        assert (row[0].lower(), row[1], length) == ("innodb", "spark parity evidence", 32)
    finally:
        _drop_table(sa_url, table)


@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_mysql_write_matches_native_spark_4_1_2(spark, mysql_ctx):
    opts, sa_url = mysql_ctx
    native_table = "wt_mysql_native_oracle"
    sail_table = "wt_mysql_sail_oracle"
    for table in (native_table, sail_table):
        _drop_table(sa_url, table)
    try:
        rows = [[1, "Alice", 9.5], [2, "Bob", 7.2]]
        schema = _schema()
        run_native_jdbc_write(
            dialect="mysql",
            jdbc_url=opts["url"],
            dbtable=native_table,
            user=opts["user"],
            password=opts["password"],
            schema_json=schema.jsonValue(),
            rows=rows,
            mode="append",
        )
        spark.createDataFrame([tuple(row) for row in rows], schema).write.format("jdbc").option(
            "dbtable", sail_table
        ).options(**opts).mode("append").save()
        assert _column_types(sa_url, native_table) == _column_types(sa_url, sail_table)
        assert _count_names(sa_url, native_table) == _count_names(sa_url, sail_table)
    finally:
        for table in (native_table, sail_table):
            _drop_table(sa_url, table)


@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_mysql_truncate_overwrite_matches_native_spark_4_1_2(spark, mysql_ctx):
    import sqlalchemy as sa
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    opts, sa_url = mysql_ctx
    native_table = "wt_mysql_native_truncate"
    sail_table = "wt_mysql_sail_truncate"
    engine = sa.create_engine(sa_url)
    try:
        with engine.begin() as conn:
            for table in (native_table, sail_table):
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
                conn.execute(
                    sa.text(
                        f"CREATE TABLE {table} "
                        "(id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(64), score DOUBLE, INDEX name_idx (name))"
                    )
                )
                conn.execute(sa.text(f"INSERT INTO {table} (name, score) VALUES ('old', 1.0)"))

        rows = [["new", 2.0]]
        schema = StructType([StructField("name", StringType()), StructField("score", DoubleType())])
        run_native_jdbc_write(
            dialect="mysql",
            jdbc_url=opts["url"],
            dbtable=native_table,
            user=opts["user"],
            password=opts["password"],
            schema_json=schema.jsonValue(),
            rows=rows,
            mode="overwrite",
            options={"truncate": "true"},
        )
        spark.createDataFrame([tuple(row) for row in rows], schema).write.format("jdbc").option(
            "dbtable", sail_table
        ).option("truncate", "true").options(**opts).mode("overwrite").save()

        assert _table_snapshot(sa_url, native_table) == _table_snapshot(sa_url, sail_table)
    finally:
        with engine.begin() as conn:
            for table in (native_table, sail_table):
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
        engine.dispose()


# ---------------------------------------------------------------------------
# Integer fidelity: NULLs stay NULL and bigints > 2^53 keep exact precision.
# Regression guard for the pandas to_sql float64 coercion bug.
# ---------------------------------------------------------------------------

_BIG = 2**53 + 1  # 9007199254740993 — not representable as a float64


def _bigint_schema():
    from pyspark.sql.types import LongType, StructField, StructType

    return StructType([StructField("id", LongType()), StructField("big", LongType())])


def _read_id_big(sa_url, table):
    import sqlalchemy as sa

    engine = sa.create_engine(sa_url)
    try:
        with engine.connect() as conn:
            rows = conn.execute(sa.text(f"SELECT id, big FROM {table} ORDER BY id")).fetchall()  # noqa: S608
    finally:
        engine.dispose()
    return rows


@pytest.fixture
def mysql_bigint_table(mysql_ctx):
    import sqlalchemy as sa

    opts, sa_url = mysql_ctx
    table = "wt_mysql_bigint"
    engine = sa.create_engine(sa_url)
    with engine.begin() as conn:
        conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
        conn.execute(sa.text(f"CREATE TABLE {table} (id BIGINT, big BIGINT)"))
    engine.dispose()
    yield table, opts, sa_url
    engine = sa.create_engine(sa_url)
    with engine.begin() as conn:
        conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
    engine.dispose()


def test_mysql_integer_fidelity(spark, mysql_bigint_table):
    """A NULL int and a bigint > 2^53 survive the write exactly (no float coercion)."""
    table, opts, sa_url = mysql_bigint_table
    df = spark.createDataFrame([(1, _BIG), (2, None)], _bigint_schema())
    df.write.format("jdbc").option("dbtable", table).options(**opts).mode("append").save()

    rows = _read_id_big(sa_url, table)
    assert rows == [(1, _BIG), (2, None)]


@pytest.fixture
def mssql_bigint_table(mssql_ctx):
    import sqlalchemy as sa

    opts, sa_url = mssql_ctx
    table = "wt_mssql_bigint"
    engine = sa.create_engine(sa_url)
    with engine.begin() as conn:
        conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
        conn.execute(sa.text(f"CREATE TABLE {table} (id BIGINT, big BIGINT)"))
    engine.dispose()
    yield table, opts, sa_url
    engine = sa.create_engine(sa_url)
    with engine.begin() as conn:
        conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
    engine.dispose()


def test_mssql_integer_fidelity(spark, mssql_bigint_table):
    """A NULL int and a bigint > 2^53 survive the write exactly (no float coercion)."""
    table, opts, sa_url = mssql_bigint_table
    df = spark.createDataFrame([(1, _BIG), (2, None)], _bigint_schema())
    df.write.format("jdbc").option("dbtable", table).options(**opts).mode("append").save()

    rows = _read_id_big(sa_url, table)
    assert rows == [(1, _BIG), (2, None)]


# ---------------------------------------------------------------------------
# SQL Server
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def mssql_ctx():
    from testcontainers.community.mssql import SqlServerContainer

    with SqlServerContainer("mcr.microsoft.com/mssql/server:2022-latest") as c:
        host = c.get_container_host_ip()
        port = c.get_exposed_port(1433)
        jdbc_url = f"jdbc:sqlserver://{host}:{port};databaseName={c.dbname}"
        sa_url = f"mssql+pymssql://{c.username}:{c.password}@{host}:{port}/{c.dbname}"
        opts = {"url": jdbc_url, "user": c.username, "password": c.password}
        yield opts, sa_url


@pytest.fixture
def mssql_table(mssql_ctx):
    import sqlalchemy as sa

    opts, sa_url = mssql_ctx
    table = "wt_mssql"
    engine = sa.create_engine(sa_url)
    with engine.begin() as conn:
        conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
        conn.execute(sa.text(f"CREATE TABLE {table} (id INT, name VARCHAR(64), score FLOAT)"))
    engine.dispose()
    yield table, opts, sa_url
    engine = sa.create_engine(sa_url)
    with engine.begin() as conn:
        conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
    engine.dispose()


def test_mssql_server_version_is_sql_server_2022(mssql_ctx):
    import sqlalchemy as sa

    _, sa_url = mssql_ctx
    engine = sa.create_engine(sa_url)
    try:
        with engine.connect() as conn:
            version = conn.execute(sa.text("SELECT SERVERPROPERTY('ProductMajorVersion')")).scalar_one()
            if isinstance(version, bytes):
                version = version.decode()
            assert version == "16"
    finally:
        engine.dispose()


def test_mssql_write_append(spark, mssql_table):
    table, opts, sa_url = mssql_table
    df = spark.createDataFrame([(1, "Alice", 9.5), (2, "Bob", 7.2)], _schema())
    df.write.format("jdbc").option("dbtable", table).options(**opts).mode("append").save()

    count, names = _count_names(sa_url, table)
    assert count == 2  # noqa: PLR2004
    assert names == {"Alice", "Bob"}


@pytest.mark.parametrize("encrypt", ["true", "false"])
def test_mssql_supported_encrypt_modes_connect(spark, mssql_ctx, encrypt):
    opts, sa_url = mssql_ctx
    table = f"wt_mssql_encrypt_{encrypt}"
    _drop_table(sa_url, table)
    try:
        encrypted_opts = {
            **opts,
            "url": f"{opts['url']};encrypt={encrypt}",
        }
        spark.createDataFrame([(1, "Alice", 9.5)], _schema()).write.format("jdbc").option("dbtable", table).options(
            **encrypted_opts
        ).mode("append").save()
        assert _count_names(sa_url, table) == (1, {"Alice"})
    finally:
        _drop_table(sa_url, table)


def test_mssql_write_overwrite(spark, mssql_table):
    table, opts, sa_url = mssql_table
    first = spark.createDataFrame([(1, "Alice", 9.5), (2, "Bob", 7.2)], _schema())
    second = spark.createDataFrame([(3, "Charlie", 8.8)], _schema())

    first.write.format("jdbc").option("dbtable", table).options(**opts).mode("append").save()
    second.write.format("jdbc").option("dbtable", table).options(**opts).mode("overwrite").save()

    count, names = _count_names(sa_url, table)
    assert count == 1
    assert names == {"Charlie"}


@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_mssql_created_types_match_native_spark_4_1_2(spark, mssql_ctx):
    """Create-table type mapping parity across Spark's SQL Server write matrix."""
    _created_types_differential(
        spark, mssql_ctx, dialect="sqlserver", oracle_options={"trustServerCertificate": "true"}
    )


def test_mssql_create_options_and_comment_ignored(spark, mssql_ctx):
    import sqlalchemy as sa
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    opts, sa_url = mssql_ctx
    table = "wt_mssql_create_options"
    _drop_table(sa_url, table)
    try:
        schema = StructType([StructField("id", IntegerType()), StructField("label", StringType())])
        # Spark's MsSqlServerDialect cannot create table comments: createTable
        # swallows the dialect error, warns, and leaves the table comment-less.
        spark.createDataFrame([(1, "one")], schema).write.format("jdbc").option("dbtable", table).options(
            **opts,
            createTableColumnTypes="LABEL VARCHAR(32)",
            createTableOptions="ON [PRIMARY]",
            tableComment="spark parity evidence",
            isolationLevel="SERIALIZABLE",
            queryTimeout="10",
        ).mode("append").save()
        engine = sa.create_engine(sa_url)
        with engine.connect() as conn:
            length = conn.execute(
                sa.text(
                    "SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.columns "
                    "WHERE table_name = :table AND column_name = 'label'"
                ),
                {"table": table},
            ).scalar_one()
            comment = conn.execute(
                sa.text(
                    "SELECT CAST(value AS NVARCHAR(MAX)) FROM sys.extended_properties "
                    "WHERE major_id = OBJECT_ID(:table) AND name = 'MS_Description'"
                ),
                {"table": table},
            ).scalar_one_or_none()
        engine.dispose()
        assert (length, comment) == (32, None)
    finally:
        _drop_table(sa_url, table)


@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_mssql_write_matches_native_spark_4_1_2(spark, mssql_ctx):
    opts, sa_url = mssql_ctx
    native_table = "wt_mssql_native_oracle"
    sail_table = "wt_mssql_sail_oracle"
    for table in (native_table, sail_table):
        _drop_table(sa_url, table)
    try:
        rows = [[1, "Alice", 9.5], [2, "Bob", 7.2]]
        schema = _schema()
        run_native_jdbc_write(
            dialect="sqlserver",
            jdbc_url=opts["url"],
            dbtable=native_table,
            user=opts["user"],
            password=opts["password"],
            schema_json=schema.jsonValue(),
            rows=rows,
            mode="append",
            options={"trustServerCertificate": "true"},
        )
        spark.createDataFrame([tuple(row) for row in rows], schema).write.format("jdbc").option(
            "dbtable", sail_table
        ).options(**opts).mode("append").save()
        assert _column_types(sa_url, native_table) == _column_types(sa_url, sail_table)
        assert _count_names(sa_url, native_table) == _count_names(sa_url, sail_table)
    finally:
        for table in (native_table, sail_table):
            _drop_table(sa_url, table)


@pytest.mark.skipif(native_spark_4_1_2_python() is None, reason="native Spark 4.1.2 oracle is not configured")
def test_mssql_truncate_overwrite_matches_native_spark_4_1_2(spark, mssql_ctx):
    import sqlalchemy as sa
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    opts, sa_url = mssql_ctx
    native_table = "wt_mssql_native_truncate"
    sail_table = "wt_mssql_sail_truncate"
    engine = sa.create_engine(sa_url)
    try:
        with engine.begin() as conn:
            for table in (native_table, sail_table):
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
                conn.execute(
                    sa.text(
                        f"CREATE TABLE {table} "
                        "(id INT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(64), score FLOAT, "
                        f"INDEX {table}_name_idx (name))"
                    )
                )
                conn.execute(sa.text(f"INSERT INTO {table} (name, score) VALUES ('old', 1.0)"))

        rows = [["new", 2.0]]
        schema = StructType([StructField("name", StringType()), StructField("score", DoubleType())])
        run_native_jdbc_write(
            dialect="sqlserver",
            jdbc_url=opts["url"],
            dbtable=native_table,
            user=opts["user"],
            password=opts["password"],
            schema_json=schema.jsonValue(),
            rows=rows,
            mode="overwrite",
            options={"truncate": "true", "trustServerCertificate": "true"},
        )
        spark.createDataFrame([tuple(row) for row in rows], schema).write.format("jdbc").option(
            "dbtable", sail_table
        ).option("truncate", "true").options(**opts).mode("overwrite").save()

        assert _table_snapshot(sa_url, native_table) == _table_snapshot(sa_url, sail_table)
    finally:
        with engine.begin() as conn:
            for table in (native_table, sail_table):
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
        engine.dispose()
