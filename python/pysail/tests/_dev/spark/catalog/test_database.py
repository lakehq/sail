# SPDX-License-Identifier: Apache-2.0

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import Row

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.common import is_jvm_spark


def test_list_databases_returns_name(spark):
    """listDatabases returns database with 'name' field."""
    databases = spark.catalog.listDatabases()
    db_names = [db.name for db in databases]
    assert "default" in db_names


@pytest.fixture
def databases(spark):
    """Creates the databases, and restores the current catalog and database after the test."""
    catalog = spark.catalog.currentCatalog()
    database = spark.catalog.currentDatabase()
    created = []

    def create(*names):
        for name in names:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS `{name}`")
            created.append(name)

    yield create
    spark.sql(f"USE CATALOG `{catalog}`" if not is_jvm_spark() else f"SET CATALOG `{catalog}`")
    spark.sql(f"USE DATABASE `{database}`")
    for name in created:
        spark.sql(f"DROP DATABASE IF EXISTS `{name}` CASCADE")


def test_use_without_keyword_sets_the_current_database(spark, databases, tmp_path):
    databases("use_db_a")
    spark.sql(f"CREATE TABLE use_db_a.use_t USING parquet LOCATION '{tmp_path}' AS SELECT 1 AS x")
    spark.sql("USE use_db_a")
    assert spark.sql("SELECT current_database()").collect()[0][0] == "use_db_a"
    assert spark.sql("SELECT x FROM use_t").collect() == [Row(x=1)]
    spark.sql("USE `default`")
    assert spark.catalog.currentDatabase() == "default"
    spark.sql("USE default")
    assert spark.catalog.currentDatabase() == "default"


@pytest.mark.parametrize("name", ["namespace", "database", "schema", "catalog"])
def test_use_without_keyword_reads_a_keyword_as_a_database_name(spark, databases, name):
    databases(name)
    spark.sql(f"USE {name}")
    assert spark.catalog.currentDatabase() == name


def test_use_with_keyword_sets_the_current_database(spark, databases):
    databases("use_db_a")
    for keyword in ["DATABASE", "SCHEMA", "NAMESPACE"]:
        spark.sql("USE `default`")
        spark.sql(f"USE {keyword} use_db_a")
        assert spark.catalog.currentDatabase() == "use_db_a"


def test_use_without_keyword_of_a_missing_database_fails(spark, databases):
    databases()
    with pytest.raises(AnalysisException, match=r"(?i)not found|SCHEMA_NOT_FOUND"):
        spark.sql("USE use_db_missing").collect()
    with pytest.raises(AnalysisException, match=r"(?i)not found|SCHEMA_NOT_FOUND"):
        spark.sql("USE use_nocat.use_db_a").collect()


def test_use_without_keyword_sets_the_catalog_of_a_qualified_name(spark, databases):
    databases("use_db_a")
    catalog = spark.catalog.currentCatalog()
    spark.sql(f"USE `{catalog}`.use_db_a")
    assert spark.catalog.currentCatalog() == catalog
    assert spark.catalog.currentDatabase() == "use_db_a"
    # The current catalog alone keeps the current database.
    spark.sql(f"USE `{catalog}`")
    assert spark.catalog.currentDatabase() == "use_db_a"
    with pytest.raises(AnalysisException, match=r"(?i)not found|SCHEMA_NOT_FOUND"):
        spark.sql(f"USE DATABASE `{catalog}`.use_db_a").collect()


@pytest.fixture(scope="module")
def catalogs(tmp_path_factory):
    """Returns a session of a server with the memory catalogs `sail`, `other` and `shared`, where `sail` has
    the databases `db_a` and `shared`, and `other` has the databases `db_o` and `db_a`."""
    if is_jvm_spark():
        pytest.skip("Sail only: memory catalogs")
    memory = 'type="memory", initial_database=["default"], initial_database_comment="default database"'
    envs = {
        "SAIL_CATALOG__DEFAULT_CATALOG": "sail",
        "SAIL_CATALOG__DEFAULT_DATABASE": '["default"]',
        "SAIL_CATALOG__LIST": "[" + ", ".join(f'{{name="{x}", {memory}}}' for x in ["sail", "other", "shared"]) + "]",
    }
    with spark_connect_server(envs=envs) as server, spark_session_factory(server.remote) as sessions:
        spark = sessions.create()
        for database in ["sail.db_a", "sail.shared", "other.db_o", "other.db_a"]:
            spark.sql(f"CREATE DATABASE {database}")
        location = tmp_path_factory.mktemp("t_other")
        spark.sql(f"CREATE TABLE other.db_a.t_other USING parquet LOCATION '{location}' AS SELECT 1 AS x")
        yield spark


def current(spark):
    return spark.sql("SELECT current_catalog(), current_database()").collect()[0]


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("USE other", ("other", "default")),
        ("USE other.db_o", ("other", "db_o")),
        ("USE sail.db_a", ("sail", "db_a")),
        ("USE sail.shared", ("sail", "shared")),
        # A catalog comes before a database of the same name, as in Spark.
        ("USE shared", ("shared", "default")),
        ("USE `shared`", ("shared", "default")),
        ("USE DATABASE shared", ("sail", "shared")),
        ("USE NAMESPACE shared", ("sail", "shared")),
        ("USE SCHEMA shared", ("sail", "shared")),
        ("USE CATALOG other", ("other", "default")),
    ],
)
def test_use_without_keyword_sets_the_catalog_that_the_name_starts_with(catalogs, sql, expected):
    spark = catalogs
    spark.sql("USE CATALOG sail")
    spark.sql("USE DATABASE default")
    spark.sql(sql)
    assert tuple(current(spark)) == expected


@pytest.mark.parametrize(
    "sql",
    [
        "USE other.missing",
        # The catalog of a name matches its letter case, so these are databases of the current catalog.
        "USE OTHER",
        "USE Other.db_o",
        "USE `other.db_o`",
        "USE DATABASE other",
        "USE DATABASE other.db_o",
        "USE nocat.db_a",
    ],
)
def test_use_of_a_missing_database_keeps_the_current_catalog_and_database(catalogs, sql):
    spark = catalogs
    spark.sql("USE CATALOG sail")
    spark.sql("USE DATABASE default")
    with pytest.raises(AnalysisException, match=r"(?i)not found|SCHEMA_NOT_FOUND"):
        spark.sql(sql).collect()
    assert tuple(current(spark)) == ("sail", "default")


def test_use_without_keyword_after_a_catalog_reads_a_database_of_that_catalog(catalogs):
    spark = catalogs
    spark.sql("USE CATALOG sail")
    spark.sql("USE DATABASE default")
    spark.sql("USE other")
    spark.sql("USE db_a")
    assert tuple(current(spark)) == ("other", "db_a")
    assert spark.sql("SELECT x FROM t_other").collect() == [Row(x=1)]
    spark.sql("USE other")
    # The current catalog keeps its current database.
    assert tuple(current(spark)) == ("other", "db_a")
    spark.sql("USE sail.db_a")
    assert tuple(current(spark)) == ("sail", "db_a")
    spark.sql("USE other.db_a")
    with pytest.raises(AnalysisException, match=r"(?i)not found|SCHEMA_NOT_FOUND"):
        spark.sql("USE DATABASE shared").collect()
    assert tuple(current(spark)) == ("other", "db_a")


@pytest.mark.parametrize("sql", ["USE", "USE 'use_db_a'"])
def test_use_without_a_name_is_a_parse_error(spark, sql):
    with pytest.raises(Exception, match=r"(?i)parse|syntax|expected"):
        spark.sql(sql).collect()
