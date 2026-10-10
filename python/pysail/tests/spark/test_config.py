import os

import pytest

from pysail.spark import SparkConnectServer
from pysail.testing.spark.session import spark_session_factory


@pytest.fixture(scope="module", autouse=True)
def server():
    # The tests below need the global state to be initialized while the environment variables
    # are still pristine. This fixture ensures this even when running the tests against a server from `SPARK_REMOTE`.
    _ = SparkConnectServer()


def test_warning_on_static_config_addition(monkeypatch):
    key = "SAIL_TELEMETRY__EXPORTER__OTLP__ENDPOINT"
    assert key not in os.environ
    monkeypatch.setenv(key, "http://localhost:4317")
    with pytest.warns(RuntimeWarning, match=f"ignored.*{key}"):
        _ = SparkConnectServer()


def test_warning_on_static_config_deletion(monkeypatch):
    key = "SAIL_RUNTIME__STACK_SIZE"
    monkeypatch.delenv(key, raising=True)
    with pytest.warns(RuntimeWarning, match=f"ignored.*{key}"):
        _ = SparkConnectServer()


def test_warning_on_static_config_modification(monkeypatch):
    key = "SAIL_RUNTIME__STACK_SIZE"
    monkeypatch.setenv(key, str(int(os.environ[key]) * 2))
    with pytest.warns(RuntimeWarning, match=f"ignored.*{key}"):
        _ = SparkConnectServer()


def test_sql_set_quoted_session_timezone_keeps_the_quotes(spark):
    with pytest.raises(Exception, match=r"INVALID_CONF_VALUE\.TIME_ZONE"):
        spark.sql("SET spark.sql.session.timeZone = 'Asia/Kolkata'").collect()


@pytest.mark.parametrize(
    "statement",
    [
        "SET datafusion.execution.batch_size = '4096'",
        "SET datafusion.execution.coalesce_batches = 'false'",
        "SET datafusion.optimizer.max_passes = '3'",
        'SET datafusion.execution.batch_size = "4096"',
        "SET datafusion.execution.batch_size = '4096';",
        "SET datafusion.sql_parser.dialect = 'generic'",
        "SET timezone = 'UTC'",
    ],
)
def test_sql_set_quoted_value_of_other_keys_is_unchanged(spark, statement):
    # Only the session time zone keeps the quotes of its value; the other keys are unaffected.
    spark.sql(statement).collect()


def test_sql_reset_of_the_time_zone_returns_no_rows(spark):
    zone = spark.conf.get("spark.sql.session.timeZone")
    spark.sql("SET TIME ZONE 'Asia/Kolkata'").collect()
    try:
        df = spark.sql("RESET spark.sql.session.timeZone")
        assert df.count() == 0
        assert df.collect() == []
    finally:
        spark.conf.set("spark.sql.session.timeZone", zone)


def test_sql_set_time_zone_local_and_reset_restore_the_same_zone(spark):
    zone = spark.conf.get("spark.sql.session.timeZone")
    try:
        spark.sql("SET TIME ZONE 'Asia/Kolkata'").collect()
        row = spark.sql("SET TIME ZONE LOCAL").collect()[0]
        local = spark.conf.get("spark.sql.session.timeZone")
        assert row.value == local

        spark.sql("SET TIME ZONE 'Asia/Kolkata'").collect()
        spark.sql("RESET spark.sql.session.timeZone").collect()
        assert spark.conf.get("spark.sql.session.timeZone") == local
    finally:
        spark.conf.set("spark.sql.session.timeZone", zone)


def test_sql_set_time_zone_is_isolated_between_sessions(spark, remote):
    if remote.startswith("local"):
        pytest.skip("a `local` remote returns the same Spark session instead of a second one")
    key = "spark.sql.session.timeZone"
    with spark_session_factory(remote) as sessions:
        other = sessions.create()
        zone, other_zone = spark.conf.get(key), other.conf.get(key)
        try:
            spark.sql("SET TIME ZONE 'Asia/Kolkata'").collect()
            assert spark.conf.get(key) == "Asia/Kolkata"
            assert other.conf.get(key) == other_zone
            assert other.sql("SELECT current_timezone()").first()[0] == other_zone

            other.sql("SET TIME ZONE 'Asia/Tokyo'").collect()
            assert other.conf.get(key) == "Asia/Tokyo"
            assert spark.conf.get(key) == "Asia/Kolkata"
            assert spark.sql("SELECT current_timezone()").first()[0] == "Asia/Kolkata"
        finally:
            spark.conf.set(key, zone)
            other.conf.set(key, other_zone)


def test_sql_set_time_zone_overrides_conf_set(spark):
    key = "spark.sql.session.timeZone"
    zone = spark.conf.get(key)
    try:
        spark.conf.set(key, "Asia/Tokyo")
        spark.sql("SET TIME ZONE 'Asia/Kolkata'").collect()
        assert spark.conf.get(key) == "Asia/Kolkata"
        assert spark.sql("SELECT current_timezone()").first()[0] == "Asia/Kolkata"
    finally:
        spark.conf.set(key, zone)


def test_conf_set_overrides_sql_set_time_zone(spark):
    key = "spark.sql.session.timeZone"
    zone = spark.conf.get(key)
    try:
        spark.sql("SET TIME ZONE 'Asia/Kolkata'").collect()
        spark.conf.set(key, "Asia/Tokyo")
        assert spark.conf.get(key) == "Asia/Tokyo"
        assert spark.sql("SELECT current_timezone()").first()[0] == "Asia/Tokyo"
    finally:
        spark.conf.set(key, zone)


def test_sql_set_time_zone_result_does_not_set_the_zone_again(spark):
    key = "spark.sql.session.timeZone"
    zone = spark.conf.get(key)
    try:
        df = spark.sql("SET TIME ZONE 'Asia/Kolkata'")
        spark.conf.set(key, "Asia/Tokyo")
        # Reading the result again returns the row but does not run the statement again.
        assert [tuple(row) for row in df.collect()] == [(key, "Asia/Kolkata")]
        assert df.count() == 1
        assert spark.conf.get(key) == "Asia/Tokyo"
    finally:
        spark.conf.set(key, zone)


# TODO: `SparkRuntimeConfig::set` does not validate the zone, so `spark.conf.set` accepts a zone that Spark rejects
#   with `INVALID_CONF_VALUE.TIME_ZONE`. Only the SQL statements validate it for now (`plan_executor.rs`).
#   Move the check into `SparkRuntimeConfig::set` and remove the `xfail`.
@pytest.mark.xfail(reason="spark.conf.set does not validate the session time zone", strict=True)
def test_conf_set_rejects_an_invalid_time_zone_and_keeps_the_zone(spark):
    key = "spark.sql.session.timeZone"
    zone = spark.conf.get(key)
    try:
        with pytest.raises(Exception, match="INVALID_CONF_VALUE"):
            spark.conf.set(key, "Nope")
        assert spark.conf.get(key) == zone
    finally:
        spark.conf.set(key, zone)
