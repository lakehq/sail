import pytest
from pyspark.sql import Row
from pyspark.sql.functions import udtf
from pyspark.sql.types import StringType, StructField, StructType

from pysail.testing.spark.session import spark_session_factory
from pysail.testing.spark.utils.common import is_jvm_spark

try:
    from pyspark.util import PythonEvalType
except ImportError:
    from pyspark.rdd import PythonEvalType


@pytest.fixture
def config_sessions(spark, remote, monkeypatch):
    if remote.startswith("local"):
        remote = f"sc://{spark.client._builder.endpoint}"  # noqa: SLF001
        monkeypatch.setenv("SPARK_REMOTE", remote)
        monkeypatch.delenv("SPARK_LOCAL_REMOTE", raising=False)
    with spark_session_factory(remote) as sessions:
        yield sessions


@pytest.mark.parametrize("value", ["False", "org.apache.spark.sql.json", "Asia/Tokyo", "'hello world'", ""])
def test_sql_set_preserves_config_value(spark, value):
    key = "sail.test.config.value"
    try:
        result = spark.sql(f"SET {key}={value};")
        assert spark.conf.get(key) == value
        assert result.schema == StructType(
            [StructField("key", StringType(), False), StructField("value", StringType(), False)]
        )
        assert result.collect() == [Row(key=key, value=value)]
    finally:
        spark.sql(f"RESET {key}")
    assert spark.conf.get(key, None) is None


def test_sql_set_controls_arrow_udtf(spark):
    class TestUDTF:
        def eval(self):
            yield (1,)

    key = "spark.sql.execution.pythonUDTF.arrow.enabled"
    original = spark.conf.get(key, None)
    try:
        for value, eval_type in [("False", PythonEvalType.SQL_TABLE_UDF), ("True", PythonEvalType.SQL_ARROW_TABLE_UDF)]:
            spark.sql(f"SET {key}={value}")
            function = udtf(TestUDTF, returnType="x: int")
            assert function.evalType == eval_type
            assert function().collect() == [Row(x=1)]
    finally:
        if original is None:
            spark.conf.unset(key)
        else:
            spark.conf.set(key, original)


def test_sql_reset_restores_default_and_isolates_sessions(config_sessions):
    key = "spark.sql.sources.default"
    first, second = config_sessions.create(), config_sessions.create()
    default = first.conf.get(key)
    first.conf.set(key, "json")
    second.conf.set(key, "csv")
    result = first.sql(f"RESET {key}")
    assert result.schema == StructType([])
    assert result.collect() == []
    assert first.conf.get(key) == default
    assert second.conf.get(key) == "csv"


def test_sql_reset_all(config_sessions):
    session = config_sessions.create()
    default = session.conf.get("spark.sql.sources.default")
    session.sql("SET spark.sql.sources.default=json")
    session.conf.set("sail.test.config.value", "custom")
    result = session.sql("RESET")
    assert result.schema == StructType([])
    assert result.collect() == []
    assert session.conf.get("spark.sql.sources.default") == default
    assert session.conf.get("sail.test.config.value", None) is None


def test_sql_set_timezone_changes_planning(spark):
    key = "spark.sql.session.timeZone"
    original = spark.conf.get(key)
    try:
        spark.sql(f"SET {key}=Asia/Tokyo")
        assert spark.conf.get(key) == "Asia/Tokyo"
        assert spark.sql("SELECT current_timezone() AS timezone").collect() == [Row(timezone="Asia/Tokyo")]
        spark.sql(f"SET {key}=UTC")
        assert spark.sql("SELECT current_timezone() AS timezone").collect() == [Row(timezone="UTC")]
    finally:
        spark.conf.set(key, original)


@pytest.mark.skipif(is_jvm_spark(), reason="DataFusion configuration is specific to Sail")
def test_datafusion_set_quoted_value(config_sessions):
    session = config_sessions.create()
    assert session.sql("SET datafusion.execution.batch_size='1024'").collect() == []
    assert session.range(3).collect() == [Row(id=0), Row(id=1), Row(id=2)]
