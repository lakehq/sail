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
    session.sql("SET TIME ZONE 'Pacific/Kiritimati'")
    result = session.sql("RESET")
    assert result.schema == StructType([])
    assert result.collect() == []
    assert session.conf.get("spark.sql.sources.default") == default
    assert session.conf.get("sail.test.config.value", None) is None
    assert session.sql("SELECT current_timezone()").first()[0] != "Pacific/Kiritimati"


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


# Spark SQLConfSuite: retain raw spelling while using the zone for timestamp conversion.
@pytest.mark.parametrize(
    ("zone", "micros"),
    [
        ("UTC", 0),
        ("Asia/Kolkata", -19800000000),
        ("America/Chicago", 21600000000),
        ("PST", 28800000000),
        ("GMT+8:00", -28800000000),
        ("+5:30", -19800000000),
        ("Z", 0),
        ("UT", 0),
    ],
)
def test_session_timezone_spelling_and_timestamp_values(config_sessions, zone, micros):
    session = config_sessions.create()
    session.conf.set("spark.sql.session.timeZone", zone)
    assert session.conf.get("spark.sql.session.timeZone") == zone
    result = session.sql("""
        SELECT id, current_timezone() AS zone,
          unix_micros(cast(concat('1970-01-01 00:00:0', id) AS timestamp)) AS micros
        FROM range(2) ORDER BY id
    """)
    assert result.collect() == [Row(id=i, zone=zone, micros=micros + i * 1000000) for i in range(2)]


@pytest.mark.parametrize(
    "key",
    [
        "spark.sql.session.timeZone",
        pytest.param(
            "datafusion.execution.time_zone",
            marks=pytest.mark.skipif(is_jvm_spark(), reason="DataFusion configuration is specific to Sail"),
        ),
    ],
)
def test_sql_timezone_invalid_set_preserves_state_and_reset(config_sessions, key):
    session = config_sessions.create()
    spark_key = "spark.sql.session.timeZone"
    session.sql("SET TIME ZONE LOCAL").collect()
    system_zone = session.conf.get(spark_key)
    session.sql(f"SET {key}=GMT+8:00").collect()
    for invalid in ["Nope", "Asia/shanghai", "pst", "+18:00:01", " UTC "]:
        # Spark SET decodes backticks; native DataFusion SET decodes string literals.
        value = f"'{invalid}'" if key.startswith("datafusion.") else f"`{invalid}`"
        with pytest.raises(Exception, match=r"INVALID_CONF_VALUE\.TIME_ZONE"):
            session.sql(f"SET {key}={value}").collect()
        assert session.conf.get(spark_key) == "GMT+8:00"
        result = session.sql("""
            SELECT current_timezone() AS zone,
              unix_micros(cast(concat('1970-01-01 00:00:0', id) AS timestamp)) AS micros
            FROM range(2) ORDER BY id
        """)
        assert result.collect() == [Row(zone="GMT+8:00", micros=-28800000000 + i * 1000000) for i in range(2)]
    for _ in range(2):
        result = session.sql(f"RESET {key}")
        assert result.schema == StructType([])
        assert result.collect() == []
        assert session.conf.get(spark_key) == system_zone
        assert session.sql("SELECT current_timezone()").first()[0] == system_zone


@pytest.mark.skipif(is_jvm_spark(), reason="DataFusion configuration is specific to Sail")
@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("datafusion.execution.batch_size", "'1024'"),
        ("datafusion.execution.batch_size", "1024 /*comment*/"),
        ("datafusion.execution.batch_size", "/*comment*/+1024"),
        ("datafusion.execution.batch_size", "'1024' /*comment*/"),
        ("datafusion.execution.parquet.pushdown_filters", "TRUE /*comment*/"),
        ("datafusion.execution.parquet.bloom_filter_fpp", "0.05 /*comment*/"),
    ],
)
def test_datafusion_set_value(config_sessions, key, value):
    session = config_sessions.create()
    assert session.sql(f"SET {key}={value}").collect() == []
    assert session.range(3).collect() == [Row(id=0), Row(id=1), Row(id=2)]


@pytest.mark.skipif(is_jvm_spark(), reason="DataFusion configuration is specific to Sail")
def test_datafusion_reset_rejects_unknown_config(config_sessions):
    session = config_sessions.create()
    for statement in [
        "SET datafusion.execution.unknown_config=1",
        "RESET datafusion.execution.unknown_config",
        "RESET datafusion.runtime.unknown_config",
    ]:
        with pytest.raises(Exception, match="unknown_config"):
            session.sql(statement).collect()
    assert session.sql("RESET").collect() == []


@pytest.mark.parametrize(
    ("sql_value", "expected"),
    [
        pytest.param("`json`", "json", id="backtick-value"),
        pytest.param("`a``b`", "a`b", id="escaped-backtick"),
        pytest.param("` a;b `;", " a;b ", id="quoted-spaces-and-semicolon"),
        pytest.param("/*before*/`json` /*after*/;", "json", id="comments-around-quoted-value"),
        pytest.param("`json`tail", "`json`tail", id="backtick-in-raw-value"),
        pytest.param("a/*b*/c", "a/*b*/c", id="block-comment-text"),
        pytest.param("a--b", "a--b", id="line-comment-text"),
        pytest.param("/*b*/abc;", "/*b*/abc", id="leading-block-comment-text"),
        pytest.param(" --b;;;", "--b", id="leading-line-comment-text"),
        pytest.param("a--b\nc;", "a--b\nc", id="line-comment-with-newline"),
    ],
)
def test_sql_set_configuration_value_syntax(spark, sql_value, expected):
    key = "sail.test.config.raw_value"
    original = spark.conf.get(key, None)
    try:
        result = spark.sql(f"SET {key}={sql_value}")
        assert spark.conf.get(key) == expected
        assert result.collect() == [Row(key=key, value=expected)]
    finally:
        if original is None:
            spark.conf.unset(key)
        else:
            spark.conf.set(key, original)


def test_sql_set_backtick_default_format(spark, tmp_path):
    key = "spark.sql.sources.default"
    original = spark.conf.get(key, None)
    path = str(tmp_path / "quoted_default_format")
    try:
        spark.sql(f"SET {key}=`json`")
        spark.range(2).write.save(path)
        assert list((tmp_path / "quoted_default_format").glob("*.json"))
        assert spark.read.load(path).orderBy("id").collect() == [Row(id=0), Row(id=1)]
    finally:
        if original is None:
            spark.conf.unset(key)
        else:
            spark.conf.set(key, original)
