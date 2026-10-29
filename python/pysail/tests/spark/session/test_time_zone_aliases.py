import pytest
from pyspark.sql import Row

from pysail.testing.spark.utils.common import is_jvm_spark

KEY = "spark.sql.session.timeZone"
ZONE = "Asia/Tokyo"


@pytest.fixture
def restore_time_zone(spark):
    original = spark.conf.get(KEY)
    yield
    spark.conf.set(KEY, original)


def assert_time_zones_agree(spark):
    # The optimizer reads the DataFusion time zone, and the comparison of a TIMESTAMP_NTZ value
    # with the same wall-clock time as TIMESTAMP only holds if that is the session time zone.
    df = spark.sql("SELECT TIMESTAMP_NTZ '2020-06-14 12:00:00' AS ntz, TIMESTAMP '2020-06-14 12:00:00' AS ltz")
    assert df.selectExpr("ntz = ltz AS equal").collect() == [Row(equal=True)]


def current_time_zone(spark):
    return spark.sql("SELECT current_timezone() AS zone").first().zone


@pytest.mark.parametrize("variable", ["timezone", "time.zone"])
@pytest.mark.usefixtures("restore_time_zone")
def test_sql_set_of_a_key_named_like_a_time_zone_leaves_the_session_time_zone(spark, variable):
    spark.conf.set(KEY, "UTC")
    assert spark.sql(f"SET {variable} = '{ZONE}'").collect() == [Row(key=variable, value=f"'{ZONE}'")]
    assert spark.conf.get(variable) == f"'{ZONE}'"
    assert current_time_zone(spark) == "UTC"
    spark.sql(f"RESET {variable}").collect()
    assert current_time_zone(spark) == "UTC"
    assert_time_zones_agree(spark)


@pytest.mark.parametrize(
    "statement",
    [
        f"SET {KEY} = {ZONE}",
        f"SET TIME ZONE '{ZONE}'",
        pytest.param(
            f"SET datafusion.execution.time_zone = '{ZONE}'",
            marks=pytest.mark.skipif(is_jvm_spark(), reason="the DataFusion time zone is specific to Sail"),
        ),
    ],
)
@pytest.mark.usefixtures("restore_time_zone")
def test_sql_set_of_the_session_time_zone_sets_the_datafusion_time_zone(spark, statement):
    spark.conf.set(KEY, "UTC")
    spark.sql(statement).collect()
    assert spark.conf.get(KEY) == ZONE
    assert current_time_zone(spark) == ZONE
    assert_time_zones_agree(spark)


@pytest.mark.usefixtures("restore_time_zone")
def test_conf_set_of_the_session_time_zone_sets_the_datafusion_time_zone(spark):
    spark.conf.set(KEY, ZONE)
    assert current_time_zone(spark) == ZONE
    assert_time_zones_agree(spark)


@pytest.mark.parametrize(
    "statement",
    [
        f"RESET {KEY}",
        pytest.param(
            "RESET datafusion.execution.time_zone",
            marks=pytest.mark.skipif(is_jvm_spark(), reason="the DataFusion time zone is specific to Sail"),
        ),
    ],
)
@pytest.mark.usefixtures("restore_time_zone")
def test_sql_reset_of_the_session_time_zone_resets_the_datafusion_time_zone(spark, statement):
    spark.conf.set(KEY, "Pacific/Kiritimati")
    spark.sql(statement).collect()
    assert current_time_zone(spark) != "Pacific/Kiritimati"
    assert_time_zones_agree(spark)


@pytest.mark.usefixtures("restore_time_zone")
def test_conf_unset_of_the_session_time_zone_resets_the_datafusion_time_zone(spark):
    spark.conf.set(KEY, "Pacific/Kiritimati")
    spark.conf.unset(KEY)
    assert current_time_zone(spark) != "Pacific/Kiritimati"
    assert_time_zones_agree(spark)


@pytest.mark.skipif(is_jvm_spark(), reason="the DataFusion time zone is specific to Sail")
def test_sql_set_of_the_datafusion_time_zone_rejects_a_zone_spark_cannot_resolve(spark):
    original = spark.conf.get(KEY)
    with pytest.raises(Exception, match=r"INVALID_CONF_VALUE\.TIME_ZONE"):
        spark.sql("SET datafusion.execution.time_zone = 'Mars/Olympus'").collect()
    assert spark.conf.get(KEY) == original


@pytest.mark.skipif(is_jvm_spark(), reason="the DataFusion time zone is specific to Sail")
@pytest.mark.usefixtures("restore_time_zone")
def test_sql_set_of_the_datafusion_time_zone_returns_the_key_and_the_value(spark):
    result = spark.sql(f"SET datafusion.execution.time_zone = '{ZONE}'").collect()
    assert result == [Row(key="datafusion.execution.time_zone", value=ZONE)]
    assert spark.conf.get(KEY) == ZONE
    assert_time_zones_agree(spark)


@pytest.mark.skipif(is_jvm_spark(), reason="the DataFusion time zone is specific to Sail")
@pytest.mark.usefixtures("restore_time_zone")
def test_sql_reset_of_the_datafusion_time_zone_is_the_reset_of_the_session_time_zone(spark):
    zones = []
    for statement in [f"RESET {KEY}", "RESET datafusion.execution.time_zone"]:
        spark.conf.set(KEY, "Pacific/Kiritimati")
        spark.sql(statement).collect()
        zones.append((spark.conf.get(KEY), current_time_zone(spark)))
        assert_time_zones_agree(spark)
    assert zones[0] == zones[1]
