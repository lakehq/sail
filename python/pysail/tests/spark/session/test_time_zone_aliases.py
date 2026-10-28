import pytest
from pyspark.sql import Row

from pysail.testing.spark.utils.common import is_jvm_spark

# Spark stores `timezone` and `time.zone` as unrelated keys. Sail names the DataFusion time zone
# with them, as DataFusion does, and the DataFusion time zone is the session time zone.
pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="the time zone aliases are specific to Sail")

KEY = "spark.sql.session.timeZone"
VARIABLES = ["timezone", "TimeZone", "time.zone", "datafusion.execution.time_zone"]


@pytest.mark.parametrize("variable", VARIABLES)
def test_sql_set_time_zone_alias_sets_the_session_time_zone(spark, variable):
    original = spark.conf.get(KEY)
    try:
        spark.sql(f"SET {variable} = 'Asia/Tokyo'").collect()
        assert spark.conf.get(KEY) == "Asia/Tokyo"
        assert spark.sql("SELECT current_timezone() AS zone").collect() == [Row(zone="Asia/Tokyo")]
    finally:
        spark.conf.set(KEY, original)


@pytest.mark.parametrize("variable", VARIABLES)
def test_sql_set_time_zone_alias_rejects_a_zone_spark_cannot_resolve(spark, variable):
    original = spark.conf.get(KEY)
    with pytest.raises(Exception, match=r"INVALID_CONF_VALUE\.TIME_ZONE"):
        spark.sql(f"SET {variable} = 'Mars/Olympus'").collect()
    assert spark.conf.get(KEY) == original


@pytest.mark.parametrize("variable", VARIABLES)
def test_sql_reset_time_zone_alias_resets_the_session_time_zone(spark, variable):
    original = spark.conf.get(KEY)
    try:
        spark.sql(f"SET {KEY}=Pacific/Kiritimati").collect()
        spark.sql(f"RESET {variable}").collect()
        assert spark.conf.get(KEY) != "Pacific/Kiritimati"
    finally:
        spark.conf.set(KEY, original)
