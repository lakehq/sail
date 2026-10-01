def test_view_uses_defaults_for_uncaptured_sql_settings(spark):
    key = "spark.sql.legacy.sizeOfNull"
    original_value = spark.conf.get(key)
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    try:
        spark.conf.unset(key)
        spark.conf.set("spark.sql.ansi.enabled", "false")
        spark.sql("CREATE VIEW view_sql_config_default AS SELECT size(CAST(NULL AS ARRAY<INT>)) AS value")
        spark.conf.set(key, "false")
        assert spark.sql("SELECT value FROM view_sql_config_default").collect() == [(-1,)]
    finally:
        spark.sql("DROP VIEW IF EXISTS view_sql_config_default")
        spark.conf.set(key, original_value)
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)
