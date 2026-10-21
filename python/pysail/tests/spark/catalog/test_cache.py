import pytest
from pyspark.storagelevel import StorageLevel


@pytest.fixture
def views(spark):
    names = ["cache_view_1", "cache_view_2"]
    for i, name in enumerate(names):
        spark.createDataFrame([(2, i), (3, i)], "a int, b int").createOrReplaceTempView(name)
    yield names
    spark.catalog.clearCache()
    for name in names:
        spark.catalog.dropTempView(name)


def test_cache_temporary_views(spark, views):
    first, second = views
    assert not spark.catalog.isCached(first)
    spark.catalog.cacheTable(first)
    assert spark.catalog.isCached(first)
    assert not spark.catalog.isCached(second)
    spark.catalog.cacheTable(second, StorageLevel.OFF_HEAP)
    spark.catalog.uncacheTable(first)
    assert not spark.catalog.isCached(first)
    assert spark.catalog.isCached(second)
    spark.catalog.clearCache()
    assert not spark.catalog.isCached(second)


def test_cache_table(spark):
    spark.sql("CREATE TABLE cache_table (a INT) USING parquet")
    try:
        names = ["cache_table", "default.cache_table"]
        spark.catalog.cacheTable("default.cache_table")
        assert all(spark.catalog.isCached(name) for name in names)
        spark.catalog.refreshTable("cache_table")
        assert all(spark.catalog.isCached(name) for name in names)
        spark.catalog.uncacheTable("cache_table")
        assert not any(spark.catalog.isCached(name) for name in names)
    finally:
        spark.sql("DROP TABLE cache_table")


def test_cache_sql_statements(spark, views):
    first, _ = views
    spark.sql(f"CACHE TABLE {first}")
    assert spark.catalog.isCached(first)
    spark.sql(f"UNCACHE TABLE {first}")
    assert not spark.catalog.isCached(first)
    spark.sql("UNCACHE TABLE IF EXISTS cache_view_missing")
    spark.sql(f"CACHE LAZY TABLE {first}")
    spark.sql("CLEAR CACHE")
    assert not spark.catalog.isCached(first)


def test_cache_missing_table(spark):
    for f in [spark.catalog.isCached, spark.catalog.cacheTable, spark.catalog.uncacheTable]:
        with pytest.raises(Exception, match="cache_view_missing"):
            f("cache_view_missing")


def test_dataframe_storage_level(spark):
    df = spark.createDataFrame([(2, 2), (3, 3)], "a int, b int")
    other = df.select("a")
    assert df.storageLevel == StorageLevel.NONE
    df.cache()
    assert df.storageLevel == StorageLevel.MEMORY_AND_DISK_DESER
    assert other.storageLevel == StorageLevel.NONE
    df.unpersist()
    assert df.storageLevel == StorageLevel.NONE
    df.persist(StorageLevel.DISK_ONLY)
    assert df.storageLevel == StorageLevel.DISK_ONLY
    spark.catalog.clearCache()
    assert df.storageLevel == StorageLevel.NONE
