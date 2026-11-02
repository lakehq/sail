import pytest
from pyspark.sql import Row
from pyspark.sql.types import LongType, StringType, StructField, StructType


@pytest.mark.parametrize("config_api", ["sql", "runtime"])
@pytest.mark.parametrize("default_format", ["json", "org.apache.spark.sql.json"])
def test_save_load_uses_session_default(spark, tmp_path, config_api, default_format):
    key = "spark.sql.sources.default"
    original = spark.conf.get(key, None)
    schema = StructType([StructField("id", LongType(), True), StructField("value", StringType(), True)])
    rows = [Row(id=1, value="a"), Row(id=2, value="b")]
    source = spark.createDataFrame(rows, schema)
    path = str(tmp_path / "default_format")
    try:
        if config_api == "sql":
            spark.sql(f"SET {key}={default_format}")
        else:
            spark.conf.set(key, default_format)
        source.write.save(path)
        assert list((tmp_path / "default_format").glob("*.json"))
        result = spark.read.load(path)
        assert result.schema == schema
        assert result.orderBy("id").collect() == rows
        assert spark.read.schema("value STRING").load(path).orderBy("value").collect() == [
            Row(value="a"),
            Row(value="b"),
        ]

        parquet_path = str(tmp_path / "explicit_format")
        source.write.format("parquet").save(parquet_path)
        assert spark.read.format("parquet").load(parquet_path).orderBy("id").collect() == rows

        spark.sql(f"RESET {key}")
        assert spark.read.load(parquet_path).orderBy("id").collect() == rows
    finally:
        if original is None:
            spark.conf.unset(key)
        else:
            spark.conf.set(key, original)
