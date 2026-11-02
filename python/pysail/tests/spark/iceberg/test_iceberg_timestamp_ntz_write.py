import pytest
from pyspark.sql.types import Row


@pytest.mark.usefixtures("session_timezone")
@pytest.mark.parametrize("session_timezone", ["Asia/Shanghai"], indirect=True)
def test_iceberg_path_append_rejects_timestamp_for_timestamp_ntz_column(spark, tmp_path):
    path = (tmp_path / "timestamp_ntz").as_uri()
    spark.sql("SELECT TIMESTAMP_NTZ '2020-06-14 12:00:00' AS ts").write.format("iceberg").mode("overwrite").save(path)

    # TODO: Spark stores the wall-clock time of the TIMESTAMP value in the session time zone.
    # Sail rejects the value rather than storing its UTC wall-clock time.
    with pytest.raises(Exception, match=r"Column 'ts' has type Timestamp\(Microsecond, None\) in the table"):
        spark.sql("SELECT TIMESTAMP '2020-06-14 12:00:00' AS ts").write.format("iceberg").mode("append").save(path)

    rows = spark.read.format("iceberg").load(path).selectExpr("CAST(ts AS STRING) AS ts").collect()
    assert rows == [Row(ts="2020-06-14 12:00:00")]
