import pytest
from pyspark.sql.types import Row

from pysail.tests.spark.utils import is_jvm_spark


class TestDeltaAdvancedFeatures:
    """Delta Lake advanced features tests"""

    @pytest.mark.skip(reason="Temporarily skipped")
    @pytest.mark.skipif(is_jvm_spark(), reason="Sail only - Delta Lake time travel")
    def test_delta_feature_time_travel(self, spark, tmp_path):
        """Test Delta Lake time travel functionality"""
        delta_path = tmp_path / "delta_table"
        delta_table_path = f"{delta_path}"
        # Version 0: Initial data
        v0_data = [Row(id=1, value="v0")]
        df0 = spark.createDataFrame(v0_data)
        df0.write.format("delta").mode("overwrite").save(str(delta_path))

        # Version 1: Add data
        v1_data = [Row(id=2, value="v1")]
        df1 = spark.createDataFrame(v1_data)
        df1.write.format("delta").mode("append").save(str(delta_path))

        # Version 2: Overwrite data
        v2_data = [Row(id=3, value="v2")]
        df2 = spark.createDataFrame(v2_data)
        df2.write.format("delta").mode("overwrite").save(str(delta_path))

        # Read latest version
        latest_df = spark.read.format("delta").load(delta_table_path)
        assert latest_df.collect() == [Row(id=3, value="v2")]

        # Read version 0
        v0_df = spark.read.format("delta").option("versionAsOf", "0").load(delta_table_path)
        assert v0_df.collect() == [Row(id=1, value="v0")]

        # Read version 1
        v1_df = spark.read.format("delta").option("versionAsOf", "1").load(delta_table_path).sort("id")
        expected_v1 = [Row(id=1, value="v0"), Row(id=2, value="v1")]
        assert v1_df.collect() == expected_v1
