import pytest
from pyspark.sql import Row

from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")


def test_csv_read_uses_sail_source_on_worker(spark, tmp_path):
    path = tmp_path / "ordinary.csv"
    path.write_text("id,label\n1,caf\u00e9\n2,crab_\U0001f980\n", encoding="utf-8")

    rows = spark.read.option("header", True).option("inferSchema", True).csv(str(path)).orderBy("id").collect()

    assert rows == [Row(id=1, label="caf\u00e9"), Row(id=2, label="crab_\U0001f980")]


def test_csv_read_replaces_surrogate_unit_once_on_worker(spark, tmp_path):
    path = tmp_path / "surrogate.csv"
    path.write_bytes(b"id,label\n1,ok\n2,before_\xed\xa0\x80_after\n")

    rows = spark.read.option("header", True).schema("id INT, label STRING").csv(str(path)).orderBy("id").collect()

    assert rows == [Row(id=1, label="ok"), Row(id=2, label="before_\ufffd_after")]
