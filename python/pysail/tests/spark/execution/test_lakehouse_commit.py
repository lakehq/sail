from pathlib import Path

import pytest

from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail local-cluster mode only")


def _data_files(table_path: Path, table_format: str) -> list[Path]:
    if table_format == "iceberg":
        return list(table_path.joinpath("data").rglob("*.parquet"))
    return list(table_path.glob("*.parquet"))


@pytest.mark.parametrize("table_format", ["delta", "iceberg"])
def test_lakehouse_file_writes_remain_parallel_on_workers(spark, tmp_path, table_format):
    table_path = tmp_path / f"{table_format}_parallel_write"

    spark.range(0, 400, 1, 4).write.format(table_format).mode("overwrite").save(str(table_path))

    data_files = _data_files(table_path, table_format)
    assert len(data_files) == 4  # noqa: PLR2004
    assert spark.read.format(table_format).load(str(table_path)).count() == 400  # noqa: PLR2004
