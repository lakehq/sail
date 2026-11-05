import uuid

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F  # noqa: N812


@pytest.mark.parametrize("mode", ["append", "replace", "overwrite", "overwritePartitions"])
def test_writer_v2_missing_table_rejected_before_writing(spark, tmp_path, mode):
    table = f"missing_{uuid.uuid4().hex}"
    destination = tmp_path / "data"
    writer = spark.range(3).writeTo(table).using("parquet").option("path", str(destination))
    arguments = (F.lit(True),) if mode == "overwrite" else ()
    with pytest.raises(AnalysisException, match="TABLE_OR_VIEW_NOT_FOUND"):
        getattr(writer, mode)(*arguments)
    assert not spark.catalog.tableExists(table)
    assert not destination.exists()
