import pytest

from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.testing.spark.utils.sql import escape_sql_string_literal


@pytest.mark.skipif(is_jvm_spark(), reason="Sail distributed write plan")
@pytest.mark.yamlsnapshot(group="plan")
def test_parquet_writer_stays_on_workers_with_partition_local_sort(spark, tmp_path, snapshot):
    location = escape_sql_string_literal(str(tmp_path / "parquet_write"))
    plan = spark.sql(
        f"""
        EXPLAIN CODEGEN
        CREATE TABLE parquet_write_plan
        USING PARQUET PARTITIONED BY (part)
        LOCATION '{location}'
        AS SELECT id, CAST(id % 8 AS STRING) AS part FROM range(0, 40000, 1, 4)
        """  # noqa: S608
    ).collect()
    text = "\n".join(row[0] for row in plan)
    physical_and_distributed = text.split("== Physical Plan ==", 1)[1]
    assert normalize_plan_text("== Physical Plan ==" + physical_and_distributed) == snapshot
