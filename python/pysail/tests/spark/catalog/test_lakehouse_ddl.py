"""Delta and Iceberg DDL in the default memory catalog."""

import pytest

from pysail.testing.spark.ddl import exercise_lakehouse_alter


@pytest.mark.parametrize("fmt", ["delta", "iceberg"])
def test_memory_lakehouse_ddl(spark, tmp_path, fmt):
    spark.conf.set("spark.sql.warehouse.dir", str(tmp_path / "warehouse"))
    table = f"default.ddl_{fmt}"
    location = (tmp_path / fmt).as_uri()
    properties = "'format-version'='3'" if fmt == "iceberg" else "'delta.enableTypeWidening'='true'"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, value STRING, part STRING) USING {fmt} "
            f"PARTITIONED BY (part) LOCATION '{location}' TBLPROPERTIES ({properties})"
        )
        exercise_lakehouse_alter(spark, table)
        assert (
            next(r.data_type for r in spark.sql(f"DESCRIBE TABLE {table}").collect() if r.col_name == "id") == "bigint"
        )
        spark.sql(f"DROP TABLE {table}")
        spark.sql(f"CREATE TABLE {table} USING {fmt} LOCATION '{location}'")
        assert spark.table(table).count() == 3  # noqa: PLR2004
        assert spark.table(table).schema["id"].dataType.simpleString() == "bigint"
        spark.sql(f"DROP TABLE {table}")
        spark.sql(f"CREATE TABLE {table} USING {fmt} AS SELECT 4 AS id, 'ctas' AS value")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} (other INT) USING {fmt}")
        assert [tuple(r) for r in spark.table(table).collect()] == [(4, "ctas")]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
