# ruff: noqa: S608
"""Reference DDL roundtrips through HMS and the JVM Iceberg client."""

import pytest

from pysail.tests.spark.catalog.hms.test_lakehouse_ddl import _iceberg_metadata


@pytest.mark.parametrize("version", [1, 2, 3])
def test_reference_ddl_roundtrip(spark, jvm_spark, hms_s3_database, hms_s3_env, version):
    table = "references"
    name = f"{hms_s3_database}.{table}"
    reference = f"iceberg.{name}"
    spark.sql(f"CREATE TABLE {name} (id INT) USING iceberg TBLPROPERTIES ('format-version'='{version}')")
    spark.sql(f"ALTER TABLE {name} CREATE BRANCH audit")
    _, empty = _iceberg_metadata(jvm_spark, hms_s3_database, table, hms_s3_env)
    assert empty.get("current-snapshot-id") in (None, -1)
    assert jvm_spark.sql(f"SELECT * FROM {reference} VERSION AS OF 'audit'").count() == 0
    spark.sql(f"INSERT INTO {name} VALUES (1)")
    spark.sql(f"ALTER TABLE {name} REPLACE BRANCH audit WITH SNAPSHOT RETENTION 2 SNAPSHOTS")
    jvm_spark.sql(f"REFRESH TABLE {reference}")
    assert jvm_spark.sql(f"SELECT * FROM {reference} VERSION AS OF 'audit'").count() == 1
    jvm_spark.sql(f"ALTER TABLE {reference} CREATE TAG release RETAIN 1 DAYS")
    spark.sql(f"ALTER TABLE {name} REPLACE TAG release RETAIN 2 HOURS")
    _, stored = _iceberg_metadata(jvm_spark, hms_s3_database, table, hms_s3_env)
    assert stored["refs"]["release"]["max-ref-age-ms"] == 7200000  # noqa: PLR2004
    assert stored["refs"]["audit"]["min-snapshots-to-keep"] == 2  # noqa: PLR2004
    spark.sql(f"ALTER TABLE {name} DROP BRANCH audit")
    spark.sql(f"ALTER TABLE {name} DROP TAG release")
    _, stored = _iceberg_metadata(jvm_spark, hms_s3_database, table, hms_s3_env)
    assert set(stored["refs"]) == {"main"}
    jvm_spark.sql(f"REFRESH TABLE {reference}")
    jvm_spark.sql(f"INSERT INTO {reference} VALUES (2)")
    assert sorted(row.id for row in spark.table(name).collect()) == [1, 2]
