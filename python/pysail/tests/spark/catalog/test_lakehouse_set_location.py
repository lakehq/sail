# ruff: noqa: PLR2004
import pytest

from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Requires lakehouse extensions")


def test_delta_set_location_repoints_catalog_without_moving_data(spark, tmp_path):
    tables = ["location_source", "location_target", "location_mismatch"]
    try:
        for name in tables:
            column = "id BIGINT" if name == "location_mismatch" else "id INT"
            spark.sql(
                f"CREATE TABLE {name} ({column}, value STRING) USING delta LOCATION '{(tmp_path / name).as_uri()}' TBLPROPERTIES ('delta.columnMapping.mode'='name')"
            )
        spark.sql("INSERT INTO location_source VALUES (1, 'old')")
        spark.sql("INSERT INTO location_target VALUES (2, 'new')")
        target = tmp_path / "location_target"
        before = set((target / "_delta_log").glob("*.json"))
        spark.sql(f"ALTER TABLE location_source SET LOCATION '{target.as_uri()}'")
        assert tuple(spark.table("location_source").first()) == (2, "new")
        assert set((target / "_delta_log").glob("*.json")) == before
        spark.sql("INSERT INTO location_source VALUES (3, 'later')")
        assert [r.id for r in spark.table("location_target").orderBy("id").collect()] == [2, 3]
        assert spark.read.format("delta").load(str(tmp_path / "location_source")).first().id == 1
        with pytest.raises(Exception, match="same schema"):
            spark.sql(f"ALTER TABLE location_source SET LOCATION '{(tmp_path / 'location_mismatch').as_uri()}'")
        assert spark.table("location_source").count() == 2
        with pytest.raises(Exception, match=r"(?i)delta|table"):
            spark.sql(f"ALTER TABLE location_source SET LOCATION '{(tmp_path / 'missing').as_uri()}'")
        assert spark.table("location_source").count() == 2
    finally:
        for name in tables:
            spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_iceberg_path_set_location_rejected(spark, tmp_path):
    try:
        spark.sql(f"CREATE TABLE location_iceberg (id INT) USING iceberg LOCATION '{tmp_path.as_uri()}'")
        spark.sql("INSERT INTO location_iceberg VALUES (1)")
        with pytest.raises(Exception, match="path-based"):
            spark.sql(f"ALTER TABLE location_iceberg SET LOCATION '{(tmp_path / 'moved').as_uri()}'")
        assert spark.table("location_iceberg").first().id == 1
    finally:
        spark.sql("DROP TABLE IF EXISTS location_iceberg")


def test_delta_set_location_uses_target_constraints(spark, tmp_path):
    tables = ["location_constraint_source", "location_constraint_target"]
    try:
        for name in tables:
            spark.sql(f"CREATE TABLE {name} (id INT) USING delta LOCATION '{(tmp_path / name).as_uri()}'")
        spark.sql(f"ALTER TABLE {tables[0]} ADD CONSTRAINT positive CHECK (id > 0)")
        spark.sql("INSERT INTO location_constraint_source VALUES (1)")
        spark.sql("INSERT INTO location_constraint_target VALUES (-1)")
        spark.sql(f"ALTER TABLE {tables[0]} SET LOCATION '{(tmp_path / tables[1]).as_uri()}'")

        spark.sql("INSERT INTO location_constraint_source VALUES (-2)")

        assert [r.id for r in spark.table(tables[1]).orderBy("id").collect()] == [-2, -1]
        assert [r.id for r in spark.read.format("delta").load(str(tmp_path / tables[0])).collect()] == [1]
    finally:
        for name in tables:
            spark.sql(f"DROP TABLE IF EXISTS {name}")
