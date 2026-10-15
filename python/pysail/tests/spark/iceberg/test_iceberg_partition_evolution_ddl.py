# ruff: noqa: S608, PLR2004
import pytest

from pysail.testing.spark.ddl import exercise_iceberg_partition_evolution
from pysail.testing.spark.steps.iceberg import _find_latest_metadata


def current_spec(metadata):
    return next(spec for spec in metadata["partition-specs"] if spec["spec-id"] == metadata["default-spec-id"])


@pytest.mark.parametrize("version", [1, 2, 3])
def test_partition_evolution_preserves_history_and_dml(spark, tmp_path, version):
    table = f"partition_evolution_v{version}"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, part STRING) USING iceberg PARTITIONED BY (part) LOCATION '{tmp_path.as_uri()}' TBLPROPERTIES ('format-version'='{version}')"
        )
        exercise_iceberg_partition_evolution(spark, table)
        metadata = _find_latest_metadata(tmp_path)
        specs = metadata["partition-specs"]
        assert specs[0]["fields"][0] == {"source-id": 2, "field-id": 1000, "name": "part", "transform": "identity"}
        fields = current_spec(metadata)["fields"]
        assert [f["transform"] for f in fields if f["transform"] != "void"] == ["truncate[2]"]
        old_bucket = next(
            f for spec in specs for f in spec["fields"] if f["name"] == "id_bucket" and f["transform"] == "bucket[4]"
        )
        spark.sql(f"ALTER TABLE {table} ADD PARTITION FIELD bucket(4, id) AS id_bucket")
        after = _find_latest_metadata(tmp_path)
        new_bucket = next(f for f in current_spec(after)["fields"] if f["name"] == "id_bucket")
        if version == 1:
            assert new_bucket["field-id"] > old_bucket["field-id"]
            assert any(f["transform"] == "void" for f in current_spec(after)["fields"])
        else:
            assert new_bucket["field-id"] == old_bucket["field-id"]
        spark.sql(f"INSERT INTO {table} VALUES (5, 'after')")
        assert spark.table(table).count() == 4
        spark.sql(f"ALTER TABLE {table} REPLACE PARTITION FIELD id_bucket WITH bucket(4, id) AS renamed_bucket")
        renamed = current_spec(_find_latest_metadata(tmp_path))
        assert next(f for f in renamed["fields"] if f["name"] == "renamed_bucket")["field-id"] == new_bucket["field-id"]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize(
    "clause",
    [
        "ADD PARTITION FIELD part",
        "DROP PARTITION FIELD missing",
        "ADD PARTITION FIELD bucket(0, id)",
        "REPLACE PARTITION FIELD part WITH bucket(4, missing)",
        "ADD PARTITION FIELD bucket(4, id) AS part",
    ],
)
def test_invalid_partition_change_is_atomic(spark, tmp_path, clause):
    table = "invalid_partition_change"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, part STRING) USING iceberg PARTITIONED BY (part) LOCATION '{tmp_path.as_uri()}'"
        )
        before = _find_latest_metadata(tmp_path)
        with pytest.raises(Exception, match=r"(?i)duplicate|exist|positive|conflict"):
            spark.sql(f"ALTER TABLE {table} {clause}")
        assert _find_latest_metadata(tmp_path) == before
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("version", [1, 2, 3])
def test_drop_partition_source_keeps_table_writable(spark, tmp_path, version):
    table = "drop_partition_source"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, part INT) USING iceberg PARTITIONED BY (part) "
            f"LOCATION '{tmp_path.as_uri()}' TBLPROPERTIES ('format-version'='{version}')"
        )
        spark.sql(f"INSERT INTO {table} VALUES (1, 10)")
        spark.sql(f"ALTER TABLE {table} DROP PARTITION FIELD part")
        spark.sql(f"ALTER TABLE {table} DROP COLUMN part")
        assert [tuple(row) for row in spark.table(table).collect()] == [(1,)]
        manifests = spark.table(f"{table}.manifests").collect()
        assert manifests[0].partition_summaries[0].lower_bound == "10"
        spark.sql(f"INSERT INTO {table} VALUES (2)")
        spark.sql(f"UPDATE {table} SET id=3 WHERE id=1")
        assert [tuple(row) for row in spark.table(table).orderBy("id").collect()] == [(2,), (3,)]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
