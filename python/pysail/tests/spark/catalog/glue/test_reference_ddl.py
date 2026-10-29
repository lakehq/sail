# ruff: noqa: S608
"""Reference DDL preserves Glue's authoritative metadata pointer."""

import pytest
from pyiceberg.table import StaticTable

from pysail.tests.spark.catalog.glue.test_iceberg_commit import _glue_client


@pytest.mark.parametrize("version", [1, 2, 3])
def test_reference_metadata_is_published(spark, tmp_path, moto_endpoint, version):
    name = "test_db.references"
    client = _glue_client(moto_endpoint)

    def load():
        stored = client.get_table(DatabaseName="test_db", Name="references")["Table"]
        return StaticTable.from_metadata(stored["Parameters"]["metadata_location"])

    try:
        spark.sql(
            f"CREATE TABLE {name} (id INT) USING iceberg LOCATION '{tmp_path.as_uri()}' "
            f"TBLPROPERTIES ('format-version'='{version}')"
        )
        spark.sql(f"ALTER TABLE {name} CREATE BRANCH audit")
        empty = load()
        assert empty.current_snapshot() is None
        assert empty.scan(snapshot_id=empty.refs()["audit"].snapshot_id).to_arrow().num_rows == 0
        spark.sql(f"INSERT INTO {name} VALUES (1)")
        spark.sql(f"ALTER TABLE {name} CREATE TAG release RETAIN 1 DAYS")
        spark.sql(f"ALTER TABLE {name} REPLACE BRANCH audit WITH SNAPSHOT RETENTION 2 SNAPSHOTS")
        current = load()
        assert current.refs()["audit"].snapshot_id == current.current_snapshot().snapshot_id
        assert current.refs()["audit"].min_snapshots_to_keep == 2  # noqa: PLR2004
        assert current.refs()["release"].max_ref_age_ms == 86400000  # noqa: PLR2004
        spark.sql(f"ALTER TABLE {name} DROP TAG release")
        spark.sql(f"ALTER TABLE {name} DROP BRANCH audit")
        assert set(load().refs()) == {"main"}
        assert load().scan().to_arrow().to_pylist() == [{"id": 1}]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
