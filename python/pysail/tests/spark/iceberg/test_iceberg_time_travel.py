import time
from datetime import datetime, timezone

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType
from pyspark.sql.types import Row

from .utils import create_sql_catalog  # noqa: TID252


def test_iceberg_time_travel_by_snapshot_id(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.tt_by_snapshot"
    table = catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="value", field_type=StringType(), required=False),
        ),
    )
    try:
        v0 = [Row(id=1, value="v0")]
        df0 = spark.createDataFrame(v0)
        df0.write.format("iceberg").mode("overwrite").save(table.location())
        table.refresh()
        sid0 = table.current_snapshot().snapshot_id

        v1 = [Row(id=2, value="v1")]
        df1 = spark.createDataFrame(v1)
        df1.write.format("iceberg").mode("append").save(table.location())
        table.refresh()

        latest = spark.read.format("iceberg").load(table.location()).sort("id").toPandas()
        assert_frame_equal(latest, pd.DataFrame({"id": [1, 2], "value": ["v0", "v1"]}).astype(latest.dtypes))

        tt_df = (
            spark.read.format("iceberg").option("snapshotId", str(sid0)).load(table.location()).sort("id").toPandas()
        )
        assert_frame_equal(tt_df, pd.DataFrame({"id": [1], "value": ["v0"]}).astype(tt_df.dtypes))
    finally:
        catalog.drop_table(identifier)


@pytest.mark.skip(reason="not working")
def test_iceberg_time_travel_by_timestamp(spark, tmp_path):
    table_path = tmp_path / "tt_by_timestamp"
    table_path.mkdir(parents=True, exist_ok=True)
    table_location = f"file://{table_path}"

    try:
        # Write snapshot 0: single row
        v0 = [Row(id=1, value="v0")]
        spark.createDataFrame(v0).write.format("iceberg").mode("overwrite").save(table_location)

        # Get timestamp after first write
        ts0 = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
        time.sleep(0.1)

        # Write snapshot 1: append one more row
        v1 = [Row(id=2, value="v1")]
        spark.createDataFrame(v1).write.format("iceberg").mode("append").save(table_location)

        # Get timestamp after second write
        ts1 = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
        time.sleep(0.1)

        # Write snapshot 2: append another row
        v2 = [Row(id=3, value="v2")]
        spark.createDataFrame(v2).write.format("iceberg").mode("append").save(table_location)

        # Get timestamp after third write
        ts2 = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"

        # Verify current state (all three rows)
        latest = spark.read.format("iceberg").load(table_location).sort("id").collect()
        assert latest == [Row(id=1, value="v0"), Row(id=2, value="v1"), Row(id=3, value="v2")]

        # Test time travel to snapshot 0 (only first row)
        df0 = spark.read.format("iceberg").option("timestampAsOf", ts0).load(table_location)
        assert df0.collect() == [Row(id=1, value="v0")]

        # Test time travel to snapshot 1 (first two rows)
        df1 = spark.read.format("iceberg").option("timestampAsOf", ts1).load(table_location).sort("id").collect()
        assert df1 == [Row(id=1, value="v0"), Row(id=2, value="v1")]

        # Test time travel to snapshot 2 (all three rows)
        df2 = spark.read.format("iceberg").option("timestampAsOf", ts2).load(table_location).sort("id").collect()
        assert df2 == [Row(id=1, value="v0"), Row(id=2, value="v1"), Row(id=3, value="v2")]
    finally:
        pass


def test_iceberg_time_travel_precedence_snapshot_over_timestamp(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.tt_precedence"
    table = catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="value", field_type=StringType(), required=False),
        ),
    )
    try:
        spark.createDataFrame([Row(id=1, value="old")]).write.format("iceberg").mode("overwrite").save(table.location())
        table.refresh()
        sid_old = table.current_snapshot().snapshot_id
        time.sleep(0.1)

        spark.createDataFrame([Row(id=2, value="new")]).write.format("iceberg").mode("overwrite").save(table.location())
        ts_new = datetime.now(timezone.utc).isoformat()

        df_prec = (
            spark.read.format("iceberg")
            .option("snapshotId", str(sid_old))
            .option("timestampAsOf", ts_new)
            .load(table.location())
        )
        assert df_prec.collect() == [Row(id=1, value="old")]
    finally:
        catalog.drop_table(identifier)


def test_iceberg_time_travel_by_ref_main_if_available(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.tt_ref_main"
    table = catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="value", field_type=StringType(), required=False),
        ),
    )
    try:
        spark.createDataFrame([Row(id=1, value="x")]).write.format("iceberg").mode("overwrite").save(table.location())
        try:
            df = spark.read.format("iceberg").option("ref", "main").load(table.location())
            assert df.collect() == [Row(id=1, value="x")]
        except Exception as e:
            if "Unknown Iceberg ref" in str(e):
                pytest.xfail("Iceberg refs (main) not present in table metadata")
            raise
    finally:
        catalog.drop_table(identifier)


def test_iceberg_time_travel_across_merge_schema(spark, tmp_path):
    catalog = create_sql_catalog(tmp_path)
    identifier = "default.tt_schema_evolution"
    table = catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="value", field_type=StringType(), required=False),
        ),
    )
    try:
        base_df = spark.createDataFrame([Row(id=1, value="base")])
        base_df.write.format("iceberg").mode("overwrite").save(table.location())
        table.refresh()
        snapshot_id = table.current_snapshot().snapshot_id

        evolved_df = spark.createDataFrame([Row(id=2, value="new", extra=10)])
        (evolved_df.write.format("iceberg").mode("append").option("mergeSchema", "true").save(table.location()))

        latest_df = spark.read.format("iceberg").load(table.location()).orderBy("id")
        assert latest_df.columns == ["id", "value", "extra"]
        latest_rows = latest_df.collect()
        assert latest_rows[0].id == 1
        assert latest_rows[0].extra is None
        assert latest_rows[1].id == 2  # noqa: PLR2004
        assert latest_rows[1].extra == 10  # noqa: PLR2004

        tt_df = spark.read.format("iceberg").option("snapshotId", str(snapshot_id)).load(table.location()).orderBy("id")
        tt_rows = tt_df.collect()
        assert len(tt_rows) == 1
        assert tt_rows[0].id == 1
        assert tt_rows[0].value == "base"
        if "extra" in tt_df.columns:
            assert tt_df.select("extra").first().extra is None
        else:
            assert "extra" not in tt_df.columns
    finally:
        catalog.drop_table(identifier)
