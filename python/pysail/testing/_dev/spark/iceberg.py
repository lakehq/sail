# SPDX-License-Identifier: Apache-2.0

import pytest


def exercise_snapshot_publication(spark, table, load_table):
    """Verify both published heads against metadata freshly loaded from the catalog."""
    spark.range(1).writeTo(table).append()
    base = load_table()[1]["current-snapshot-id"]
    spark.range(1, 2).writeTo(table).option("snapshot-property.wap.id", "audit").append()
    source = load_table()[1]["current-snapshot-id"]
    rollback = f"CALL system.rollback_to_snapshot('{table}', {base})"
    pick = f"CALL system.cherrypick_snapshot('{table}', {source})"
    publish = f"CALL system.publish_changes(wap_id => 'audit', table => '{table}')"

    spark.sql(rollback).collect()
    assert tuple(spark.sql(pick).first()) == (source, source)
    assert load_table()[1]["current-snapshot-id"] == source
    spark.sql(rollback).collect()
    spark.range(2, 3).writeTo(table).append()
    parent = load_table()[1]["current-snapshot-id"]
    row = spark.sql(publish).first()
    pointer, after = load_table()
    current = after["current-snapshot-id"]
    assert tuple(row) == (source, current)
    assert current != source
    assert set(after["refs"]) == {"main"}
    assert after["refs"]["main"]["snapshot-id"] == current
    snapshot = next(s for s in after["snapshots"] if s["snapshot-id"] == current)
    assert snapshot["parent-snapshot-id"] == parent
    assert snapshot["summary"]["source-snapshot-id"] == str(source)
    assert snapshot["summary"]["published-wap-id"] == "audit"
    assert [r.id for r in spark.table(table).orderBy("id").collect()] == [0, 1, 2]
    for call in (pick, publish):
        with pytest.raises(Exception, match="published already"):
            spark.sql(call).collect()
    assert load_table()[0] == pointer
