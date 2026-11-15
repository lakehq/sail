"""Insert-only identity MERGE must not rewrite untouched target files."""

import json

import pyarrow.parquet as pq
import pytest

from pysail.tests._dev.spark.delta import test_delta_identity_merge
from pysail.tests._dev.spark.delta.test_delta_cdf import _feed
from pysail.tests._dev.spark.delta.test_delta_identity_merge import _log, _watermark

identity_table = test_delta_identity_merge.identity_table


@pytest.mark.parametrize("deletion_vectors", [False, True], ids=["cow", "mor"])
@pytest.mark.parametrize("generated", [False, True], ids=["explicit", "generated"])
@pytest.mark.parametrize("source", ["unmatched", "matched", "empty"])
def test_insert_only_identity_cdf_preserves_untouched_files(spark, identity_table, deletion_vectors, generated, source):
    name, path = identity_table(
        mode="BY DEFAULT",
        properties=f"""TBLPROPERTIES ('delta.enableChangeDataFeed'='true',
            'delta.enableDeletionVectors'='{str(deletion_vectors).lower()}')""",
    )
    spark.sql(f"INSERT INTO {name} (k,v) VALUES (1,10),(2,20)").collect()  # noqa: S608
    before_rows = [tuple(row) for row in spark.table(name).orderBy("k").collect()]
    before_log = _log(path)
    before_watermark = _watermark(path)
    source_sql = {
        "unmatched": "SELECT 3 AS k,30 AS v",
        "matched": "SELECT 1 AS k,30 AS v",
        "empty": "SELECT 3 AS k,30 AS v WHERE false",
    }[source]
    explicit_id = 999999
    insert = "(k,v) VALUES (s.k,s.v)" if generated else f"(id,k,v) VALUES ({explicit_id},s.k,s.v)"
    spark.sql(f"""
        MERGE INTO {name} t USING ({source_sql}) s ON t.k=s.k
        WHEN NOT MATCHED THEN INSERT {insert}
    """).collect()
    rows = [tuple(row) for row in spark.table(name).orderBy("k").collect()]
    assert rows[:2] == before_rows
    expected_inserts = int(source == "unmatched")
    assert len(rows) == len(before_rows) + expected_inserts
    assert len({row[1] for row in rows}) == len(rows)
    inserted = rows[2:]
    if inserted:
        assert inserted[0][1:] == (3, 30)
        if generated:
            assert inserted[0][0] > before_watermark
            assert (inserted[0][0] - 10) % 3 == 0
            assert _watermark(path) == inserted[0][0]
        else:
            assert inserted[0][0] == explicit_id
    if not generated or not inserted:
        assert _watermark(path) == before_watermark

    after_log = _log(path)
    assert all(after_log[name] == contents for name, contents in before_log.items())
    new_logs = {name: data for name, data in after_log.items() if name not in before_log}
    actions = [json.loads(line) for data in new_logs.values() for line in data.splitlines()]
    assert not any("remove" in action for action in actions)
    new_rows = [
        (row["id"], row["k"], row["v"])
        for action in actions
        if "add" in action
        for row in pq.ParquetFile(path / action["add"]["path"]).read().to_pylist()
    ]
    assert sorted(new_rows) == sorted(inserted)
    for action in actions:
        if "commitInfo" in action:
            metrics = action["commitInfo"]["operationMetrics"]
            assert int(metrics["numTargetRowsInserted"]) == expected_inserts
            assert int(metrics["numTargetRowsUpdated"]) == 0
            assert int(metrics["numTargetRowsDeleted"]) == 0
            assert int(metrics["numTargetRowsCopied"]) == 0
    if new_logs:
        changes = _feed(spark, path, min(int(name[:-5]) for name in new_logs), max(int(name[:-5]) for name in new_logs))
        assert sorted(tuple(row) for row in changes.select("id", "k", "v", "_change_type").collect()) == sorted(
            (*row, "insert") for row in inserted
        )
