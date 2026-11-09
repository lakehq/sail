"""Partitioned CDC preserves identity values even when event streams revisit partitions."""

from urllib.parse import unquote

import pyarrow.parquet as pq
import pytest

from .test_delta_cdf import _actions, _assert_cdc_metrics, _feed
from .test_delta_identity_merge import _watermark, identity_table  # noqa: F401


@pytest.mark.parametrize("deletion_vectors", [False, True], ids=["cow", "mor"])
@pytest.mark.parametrize("move_partitions", [False, True], ids=["same-partitions", "swap-partitions"])
@pytest.mark.parametrize("batch_size", [1, 8192], ids=["small-batches", "single-batch"])
def test_identity_merge_cdf_partition_revisits(spark, identity_table, deletion_vectors, move_partitions, batch_size):  # noqa: F811
    spark.sql("SET datafusion.execution.target_partitions = 1").collect()
    spark.sql(f"SET datafusion.execution.batch_size = {batch_size}").collect()
    try:
        name, path = identity_table(
            schema="id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 10 INCREMENT BY 3), k INT, v INT, p INT",
            properties=f"""PARTITIONED BY (p) TBLPROPERTIES (
                'delta.enableChangeDataFeed'='true',
                'delta.enableDeletionVectors'='{str(deletion_vectors).lower()}')""",
        )
        spark.sql(f"INSERT INTO {name} (k,v,p) VALUES (1,100,10),(2,200,20)").collect()
        before = {row.k: tuple(row) for row in spark.table(name).collect()}
        old_hwm = _watermark(path)
        p1, p2 = (20, 10) if move_partitions else (10, 20)
        result = spark.sql(f"""
            MERGE INTO {name} t
            USING (SELECT * FROM VALUES (1,101,{p1}),(2,201,{p2}),(3,300,10) AS s(k,v,p)) s
            ON t.k=s.k
            WHEN MATCHED THEN UPDATE SET v=s.v,p=s.p
            WHEN NOT MATCHED THEN INSERT (k,v,p) VALUES (s.k,s.v,s.p)
        """).collect()
        assert result == [(3, 2, 0, 1)]
        rows = spark.read.format("delta").load(str(path)).orderBy("k").collect()
        generated = rows[2].id
        assert [tuple(row) for row in rows] == [
            (before[1][0], 1, 101, p1),
            (before[2][0], 2, 201, p2),
            (generated, 3, 300, 10),
        ]
        assert generated > old_hwm and (generated - 10) % 3 == 0
        assert len({row.id for row in rows}) == 3
        assert _watermark(path) == generated
        expected = sorted(
            [
                (*before[1], "update_preimage"),
                (*before[2], "update_preimage"),
                (*rows[0], "update_postimage"),
                (*rows[1], "update_postimage"),
                (*rows[2], "insert"),
            ]
        )
        assert (
            sorted(tuple(row) for row in _feed(spark, path, 2, 2).select("id", "k", "v", "p", "_change_type").collect())
            == expected
        )
        metrics = _assert_cdc_metrics(path, 2, "MERGE")
        assert [
            int(metrics[key]) for key in ("numTargetRowsUpdated", "numTargetRowsDeleted", "numTargetRowsInserted")
        ] == [2, 0, 1]
        physical = []
        for action in _actions(path, 2):
            if "cdc" not in action:
                continue
            cdc = action["cdc"]
            assert set(cdc["partitionValues"]) == {"p"}
            file = pq.ParquetFile(path / unquote(cdc["path"]))
            if cdc["partitionValues"]["p"] is None:
                # Empty task sentinels suppress Add/Remove-based CDC inference.
                assert file.metadata.num_rows == 0
                assert file.schema_arrow.names == ["id", "k", "v", "p", "_change_type"]
                continue
            p = int(cdc["partitionValues"]["p"])
            assert p in (10, 20)
            assert file.schema_arrow.names == ["id", "k", "v", "_change_type"]
            physical.extend((row["id"], row["k"], row["v"], p, row["_change_type"]) for row in file.read().to_pylist())
        assert sorted(physical) == expected
    finally:
        spark.sql("RESET datafusion.execution.batch_size").collect()
        spark.sql("RESET datafusion.execution.target_partitions").collect()
