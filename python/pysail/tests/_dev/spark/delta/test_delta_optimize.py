from __future__ import annotations

import json
import shutil
from collections import Counter
from contextlib import nullcontext
from datetime import date
from urllib.parse import unquote

import pytest
from pyspark.sql import Row

from pysail.testing._dev.spark.utils.jvm import classic_spark_mode
from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests._dev.spark.delta.utils import _active_files, _commits, _settings, _tree


def _assert_file_sizes(metrics, files):
    sizes = [file.size for file in files]
    assert metrics.asDict() == {
        "min": min(sizes) if sizes else None,
        "max": max(sizes) if sizes else None,
        "avg": sum(sizes) / len(sizes) if sizes else 0.0,
        "totalFiles": len(sizes),
        "totalSize": sum(sizes),
    }


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/optimize/OptimizeCompactionSuite.scala#L359-L457
@pytest.mark.parametrize("mapping", ["none", "name", "id"])
@pytest.mark.parametrize("multiple", [False, True], ids=["null-empty", "date-multiple"])
def test_optimize_partition_groups(spark, tmp_path, request, mapping, multiple):
    path = tmp_path / "groups"
    name = "maintenance_partition_groups"
    target = f"delta.`{path}`"
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, part STRING, day DATE) USING delta "
        f"PARTITIONED BY ({'day, part' if multiple else 'part'}) "
        f"LOCATION '{escape_sql_string_literal(str(path))}' "
        f"TBLPROPERTIES ('delta.columnMapping.mode' = '{mapping}')"
    ).collect()
    request.addfinalizer(lambda: spark.sql(f"DROP TABLE IF EXISTS {name}").collect())
    for _ in range(3):
        spark.sql(
            f"INSERT INTO {name} VALUES (1, 'a', DATE '2017-10-10'), "  # noqa: S608
            "(2, 'b', DATE '2017-10-10'), (3, NULL, DATE '2017-10-10'), "
            "(4, '', DATE '2017-10-10'), (5, 'a', DATE '2017-10-11')"
        ).collect()
    before_rows = spark.read.format("delta").load(str(path)).orderBy("id").collect()
    assert before_rows == [
        Row(id=i, part=p, day=date(2017, 10, 11 if i == 5 else 10))
        for i, p in [(1, "a"), (2, "b"), (3, None), (4, None), (5, "a")]
        for _ in range(3)
    ]
    files = _active_files(path)
    version = max(_commits(path))
    predicate = " WHERE day = '2017-10-10' AND part IS NULL" if multiple else ""
    selected = {
        file.path
        for file in files
        if not multiple
        or (
            any(value in (None, "") for value in file.partitionValues.values())
            and "2017-10-10" in file.partitionValues.values()
        )
    }
    untouched = {file.path for file in files} - selected
    metrics = spark.sql(f"OPTIMIZE {target}{predicate}").first().metrics
    after = _active_files(path)
    assert metrics.numFilesRemoved == len(selected)
    assert {file.path for file in after} & selected == set()
    assert untouched <= {file.path for file in after}
    groups = Counter(tuple(sorted(file.partitionValues.items())) for file in after if file.path not in untouched)
    assert len(groups) == (1 if multiple else 3)
    assert set(groups.values()) == {1}
    assert max(_commits(path)) == version + 1
    assert spark.read.format("delta").load(str(path)).orderBy("id").collect() == before_rows
    actions = _commits(path)[version + 1]
    assert all(not action[k]["dataChange"] for action in actions for k in ("add", "remove") if k in action)
    historical = spark.read.format("delta").option("versionAsOf", version).load(str(path))
    assert historical.orderBy("id").collect() == before_rows


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/optimize/OptimizeCompactionSuite.scala#L248-L309
@pytest.mark.parametrize(
    "compactable",
    [(2, 4, 6), (3, 4), (1, 6), (2, 3, 4, 5, 6), (1,)],
    ids=["interleaved", "middle", "ends", "all-but-first", "first-only"],
)
def test_optimize_interleaved_singleton_partitions(spark, semantic_table, compactable):
    name, path = semantic_table
    for part in range(1, 7):
        for value in range(3 if part in compactable else 1):
            spark.sql(f"INSERT INTO {name} VALUES ({value}, '{part}')").collect()  # noqa: S608
    before = _active_files(path)
    untouched = {f.path for f in before if int(f.partitionValues["part"]) not in compactable}
    rows = spark.table(name).orderBy("id", "part").collect()
    version = max(_commits(path))
    result = spark.sql(f"OPTIMIZE {name}").first().metrics
    after = _active_files(path)
    assert Counter(f.partitionValues["part"] for f in after) == {str(p): 1 for p in range(1, 7)}
    assert untouched <= {f.path for f in after}
    assert result.numFilesRemoved == 3 * len(compactable)
    assert result.numFilesAdded == len(compactable)
    assert max(_commits(path)) == version + 1
    assert spark.table(name).orderBy("id", "part").collect() == rows


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/optimize/OptimizeMetricsSuite.scala#L41-L264
@pytest.mark.parametrize("indexed", [0, 1, 2])
def test_optimize_metrics_history_and_artifacts(spark, semantic_table, indexed):
    name, path = semantic_table
    spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '{indexed}')").collect()
    for i in range(3):
        spark.sql(f"INSERT INTO {name} VALUES ({i}, 'a'), ({i}, 'bb'), ({i}, NULL)").collect()  # noqa: S608
    rows = spark.table(name).orderBy("id", "part").collect()
    before = _active_files(path)
    version = max(_commits(path))
    metrics = spark.sql(f"OPTIMIZE {name}").first().metrics
    after = _active_files(path)
    _assert_file_sizes(metrics.filesRemoved, before)
    _assert_file_sizes(metrics.filesAdded, after)
    assert metrics.numFilesRemoved == metrics.totalConsideredFiles == len(before)
    assert metrics.numFilesAdded == metrics.partitionsOptimized == metrics.numBins == len(after)
    assert metrics.totalFilesSkipped == 0
    assert metrics.numTableColumns == 2  # noqa: PLR2004
    assert metrics.numTableColumnsWithStats == indexed
    assert metrics.zOrderStats is None
    assert metrics.clusteringStats is None
    assert metrics.autoCompactParallelismStats is None
    assert metrics.preserveInsertionOrder is False
    assert metrics.numFilesSkippedToReduceWriteAmplification == 0
    assert metrics.numBytesSkippedToReduceWriteAmplification == 0
    assert metrics.numBatches == 1
    # Delta Spark may leave task instrumentation and the end timestamp unset.
    assert metrics.totalScheduledTasks >= 0
    assert metrics.totalClusterParallelism > 0
    assert metrics.startTimeMs >= 0
    assert metrics.endTimeMs == 0 or metrics.startTimeMs <= metrics.endTimeMs
    assert max(_commits(path)) == version + 1
    actions = _commits(path)[version + 1]
    assert {a["remove"]["path"] for a in actions if "remove" in a} == {f.path for f in before}
    assert {a["add"]["path"] for a in actions if "add" in a} == {f.path for f in after}
    assert all(not a[k]["dataChange"] for a in actions for k in ("add", "remove") if k in a)
    for file in before + after:
        assert (path / unquote(file.path)).stat().st_size == file.size
    history = spark.sql(f"DESCRIBE HISTORY {name} LIMIT 1").first()
    assert history.operation == "OPTIMIZE"
    sizes = sorted(file.size for file in after)
    expected = {
        "numAddedFiles": len(after),
        "numRemovedFiles": len(before),
        "numAddedBytes": sum(sizes),
        "numRemovedBytes": sum(file.size for file in before),
        "minFileSize": min(sizes),
        "maxFileSize": max(sizes),
        "p25FileSize": sizes[len(sizes) // 4],
        "p50FileSize": sizes[len(sizes) // 2],
        "p75FileSize": sizes[3 * len(sizes) // 4],
        "numDeletionVectorsRemoved": 0,
    }
    assert history.operationMetrics == {key: str(value) for key, value in expected.items()}
    assert spark.table(name).orderBy("id", "part").collect() == rows
    before_tree = _tree(path)
    noop = spark.sql(f"OPTIMIZE {name}").first().metrics
    assert noop.numFilesAdded == noop.numFilesRemoved == noop.partitionsOptimized == noop.numBins == 0
    assert noop.totalConsideredFiles == noop.totalFilesSkipped == len(after)
    assert noop.totalScheduledTasks == 0
    _assert_file_sizes(noop.filesAdded, [])
    _assert_file_sizes(noop.filesRemoved, [])
    assert _tree(path) == before_tree


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/optimize/OptimizeMetricsSuite.scala#L201-L236
@pytest.mark.parametrize("populated", [False, True], ids=["empty", "singleton"])
def test_optimize_noop_metrics(spark, semantic_table, populated):
    name, path = semantic_table
    if populated:
        spark.sql(f"INSERT INTO {name} VALUES (1, NULL)").collect()  # noqa: S608
    before = _tree(path)
    metrics = spark.sql(f"OPTIMIZE {name}").first().metrics
    assert metrics.numFilesAdded == metrics.numFilesRemoved == metrics.partitionsOptimized == metrics.numBins == 0
    assert metrics.totalConsideredFiles == metrics.totalFilesSkipped == int(populated)
    assert metrics.totalScheduledTasks == 0
    _assert_file_sizes(metrics.filesAdded, [])
    _assert_file_sizes(metrics.filesRemoved, [])
    assert _tree(path) == before
    assert spark.table(name).collect() == ([Row(id=1, part=None)] if populated else [])


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/optimize/OptimizeCompactionSuite.scala#L167-L224
@pytest.mark.integration
@pytest.mark.parametrize("deleted", [16, 32, 48], ids=["ratio-below", "ratio-equal", "ratio-above"])
@pytest.mark.parametrize("size_delta", [-1, 0, 1], ids=["size-ineligible", "size-equal", "size-eligible"])
def test_optimize_dv_ratio_and_file_size_boundaries(spark, delta_jvm_spark, tmp_path, deleted, size_delta):
    jvm = delta_jvm_spark
    reference = tmp_path / "reference"
    candidate = tmp_path / "candidate"
    jvm.range(128).coalesce(1).write.format("delta").option("delta.enableDeletionVectors", "true").save(str(reference))
    jvm.sql(f"DELETE FROM delta.`{reference}` WHERE id < {deleted}").collect()  # noqa: S608
    source_files = _active_files(reference)
    assert len(source_files) == 1
    source = source_files[0]
    assert source.deletionVector is not None
    assert source.deletionVector["cardinality"] == deleted
    assert json.loads(source.stats)["numRecords"] == 128  # noqa: PLR2004
    # A companion makes a two-file bin; Delta 4.0.1 does not rewrite singleton DV bins.
    jvm.range(1000, 1001).coalesce(1).write.format("delta").mode("append").save(str(reference))
    source_files = _active_files(reference)
    assert len(source_files) == 2  # noqa: PLR2004
    companion = next(file for file in source_files if file.path != source.path)
    assert companion.deletionVector is None
    assert companion.size < source.size - 1
    shutil.copytree(reference, candidate)

    # Binary-exact ratios distinguish strict comparison from >= without rounding ambiguity.
    rewrite = deleted > 32 or size_delta > 0  # noqa: PLR2004
    expected = [Row(id=value) for value in [*range(deleted, 128), 1000]]
    settings = {
        "spark.databricks.delta.optimize.maxDeletedRowsRatio": "0.25",
        "spark.databricks.delta.optimize.minFileSize": str(source.size + size_delta),
        "spark.databricks.delta.optimize.maxFileSize": str(1 << 30),
    }
    for session, path in [(spark, candidate), (jvm, reference)]:
        target = f"'{escape_sql_string_literal(str(path))}'"
        before = _commits(path)
        version = max(before)
        with _settings(session, **settings):
            metrics = session.sql(f"OPTIMIZE {target}").first().metrics
            assert metrics.numFilesRemoved == 2 * int(rewrite)
            assert metrics.numFilesAdded == int(rewrite)
            after = _active_files(path)
            assert len(after) == (1 if rewrite else 2)
            commits = _commits(path)
            assert max(commits) == version + int(rewrite)
            if rewrite:
                actions = commits[version + 1]
                removes = [action["remove"] for action in actions if "remove" in action]
                adds = [action["add"] for action in actions if "add" in action]
                assert len(removes) == 2  # noqa: PLR2004
                assert len(adds) == 1
                assert {action["path"] for action in removes} == {source.path, companion.path}
                dv_remove = next(action for action in removes if action["path"] == source.path)
                assert dv_remove["deletionVector"] == source.deletionVector
                assert adds[0]["path"] != source.path
                assert after[0].deletionVector is None
                assert all(not action["dataChange"] for action in removes + adds)
                info = next(action["commitInfo"] for action in actions if "commitInfo" in action)
                assert info["operation"] == "OPTIMIZE"
                assert metrics.deletionVectorStats.numDeletionVectorsRemoved == 1
                assert metrics.deletionVectorStats.numDeletionVectorRowsRemoved == deleted
            else:
                assert commits == before
                assert after == source_files
            assert session.sql(f"OPTIMIZE {target}").first().metrics.numFilesRemoved == 0
            assert _commits(path) == commits

        # Reopen both outputs in both readers, including the pre-OPTIMIZE DV snapshot.
        for reader in [spark, jvm]:
            with classic_spark_mode() if reader is jvm else nullcontext():
                assert reader.read.format("delta").load(str(path)).orderBy("id").collect() == expected
                assert (
                    reader.read.format("delta").option("versionAsOf", version).load(str(path)).orderBy("id").collect()
                    == expected
                )
