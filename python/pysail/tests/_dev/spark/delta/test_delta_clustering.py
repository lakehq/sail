# SPDX-License-Identifier: Apache-2.0

# ruff: noqa: S608

"""Writes to clustered tables, which are the tables with the `clustering` table feature.

Sail does not cluster data. It adds files that are not clustered, keeps the clustering
columns, and writes statistics for them, which is what the Delta protocol requires.
"""

from __future__ import annotations

import json

import pytest
from pyspark.sql import Row

from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests._dev.spark.delta.utils import _commits, _effective

pytestmark = pytest.mark.integration


def _clustering_columns(path):
    """The clustering columns of the last commit that sets them."""
    return next(
        json.loads(action["domainMetadata"]["configuration"])["clusteringColumns"]
        for _, actions in sorted(_commits(path).items(), reverse=True)
        for action in reversed(actions)
        if action.get("domainMetadata", {}).get("domain") == "delta.clustering"
    )


@pytest.fixture
def clustered_table(spark, tmp_path):
    """A table that another engine clusters by `key`, which is not among the columns that
    the table collects statistics for by position."""
    name = "clustered_writes"
    path = tmp_path / "clustered"
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT, key BIGINT, value STRING) USING delta "
        f"LOCATION '{escape_sql_string_literal(str(path))}' "
        "TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '1')"
    ).collect()
    try:
        spark.sql(f"INSERT INTO {name} VALUES (1, 10, 'a'), (2, 20, 'b')").collect()
        protocol = _effective(path, "protocol")
        features = set(protocol.get("writerFeatures") or ["appendOnly", "invariants"])
        protocol = {
            **protocol,
            "minWriterVersion": 7,
            "writerFeatures": sorted(features | {"clustering", "domainMetadata"}),
        }
        domain = {
            "domain": "delta.clustering",
            "configuration": json.dumps({"clusteringColumns": [["key"]]}),
            "removed": False,
        }
        version = max(_commits(path)) + 1
        (path / "_delta_log" / f"{version:020d}.json").write_text(
            json.dumps({"protocol": protocol}) + "\n" + json.dumps({"domainMetadata": domain}) + "\n"
        )
        yield name, path
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


def test_clustered_table_is_written_and_stays_clustered(spark, clustered_table):
    name, path = clustered_table
    versions = set(_commits(path))
    assert spark.sql(f"DESCRIBE DETAIL {name}").first().clusteringColumns == ["key"]

    spark.sql(f"INSERT INTO {name} VALUES (3, 30, 'c'), (4, 40, 'd')").collect()
    spark.sql(f"UPDATE {name} SET value = 'B' WHERE id = 2").collect()
    spark.sql(f"DELETE FROM {name} WHERE id = 1").collect()
    spark.sql(
        f"MERGE INTO {name} t USING (SELECT 4 AS id, 44 AS key, 'D' AS value) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET key = s.key, value = s.value"
    ).collect()
    expected = [Row(id=2, key=20, value="B"), Row(id=3, key=30, value="c"), Row(id=4, key=44, value="D")]
    assert spark.table(name).orderBy("id").collect() == expected

    # The commits keep the clustering columns, and add files that are not clustered, with
    # statistics for the clustering columns after the columns that are indexed by position.
    assert _clustering_columns(path) == [["key"]]
    written = [
        action["add"]
        for version, actions in _commits(path).items()
        if version not in versions
        for action in actions
        if "add" in action
    ]
    assert written
    for add in written:
        assert add.get("clusteringProvider") is None
        stats = json.loads(add["stats"])
        assert set(stats["minValues"]) == {"id", "key"}
        assert set(stats["maxValues"]) == {"id", "key"}
    assert spark.sql(f"DESCRIBE DETAIL {name}").first().clusteringColumns == ["key"]

    spark.sql(f"INSERT OVERWRITE {name} VALUES (5, 50, 'e')").collect()
    assert _clustering_columns(path) == [["key"]]
    assert spark.table(name).collect() == [Row(id=5, key=50, value="e")]


def test_clustered_table_cannot_be_partitioned_and_is_unclustered_without_the_column(spark, clustered_table):
    name, path = clustered_table
    rows = spark.table(name).orderBy("id").collect()
    versions = set(_commits(path))

    df = spark.createDataFrame([(7, 70, "g")], "id BIGINT, key BIGINT, value STRING")
    with pytest.raises(Exception, match="cannot be partitioned"):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("value")
            .save(str(path))
        )
    assert set(_commits(path)) == versions
    assert spark.table(name).orderBy("id").collect() == rows

    # An overwrite of the table with a schema without the clustering column leaves the
    # table without clustering columns, as a table that Delta Spark replaces.
    df.drop("key").write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(path))
    assert _clustering_columns(path) == []
    assert "clustering" in _effective(path, "protocol")["writerFeatures"]
    assert spark.read.format("delta").load(str(path)).collect() == [Row(id=7, value="g")]
    assert spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first().clusteringColumns == []
