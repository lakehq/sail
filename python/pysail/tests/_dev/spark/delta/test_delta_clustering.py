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
from pysail.tests._dev.spark.delta.utils import _active_files, _commits, _effective

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
def clustered_table(spark, delta_jvm_spark, tmp_path):
    path = tmp_path / "clustered"
    jvm = delta_jvm_spark
    jvm.sql(f"CREATE TABLE delta.`{path}` (id BIGINT, key BIGINT, value STRING) USING delta CLUSTER BY (key)")
    jvm.sql(f"INSERT INTO delta.`{path}` VALUES (1, 10, 'a'), (2, 20, 'b')")
    assert "clustering" in _effective(path, "protocol")["writerFeatures"]
    name = "clustered_writes"
    spark.sql(f"CREATE TABLE {name} USING delta LOCATION '{escape_sql_string_literal(path.as_uri())}'")
    try:
        yield name, path
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_clustered_table_is_written_and_stays_clustered(spark, delta_jvm_spark, clustered_table):
    name, path = clustered_table
    jvm = delta_jvm_spark
    versions = set(_commits(path))

    spark.sql(f"INSERT INTO {name} VALUES (3, 30, 'c'), (4, 40, 'd')").collect()
    spark.sql(f"UPDATE {name} SET value = 'B' WHERE id = 2").collect()
    spark.sql(f"DELETE FROM {name} WHERE id = 1").collect()
    spark.sql(
        f"MERGE INTO {name} t USING (SELECT 4 AS id, 44 AS key, 'D' AS value) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET key = s.key, value = s.value"
    ).collect()
    expected = [Row(id=2, key=20, value="B"), Row(id=3, key=30, value="c"), Row(id=4, key=44, value="D")]
    assert spark.table(name).orderBy("id").collect() == expected

    # The commits of Sail keep the clustering columns, and add files that are not clustered,
    # with statistics for the clustering columns.
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
        assert "key" in stats["minValues"]
        assert "key" in stats["maxValues"]
    assert spark.sql(f"DESCRIBE DETAIL {name}").first().clusteringColumns == ["key"]

    # Delta Spark reads the table, and clusters the files that Sail added.
    assert jvm.sql(f"SELECT * FROM delta.`{path}` ORDER BY id").collect() == expected
    assert jvm.sql(f"DESCRIBE DETAIL delta.`{path}`").first().clusteringColumns == ["key"]
    jvm.sql(f"OPTIMIZE delta.`{path}`").collect()
    assert all(add["clusteringProvider"] == "liquid" for add in _active_files(path))
    assert spark.table(name).orderBy("id").collect() == expected

    spark.sql(f"INSERT OVERWRITE {name} VALUES (5, 50, 'e')").collect()
    assert _clustering_columns(path) == [["key"]]
    assert jvm.sql(f"SELECT * FROM delta.`{path}`").collect() == [Row(id=5, key=50, value="e")]


def test_clustered_table_cannot_be_partitioned_and_is_unclustered_without_the_column(
    spark, delta_jvm_spark, clustered_table
):
    name, path = clustered_table
    jvm = delta_jvm_spark
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
    assert jvm.sql(f"SELECT * FROM delta.`{path}`").collect() == [Row(id=7, value="g")]
    assert jvm.sql(f"DESCRIBE DETAIL delta.`{path}`").first().clusteringColumns == []
