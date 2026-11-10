"""Replacement boundary contracts verified with Spark 4.1.3 / Iceberg 1.11.0."""

# ruff: noqa: S608

import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from pyiceberg.manifest import ManifestContent
from pyspark.errors import AnalysisException, PySparkException
from pyspark.sql import functions as sf

from pysail.testing._dev.spark.steps.iceberg import _find_latest_metadata
from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests._dev.spark.iceberg.utils import _current_manifest_entries


@pytest.fixture
def target(spark, tmp_path):
    name = "iceberg_replacement_boundaries"
    path = tmp_path / "table"
    try:
        yield name, path
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("version", [1, 2, 3])
@pytest.mark.parametrize("api", ["sql", "replace"])
def test_replacement_drops_v1_partition_source(spark, target, version, api):
    name, path = target
    spark.sql(
        f"CREATE TABLE {name} (a INT, b INT) USING iceberg PARTITIONED BY (a) "
        f"LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES ('format-version'='1')"
    )
    spark.sql(f"INSERT INTO {name} VALUES (1, 2)")
    before = _find_latest_metadata(path)
    old_files = {p: p.read_bytes() for p in (path / "metadata").glob("*.metadata.json")}
    if api == "sql":
        spark.sql(
            f"REPLACE TABLE {name} USING iceberg TBLPROPERTIES ('format-version'='{version}') AS SELECT 20 AS b"
        ).collect()
    else:
        spark.createDataFrame([(20,)], "b INT").writeTo(name).using("iceberg").tableProperty(
            "format-version", str(version)
        ).replace()
    after = _find_latest_metadata(path)
    assert after["format-version"] == version
    assert after["table-uuid"] == before["table-uuid"]
    old_spec = next(spec for spec in before["partition-specs"] if spec["spec-id"] == before["default-spec-id"])
    new_spec = next(spec for spec in after["partition-specs"] if spec["spec-id"] == after["default-spec-id"])
    assert new_spec["spec-id"] != old_spec["spec-id"]
    assert new_spec["fields"] == [{**old_spec["fields"][0], "transform": "void"}]
    assert after["last-partition-id"] == before["last-partition-id"]
    assert all(spec in after["partition-specs"] for spec in before["partition-specs"])
    assert all(schema in after["schemas"] for schema in before["schemas"])
    entries = _current_manifest_entries(path, ManifestContent.DATA)
    assert sum(entry.data_file.record_count for entry in entries) == 1
    assert all(tuple(entry.data_file.partition) == (None,) for entry in entries)
    assert spark.table(name).columns == ["b"]
    assert spark.table(name).collect() == [(20,)]
    assert spark.read.format("iceberg").load(str(path)).collect() == [(20,)]
    assert spark.read.format("iceberg").option("versionAsOf", str(before["current-snapshot-id"])).load(
        str(path)
    ).collect() == [(1, 2)]
    assert all(p.read_bytes() == contents for p, contents in old_files.items())
    spark.sql(f"INSERT INTO {name} VALUES (30)")
    assert sorted(spark.table(name).collect()) == [(20,), (30,)]
    entries = _current_manifest_entries(path, ManifestContent.DATA)
    assert sum(entry.data_file.record_count for entry in entries) == 2  # noqa: PLR2004
    assert all(tuple(entry.data_file.partition) == (None,) for entry in entries)
    assert all(p.read_bytes() == contents for p, contents in old_files.items())


@pytest.mark.parametrize("spelling", ["file-uri", "trailing-slash", "different"])
@pytest.mark.parametrize("api", ["sql", "replace"])
def test_replacement_compares_location_identity(spark, target, spelling, api):
    name, path = target
    spark.sql(f"CREATE TABLE {name} (a INT) USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'")
    spark.sql(f"INSERT INTO {name} VALUES (1)")
    requested = {
        "file-uri": path.as_uri() + "/",
        "trailing-slash": str(path) + "/",
        "different": str(path) + "-other",
    }[spelling]
    before = {p: p.read_bytes() for p in path.rglob("*") if p.is_file()}
    registered_location = next(
        row.data_type for row in spark.sql(f"DESCRIBE EXTENDED {name}").collect() if row.col_name == "Location"
    )

    def replace():
        if api == "sql":
            spark.sql(
                f"REPLACE TABLE {name} USING iceberg LOCATION '{escape_sql_string_literal(requested)}' AS SELECT 2 AS a"
            ).collect()
        else:
            spark.createDataFrame([(2,)], "a INT").writeTo(name).using("iceberg").option("path", requested).replace()

    if spelling == "different":
        with pytest.raises(PySparkException, match="relocating"):
            replace()
        assert spark.table(name).collect() == [(1,)]
        assert {p: p.read_bytes() for p in path.rglob("*") if p.is_file()} == before
        assert not path.with_name(path.name + "-other").exists()
    else:
        assert spark.read.format("iceberg").load(requested).collect() == [(1,)]
        replace()
        assert spark.table(name).collect() == [(2,)]
        assert spark.read.format("iceberg").load(requested).collect() == [(2,)]
        assert all(p.read_bytes() == contents for p, contents in before.items() if p.name.endswith(".metadata.json"))
    assert (
        next(row.data_type for row in spark.sql(f"DESCRIBE EXTENDED {name}").collect() if row.col_name == "Location")
        == registered_location
    )


@pytest.mark.parametrize("mutation", ["append", "recreate"])
def test_path_replacement_conflict_requires_replanning(spark, target, tmp_path, mutation):
    name, path = target
    spark.sql(f"CREATE TABLE {name} (old INT) USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'")
    spark.sql(f"INSERT INTO {name} VALUES (1)")
    ready = str(tmp_path / "writer-ready")
    release = str(tmp_path / "writer-release")

    @sf.udf("int")
    def blocked_source(value):
        import time
        from pathlib import Path

        Path(ready).touch()
        deadline = time.monotonic() + 30
        while not Path(release).exists():
            if time.monotonic() >= deadline:
                message = "replacement source barrier timed out"
                raise RuntimeError(message)
            time.sleep(0.01)
        return value

    source = spark.range(1, numPartitions=1).select(blocked_source(sf.lit(7)).alias("replacement"))
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(lambda: source.writeTo(name).using("iceberg").replace())
        try:
            deadline = time.monotonic() + 30
            while not (tmp_path / "writer-ready").exists() and time.monotonic() < deadline:
                if future.done():
                    future.result()
                time.sleep(0.01)
            assert (tmp_path / "writer-ready").exists()
            winner_path = path
            if mutation == "append":
                spark.sql(f"INSERT INTO {name} VALUES (2)")
            else:
                spark.sql(f"DROP TABLE {name}")
                winner_path = tmp_path / "recreated"
                spark.sql(
                    f"CREATE TABLE {name} (old INT) USING iceberg "
                    f"LOCATION '{escape_sql_string_literal(str(winner_path))}'"
                )
                spark.sql(f"INSERT INTO {name} VALUES (9)")
            winner = _find_latest_metadata(winner_path)
            original_location = _find_latest_metadata(path)
            expected = sorted(spark.table(name).collect())
        finally:
            (tmp_path / "writer-release").touch()
        with pytest.raises(AnalysisException, match="commit requires replanning"):
            future.result(timeout=30)
    assert _find_latest_metadata(winner_path) == winner
    assert _find_latest_metadata(path) == original_location
    assert sorted(spark.table(name).collect()) == expected
    assert spark.table(name).columns == ["old"]
