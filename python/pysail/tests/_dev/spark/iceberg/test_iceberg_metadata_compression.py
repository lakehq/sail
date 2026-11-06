# ruff: noqa: S608
# Historical suffix coverage follows Iceberg 1.11 TestHadoopCommits;
# the physical one-publication lag is a Sail path-table policy.
import gzip
import json

import pytest

from pysail.testing.spark.utils.sql import escape_sql_string_literal

CODEC = "write.metadata.compression-codec"


@pytest.fixture
def target(spark, tmp_path):
    name = "iceberg_metadata_compression"
    path = tmp_path / "table"
    try:
        yield name, path
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}").collect()


def publication(path, version, encoding, requested, previous):
    suffix = ".gz.metadata.json" if encoding == "gzip" else ".metadata.json"
    files = list((path / "metadata").glob(f"v{version}.*"))
    assert [p.name for p in files] == [f"v{version}{suffix}"]
    assert all(p.read_bytes() == data for p, data in previous.items())
    data = files[0].read_bytes()
    assert data.startswith(b"\x1f\x8b") == (encoding == "gzip")
    metadata = json.loads(gzip.decompress(data) if encoding == "gzip" else data)
    assert metadata["properties"][CODEC] == requested
    previous[files[0]] = data
    return metadata


@pytest.mark.parametrize("codec", ["none", "gzip"])
@pytest.mark.parametrize("api", ["empty", "ctas", "writer-create", "create-or-replace"])
def test_initial_publication_is_uncompressed(spark, target, codec, api):
    name, path = target
    location = escape_sql_string_literal(str(path))
    if api == "empty":
        spark.sql(
            f"CREATE TABLE {name} (id BIGINT) USING iceberg LOCATION '{location}' TBLPROPERTIES ('{CODEC}'='{codec}')"
        ).collect()
    elif api == "ctas":
        spark.sql(
            f"CREATE TABLE {name} USING iceberg LOCATION '{location}' "
            f"TBLPROPERTIES ('{CODEC}'='{codec}') AS SELECT 1L AS id"
        ).collect()
    elif api == "create-or-replace":
        spark.createDataFrame([(1,)], "id BIGINT").writeTo(name).using("iceberg").option(
            "path", str(path)
        ).tableProperty(CODEC, codec).createOrReplace()
    else:
        spark.createDataFrame([(1,)], "id BIGINT").writeTo(name).using("iceberg").option(
            "path", str(path)
        ).tableProperty(CODEC, codec).create()
    previous = {}
    first = publication(path, 1, "none", codec, previous)
    expected = [] if api == "empty" else [(1,)]
    assert spark.read.format("iceberg").load(str(path)).collect() == expected
    spark.createDataFrame([(2,)], "id BIGINT").write.format("iceberg").mode("append").save(str(path))
    second = publication(path, 2, codec, codec, previous)
    assert second["table-uuid"] == first["table-uuid"]
    assert sorted(spark.read.format("iceberg").load(str(path)).collect()) == [*expected, (2,)]


@pytest.mark.parametrize(
    ("base_codec", "requested"), [("none", "gzip"), ("gzip", "none"), ("none", "none"), ("gzip", "gzip")]
)
@pytest.mark.parametrize("operation", ["alter", "replace", "empty-replace"])
def test_codec_change_takes_effect_on_the_following_publication(spark, target, base_codec, requested, operation):
    name, path = target
    location = escape_sql_string_literal(str(path))
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT) USING iceberg LOCATION '{location}' TBLPROPERTIES ('{CODEC}'='{base_codec}')"
    ).collect()
    previous = {}
    publication(path, 1, "none", base_codec, previous)
    spark.sql(f"INSERT INTO {name} VALUES (1)").collect()
    before = publication(path, 2, base_codec, base_codec, previous)
    if operation == "alter":
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('{CODEC}'='{requested}')").collect()
        expected, column = [(1,)], "id"
    elif operation == "replace":
        spark.sql(
            f"REPLACE TABLE {name} USING iceberg TBLPROPERTIES ('{CODEC}'='{requested}') AS SELECT 9L AS replacement"
        ).collect()
        expected, column = [(9,)], "replacement"
    else:
        spark.sql(
            f"REPLACE TABLE {name} (replacement BIGINT) USING iceberg LOCATION '{location}' "
            f"TBLPROPERTIES ('{CODEC}'='{requested}')"
        ).collect()
        expected, column = [], "replacement"
    changed = publication(path, 3, base_codec, requested, previous)
    assert changed["table-uuid"] == before["table-uuid"]
    frame = spark.read.format("iceberg").load(str(path))
    assert frame.columns == [column]
    assert frame.collect() == expected
    # The hint must not hide the property-changing publication.
    (path / "metadata" / "version-hint.text").write_text("1", encoding="utf-8")
    spark.createDataFrame([(10,)], f"{column} BIGINT").write.format("iceberg").mode("append").save(str(path))
    after = publication(path, 4, requested, requested, previous)
    assert sorted(spark.read.format("iceberg").load(str(path)).collect()) == [*expected, (10,)]
    assert spark.read.format("iceberg").option("snapshot-id", str(before["current-snapshot-id"])).load(
        str(path)
    ).collect() == [(1,)]
    if operation == "alter":
        spark.sql(
            f"CALL system.set_current_snapshot(table => '{name}', snapshot_id => {before['current-snapshot-id']})"
        ).collect()
        rolled_back = publication(path, 5, requested, requested, previous)
        assert rolled_back["current-snapshot-id"] == before["current-snapshot-id"]
        assert rolled_back["current-snapshot-id"] != after["current-snapshot-id"]
        assert spark.read.format("iceberg").load(str(path)).collect() == [(1,)]


@pytest.mark.parametrize("suffix", [".gz.metadata.json", ".metadata.json.gz"])
@pytest.mark.parametrize("base_codec", ["none", "gzip"])
def test_historical_gzip_encoding_does_not_choose_next_codec(spark, target, suffix, base_codec):
    name, path = target
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT) USING iceberg LOCATION '{escape_sql_string_literal(str(path))}' "
        f"TBLPROPERTIES ('{CODEC}'='{base_codec}')"
    ).collect()
    initial = path / "metadata" / "v1.metadata.json"
    compressed = path / "metadata" / f"v1{suffix}"
    compressed.write_bytes(gzip.compress(initial.read_bytes()))
    initial.unlink()
    previous = {compressed: compressed.read_bytes()}
    assert spark.read.format("iceberg").load(str(path)).collect() == []
    spark.createDataFrame([(3,)], "id BIGINT").write.format("iceberg").mode("append").save(str(path))
    publication(path, 2, base_codec, base_codec, previous)
    assert spark.read.format("iceberg").load(str(path)).collect() == [(3,)]


def test_ambiguous_versions_reject_reads_and_writes_without_cleanup(spark, target):
    name, path = target
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT) USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'"
    ).collect()
    original = path / "metadata" / "v1.metadata.json"
    (path / "metadata" / "v1.metadata.json.gz").write_bytes(gzip.compress(original.read_bytes()))
    before = {p: p.read_bytes() for p in path.rglob("*") if p.is_file()}
    with pytest.raises(Exception, match="explicit recovery required"):
        spark.read.format("iceberg").load(str(path)).collect()
    with pytest.raises(Exception, match="explicit recovery required"):
        spark.createDataFrame([(1,)], "id BIGINT").write.format("iceberg").mode("append").save(str(path))
    assert {p: p.read_bytes() for p in path.rglob("*") if p.is_file()} == before
