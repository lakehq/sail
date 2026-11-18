import hashlib
import json
import os
import shutil
from pathlib import Path

import pytest

from pysail.testing._dev.spark.steps.iceberg import _find_latest_metadata
from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests._dev.spark.iceberg.test_iceberg_expire_snapshots import expire


def pytest_generate_tests(metafunc):
    if "reference_case" not in metafunc.fixturenames:
        return
    root = os.environ.get("ICEBERG_EXPIRATION_REFERENCE")
    if not root:
        metafunc.parametrize(
            "reference_case",
            [pytest.param(None, marks=pytest.mark.skip(reason="ICEBERG_EXPIRATION_REFERENCE is not configured"))],
        )
        return
    cases = json.loads((Path(root) / "cases.json").read_text())
    assert cases, "Reference fixture manifest must not be empty"
    metafunc.parametrize("reference_case", cases, ids=[case["name"] for case in cases])


def _files(path):
    return {
        str(file.relative_to(path)): hashlib.sha256(file.read_bytes()).hexdigest()
        for file in path.rglob("*")
        if file.is_file() and not file.name.startswith(".") and file.name != "version-hint.text"
    }


def _assert_state(path, expected):
    metadata = _find_latest_metadata(path)
    assert [s["snapshot-id"] for s in metadata["snapshots"]] == expected["snapshots"]
    assert metadata["current-snapshot-id"] == expected["current"]
    assert {name: ref["snapshot-id"] for name, ref in metadata["refs"].items()} == expected["refs"]


def _assert_snapshot_rows(spark, path, expected):
    for snapshot_id, values in expected.items():
        frame = spark.read.format("iceberg").option("snapshot-id", snapshot_id).load(str(path))
        assert sorted(row.value for row in frame.selectExpr("id + 100 AS value").collect()) == values


@pytest.mark.integration
def test_expiration_matches_official_file_inventory(spark, reference_case):
    root = Path(os.environ["ICEBERG_EXPIRATION_REFERENCE"]).resolve()
    case = reference_case
    path = Path(case["path"])
    backup = Path(case["backup"])
    # Only replace the generated table belonging to this exact reference case.
    assert path.resolve() == root / "warehouse" / "default" / case["name"]
    assert backup.resolve() == root / "backups" / case["name"]
    assert (backup / "metadata" / "version-hint.text").is_file()
    shutil.rmtree(path)
    shutil.copytree(backup, path)
    name = f"expiration_interop_{case['name']}"
    spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'")
    try:
        _assert_state(path, case["initial_state"])
        _assert_snapshot_rows(spark, path, case["initial_rows"])
        for step in case["steps"]:
            if step.get("before"):
                spark.sql(f"ALTER TABLE {name} {step['before']}").collect()
            before = _files(path)
            counts = expire(spark, name, step["args"])
            after = _files(path)
            assert list(counts) == step["counts"]
            assert sorted(before.keys() - after.keys()) == step["removed"]
            preserved = {file: digest for file, digest in before.items() if file not in step["removed"]}
            assert {file: after[file] for file in preserved} == preserved
            # Each engine publishes its own metadata JSON (timestamps and logs differ).
            # Data, manifests, and Puffin files must match the reference byte for byte.
            reference_files = {
                file: digest for file, digest in step["preserved"].items() if not file.endswith(".metadata.json")
            }
            assert {file: after[file] for file in reference_files} == reference_files
            _assert_state(path, step["state"])
            _assert_snapshot_rows(spark, path, step["rows"])
        assert (
            sorted(row.value for row in spark.table(name).selectExpr("id + 100 AS value").collect())
            == (case["steps"][-1]["rows"][str(case["steps"][-1]["state"]["current"])])
        )
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
