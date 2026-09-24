"""Verify the immutable artifacts that define the Jev integration contract."""

import hashlib
import json
from pathlib import Path

import pytest


@pytest.fixture(scope="module", autouse=True)
def spark_doctest():
    """Contract artifacts can be verified offline without starting a Spark server."""


def test_reference_artifact_hashes():
    references = Path(__file__).parent / "references"
    manifest = json.loads((references / "manifest.json").read_text())
    assert len(manifest["python_sdk_commit"]) == 40
    for artifact in manifest["artifacts"]:
        assert hashlib.sha256((references / artifact["path"]).read_bytes()).hexdigest() == artifact["sha256"]


def test_documented_provider_surface_is_fully_accounted_for():
    schema = json.loads((Path(__file__).parent / "references" / "openapi.json").read_text())
    assert set(schema["paths"]) == {"/v1/systemone", "/v1/models"}
    answer = schema["components"]["schemas"]["Answer"]
    assert set(answer["discriminator"]["mapping"]) == {"noul", "choice", "score"}
