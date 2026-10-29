import json

import pytest

from pysail.testing.spark.steps.delta import _delta_log_compute
from pysail.testing.spark.steps.sql import PathWrapper


@pytest.mark.parametrize(
    "which",
    ["latest commit info", "latest effective protocol and metadata", "first commit protocol and metadata"],
)
def test_delta_log_assertions_observe_new_commits(tmp_path, which):
    log = tmp_path / "_delta_log"
    log.mkdir()
    variables = {"location": PathWrapper(tmp_path)}
    cache = {}

    def commit(version, watermark):
        schema = {
            "type": "struct",
            "fields": [
                {
                    "name": "id",
                    "type": "long",
                    "nullable": True,
                    "metadata": {"delta.identity.highWaterMark": watermark},
                }
            ],
        }
        actions = [
            {"metaData": {"schemaString": json.dumps(schema)}},
            {"commitInfo": {"operation": "WRITE", "operationParameters": {"watermark": str(watermark)}}},
        ]
        if version == 0:
            actions.insert(0, {"protocol": {"minReaderVersion": 1, "minWriterVersion": 6}})
        (log / f"{version:020}.json").write_text("\n".join(json.dumps(action) for action in actions))

    def value(obj):
        if which == "latest commit info":
            return obj["operationParameters"]["watermark"]
        return obj["metaData"]["schemaString"]["fields"][0]["metadata"]["delta.identity.highWaterMark"]

    commit(0, 2)
    first = _delta_log_compute(which, variables, cache)
    assert value(first) == 2
    commit(1, 3)
    latest = _delta_log_compute(which, variables, cache)
    if which == "first commit protocol and metadata":
        assert latest is first
        assert value(latest) == 2
        assert cache == {which: first}
    else:
        assert value(latest) == 3
        assert value(first) == 2
        assert cache == {}
