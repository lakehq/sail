from __future__ import annotations

import contextlib
import uuid
from typing import TYPE_CHECKING

import grpc
import pytest

from pysail.testing.spark.session import spark_session_factory
from pysail.testing.spark.utils.common import pyspark_version

PARENT_VIEW_ROWS = 2
CLONE_VIEW_ROWS = 3

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pyspark.sql import SparkSession

pytestmark = pytest.mark.skipif(
    pyspark_version() < (4, 2),
    reason="cloneSession requires PySpark 4.2+",
)


@pytest.fixture
def clone_context(remote) -> Iterator[tuple[SparkSession, list[SparkSession]]]:
    with spark_session_factory(remote) as factory:
        source = factory.create()
        clones = []
        yield source, clones
        for clone in clones:
            with contextlib.suppress(Exception):
                clone.release_session_on_close = True
                clone.stop()


def test_clone_copies_state_then_mutates_independently(clone_context) -> None:
    source, clones = clone_context
    source.conf.set("spark.test.clone", "source")
    source.sql("CREATE OR REPLACE TEMP VIEW clone_view AS SELECT 1 AS id")
    clone = source.cloneSession()
    clones.append(clone)

    _ = uuid.UUID(clone.session_id)
    assert clone.session_id != source.session_id
    assert clone.conf.get("spark.test.clone") == "source"
    assert clone.table("clone_view").count() == 1

    source.conf.set("spark.test.clone", "parent")
    clone.conf.set("spark.test.clone", "clone")
    source.sql("CREATE OR REPLACE TEMP VIEW clone_view AS SELECT * FROM VALUES (1), (2)")
    clone.sql("CREATE OR REPLACE TEMP VIEW clone_view AS SELECT * FROM VALUES (1), (2), (3)")
    assert source.conf.get("spark.test.clone") == "parent"
    assert clone.conf.get("spark.test.clone") == "clone"
    assert source.table("clone_view").count() == PARENT_VIEW_ROWS
    assert clone.table("clone_view").count() == CLONE_VIEW_ROWS

    clone.release_session_on_close = True
    clone.stop()
    clones.remove(clone)
    assert source.range(1).count() == 1


def test_clone_accepts_explicit_target_and_survives_source_release(clone_context) -> None:
    source, clones = clone_context
    target = str(uuid.uuid4())
    clone = source.cloneSession(target)
    clones.append(clone)

    assert clone.session_id == target
    source.stop()
    assert clone.range(1).count() == 1


def test_clone_rejects_invalid_target(clone_context) -> None:
    source, _ = clone_context
    with pytest.raises(grpc.RpcError, match="target session ID must be a UUID"):
        source.cloneSession("invalid")
    assert source.range(1).count() == 1
