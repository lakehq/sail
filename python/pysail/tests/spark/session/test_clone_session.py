from __future__ import annotations

import contextlib
import uuid
from typing import TYPE_CHECKING, TypedDict

import grpc
import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from pysail.testing.spark.session import spark_session_factory

PARENT_VIEW_ROWS = 2
CLONE_VIEW_ROWS = 3

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pyspark.sql import SparkSession

scenarios("features/clone_session.feature")


class CloneState(TypedDict, total=False):
    source: SparkSession
    clone: SparkSession
    target: str
    error: Exception


@pytest.fixture
def clone_sessions(remote) -> Iterator[CloneState]:
    with spark_session_factory(remote) as factory:
        state = CloneState(source=factory.create())
        yield state
        if (clone := state.get("clone")) is not None:
            with contextlib.suppress(Exception):
                clone.release_session_on_close = True
                clone.stop()


@given("a running source session")
def running_source_session(clone_sessions) -> None:
    assert clone_sessions["source"].range(1).count() == 1


@given("a source session with configuration and a temporary view")
def source_session_with_state(clone_sessions) -> None:
    source = clone_sessions["source"]
    source.conf.set("spark.test.clone", "source")
    source.sql("CREATE OR REPLACE TEMP VIEW clone_view AS SELECT 1 AS id")


@given("a source session and its clone")
def source_session_and_clone(clone_sessions) -> None:
    running_source_session(clone_sessions)
    clone_sessions["clone"] = clone_sessions["source"].cloneSession()


@when("the client clones the source session")
def clone_source_session(clone_sessions) -> None:
    clone_sessions["clone"] = clone_sessions["source"].cloneSession()


@when("the client clones it with a valid target UUID")
def clone_with_target_id(clone_sessions) -> None:
    target = str(uuid.uuid4())
    clone_sessions["target"] = target
    clone_sessions["clone"] = clone_sessions["source"].cloneSession(target)


@when("the client clones it with an invalid target UUID")
def clone_with_invalid_target_id(clone_sessions) -> None:
    with pytest.raises(grpc.RpcError) as error:
        clone_sessions["source"].cloneSession("invalid")
    clone_sessions["error"] = error.value


@when(parsers.parse("the client releases the {released} session"))
def release_session(clone_sessions, released: str) -> None:
    session = clone_sessions[released]
    if released == "clone":
        session.release_session_on_close = True
    session.stop()


@then("the clone has a different valid session UUID")
def clone_has_generated_uuid(clone_sessions) -> None:
    source = clone_sessions["source"]
    clone = clone_sessions["clone"]
    _ = uuid.UUID(clone.session_id)
    assert clone.session_id != source.session_id


@then("the clone inherits the configuration and temporary view")
def clone_inherits_state(clone_sessions) -> None:
    clone = clone_sessions["clone"]
    assert clone.conf.get("spark.test.clone") == "source"
    assert clone.table("clone_view").count() == 1


@then("later configuration and temporary-view changes remain isolated")
def cloned_state_remains_isolated(clone_sessions) -> None:
    source = clone_sessions["source"]
    clone = clone_sessions["clone"]
    source.conf.set("spark.test.clone", "parent")
    clone.conf.set("spark.test.clone", "clone")
    source.sql("CREATE OR REPLACE TEMP VIEW clone_view AS SELECT * FROM VALUES (1), (2)")
    clone.sql("CREATE OR REPLACE TEMP VIEW clone_view AS SELECT * FROM VALUES (1), (2), (3)")
    assert source.conf.get("spark.test.clone") == "parent"
    assert clone.conf.get("spark.test.clone") == "clone"
    assert source.table("clone_view").count() == PARENT_VIEW_ROWS
    assert clone.table("clone_view").count() == CLONE_VIEW_ROWS


@then("the clone uses that target UUID")
def clone_uses_target_id(clone_sessions) -> None:
    assert clone_sessions["clone"].session_id == clone_sessions["target"]


@then("the clone request fails and the source remains usable")
def invalid_clone_fails(clone_sessions) -> None:
    assert "target session ID must be a UUID" in str(clone_sessions["error"])
    assert clone_sessions["source"].range(1).count() == 1


@then(parsers.parse("the {remaining} session can execute a query"))
def remaining_session_is_usable(clone_sessions, remaining: str) -> None:
    assert clone_sessions[remaining].range(1).count() == 1
