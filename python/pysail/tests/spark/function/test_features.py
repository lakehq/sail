from __future__ import annotations

import os

import pytest
from pytest_bdd import scenarios

from pysail.tests.spark.utils import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail only")


@pytest.fixture(scope="session")
def remote():
    if remote := os.environ.get("SPARK_REMOTE"):
        return remote
    port = os.environ.get("SPARK_TESTING_REMOTE_PORT", "50051")
    return f"sc://localhost:{port}"


scenarios("features")
