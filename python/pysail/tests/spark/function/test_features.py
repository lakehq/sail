"""BDD tests for DataFrame operations using pytest-bdd."""

from __future__ import annotations

import pytest
from pytest_bdd import scenarios

from pysail.tests.spark.utils import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail only")

scenarios("features")
