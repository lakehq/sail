from __future__ import annotations

import pytest
from pytest_bdd import scenarios

from pysail.tests.spark.utils import is_jvm_spark


scenarios("features")
