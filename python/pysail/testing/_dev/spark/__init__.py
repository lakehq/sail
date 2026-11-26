# SPDX-License-Identifier: Apache-2.0

import pytest

# Test modules can import step helpers before the step plugins are registered.
pytest.register_assert_rewrite("pysail.testing._dev.spark.steps")
