# SPDX-License-Identifier: Apache-2.0

"""Defaults shared by Unity Catalog test fixtures."""

import os

DEFAULT_CATALOG = "sail_test_catalog"
# Override PYSAIL_UNITY_CATALOG_IMAGE to run the suite against another server version.
UNITY_CATALOG_IMAGE = os.environ.get(
    "PYSAIL_UNITY_CATALOG_IMAGE",
    "unitycatalog/unitycatalog:v0.7.0",
)
