# SPDX-License-Identifier: Apache-2.0

"""Defaults shared by Unity Catalog test fixtures."""

import os

DEFAULT_CATALOG = "sail_test_catalog"
# The default server image does not have the Unity Catalog Delta REST API, so the
# tests cover the API that Sail uses with such servers. Set
# PYSAIL_UNITY_CATALOG_IMAGE to a server image that has the Delta REST API
# (v0.7.0 or later) to cover the other API. CI runs the tests with both images.
UNITY_CATALOG_IMAGE = os.environ.get(
    "PYSAIL_UNITY_CATALOG_IMAGE",
    "unitycatalog/unitycatalog:v0.4.0",
)
