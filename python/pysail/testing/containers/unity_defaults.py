"""Defaults shared by Unity Catalog test fixtures."""

import os

DEFAULT_CATALOG = "sail_test_catalog"
# GitHub has a v0.4.1 release, but Docker Hub currently publishes versioned
# server images only up to v0.4.0. Keep this on an existing tag so the black-box
# integration suite is runnable; override PYSAIL_UNITY_CATALOG_IMAGE when a
# newer server image is published.
UNITY_CATALOG_IMAGE = os.environ.get(
    "PYSAIL_UNITY_CATALOG_IMAGE",
    "unitycatalog/unitycatalog:v0.4.0",
)
