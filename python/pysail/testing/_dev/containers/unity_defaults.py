# SPDX-License-Identifier: Apache-2.0

"""Defaults shared by Unity Catalog test fixtures."""

from dataclasses import dataclass

DEFAULT_CATALOG = "sail_test_catalog"


@dataclass(frozen=True)
class UnityCatalogServer:
    """A Unity Catalog server image that the tests run against."""

    id: str
    image: str
    # Whether the server has the Unity Catalog Delta REST API, which Sail then
    # uses for managed Delta tables instead of the legacy API.
    delta_api: bool


UNITY_CATALOG_SERVERS = (
    UnityCatalogServer("uc-0.7.0", "unitycatalog/unitycatalog:v0.7.0", delta_api=True),
    UnityCatalogServer("uc-0.4.0", "unitycatalog/unitycatalog:v0.4.0", delta_api=False),
)
