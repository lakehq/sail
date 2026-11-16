"""Unity Catalog object names that need quoting in SQL.

This module changes the current database, so it uses its own module-scoped Spark session.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import quote

import requests

from pysail.testing._dev.containers.unity_defaults import DEFAULT_CATALOG

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def test_set_current_database_with_dashes(spark: SparkSession, unity_rest_url: str) -> None:
    # Create the schema outside Sail, like a schema that already exists in the catalog.
    name = "dash-current-db-unity"
    response = requests.post(
        f"{unity_rest_url}/schemas",
        json={"catalog_name": DEFAULT_CATALOG, "name": name},
        timeout=10,
    )
    response.raise_for_status()
    try:
        spark.catalog.setCurrentDatabase(f"`{name}`")
        assert spark.catalog.currentDatabase() == f"`{name}`"

        spark.sql(f"USE SCHEMA {DEFAULT_CATALOG}.`{name}`")
        assert spark.sql("SELECT current_database()").collect()[0][0] == f"{DEFAULT_CATALOG}.`{name}`"
    finally:
        requests.delete(
            f"{unity_rest_url}/schemas/{quote(f'{DEFAULT_CATALOG}.{name}', safe='')}",
            timeout=10,
        ).raise_for_status()
