"""Iceberg-specific BDD step definitions."""

# Import step definitions to register them with pytest-bdd
from pysail.tests.spark.steps.iceberg import metadata

__all__ = ["metadata"]
