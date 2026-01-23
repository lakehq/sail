"""BDD tests for Iceberg integration."""

from pytest_bdd import scenarios

from pysail.tests.spark.steps import iceberg  # noqa: F401

scenarios("features")
