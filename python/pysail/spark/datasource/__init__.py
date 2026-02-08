"""Python DataSource discovery for Sail."""

from pysail.spark.datasource.registry import discover_entry_points

__all__ = ["discover_entry_points"]
