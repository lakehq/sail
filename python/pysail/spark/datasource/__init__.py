"""Optional PySpark data sources bundled with Sail.

Each data source depends on an optional third-party client (see
``pyproject.toml`` extras: ``jdbc``, ``kafka``, ``vortex``). To keep importing
this package cheap and free of hard dependencies, the classes are loaded
lazily via ``__getattr__``. Missing extras only fail when the class is
actually referenced.

Usage::

    from pysail.spark.datasource import KafkaDataSource

    spark.dataSource.register(KafkaDataSource)
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pysail.spark.datasource.jdbc import JdbcDataSource
    from pysail.spark.datasource.kafka import KafkaDataSource
    from pysail.spark.datasource.vortex import VortexDataSource

__all__ = ["JdbcDataSource", "KafkaDataSource", "VortexDataSource"]

_LAZY = {
    "JdbcDataSource": ("pysail.spark.datasource.jdbc", "JdbcDataSource"),
    "KafkaDataSource": ("pysail.spark.datasource.kafka", "KafkaDataSource"),
    "VortexDataSource": ("pysail.spark.datasource.vortex", "VortexDataSource"),
}


def __getattr__(name: str):
    target = _LAZY.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module = importlib.import_module(target[0])
    return getattr(module, target[1])
