"""Backward-compatible re-export. The implementation has moved to
:mod:`pysail.spark.datasource.jdbc.datasource`.
"""

from pysail.spark.datasource.jdbc.datasource import (
    JdbcDataSource,
    JdbcDataSourceReader,
    JdbcInputPartition,
    _apply_custom_schema,
    _filter_to_sql,
    _jdbc_url_to_dsn,
    _parse_custom_schema,
    _quote_identifier,
)

__all__ = [
    "JdbcDataSource",
    "JdbcDataSourceReader",
    "JdbcInputPartition",
    "_apply_custom_schema",
    "_filter_to_sql",
    "_jdbc_url_to_dsn",
    "_parse_custom_schema",
    "_quote_identifier",
]
