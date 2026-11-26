# SPDX-License-Identifier: Apache-2.0

# ruff: noqa: S608

"""Helpers for the tests of temporary views over catalog tables."""

from __future__ import annotations

import uuid

import pytest

LAKE_FORMATS = ["delta", "iceberg"]
FORMATS = [*LAKE_FORMATS, "parquet"]
# The kinds of temporary views: SQL, SQL with a column list, global SQL, DataFrame, and global DataFrame.
KINDS = ["sv", "svc", "gv", "dv", "dgv"]
# The kinds whose stored plan refers to the table by its columns, as a Spark DataFrame view does.
PLAN_KINDS = ["gv", "dv", "dgv"]
# The kinds of views defined by a SQL query, which resolve the query again when their plans no longer
# fit their tables, as Spark analyzes the query of such a view at each read.
SQL_KINDS = ["sv", "svc", "gv"]
# The query of each view of a SQL kind and its columns, by the name that reads the view.
SQL_VIEWS = {}

COLUMNS_CHANGED = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION"
NOT_FOUND = "TABLE_OR_VIEW_NOT_FOUND"


def create_view(spark, kind, query, frame=None):
    """Creates a temporary view of the kind over the query, or over `frame` for a DataFrame view if given,
    and returns the name to read the view by."""
    view = f"{kind}_{uuid.uuid4().hex}"
    if kind == "sv":
        spark.sql(f"CREATE TEMPORARY VIEW {view} AS {query}")
    elif kind == "svc":
        columns = ", ".join(spark.sql(query).columns)
        spark.sql(f"CREATE TEMPORARY VIEW {view} ({columns}) AS {query}")
    elif kind == "gv":
        spark.sql(f"CREATE GLOBAL TEMPORARY VIEW {view} AS {query}")
    elif kind == "dv":
        (spark.sql(query) if frame is None else frame).createOrReplaceTempView(view)
    elif kind == "dgv":
        (spark.sql(query) if frame is None else frame).createOrReplaceGlobalTempView(view)
    else:
        raise ValueError(kind)
    name = f"global_temp.{view}" if kind in ("gv", "dgv") else view
    if kind in SQL_KINDS:
        SQL_VIEWS[name] = (query, spark.sql(query).columns)
    return name


def drop_view(spark, view):
    if view.startswith("global_temp."):
        spark.catalog.dropGlobalTempView(view.removeprefix("global_temp."))
    else:
        spark.catalog.dropTempView(view)


class Table:
    """A catalog table with the column `id`, and the temporary views over it."""

    def __init__(self, spark, tmp_path, fmt, columns="id BIGINT", properties=""):
        self.spark = spark
        self.tmp_path = tmp_path
        self.fmt = fmt
        self.name = f"t_{uuid.uuid4().hex}"
        self.locations = 0
        self.views = []
        self.create(columns, properties=properties)

    def create(self, columns, fmt=None, properties=""):
        """Creates the table with the columns, and the clause of the table properties if given."""
        self.locations += 1
        location = self.tmp_path / f"{self.name}_{self.locations}"
        self.spark.sql(
            f"CREATE TABLE {self.name} ({columns}) USING {fmt or self.fmt} LOCATION '{location}'{properties}"
        )

    def recreate(self, columns, values=None, fmt=None, properties=""):
        """Drops the table and creates it again at a new location, with the rows if given."""
        self.spark.sql(f"DROP TABLE {self.name}")
        self.create(columns, fmt, properties)
        if values is not None:
            self.insert(values)

    def insert(self, values):
        self.spark.sql(f"INSERT INTO {self.name} VALUES {values}")

    def create_view(self, kind, query=None, frame=None):
        """Creates a view of the kind over the query, which reads all the columns of the table by default,
        or over `frame` for a DataFrame view if given."""
        view = create_view(self.spark, kind, query or f"SELECT * FROM {self.name}", frame)
        self.views.append(view)
        return view

    def drop(self):
        for view in self.views:
            drop_view(self.spark, view)
        self.spark.sql(f"DROP TABLE IF EXISTS {self.name}")
        self.spark.sql(f"DROP VIEW IF EXISTS {self.name}")


def rows(spark, sql):
    return sorted((tuple(row) for row in spark.sql(sql).collect()), key=lambda row: [(x is None, x or 0) for x in row])


def error(spark, sql):
    with pytest.raises(Exception) as info:  # noqa: PT011
        spark.sql(sql).collect()
    return str(info.value)
