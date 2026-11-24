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

COLUMNS_CHANGED = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION"
NOT_FOUND = "TABLE_OR_VIEW_NOT_FOUND"


class Table:
    """A catalog table with the column `id`, and the temporary views of each kind over it."""

    def __init__(self, spark, tmp_path, fmt, columns="id BIGINT"):
        self.spark = spark
        self.tmp_path = tmp_path
        self.fmt = fmt
        self.name = f"t_{uuid.uuid4().hex}"
        self.locations = 0
        self.views = {}
        self.create(columns)

    def create(self, columns, fmt=None):
        self.locations += 1
        location = self.tmp_path / f"{self.name}_{self.locations}"
        self.spark.sql(f"CREATE TABLE {self.name} ({columns}) USING {fmt or self.fmt} LOCATION '{location}'")

    def insert(self, values):
        self.spark.sql(f"INSERT INTO {self.name} VALUES {values}")

    def create_view(self, kind, query=None):
        """Creates a view of the kind over the query, which reads all the columns of the table by default."""
        view = f"{kind}_{uuid.uuid4().hex}"
        query = query or f"SELECT * FROM {self.name}"
        if kind == "sv":
            self.spark.sql(f"CREATE TEMPORARY VIEW {view} AS {query}")
        elif kind == "svc":
            columns = ", ".join(self.spark.sql(query).columns)
            self.spark.sql(f"CREATE TEMPORARY VIEW {view} ({columns}) AS {query}")
        elif kind == "gv":
            self.spark.sql(f"CREATE GLOBAL TEMPORARY VIEW {view} AS {query}")
        elif kind == "dv":
            self.spark.sql(query).createOrReplaceTempView(view)
        elif kind == "dgv":
            self.spark.sql(query).createOrReplaceGlobalTempView(view)
        else:
            raise ValueError(kind)
        self.views[kind] = f"global_temp.{view}" if kind in ("gv", "dgv") else view
        return self.views[kind]

    def drop(self):
        for kind, view in self.views.items():
            if kind in ("gv", "dgv"):
                self.spark.catalog.dropGlobalTempView(view.removeprefix("global_temp."))
            else:
                self.spark.catalog.dropTempView(view)
        self.spark.sql(f"DROP TABLE IF EXISTS {self.name}")
        self.spark.sql(f"DROP VIEW IF EXISTS {self.name}")


def rows(spark, sql):
    return sorted((tuple(row) for row in spark.sql(sql).collect()), key=lambda row: [(x is None, x or 0) for x in row])


def error(spark, sql):
    with pytest.raises(Exception) as info:  # noqa: PT011
        spark.sql(sql).collect()
    return str(info.value)
