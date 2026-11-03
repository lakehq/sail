import json
import re
import shutil
import time
import uuid
from pathlib import Path

import pytest

from pysail.testing.spark.utils.common import pyspark_version
from pysail.testing.spark.utils.sql import escape_sql_string_literal

SCALAR_SUBQUERY_SLOTS = re.compile(r"ScalarSubqueryExec: subqueries=(\d+)")


def scalar_subquery_slots(df):
    return [int(n) for n in SCALAR_SUBQUERY_SLOTS.findall(df._explain_string())]  # noqa: SLF001


@pytest.fixture
def versioned_table(spark, tmp_path):
    """A Delta table whose row for 2000-01-08 has w = 2 at version 1 and w = 3 at version 2."""
    name = f"delta_query_snapshot_{uuid.uuid4().hex[:8]}"
    location = escape_sql_string_literal(str(tmp_path / name))
    spark.sql(f"CREATE TABLE {name} (d DATE, w INT) USING DELTA LOCATION '{location}'")
    spark.sql(f"INSERT INTO {name} VALUES (DATE'2000-01-01', 1), (DATE'2000-01-08', 2)")  # noqa: S608
    spark.sql(f"UPDATE {name} SET w = 3 WHERE d = DATE'2000-01-08'")  # noqa: S608
    yield name
    spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.skipif(pyspark_version() < (4,), reason="DataFrame.scalar requires PySpark 4+")
@pytest.mark.parametrize(
    ("left_version", "right_version", "expected", "slots"),
    [
        (None, None, (3, 3), [1]),
        (1, 1, (2, 2), [1]),
        (1, None, (2, 3), [2]),
    ],
)
def test_dataframe_table_reads_share_sources_by_version(
    spark, versioned_table, left_version, right_version, expected, slots
):
    def lookup(version):
        reader = spark.read if version is None else spark.read.option("versionAsOf", version)
        return reader.table(versioned_table).where("d = DATE'2000-01-08'").select("w")

    # Each lookup is a separately built DataFrame, so each one resolves the table on its own.
    df = spark.range(1).select(lookup(left_version).scalar().alias("a"), lookup(right_version).scalar().alias("b"))
    assert tuple(df.collect()[0]) == expected
    assert scalar_subquery_slots(df) == slots


@pytest.fixture
def commit_on_schema(spark, tmp_path):
    """Returns (delta_table, hook_table, log_dir).

    The hook table is a Python data source table without declared columns, so Sail asks the
    data source for its schema while it resolves a query that reads the table. The schema
    callback appends a commit to the Delta table, which makes the Delta table change in the
    middle of resolving the query.
    """
    suffix = uuid.uuid4().hex[:8]
    delta_table = f"delta_query_snapshot_{suffix}"
    hook_table = f"delta_query_snapshot_hook_{suffix}"
    location = tmp_path / delta_table
    log_dir = location / "_delta_log"
    spark.sql(f"CREATE TABLE {delta_table} (id INT) USING DELTA LOCATION '{escape_sql_string_literal(str(location))}'")
    spark.sql(f"INSERT INTO {delta_table} VALUES (1)")  # noqa: S608

    from pyspark.sql.datasource import DataSource, DataSourceReader
    from pyspark.sql.types import IntegerType, StructField, StructType

    root, log = str(location), str(log_dir)

    class CommitOnSchemaDataSource(DataSource):
        @classmethod
        def name(cls):
            return hook_table

        def schema(self):
            # Append a copy of the first data file as a new commit.
            versions = sorted(int(path.stem) for path in Path(log).glob("*.json"))
            add = next(
                json.loads(line)["add"]
                for version in versions
                for line in (Path(log) / f"{version:020}.json").read_text().splitlines()
                if '"add"' in line
            )
            target = f"copy-{versions[-1] + 1}.parquet"
            shutil.copyfile(Path(root) / add["path"], Path(root) / target)
            action = {"add": {**add, "path": target, "modificationTime": int(time.time() * 1000)}}
            (Path(log) / f"{versions[-1] + 1:020}.json").write_text(json.dumps(action) + "\n")
            return StructType([StructField("x", IntegerType())])

        def reader(self, schema):  # noqa: ARG002
            return OneRowReader()

    class OneRowReader(DataSourceReader):
        def read(self, partition):  # noqa: ARG002
            yield (1,)

    spark.dataSource.register(CommitOnSchemaDataSource)
    spark.sql(f"CREATE TABLE {hook_table} USING {hook_table}")
    yield delta_table, hook_table, log_dir
    spark.sql(f"DROP TABLE IF EXISTS {hook_table}")
    spark.sql(f"DROP TABLE IF EXISTS {delta_table}")


@pytest.mark.skipif(pyspark_version() < (4,), reason="Python data sources require PySpark 4+")
def test_references_to_a_delta_table_in_one_query_read_one_version(spark, commit_on_schema):
    delta_table, hook_table, log_dir = commit_on_schema
    rows = spark.sql(f"SELECT count(*) FROM {delta_table}").collect()[0][0]  # noqa: S608
    commits = len(list(log_dir.glob("*.json")))
    row = spark.sql(
        f"SELECT (SELECT count(*) FROM {delta_table}) AS before, (SELECT count(*) FROM {hook_table}) AS hook, "  # noqa: S608
        f"(SELECT count(id) FROM {delta_table}) AS after"
    ).collect()[0]
    # The hook committed once while the query was being resolved.
    assert len(list(log_dir.glob("*.json"))) == commits + 1
    assert (row.before, row.hook, row.after) == (rows, 1, rows)
    # The next query sees the new commit.
    assert spark.sql(f"SELECT count(*) FROM {delta_table}").collect()[0][0] == rows + 1  # noqa: S608
