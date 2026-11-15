import json
import re
import time
import uuid
from pathlib import Path

import pytest

SCALAR_SUBQUERY_SLOTS = re.compile(r"ScalarSubqueryExec: subqueries=(\d+)")
# The rows of the table that the rollback fixture creates, at its first and current snapshots.
ROWS_AT_FIRST_SNAPSHOT = 1
ROWS_AT_CURRENT_SNAPSHOT = 2
METADATA_FILE = re.compile(r"^(?:v(\d+)|(\d+)-[^.]+)\.metadata\.json$")


def scalar_subquery_slots(df):
    return [int(n) for n in SCALAR_SUBQUERY_SLOTS.findall(df._explain_string())]  # noqa: SLF001


def metadata_files(location):
    """Returns (version, path) for each metadata file of the table, oldest first."""
    paths = (Path(location) / "metadata").glob("*.metadata.json")
    return sorted(
        (int(match.group(1) or match.group(2)), path) for path in paths if (match := METADATA_FILE.match(path.name))
    )


def latest_metadata(location):
    return json.loads(metadata_files(location)[-1][1].read_text())


def snapshot_ids(location):
    """Returns the snapshot IDs of the table, oldest first."""
    metadata = latest_metadata(location)
    return [entry["snapshot-id"] for entry in metadata["snapshot-log"]]


@pytest.fixture
def versioned_table(spark, tmp_path):
    """Returns (table, first snapshot ID) for an Iceberg table whose row for 2000-01-08
    has w = 2 at its first snapshot and w = 3 at its current snapshot."""
    name = f"iceberg_query_snapshot_{uuid.uuid4().hex[:8]}"
    location = tmp_path / name
    spark.sql(f"CREATE TABLE {name} (d DATE, w INT) USING iceberg LOCATION '{location.as_uri()}'")
    spark.sql(f"INSERT INTO {name} VALUES (DATE'2000-01-01', 1), (DATE'2000-01-08', 2)")  # noqa: S608
    first = snapshot_ids(location)[-1]
    spark.sql(f"UPDATE {name} SET w = 3 WHERE d = DATE'2000-01-08'")  # noqa: S608
    yield name, first
    spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.spark(version=">=4", reason="Uses DataFrame.scalar")
@pytest.mark.parametrize(
    ("left_first", "right_first", "expected", "slots"),
    [
        (False, False, (3, 3), [1]),
        (True, True, (2, 2), [1]),
        (True, False, (2, 3), [2]),
    ],
)
def test_dataframe_table_reads_share_sources_by_snapshot(
    spark, versioned_table, left_first, right_first, expected, slots
):
    name, first = versioned_table

    def lookup(at_first):
        reader = spark.read.option("snapshotId", str(first)) if at_first else spark.read
        return reader.table(name).where("d = DATE'2000-01-08'").select("w")

    # Each lookup is a separately built DataFrame, so each one resolves the table on its own.
    df = spark.range(1).select(lookup(left_first).scalar().alias("a"), lookup(right_first).scalar().alias("b"))
    assert tuple(df.collect()[0]) == expected
    assert scalar_subquery_slots(df) == slots


def test_sql_snapshot_reads_share_sources_by_snapshot(spark, versioned_table):
    name, first = versioned_table
    df = spark.sql(
        f"SELECT (SELECT w FROM {name} VERSION AS OF {first} WHERE d = DATE'2000-01-08') AS a, "  # noqa: S608
        f"(SELECT w FROM {name} VERSION AS OF {first} WHERE d = DATE'2000-01-08') AS b, "
        f"(SELECT w FROM {name} WHERE d = DATE'2000-01-08') AS c"
    )
    assert tuple(df.collect()[0]) == (2, 2, 3)
    assert scalar_subquery_slots(df) == [2]


@pytest.fixture(params=[False, True], ids=["unpartitioned", "partitioned"])
def rollback_on_schema(spark, tmp_path, request):
    """Returns (iceberg_table, hook_table, location).

    The Iceberg table has one row at its first snapshot and two rows at its current snapshot.
    The hook table is a Python data source table without declared columns, so Sail asks the
    data source for its schema while it resolves a query that reads the table. The schema
    callback writes a new metadata file that rolls the Iceberg table back to its first snapshot,
    which makes the Iceberg table change in the middle of resolving the query.
    """
    suffix = uuid.uuid4().hex[:8]
    iceberg_table = f"iceberg_query_snapshot_{suffix}"
    hook_table = f"iceberg_query_snapshot_hook_{suffix}"
    location = tmp_path / iceberg_table
    partition = "PARTITIONED BY (p)" if request.param else ""
    spark.sql(f"CREATE TABLE {iceberg_table} (id INT, p INT) USING iceberg {partition} LOCATION '{location.as_uri()}'")
    spark.sql(f"INSERT INTO {iceberg_table} VALUES (1, 1)")  # noqa: S608
    first = snapshot_ids(location)[-1]
    spark.sql(f"INSERT INTO {iceberg_table} VALUES (2, 1)")  # noqa: S608

    from pyspark.sql.datasource import DataSource, DataSourceReader
    from pyspark.sql.types import IntegerType, StructField, StructType

    root = str(location)

    class RollbackOnSchemaDataSource(DataSource):
        @classmethod
        def name(cls):
            return hook_table

        def schema(self):
            files = metadata_files(root)
            version, path = files[-1]
            metadata = json.loads(path.read_text())
            now = int(time.time() * 1000)
            metadata["current-snapshot-id"] = first
            metadata["last-updated-ms"] = now
            metadata.setdefault("refs", {})["main"] = {"snapshot-id": first, "type": "branch"}
            metadata["snapshot-log"].append({"snapshot-id": first, "timestamp-ms": now})
            metadata.setdefault("metadata-log", []).append(
                {"metadata-file": path.absolute().as_uri(), "timestamp-ms": now}
            )
            target = f"v{version + 1}.metadata.json" if path.name.startswith("v") else None
            target = target or f"{version + 1:05}-{uuid.uuid4()}.metadata.json"
            (Path(root) / "metadata" / target).write_text(json.dumps(metadata))
            return StructType([StructField("x", IntegerType())])

        def reader(self, schema):  # noqa: ARG002
            return OneRowReader()

    class OneRowReader(DataSourceReader):
        def read(self, partition):  # noqa: ARG002
            yield (1,)

    spark.dataSource.register(RollbackOnSchemaDataSource)
    spark.sql(f"CREATE TABLE {hook_table} USING {hook_table}")
    yield iceberg_table, hook_table, location
    spark.sql(f"DROP TABLE IF EXISTS {hook_table}")
    spark.sql(f"DROP TABLE IF EXISTS {iceberg_table}")


@pytest.mark.spark(version=">=4", reason="Uses the Python DataSource API")
def test_references_to_an_iceberg_table_in_one_query_read_one_snapshot(spark, rollback_on_schema):
    iceberg_table, hook_table, location = rollback_on_schema
    assert spark.sql(f"SELECT count(*) FROM {iceberg_table}").collect()[0][0] == ROWS_AT_CURRENT_SNAPSHOT  # noqa: S608
    files = len(metadata_files(location))
    row = spark.sql(
        f"SELECT (SELECT count(*) FROM {iceberg_table}) AS before, (SELECT count(*) FROM {hook_table}) AS hook, "  # noqa: S608
        f"(SELECT count(id) FROM {iceberg_table}) AS after"
    ).collect()[0]
    # The hook rolled the table back once while the query was being resolved.
    assert len(metadata_files(location)) == files + 1
    assert (row.before, row.hook, row.after) == (ROWS_AT_CURRENT_SNAPSHOT, 1, ROWS_AT_CURRENT_SNAPSHOT)
    # The next query sees the rollback.
    assert spark.sql(f"SELECT count(*) FROM {iceberg_table}").collect()[0][0] == ROWS_AT_FIRST_SNAPSHOT  # noqa: S608
