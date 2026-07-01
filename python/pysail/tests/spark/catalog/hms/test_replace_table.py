# ruff: noqa: S608
"""HMS CREATE OR REPLACE TABLE tests that need a second query to inspect state.

Behaviour that is fully observable in a single SQL result lives in the
``.feature`` scenarios (``features/replace_table.feature``). The cases here need
to parse a *second* query result -- the partition section of
``DESCRIBE EXTENDED`` -- to assert precisely which columns are partition keys
after a REPLACE, which the row-existence ``.feature`` steps cannot distinguish
from ordinary columns.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def _partition_columns(spark: SparkSession, table_fqn: str) -> list[str]:
    """Return the partition column names from Sail's DESCRIBE EXTENDED output.

    Sail emits a ``# Partition Information`` marker row, then a ``# col_name``
    header row, then one row per partition column, then a blank row followed by
    ``# Detailed Table Information``. Everything between the header and that
    trailing block is a partition column.
    """
    rows = spark.sql(f"DESCRIBE EXTENDED {table_fqn}").collect()
    names = [(row.col_name or "").strip() for row in rows]
    if "# Partition Information" not in names:
        return []
    start = names.index("# Partition Information") + 1
    # Skip the "# col_name" header row that follows the marker.
    if start < len(names) and names[start] == "# col_name":
        start += 1
    partitions: list[str] = []
    for name in names[start:]:
        if name == "" or name.startswith("#"):
            break
        partitions.append(name)
    return partitions


def test_create_or_replace_changes_partitioning_region_to_country(
    spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """The headline capability: REPLACE swaps the partition key set entirely.

    HMS ``alter_table`` cannot change ``partition_keys``; Sail implements REPLACE
    as drop+create under an exclusive lock, so the replaced table is a fresh
    definition partitioned by ``country`` alone -- the old ``region`` partition
    key must be gone.
    """
    table_fqn = f"{hms_s3_database}.t_repartition"

    spark.sql(
        f"""
        CREATE TABLE {table_fqn} (id INT, region STRING, country STRING)
        USING PARQUET
        PARTITIONED BY (region)
        """
    )
    assert _partition_columns(spark, table_fqn) == ["region"]

    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {table_fqn} (id INT, region STRING, country STRING)
        USING PARQUET
        PARTITIONED BY (country)
        """
    )
    assert _partition_columns(spark, table_fqn) == ["country"], (
        "REPLACE must swap the partition key from region to country, not merge them"
    )


def test_invalid_replacement_preserves_original_partitioning(
    spark: SparkSession,
    hms_s3_database: str,
) -> None:
    """A rejected REPLACE must leave the original table -- schema, rows and
    partitioning -- untouched (pre-validate before drop / compensating restore).
    """
    import pytest

    table_fqn = f"{hms_s3_database}.t_survive_part"

    spark.sql(
        f"""
        CREATE TABLE {table_fqn} (id INT, region STRING) USING PARQUET
        PARTITIONED BY (region)
        """
    )
    spark.sql(f"INSERT INTO {table_fqn} VALUES (1, 'eu'), (2, 'us')")

    with pytest.raises(Exception, match="(?i)duplicate|already exists|COLUMN_ALREADY_EXISTS"):
        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {table_fqn} (id INT, id INT) USING PARQUET
            """
        )

    # Original schema, partitioning and rows must all survive.
    assert _partition_columns(spark, table_fqn) == ["region"]
    rows = spark.sql(f"SELECT id, region FROM {table_fqn} ORDER BY id").collect()
    assert [(row.id, row.region) for row in rows] == [(1, "eu"), (2, "us")]
