"""JDBC Data Source - Server-side implementation for Lakesail.

This module runs on the Lakesail server (not client-side) and implements
the three-method interface required by Lakesail's sail-python-datasource bridge.

The JDBC format is automatically registered at server startup. Client applications
use standard PySpark APIs with no imports from pysail.jdbc needed.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from pysail.jdbc.backends import get_backend
from pysail.jdbc.jdbc_options import NormalizedJDBCOptions
from pysail.jdbc.jdbc_url_parser import parse_jdbc_url, validate_driver_supported
from pysail.jdbc.partition_planner import PartitionPlanner
from pysail.jdbc.query_builder import (
    build_query_for_partition,
    build_schema_inference_query,
)
from pysail.jdbc.utils import mask_credentials

if TYPE_CHECKING:
    from collections.abc import Iterator

    import pyarrow as pa

logger = logging.getLogger("lakesail.jdbc")


class JDBCArrowDataSource:
    """
    Server-side JDBC data source using Arrow-native backends.

    This class runs in the Lakesail server process and implements the interface
    required by sail-python-datasource bridge. It uses high-performance backends
    (ConnectorX, ADBC) to read from JDBC databases with zero-copy Arrow data transfer.

    The three required methods are:
    - infer_schema(options) -> pa.Schema
    - plan_partitions(options) -> List[dict]
    - read_partition(partition_spec, options) -> Iterator[pa.RecordBatch]

    Client usage (pure PySpark - no pysail imports needed):
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

        df = spark.read.format("jdbc") \\
            .option("url", "jdbc:postgresql://localhost/mydb") \\
            .option("dbtable", "orders") \\
            .option("user", "admin") \\
            .option("password", "secret") \\
            .load()

    Advanced: Direct Python format usage (if needed):
        spark.read.format("python") \\
            .option("python_module", "pysail.jdbc.datasource") \\
            .option("python_class", "JDBCArrowDataSource") \\
            .option("url", "jdbc:postgresql://localhost/mydb") \\
            .option("dbtable", "orders") \\
            .load()
    """

    def infer_schema(self, options: dict[str, str]) -> pa.Schema:
        """
        Infer Arrow schema from the JDBC data source.

        This fetches a small sample from the database to determine the schema.
        The schema is returned as-is from the backend (no type conversion).

        Args:
            options: User-provided options including:
                - url: JDBC URL (e.g., "jdbc:postgresql://localhost/mydb")
                - dbtable: Table name or subquery
                - query: Full SQL query (alternative to dbtable)
                - user: Database user
                - password: Database password
                - engine: Backend engine ("connectorx", "adbc", or "fallback")

        Returns:
            PyArrow Schema describing the data

        Raises:
            SchemaInferenceError: If schema cannot be inferred
            InvalidJDBCUrlError: If JDBC URL is malformed
            BackendNotAvailableError: If backend is not available
        """
        # Normalize options
        norm_opts = NormalizedJDBCOptions.from_spark_options(options)
        norm_opts.validate()

        # Get backend
        backend = get_backend(norm_opts.engine)

        # Parse JDBC URL
        parsed = parse_jdbc_url(norm_opts.url, norm_opts.user, norm_opts.password)
        validate_driver_supported(parsed.driver)

        # Build schema inference query (LIMIT 1)
        query = build_schema_inference_query(norm_opts.dbtable, norm_opts.query)

        masked_connection = mask_credentials(parsed.connection_string)
        logger.info("Inferring schema from %s", masked_connection)
        logger.debug("Schema inference query: %s", query)

        # Fetch single batch to get schema
        batches = backend.read_batches(connection_string=parsed.connection_string, query=query, fetch_size=1)

        if not batches:
            msg = "Query returned no results for schema inference"
            raise ValueError(msg)

        schema = batches[0].schema
        logger.info(
            "Inferred schema with %s columns: %s",
            len(schema),
            schema.names,
        )

        return schema

    def plan_partitions(self, options: dict[str, str]) -> list[dict[str, Any]]:
        """
        Plan how to partition the data for parallel reading.

        This determines how to split the data across multiple partitions for
        distributed execution. Partitions can be based on:
        - Range partitioning on a numeric column (lower_bound to upper_bound)
        - Custom predicates (WHERE clauses)
        - Single partition (default)

        Args:
            options: User-provided options including partitioning parameters:
                - partitionColumn: Column for range partitioning
                - lowerBound: Range lower bound
                - upperBound: Range upper bound
                - numPartitions: Number of partitions
                - predicates: Comma-separated WHERE predicates

        Returns:
            List of partition specifications (JSON-serializable dicts).
            Each dict contains:
                - partition_id: Partition index
                - predicate: WHERE clause predicate (or None for no filter)
        """
        # Normalize options
        norm_opts = NormalizedJDBCOptions.from_spark_options(options)
        norm_opts.validate()

        if norm_opts.predicates:
            explicit_predicates = norm_opts.predicates
            logger.info(
                "Using %d explicit predicate(s) supplied via options",
                len(explicit_predicates),
            )
            predicates = explicit_predicates
        else:
            planner = PartitionPlanner(
                partition_column=norm_opts.partition_column,
                lower_bound=norm_opts.lower_bound,
                upper_bound=norm_opts.upper_bound,
                num_partitions=norm_opts.num_partitions,
                predicates=None,
            )
            predicates = planner.generate_predicates()

        # Convert to list of dicts for JSON serialization
        result = [
            {
                "partition_id": index,
                "predicate": predicate,
            }
            for index, predicate in enumerate(predicates)
        ]

        logger.info("Planned %s partition(s)", len(result))
        return result

    def read_partition(
        self,
        partition_spec: dict[str, Any],
        options: dict[str, str],
    ) -> Iterator[pa.RecordBatch]:
        """
        Read one partition and yield Arrow RecordBatches.

        This is called by the Rust execution plan for each partition in parallel.
        It uses the selected backend (ConnectorX, ADBC, or fallback) to read
        data from the database and yields Arrow RecordBatches via zero-copy FFI.

        Args:
            partition_spec: Partition specification from plan_partitions()
                - partition_id: Partition index
                - predicate: WHERE clause predicate (or None)
            options: User-provided options (same as infer_schema)

        Yields:
            PyArrow RecordBatches containing the partition data

        Raises:
            DatabaseError: If database read fails
            BackendNotAvailableError: If backend is not available
        """
        # Normalize options
        norm_opts = NormalizedJDBCOptions.from_spark_options(options)

        # Get backend
        backend = get_backend(norm_opts.engine)

        # Parse JDBC URL
        parsed = parse_jdbc_url(norm_opts.url, norm_opts.user, norm_opts.password)

        # Get predicate from partition spec
        predicate = partition_spec.get("predicate")
        partition_id = partition_spec.get("partition_id", 0)
        logger.info("Partition spec: %s", partition_spec)
        logger.info("Read options keys: %s", sorted(options.keys()))

        # Build query from dbtable/query + predicate
        query = build_query_for_partition(dbtable=norm_opts.dbtable, query=norm_opts.query, predicate=predicate)

        masked_connection = mask_credentials(parsed.connection_string)
        logger.info(
            "Reading partition %s from %s",
            partition_id,
            masked_connection,
        )
        logger.debug("Query: %s", query)

        # Read batches from backend
        batches = backend.read_batches(
            connection_string=parsed.connection_string,
            query=query,
            fetch_size=norm_opts.fetch_size,
        )

        # Yield each batch
        total_rows = 0
        for batch in batches:
            total_rows += batch.num_rows
            yield batch

        logger.info("Partition %s read %s rows", partition_id, total_rows)
