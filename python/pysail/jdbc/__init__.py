"""Lakesail JDBC Data Source - Server-side database reading with Arrow backends.

This module provides server-side JDBC database reading capabilities using high-performance
backends (ConnectorX, ADBC) with Arrow-native data transfer.

The JDBC format is automatically registered on the Lakesail server. Clients use
standard PySpark APIs with no additional imports required.

Client Usage (Pure PySpark):
    from pyspark.sql import SparkSession

    # Connect to Lakesail server via Spark Connect
    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

    # Use spark.read.format("jdbc")
    df = spark.read.format("jdbc") \\
        .option("url", "jdbc:postgresql://localhost:5432/mydb") \\
        .option("dbtable", "orders") \\
        .option("user", "myuser") \\
        .option("password", "mypass") \\
        .option("partitionColumn", "order_id") \\
        .option("lowerBound", "1") \\
        .option("upperBound", "1000000") \\
        .option("numPartitions", "10") \\
        .load()

Note: This module contains server-side implementation code. Client applications
do not need to import anything from pysail.jdbc.
"""

import logging

from pysail.jdbc.exceptions import (
    BackendNotAvailableError,
    DatabaseError,
    InvalidJDBCUrlError,
    InvalidOptionsError,
    JDBCReaderError,
    SchemaInferenceError,
    UnsupportedDatabaseError,
)

__all__ = [
    "JDBCReaderError",
    "InvalidJDBCUrlError",
    "BackendNotAvailableError",
    "UnsupportedDatabaseError",
    "DatabaseError",
    "SchemaInferenceError",
    "InvalidOptionsError",
]

# Set up logging for server-side operations
logger = logging.getLogger("lakesail.jdbc")
logger.setLevel(logging.INFO)

# Add console handler if not already added
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
