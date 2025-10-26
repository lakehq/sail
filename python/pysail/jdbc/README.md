# Lakesail JDBC Module

High-performance JDBC database reading for Lakesail using Arrow-native backends.

## Overview

The `pysail.jdbc` module provides JDBC database connectivity using high-performance Arrow-native backends:

- **ConnectorX** (primary): Rust-based, fastest performance
- **ADBC** (alternative): Arrow Database Connectivity standard
- **Fallback** (backup): Pure Python (PyArrow + JDBC drivers)

Features:
- ✅ Zero-copy Arrow data transfer
- ✅ Partitioned reads for parallelism
- ✅ Automatic credential masking in logs
- ✅ Support for PostgreSQL, MySQL, SQLite, SQL Server, Oracle
- ✅ Works with Spark Connect

## Quick Start

### Use spark.read.format("jdbc") (Recommended)

The JDBC format is automatically registered on the Lakesail server. Simply use it directly:

```python
from pyspark.sql import SparkSession

# Use Spark Connect
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

# Read using JDBC format (no setup needed!)
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("dbtable", "orders") \
    .option("user", "admin") \
    .option("password", "secret") \
    .option("partitionColumn", "order_id") \
    .option("lowerBound", "1") \
    .option("upperBound", "1000000") \
    .option("numPartitions", "10") \
    .load()

df.show()
```

### Alternative: Direct Python Format (Advanced)

For advanced usage, you can use the generic Python data source format:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

# Read using generic Python data source format
df = spark.read.format("python") \
    .option("python_module", "pysail.jdbc.datasource") \
    .option("python_class", "JDBCArrowDataSource") \
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("dbtable", "orders") \
    .option("user", "admin") \
    .option("password", "secret") \
    .option("engine", "connectorx") \
    .load()

df.show()
```

## Supported Databases

| Database   | JDBC URL Format                                  | ConnectorX | ADBC |
|------------|--------------------------------------------------|------------|------|
| PostgreSQL | `jdbc:postgresql://host:port/database`           | ✅         | ✅   |
| MySQL      | `jdbc:mysql://host:port/database`                | ✅         | ✅   |
| SQLite     | `jdbc:sqlite:/path/to/file.db`                   | ✅         | ❌   |
| SQL Server | `jdbc:sqlserver://host:port;databaseName=db`     | ✅         | ❌   |
| Oracle     | `jdbc:oracle:thin:@host:port:sid`                | ✅         | ❌   |

## Partitioned Reads

For large tables, use partitioning to read in parallel:

### Range Partitioning

```python
# Read 10M rows in 10 partitions (1M each)
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("dbtable", "orders") \
    .option("partitionColumn", "order_id") \
    .option("lowerBound", "1") \
    .option("upperBound", "10000000") \
    .option("numPartitions", "10") \
    .load()
```

### Using Direct Python Format

```python
df = spark.read.format("python") \
    .option("python_module", "pysail.jdbc.datasource") \
    .option("python_class", "JDBCArrowDataSource") \
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("dbtable", "orders") \
    .option("partitionColumn", "order_id") \
    .option("lowerBound", "1") \
    .option("upperBound", "10000000") \
    .option("numPartitions", "10") \
    .load()
```

## Examples

### PostgreSQL

```python
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("dbtable", "users") \
    .option("user", "admin") \
    .option("password", "secret") \
    .load()
```

### MySQL with Partitioning

```python
df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/mydb") \
    .option("dbtable", "orders") \
    .option("partitionColumn", "order_id") \
    .option("lowerBound", "1") \
    .option("upperBound", "1000000") \
    .option("numPartitions", "10") \
    .load()
```

### SQLite

```python
df = spark.read.format("jdbc") \
    .option("url", "jdbc:sqlite:/tmp/mydata.db") \
    .option("dbtable", "transactions") \
    .load()
```

### Custom Query

```python
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("dbtable", "(SELECT * FROM orders WHERE created_at > '2024-01-01') AS filtered") \
    .option("engine", "connectorx") \
    .load()
```

## Configuration Options

### Required Options

- `url`: JDBC URL (e.g., `jdbc:postgresql://localhost:5432/mydb`)
- `dbtable`: Table name or subquery

### Optional Options

- `user`: Database username (can be in URL)
- `password`: Database password (can be in URL)
- `engine`: Backend engine (`connectorx`, `adbc`, or `fallback`, default: `connectorx`)

### Partitioning Options

- `partitionColumn`: Column name for range partitioning (must be numeric)
- `lowerBound`: Minimum value of partition column
- `upperBound`: Maximum value of partition column
- `numPartitions`: Number of partitions (default: 1)

### Performance Options

- `fetchsize`: Number of rows to fetch per batch (default: 10000)

## Backend Selection

### ConnectorX (Recommended)

Fastest backend, implemented in Rust with zero-copy Arrow support.

```bash
pip install connectorx
```

### ADBC (Alternative)

Arrow Database Connectivity standard, good cross-database support.

```bash
pip install adbc-driver-postgresql  # or adbc-driver-mysql
```

### Fallback (Automatic)

Pure Python implementation, used when neither ConnectorX nor ADBC is available.

## Architecture

The JDBC module implements Lakesail's generic Python data source interface and integrates with Lakesail's Rust `sail-python-datasource` bridge for:
- Zero-copy Arrow transfer (Python ↔ Rust via Arrow C Data Interface)
- Distributed execution via DataFusion's ExecutionPlan
- Parallel partition processing

## Security

### Credential Protection
- Credentials are automatically masked in logs using `mask_credentials()` utility
- Passwords in connection strings are replaced with `***` in all log output
- Use connection parameters instead of embedding credentials in URL when possible

### SQL Injection Protection
- **Column identifiers** are validated against a safe pattern (alphanumeric + underscore)
- **Reserved SQL keywords** are rejected as column names
- **User-provided predicates**: If using custom predicates via the `predicates` option, ensure they come from trusted sources only
- **Table names**: Validated for length and safe characters

### Input Validation
- URL length limited to 2048 characters
- Query length limited to 1MB
- Table name length limited to 256 characters
- All inputs are validated before use

### Best Practices
1. **Never use untrusted user input** directly in `dbtable`, `query`, or `predicates` options
2. **Use parameterized queries** where supported by backend (ADBC)
3. **Validate and sanitize** any user input before passing to JDBC options
4. **Use least privilege** database accounts (read-only for read operations)
5. **Enable TLS/SSL** in JDBC connection strings for production databases
6. **Rotate credentials** regularly and use secrets management systems

### Session Initialization Safety
The `sessionInitStatement` option only allows read-only configuration statements:
- ✅ Allowed: `SET`, `CREATE TEMPORARY TABLE`
- ❌ Rejected: `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `TRUNCATE`

## Performance Tips

1. **Use ConnectorX backend** for best performance
2. **Enable partitioning** for large tables (10M+ rows)
3. **Choose partition column** with even distribution
4. **Adjust numPartitions** based on cluster size
5. **Tune fetchsize** for memory vs speed tradeoff

## Testing

Tests are located in `python/pysail/tests/spark/datasource/test_jdbc.py`:

```bash
# Run JDBC tests
pytest python/pysail/tests/spark/datasource/test_jdbc.py
```

## See Also

- [Examples](../examples/python_datasource/) - Complete usage examples
- [Architecture](../../../docs/guide/integrations/python-data-source-architecture.md) - Generic Python data source design
- [Tests](../tests/spark/datasource/test_jdbc.py) - Test suite
