# Python Data Source Examples

This directory contains examples demonstrating Lakesail's generic Python data source bridge.

## Overview

Lakesail's `sail-python-datasource` crate provides a generic bridge that allows any Python code returning Apache Arrow data to be used as a data source. This enables:

- **Zero-copy Arrow transfer** between Python and Rust via Arrow C Data Interface
- **Distributed execution** via DataFusion's ExecutionPlan
- **Extensibility** - implement data sources in Python without touching Rust
- **Ecosystem access** - leverage Python libraries (requests, boto3, pandas, etc.)

## Examples

### 1. JDBC Example (`jdbc_example.py`)

Demonstrates reading from JDBC databases using Lakesail's JDBC format. The JDBC format is automatically registered on the Lakesail server:

```python
from pyspark.sql import SparkSession

# Connect to Lakesail server
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

# Use spark.read.format("jdbc") - automatically available!
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

### 2. Custom Data Source Example (`custom_datasource_example.py`)

Shows how to create custom data sources by implementing the three-method interface:

**Required Methods:**
```python
class MyDataSource:
    def infer_schema(self, options: dict) -> pa.Schema:
        """Return Arrow schema for the data."""
        pass

    def plan_partitions(self, options: dict) -> List[dict]:
        """Return partition specifications (JSON-serializable dicts)."""
        pass

    def read_partition(
        self,
        partition_spec: dict,
        options: dict
    ) -> Iterator[pa.RecordBatch]:
        """Read one partition, yield Arrow RecordBatches."""
        pass
```

**Example Implementations:**
- `RestAPIDataSource` - Read from REST APIs with pagination
- `S3DataSource` - Read from S3 with custom partitioning

## Running the Examples

1. **Install dependencies:**
   ```bash
   pip install pysail pyarrow connectorx
   ```

2. **JDBC Example:**
   ```bash
   # Edit jdbc_example.py to uncomment desired function
   python python/pysail/examples/python_datasource/jdbc_example.py
   ```

3. **Custom Data Source:**
   ```bash
   python python/pysail/examples/python_datasource/custom_datasource_example.py
   ```

## Creating Your Own Data Source

1. **Create a Python class** with the three required methods:
   - `infer_schema(options)` - Return `pa.Schema`
   - `plan_partitions(options)` - Return `List[Dict]`
   - `read_partition(partition_spec, options)` - Yield `pa.RecordBatch`

2. **Use it with Lakesail:**
   ```python
   df = spark.read.format("python") \
       .option("python_module", "my_package.my_module") \
       .option("python_class", "MyDataSource") \
       .option("custom_option", "value") \
       .load()
   ```

3. **Options are passed to your methods** as dictionaries:
   - All options from `.option()` calls
   - Standard options: `numPartitions`, `partitionColumn`, etc.
   - Custom options specific to your data source

## Use Cases

- **Databases:** JDBC, ODBC, NoSQL (MongoDB, DynamoDB, Cassandra)
- **Cloud Storage:** S3, GCS, Azure Blob with custom partitioning
- **APIs:** REST, GraphQL, gRPC endpoints
- **Data Warehouses:** Snowflake, BigQuery, Redshift
- **Streaming:** Batch reads from Kafka, Kinesis
- **File Formats:** Custom or proprietary formats
- **Machine Learning:** Model predictions as data source

## Architecture

```
┌──────────────────────────────────────┐
│      Lakesail (Rust/DataFusion)      │
│  - PythonDataSourceFormat            │
│  - PythonTableProvider               │
│  - PythonExec (ExecutionPlan)        │
└───────────┬──────────────────────────┘
            │ PyO3 + Arrow FFI
            │ (zero-copy transfer)
┌───────────▼──────────────────────────┐
│      Python Data Source              │
│  - infer_schema()                    │
│  - plan_partitions()                 │
│  - read_partition()                  │
└──────────────────────────────────────┘
```

## Performance

- **Zero-copy transfer:** Arrow data is transferred via pointers (Arrow C Data Interface)
- **Parallel execution:** Partitions executed in parallel via DataFusion
- **Minimal overhead:** PyO3 provides efficient Python ↔ Rust interop

## See Also

- [Python Data Source Architecture](../../../../docs/guide/integrations/python-data-source-architecture.md) - Detailed architecture documentation
- [python/pysail/jdbc/](../../jdbc/) - JDBC module implementation
- [crates/sail-python-datasource/](../../../crates/sail-python-datasource/) - Rust bridge implementation
