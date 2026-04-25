# DataSource Expert

Expert in data source connectors and table formats for Sail. Guides implementation of file formats, storage systems, catalogs, and performance optimization.

## Context

Sail supports multiple file formats, storage backends, table formats, and catalog providers through a unified object store abstraction.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        User Query                            │
│  "SELECT * FROM s3://bucket/table WHERE date > '2024-01-01'" │
└───────────────────────────────┬───────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                     sail-plan-lakehouse                      │
│  (Lakehouse planning: Delta Lake, Iceberg)                   │
└───────────────────────────────┬───────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                    sail-object-store                         │
│  (Unified storage abstraction)                               │
└───────────────────────────────┬───────────────────────────────┘
                                │
            ┌───────────────────┼───────────────────┐
            ▼                   ▼                   ▼
     ┌──────────┐        ┌──────────┐        ┌──────────┐
     │    S3    │        │   Azure  │        │   GCS    │
     └──────────┘        └──────────┘        └──────────┘
```

## Supported File Formats

| Format | Read | Write | Location |
|--------|------|-------|----------|
| Parquet | ✓ | ✓ | DataFusion built-in |
| CSV | ✓ | ✓ | DataFusion built-in |
| JSON | ✓ | ✓ | DataFusion built-in |
| Text | ✓ | ✓ | DataFusion built-in |
| Avro | ✓ | ✗ | Planned |
| Binary | ✓ | ✓ | DataFusion built-in |

### Reading Parquet
```python
# Via DataFrame API
df = spark.read.parquet("s3://bucket/data.parquet")

# Via SQL
df = spark.sql("SELECT * FROM parquet.`s3://bucket/data.parquet`")
```

### Reading CSV
```python
# With options
df = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .csv("s3://bucket/data.csv")
```

### Reading JSON
```python
df = spark.read.json("s3://bucket/data.json")
```

## Table Formats

### Delta Lake

**Status**: Partial support

```python
# Read Delta Lake table
df = spark.read.format("delta").load("s3://bucket/delta_table")

# Write to Delta Lake
df.write.format("delta").mode("overwrite").save("s3://bucket/delta_table")

# Delta Lake specific
from delta import *

# Time travel
df = spark.read.format("delta").option("versionAsOf", "1").load(...)

# DDL-style column mapping
spark.conf.set("spark.sql.columnMapping.mode", "name")
```

**Features**:
- Read and write operations
- Time travel (Sail-only, not Spark compatible)
- DDL-style column mapping
- Schema evolution

**Critical Files**:
- `crates/sail-delta-lake/src/` - Delta Lake implementation
- `crates/sail-plan-lakehouse/src/` - Lakehouse planning

### Apache Iceberg

**Status**: Partial support

```python
# Read Iceberg table
df = spark.read.format("iceberg").load("s3://bucket/iceberg_table")

# Write to Iceberg
df.write.format("iceberg").mode("append").save("s3://bucket/iceberg_table")

# Via catalog
df = spark.table("catalog.db.table")
```

**Features**:
- Read operations
- Partitioning
- Time travel
- Cross-format compatibility

**Critical Files**:
- `crates/sail-iceberg/src/` - Iceberg implementation

## Storage Backends

### AWS S3

**Supported**:
- Standard S3
- S3 Express One Zone

```python
# Read from S3
df = spark.read.parquet("s3://bucket/path/to/data")

# Via config
spark.conf.set("fs.s3a.access.key", "ACCESS_KEY")
spark.conf.set("fs.s3a.secret.key", "SECRET_KEY")
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
```

### Azure Blob Storage

```python
# Read from Azure
df = spark.read.parquet("abfs://container@account/path/to/data")

# Via config
spark.conf.set("fs.azure.account.key.account.blob.core.windows.net", "KEY")
```

### Google Cloud Storage

```python
# Read from GCS
df = spark.read.parquet("gs://bucket/path/to/data")

# Via config
spark.conf.set("fs.gs.auth.service.account.enable", "true")
spark.conf.set("fs.gs.auth.service.account.json.keyfile", "/path/to/key.json")
```

### HDFS

```python
# Read from HDFS
df = spark.read.parquet("hdfs://namenode:8020/path/to/data")
```

### Local File System

```python
# Read from local files
df = spark.read.parquet("/path/to/local/data")
```

### In-Memory

```python
# Register in-memory data
df.createOrReplaceTempView("temp_table")
```

## Catalogs

### Memory Catalog

```python
# Default catalog
spark.catalog.listTables()
spark.catalog.listDatabases()
spark.catalog.listFunctions()
```

### Iceberg REST Catalog

```python
# Configure Iceberg REST catalog
spark.conf.set("spark.sql.catalog.my_catalog", "rest")
spark.conf.set("spark.sql.catalog.my_catalog.uri", "http://catalog:8181")

# Use catalog
df = spark.table("my_catalog.db.table")
```

### Unity Catalog

```python
# Configure Unity Catalog
spark.conf.set("spark.sql.catalog.my_catalog", "unity")
spark.conf.set("spark.sql.catalog.my_catalog.uri", "https://unity.cloud.databricks.com")
spark.conf.set("spark.sql.catalog.my_catalog.token", "TOKEN")

# Use catalog
df = spark.table("my_catalog.db.table")
```

## Object Store Abstraction

### Unified Interface

```rust
// crates/sail-object-store/src/

use object_store::{ObjectStore, path::Path};

async fn list_files(store: &dyn ObjectStore, prefix: &Path) -> Result<Vec<ObjectMeta>> {
    let mut stream = store.list(prefix);
    let mut files = Vec::new();

    while let Some(result) = stream.next().await {
        files.push(result?);
    }

    Ok(files)
}
```

### Configuration

```rust
// Configure S3
use object_store::aws::AmazonS3Builder;

let s3 = AmazonS3Builder::new()
    .with_region("us-west-2")
    .with_bucket_name("my-bucket")
    .with_access_key_id("ACCESS_KEY")
    .with_secret_access_key("SECRET_KEY")
    .build()?;
```

## Partitioning

### Hive-Style Partitioning

```
s3://bucket/table/
    year=2024/
        month=01/
            day=01/
                data.parquet
```

### Reading Partitioned Data

```python
# Partition discovery is automatic
df = spark.read.parquet("s3://bucket/table/")

# Partitions become columns
df.select("year", "month", "day").distinct().show()
```

### Writing Partitioned Data

```python
# Write with partitioning
df.write.partitionBy("year", "month", "day") \
    .mode("overwrite") \
    .parquet("s3://bucket/table/")
```

## Schema Evolution

### Delta Lake

```python
# Add column
df_new = df.withColumn("new_col", lit("default"))
df_new.write.format("delta").mode("overwrite").save(...)

# Schema evolves automatically
spark.read.format("delta").load(...).printSchema()
```

### Iceberg

```python
# Iceberg supports schema evolution via SQL
spark.sql("""
    ALTER TABLE my_catalog.db.table
    ADD COLUMN new_col STRING
""")
```

## Performance Optimization

### Predicate Pushdown

```python
# Filters are pushed to data source
df = spark.read.parquet("s3://bucket/data") \
    .filter(col("date") > "2024-01-01")
```

### Data Skipping

Delta Lake and Iceberg automatically maintain statistics for data skipping:

```python
# Only reads relevant files
df = spark.read.format("delta").load(...) \
    .filter(col("partition_col") == "value")
```

### Column Pruning

```python
# Only reads required columns
df = spark.read.parquet("s3://bucket/data") \
    .select("col1", "col2", "col3")
```

## Critical Files

| File | Purpose |
|------|---------|
| `crates/sail-object-store/src/` | Storage abstraction |
| `crates/sail-delta-lake/src/` | Delta Lake implementation |
| `crates/sail-iceberg/src/` | Iceberg implementation |
| `crates/sail-catalog-*/` | Catalog implementations |
| `crates/sail-plan-lakehouse/src/` | Lakehouse planning |

## Common Patterns

### Custom Data Source

```rust
use datafusion::datasource::{TableProvider, TableType};
use arrow::record_batch::RecordBatch;

pub struct MyCustomTable {
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for MyCustomTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Create execution plan
    }
}
```

### Register Table Provider

```python
# Register custom table
spark._jvm.SessionHolder().registerTable("my_table", table_provider)

# Or via SQL
spark.sql("CREATE TABLE my_table USING my_format OPTIONS (...)")
```

## Debugging Tips

### Check File Listing
```python
# List files in a path
spark._jvm.JavaHelpers.list("s3://bucket/path")
```

### Verify Partition Discovery
```python
# Show discovered partitions
df.inputFiles()
```

### Check Schema
```python
# Show table schema
spark.table("table").printSchema()
```

## References

- Object Store: https://docs.rs/object-store/
- Delta Lake: `crates/sail-delta-lake/`
- Iceberg: `crates/sail-iceberg/`
