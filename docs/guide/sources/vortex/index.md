---
title: Vortex
rank: 5
---

# Vortex Data Source

Sail provides a columnar data source exposed under the `vortex` format name.
The implementation is based on the Python `vortex-data` library and supports
filter pushdown with zero-copy Arrow data reads.

[Vortex](https://github.com/vortex-data/vortex) is a columnar file format designed for fast analytical reads
with adaptive encoding and efficient compression.

<!--@include: ../../_common/spark-session.md-->

## Installation

You need to install the `pysail` package with the `vortex` extra to use the Vortex data source.
The `vortex` extra installs the `vortex-data` Python library as a dependency.

```bash
pip install pysail[vortex]
```

::: warning
The `vortex-data` Python library is not yet available on Windows.
:::

::: info
The Vortex data source requires PySpark 4.1+ for filter pushdown support.
:::

## Quick Start

Register the data source once per Spark session.

```python
from pysail.spark.datasource.vortex import VortexDataSource

spark.dataSource.register(VortexDataSource)
```

Then read a Vortex file using the standard PySpark API.

```python
df = (
    spark.read.format("vortex")
    .option("path", "/path/to/data.vortex")
    .load()
)
```

## Options

| Name   | Required | Default | Description                  |
| ------ | -------- | ------- | ---------------------------- |
| `path` | Yes      |         | The path to the Vortex file. |

## Filter Pushdown

The data source pushes supported filters down to the Vortex reader so that
only matching rows are materialized. This can significantly reduce IO and
memory usage for selective queries.

Here are the supported filter types that can be pushed down to Vortex:

| Filter               | Example                                                 |
| -------------------- | ------------------------------------------------------- |
| `EqualTo`            | `df.filter(df.id == 3)`                                 |
| `GreaterThan`        | `df.filter(df.id > 10)`                                 |
| `GreaterThanOrEqual` | `df.filter(df.id >= 10)`                                |
| `LessThan`           | `df.filter(df.id < 10)`                                 |
| `LessThanOrEqual`    | `df.filter(df.id <= 10)`                                |
| `In`                 | `df.filter(df.id.isin(1, 2, 3))`                        |
| `Not`                | `df.filter(df.id != 3)` (negation of supported filters) |

Filters that cannot be pushed down (such as `IS NULL`, `IS NOT NULL`, `startswith`, `endswith`, `contains`, or other string predicates) are still supported, but they are applied after the read. When combining filters with `&`, each supported filter is pushed down individually and unsupported ones are applied post-read.

## Examples

### Basic Read

```python
from pysail.spark.datasource.vortex import VortexDataSource

spark.dataSource.register(VortexDataSource)

df = spark.read.format("vortex").option("path", "/data/events.vortex").load()
df.show()
```

### Filter Pushdown

Filters are pushed down to the Vortex reader automatically:

```python
df = spark.read.format("vortex").option("path", "/data/events.vortex").load()

# The comparison filter is pushed down to Vortex.
# The `isNotNull` filter is applied post-read.
df.filter((df.score > 90.0) & (df.name.isNotNull())).show()
```

### SQL Queries

```python
df = spark.read.format("vortex").option("path", "/data/events.vortex").load()
df.createOrReplaceTempView("events")

spark.sql("SELECT * FROM events WHERE id > 100 ORDER BY score DESC").show()
```

### Creating Vortex Files

You can create Vortex files from any PyArrow table using the `vortex-data` library directly:

```python
import pyarrow as pa
import vortex

table = pa.table({
    "id": [1, 2, 3, 4, 5],
    "name": ["alice", "bob", "carol", "dave", "eve"],
    "score": [90.5, 85.0, 92.3, 78.1, 88.7],
})

vortex.io.write(table, "/path/to/data.vortex")
```
