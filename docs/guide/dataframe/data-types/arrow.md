---
title: Arrow Semantics
rank: 3
---

# Arrow Semantics

Sail supports Arrow data types that are not compatible with Spark.
This enhances interoperability with other libraries that use Arrow.
However, since the Spark Connect client is still used,
you need to consider a few limitations when using these data types.

<!--@include: ../../_common/spark-session.md-->

## Data Reading and Writing

Sail allows you to read and write all Arrow data types as long as the data source supports them.
For example, the following operation works in Sail but is not supported in JVM Spark.

```python
spark.sql("SELECT 0::UINT16 AS c").write.parquet("data.parquet")
```

The Arrow schema is stored in the Parquet file metadata.
This allows other libraries that support Arrow to read the data back correctly.
For example, DuckDB will read the data as unsigned 16-bit integers.

If you read the data back in Sail, the data type is preserved.
Note that this behavior is different from JVM Spark, where the data type is changed to **IntegerType** (32-bit signed integer) for the dataset written above.

```python
spark.read.parquet("data.parquet").selectExpr("typeof(c)").show()
```

:::info
We may change this Parquet reading behavior in the future,
if it turns out to be important to match the JVM Spark behavior for compatibility reasons.
:::

## Query Results in Python

Sail uses Arrow internally, so all Arrow data types work naturally in the entire query execution process.
For PySpark applications, however, the Spark Connect client library controls how to interpret Arrow data received from the server.
So you need to be aware of the complications when "previewing" query results in Python.

For example, the following operations do not fully work even if query execution is successful within Sail. The query results are either cast to a different type implicitly, or an error is raised due to unsupported Arrow types in Spark.

```python
spark.sql("SELECT 0::UINT16").collect()
spark.sql("SELECT 0::UINT16").toPandas()
spark.sql("SELECT 0::UINT16").toArrow()
```

You can consider casting the data explicitly to a supported Spark data type before collecting the results in Python.
