---
title: Migrating from Spark
rank: 4
---

# Migrating from Spark

Sail is a drop-in replacement for Apache Spark.
Suppose you have a Sail server running on `localhost:50051`, you only need to change the way you create a `SparkSession` in your PySpark code.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()  # [!code --]
spark = SparkSession.builder.remote(f"sc://localhost:50051").getOrCreate()  # [!code ++]
```

## Considerations

Is Sail production ready? There is no single answer to this question, as it depends on your specific use case and requirements.
Here we recommend some practices to help you adopt Sail in your production environment.

- Make sure you PySpark code works with a recent Spark version. Sail is designed to be compatible with Spark 3.5 and later versions. If your existing code was developed for Spark 2.x or earlier Spark 3.x versions, you may need to update it to make it working with newer versions of Spark.
- Prepare a small dataset to test your PySpark code with Sail. This will help you identify any potential issues before running it on a larger dataset.
- Run your PySpark code with Sail using larger datasets and compare the output with those from Spark. You may want to collect statistics or samples from the output to ensure the results are consistent.
- Do not overwrite existing data. Always write to a new location for each data processing job. This ensures that no data loss occurs if something goes wrong.

## Notable Differences

1. Sail has a different way to configure external data storage. The configuration options for Hadoop file systems (e.g. `s3a`) will not have effects in Sail. Instead, refer to the [Storage](/guide/storage/) guide for how to configure data storage in Sail.
1. Error messages returned by Sail may differ from those in Spark.
1. Many Spark configuration options do not have effects in Sail. They are either unsupported or irrelevant to Sail.
1. The `pyspark.sql.DataFrame.explain` method and the `EXPLAIN` SQL statement show the Sail logical and physical plans.

## Supported Features

| Feature                  | PySpark API                | Supported          |
| ------------------------ | -------------------------- | ------------------ |
| RDD                      | `pyspark.SparkContext`     | :x:                |
| DataFrame and SQL        | `pyspark.sql.SparkSession` | :white_check_mark: |
| Pandas on Spark          | `pyspark.pandas`           | :construction:     |
| Structured Streaming     | `pyspark.sql.streaming`    | :construction:     |
| Spark Streaming (Legacy) | `pyspark.streaming`        | :x:                |
| MLlib (RDD-based)        | `pyspark.mllib`            | :x:                |
| MLlib (DataFrame-based)  | `pyspark.ml`               | :x:                |
| GraphX                   | -                          | :x:                |
