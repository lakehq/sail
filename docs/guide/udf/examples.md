---
title: Examples
rank: 1
---

# Examples

This page provides examples for all types of PySpark UDFs supported by Sail.
For more information about the API, please refer to the Spark documentation and user guide.

<!--@include: ../_common/spark-session.md-->

```python-console
>>> from pyspark.sql import SparkSession
>>> spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
```

## Python UDF

You can define a Python scalar UDF by wrapping a Python lambda function with `udf()`,
or using the `@udf` decorator.

<<< @/../python/pysail/tests/spark/udf/test_scalar_udf.txt{python-console}

The UDF can be registered and used in SQL queries.

<<< @/../python/pysail/tests/spark/udf/test_register_scalar_udf.txt{python-console}

## Pandas UDF

You can define a Pandas scalar UDF via the `@pandas_udf` decorator.
The UDF takes a batch of data as a Pandas object, and must return a Pandas object of the same length as the input.
The UDF can also take an iterator of Pandas objects and returns an iterator of Pandas objects, one for each input batch.

<<< @/../python/pysail/tests/spark/udf/test_pandas_scalar_udf.txt{python-console}

You can define a Pandas UDF for aggregation. The UDF returns a single value for the input group.

<<< @/../python/pysail/tests/spark/udf/test_pandas_agg_udf.txt{python-console}

The Pandas UDF can be registered for use in SQL queries, in the same way as the Python scalar UDF.

You can define a Pandas UDF to transform data partitions using `mapInPandas()`.

<<< @/../python/pysail/tests/spark/udf/test_pandas_map_udf.txt{python-console}

You can define a Pandas UDF to transform grouped data or co-grouped data using `applyInPandas()`.

<<< @/../python/pysail/tests/spark/udf/test_pandas_grouped_map_udf.txt{python-console}

## Arrow UDF

You can define an Arrow UDF to transform data partitions using `mapInArrow()`.
This is similar to `mapInPandas()` but the input and output are Arrow record batches.

<<< @/../python/pysail/tests/spark/udf/test_arrow_map_udf.txt{python-console}

## Python UDTF

You can define a Python UDTF class that produces multiple rows for each input row.

<<< @/../python/pysail/tests/spark/udf/test_udtf.txt{python-console}

The UDTF can be registered and used in SQL queries.

<<< @/../python/pysail/tests/spark/udf/test_register_udtf.txt{python-console}
