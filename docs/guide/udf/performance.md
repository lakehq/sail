---
title: Performance
rank: 2
---

# Performance

In general, the Sail implementation of PySpark UDFs and UDTFs is more efficient than the original implementation. The Python interpreter running the UDF is in the same process as the Rust-based execution engine. Therefore, there is no data (de)serialization overhead, while the JVM-based implementation has to move data between the JVM and the Python worker.

For scalar UDFs, the user-provided Python function is called on each input row. This is usually less efficient than built-in functions which can potentially be applied in a vectorized manner in Sail, where the Arrow columnar in-memory format is used.

Since Spark 2.3, Pandas UDFs can be used to transform data in Spark, where the Python function operates on batches of rows represented as `pandas.DataFrame` or `pandas.Series` objects.
This is usually more performant than the scalar UDF.
In Sail, the internal in-memory data in Arrow format is converted to Pandas objects before Python function invocation, and the result is converted back to the Arrow format. Such conversion can be zero-copy, but data copy occurs when the `pyarrow` library decides it is unavoidable.

Since Spark 3.3, the `pyspark.sql.DataFrame.mapInArrow()` method can be used to call Arrow UDFs on Spark data. This method expects a Python function that operates on Arrow record batches (`pyarrow.RecordBatch`).
**It is recommended to use such Arrow UDFs to maximize performance.**
In Sail, the implementation does not involve any data copy. The Python function and the Rust-based execution engine share Arrow data directly via pointers.

For all types of UDFs, in the JVM-based Spark implementation, the Python worker wraps the user-provided Python function with additional code for type conversion and data validation.
To ensure full parity with PySpark, such wrappers are also used in Sail.
It is recommended to use Pandas or Arrow UDFs so that the wrapper overhead is amortized over a batch of rows.
