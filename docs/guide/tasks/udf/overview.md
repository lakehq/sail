---
title: Overview
rank: 1
---

# Overview

Sail provides comprehensive support for PySpark user-defined functions (UDFs) and user-defined table functions (UDTFs).

## Performance Considerations

In general, the Sail implementation of PySpark UDFs and UDTFs is more efficient than the original implementation. The Python interpreter running the UDF is in the same process as the Rust-based execution engine. Therefore, there is no data (de)serialization overhead, while the JVM-based implementation has to move data between the JVM and the Python worker.

For scalar UDFs, the user-provided Python function is called on each input row. This is usually less efficient than built-in functions which can potentially be applied in a vectorized manner in Sail, where the Arrow columnar in-memory format is used.

Since Spark 2.3, Pandas UDFs can be used to transform data in Spark, where the Python function operates on batches of rows represented as `pandas.DataFrame` or `pandas.Series` objects.
This is usually more performant than the scalar UDF.
In Sail, the internal in-memory data in Arrow format is converted to Pandas objects before the Python function invocation, and the result is converted back to Arrow format. Such conversion can be zero-copy, but data copy occurs when the `pyarrow` library decides it is unavoidable.

Since Spark 3.3, the `pyspark.sql.DataFrame.mapInArrow()` method can be used to call Arrow UDFs on Spark data. This method expects a Python function that operates on Arrow record batches (`pyarrow.RecordBatch`).
**It is recommended to use such Arrow UDFs to maximize performance.**
In Sail, the implementation does not involve any data copy. The Python function and the Rust-based execution engine share Arrow data directly via pointers.

For all types of UDFs, in the JVM-based Spark implementation, the Python worker wraps the user-provided Python function with additional code for type conversion and data validation.
To ensure full parity with PySpark, such code path is also followed in Sail.
It is recommended to use Pandas or Arrow UDFs so that the wrapper overhead is amortized over a batch of rows.

## Supported APIs

The following PySpark APIs are supported.

- <PySparkApi name="pyspark.sql.DataFrame.mapInArrow" />
- <PySparkApi name="pyspark.sql.DataFrame.mapInPandas" />
- <PySparkApi name="pyspark.sql.functions.call_function" />
- <PySparkApi name="pyspark.sql.functions.call_udf" />
- <PySparkApi name="pyspark.sql.functions.pandas_udf" />
- <PySparkApi name="pyspark.sql.functions.udf" />
- <PySparkApi name="pyspark.sql.functions.udtf" />
- <PySparkApi name="pyspark.sql.GroupedData.applyInPandas" />
- <PySparkApi name="pyspark.sql.PandasCogroupedOps.applyInPandas" />
- <PySparkApi name="pyspark.sql.UDFRegistration.register" />
- <PySparkApi name="pyspark.sql.UDTFRegistration.register" />

::: info
The PySpark library follows different code path for input and output conversion, depending on whether Arrow optimization is enabled.
Arrow optimization is controlled by the `useArrow` argument of the `udf()` and `udtf()` wrappers, and the `spark.sql.execution.pythonUDTF.arrow.enabled` configuration.
Sail respects such configuration for input and output conversion. But note that Sail uses Arrow for query execution regardless of whether Arrow is enabled in PySpark.
:::

The following PySpark API is not supported yet.

- <PySparkApi name=pyspark.sql.GroupedData.applyInPandasWithState />

<script setup>
import PySparkApi from '@theme/components/PySparkApi.vue';
</script>
