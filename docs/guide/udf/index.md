---
title: User-defined Functions
rank: 3
---

# User-defined Functions

Sail provides comprehensive support for PySpark user-defined functions (UDFs) and user-defined table functions (UDTFs).
You can use UDFs and UDTFs in the PySpark DataFrame API.
You can also register UDFs and UDTFs and then use them in Spark SQL queries.

:::info
Spark Java or Scala UDFs are not supported in Sail.
:::

## Supported APIs

Here is a list of supported (:white_check_mark:) and unsupported (:construction:) PySpark APIs for UDFs and UDTFs.

| API                                                                | Status             |
| ------------------------------------------------------------------ | ------------------ |
| <PySparkApi name="pyspark.sql.DataFrame.mapInArrow" />             | :white_check_mark: |
| <PySparkApi name="pyspark.sql.DataFrame.mapInPandas" />            | :white_check_mark: |
| <PySparkApi name="pyspark.sql.functions.call_function" />          | :white_check_mark: |
| <PySparkApi name="pyspark.sql.functions.call_udf" />               | :white_check_mark: |
| <PySparkApi name="pyspark.sql.functions.pandas_udf" />             | :white_check_mark: |
| <PySparkApi name="pyspark.sql.functions.udf" />                    | :white_check_mark: |
| <PySparkApi name="pyspark.sql.functions.udtf" />                   | :white_check_mark: |
| <PySparkApi name="pyspark.sql.GroupedData.applyInPandas" />        | :white_check_mark: |
| <PySparkApi name="pyspark.sql.PandasCogroupedOps.applyInPandas" /> | :white_check_mark: |
| <PySparkApi name="pyspark.sql.UDFRegistration.register" />         | :white_check_mark: |
| <PySparkApi name="pyspark.sql.UDTFRegistration.register" />        | :white_check_mark: |
| <PySparkApi name=pyspark.sql.GroupedData.applyInPandasWithState /> | :construction:     |

::: info
The PySpark library uses different logic for input and output conversion, depending on whether Arrow optimization is enabled.
Arrow optimization is controlled by the `useArrow` argument of the `udf()` and `udtf()` wrappers, and the `spark.sql.execution.pythonUDTF.arrow.enabled` configuration.
Sail respects such configuration for input and output conversion. But note that Sail uses Arrow for query execution regardless of whether Arrow is enabled in PySpark.
:::

## Topics

<PageList :data="data" :prefix="['guide', 'udf']" />

<script setup>
import PageList from "@theme/components/PageList.vue";
import PySparkApi from '@theme/components/PySparkApi.vue';
import { data } from "./index.data.ts";
</script>
