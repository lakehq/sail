---
title: Spark DataFrame API
rank: 1
---

# Spark DataFrame API

You can use the Spark DataFrame API to work with structured data. Here is an example of reading a Parquet dataset and querying it in PySpark.

<!--@include: ../_common/spark-session.md-->

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()

df = spark.read.parquet("/data/users.parquet")
df.filter(col("age") > 30).select("name", "email").show()
```

## Topics

<PageList :data="data" :prefix="['guide', 'dataframe']" />

<script setup>
import PageList from "@theme/components/PageList.vue";
import { data } from "./index.data.ts";
</script>
