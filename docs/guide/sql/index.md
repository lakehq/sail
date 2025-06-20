---
title: Spark SQL
rank: 2
---

# Spark SQL

You can use Spark SQL to run SQL queries on data stored in various formats and storage systems. A common way to get started is to create a temporary view from a DataFrame and then run SQL queries against that view.

<!--@include: ../_common/spark-session.md-->

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()

df = spark.read.parquet("/data/users.parquet")
df.createOrReplaceTempView("users")

spark.sql("SELECT name, email FROM users WHERE age > 30").show()
```

## Topics

<PageList :data="data" :prefix="['guide', 'sql']" />

<script setup>
import PageList from "@theme/components/PageList.vue";
import { data } from "./index.data.ts";
</script>
