---
title: Iceberg
rank: 2
---

# Iceberg

Sail reads and writes [Apache Iceberg](https://iceberg.apache.org/) tables through the `iceberg` format, using either the Spark DataFrame API or Spark SQL. It supports Parquet tables in format versions 1, 2, and 3, including snapshot reads, schema evolution, time travel, and DML statements. New tables use version 2 by default. See [supported features](./features) for details by version and operation.

## Topics

<PageList :data="data" :prefix="['guide', 'sources', 'iceberg']" />

<script setup>
import PageList from "@theme/components/PageList.vue";
import { data } from "./index.data.ts";
</script>
