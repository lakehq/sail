---
title: Iceberg
rank: 2
---

# Iceberg

You can use the `iceberg` format in Sail to work with [Apache Iceberg](https://iceberg.apache.org/).
You can use the Spark DataFrame API or Spark SQL to read and write Iceberg tables.
Sail supports format versions 1, 2, and 3 with Parquet data files, including snapshot reads, schema evolution, time travel, and DML statements.
New Iceberg tables created in Sail default to version 2. See [Supported Features](./features) for details by version and operation.

## Topics

<PageList :data="data" :prefix="['guide', 'sources', 'iceberg']" />

<script setup>
import PageList from "@theme/components/PageList.vue";
import { data } from "./index.data.ts";
</script>
