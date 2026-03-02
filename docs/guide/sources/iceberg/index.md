---
title: Iceberg
rank: 2
---

# Iceberg

You can use the `iceberg` format in Sail to work with [Apache Iceberg](https://iceberg.apache.org/).
You can use the Spark DataFrame API or Spark SQL to read and write Iceberg tables.

## Topics

<PageList :data="data" :prefix="['guide', 'sources', 'iceberg']" />

<script setup>
import PageList from "@theme/components/PageList.vue";
import { data } from "./index.data.ts";
</script>
