---
title: Delta Lake
rank: 1
---

# Delta Lake

Sail reads and writes [Delta Lake](https://delta.io/) tables through the `delta` format, using either the Spark DataFrame API or Spark SQL. You can read the current snapshot, evolve the table schema, query earlier versions, and change rows with `DELETE`, `UPDATE`, or `MERGE INTO`. Sail also provides batch Change Data Feed reads, table inspection, file compaction, and vacuuming. The [supported features](./features) page covers protocol requirements, catalog-specific DDL, and operation limits.

## Topics

<PageList :data="data" :prefix="['guide', 'sources', 'delta']" />

<script setup>
import PageList from "@theme/components/PageList.vue";
import { data } from "./index.data.ts";
</script>
