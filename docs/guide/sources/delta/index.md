---
title: Delta Lake
rank: 1
---

# Delta Lake

Sail reads and writes [Delta Lake](https://delta.io/) tables through the `delta` format, using either the Spark DataFrame API or Spark SQL. You can read the current snapshot, evolve the table schema, query earlier versions, and change rows with `DELETE`, `UPDATE`, or `MERGE INTO`. The [supported features](./features) page covers protocol requirements and operation details.

## Topics

<PageList :data="data" :prefix="['guide', 'sources', 'delta']" />

<script setup>
import PageList from "@theme/components/PageList.vue";
import { data } from "./index.data.ts";
</script>
