---
title: Delta Lake
rank: 1
---

# Delta Lake

You can use the `delta` format in Sail to work with [Delta Lake](https://delta.io/).
You can use the Spark DataFrame API or Spark SQL to read and write Delta tables.
Sail supports snapshot reads, schema evolution, time travel, and DML statements including `DELETE`, `UPDATE`, and `MERGE INTO`.
See [Supported Features](./features) for protocol and operation support.

## Topics

<PageList :data="data" :prefix="['guide', 'sources', 'delta']" />

<script setup>
import PageList from "@theme/components/PageList.vue";
import { data } from "./index.data.ts";
</script>
