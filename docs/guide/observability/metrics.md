---
title: Metrics
rank: 1
---

# Metrics

The following sections list the metrics that Sail defines.
Sail reports a metric only when the operator or service that produces it runs.
Currently, Sail does not report `execution.memory_used`.
Refer to the [Observability](./index.md#telemetry) page for how to enable metrics export.

<MetricGroupList :groups="data" />

<script setup lang="ts">
import MetricGroupList from "@theme/components/MetricGroupList.vue";
import { data } from "./metrics.data.ts";
</script>
