---
title: Deployment
rank: 9
---

# Deployment

<!--@include: ./_common/support.md-->

Sail can run locally, in a container, or on Kubernetes.
Start with [Architecture](./architecture.md) for how the server, sessions, and workers run on Kubernetes.
The [Kubernetes guide](./kubernetes.md) covers a local cluster walkthrough and the production resources in `deployments/production/`, which you adapt to your environment.

See [Performance Tuning](/guide/performance/) for settings that affect query performance in any mode.
See [Observability](/guide/observability/) for logs, the system catalog, and telemetry.

<PageList :data="data" :prefix="['guide', 'deployment']" />

<script setup>
import PageList from "@theme/components/PageList.vue";
import { data } from "./index.data.ts";
</script>
