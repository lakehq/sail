---
title: Building Docker Images
rank: 1
---

# Building Docker Images

Deploying Sail in cluster environments (e.g. Kubernetes) typically involves launching Sail applications inside containers. This guide presents various methods to build Docker images for Sail.

<PageList :data="data" :prefix="['guide', 'deployment', 'docker-images']" />

<script setup>
import PageList from "@theme/components/PageList.vue";
import { data } from "./index.data.ts";
</script>
