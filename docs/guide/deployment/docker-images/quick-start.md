---
title: Quick Start
rank: 1
---

# Quick Start

You can quickly experiment with Sail by building the Docker image from the Sail Python package.

::: info
You may want to build the Sail Docker image from source for production deployments.
Please follow [this guide](./production.md) for more information.
:::

In an empty directory, create a `Dockerfile` with the following content.

::: code-group

<<< ../../../../docker/quickstart/Dockerfile{docker}

:::

In the same directory, run the following command to build the Docker image.

```bash-vue
docker build -t sail:latest --build-arg PYSAIL_VERSION={{ libVersion }} .
```

<script setup>
import { useData } from "vitepress";
import { computed } from "vue";

const { site } = useData();

const libVersion = computed(() => site.value.contentProps?.libVersion);
</script>
