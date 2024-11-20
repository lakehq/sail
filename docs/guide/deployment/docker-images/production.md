---
title: Production
rank: 2
---

# Production

Building the Sail Docker image from source is recommended when performance is critical for your application.

::: info

The release build may take some time to complete.

:::

In an empty directory, create a `Dockerfile` with the following content.

::: code-group

<<< ../../../../docker/release/Dockerfile{docker}

:::

In the same directory, run the following command with the desired release tag or branch name to build the Docker image.

```bash-vue
docker build -t sail:latest --build-arg RELEASE_TAG="{{ libVersion }}" .
```

<script setup>
import { useData } from "vitepress";
import { computed } from "vue";

const { site } = useData();

const libVersion = computed(() => site.value.contentProps?.libVersion);
</script>
