<template>
  <div>
    <div v-for="db in databases" :key="db.name">
      <h2 :id="`database-${db.name}`">
        <code>system.{{ db.name }}</code>
        <a
          class="header-anchor"
          :href="`#database-${db.name}`"
          :aria-label="`Permalink to &quot;system.${db.name}&quot;`"
        ></a>
      </h2>
      <div class="mt-4">
        <MarkdownBlock :raw="db.description" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { SystemDatabase } from "@theme/utils/system-catalog";
import MarkdownBlock from "./MarkdownBlock.vue";

defineProps<{
  databases: SystemDatabase[];
}>();
</script>
