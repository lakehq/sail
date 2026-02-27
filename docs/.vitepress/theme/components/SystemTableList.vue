<template>
  <div>
    <div v-for="table in tables" :key="table.key">
      <h2 :id="`table-${table.key}`">
        <code>system.{{ table.database }}.{{ table.name }}</code>
        <a
          class="header-anchor"
          :href="`#table-${table.key}`"
          :aria-label="`Permalink to &quot;system.${table.database}.${table.name}&quot;`"
        ></a>
      </h2>

      <div class="mt-4">
        <MarkdownBlock :raw="table.description" />
      </div>

      <div class="mt-6">
        <h3>Columns</h3>
        <ul class="m-0 list-none p-0">
          <li v-for="col in table.columns" :key="col.name" class="py-1">
            <div class="flex items-baseline gap-2">
              <span
                ><code>{{ col.name }}</code></span
              >
              <span
                class="rounded border border-zinc-200 bg-zinc-100 px-1.5 py-0.5 font-mono text-sm text-zinc-600 dark:border-zinc-700 dark:bg-zinc-800 dark:text-zinc-400"
              >
                {{ col.sql_type }}{{ col.nullable ? "" : " NOT NULL" }}
              </span>
            </div>
            <div class="mt-0.5">
              <MarkdownBlock :raw="col.description" />
            </div>
          </li>
        </ul>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";

import type { SystemDatabase, SystemTable } from "@theme/utils/system-catalog";
import MarkdownBlock from "./MarkdownBlock.vue";

const props = defineProps<{
  databases: SystemDatabase[];
}>();

const tables = computed(() => {
  const result: (SystemTable & { database: string; key: string })[] = [];
  for (const db of props.databases) {
    for (const table of db.tables) {
      result.push({
        ...table,
        database: db.name,
        key: `${db.name}-${table.name}`,
      });
    }
  }
  return result;
});
</script>
