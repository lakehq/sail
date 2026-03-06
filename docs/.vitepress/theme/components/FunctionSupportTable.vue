<template>
  <table>
    <thead>
      <tr>
        <th>Function</th>
        <th>Supported</th>
        <th>Note</th>
      </tr>
    </thead>
    <tbody>
      <tr v-for="item in functions" :key="item.function">
        <td>
          <code>{{ item.function }}</code>
        </td>
        <td>
          <span v-if="item.status === 'supported'">✅</span>
          <span v-else-if="item.status === 'partially supported'"
            >✅ (partial)</span
          >
          <span v-else-if="item.status === 'planned'">🚧</span>
          <span v-else>❌</span>
        </td>
        <td>
          <MarkdownBlock v-if="item.note" :raw="item.note" />
        </td>
      </tr>
    </tbody>
  </table>
</template>

<script setup lang="ts">
import { computed } from "vue";

import type { FunctionInfo } from "@theme/utils/function-support";
import MarkdownBlock from "./MarkdownBlock.vue";

const props = defineProps<{
  data: FunctionInfo[];
}>();

const functions = computed(() => {
  return [...props.data].sort((a, b) => {
    const getPriority = (status: string) => {
      switch (status) {
        case "supported":
        case "partially supported":
          return 0;
        case "planned":
          return 1;
        default:
          return 2;
      }
    };
    const priorityA = getPriority(a.status);
    const priorityB = getPriority(b.status);
    if (priorityA !== priorityB) {
      return priorityA - priorityB;
    }
    return a.function.localeCompare(b.function);
  });
});
</script>
