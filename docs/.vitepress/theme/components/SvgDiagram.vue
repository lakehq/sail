<template>
  <!-- eslint-disable-next-line vue/no-v-html -->
  <div v-if="props.svg" class="diagram" v-html="svg"></div>
</template>

<script setup lang="ts">
const props = defineProps<{
  svg?: string;
}>();
</script>

<style scoped>
@reference "../app.css";

.diagram {
  @apply my-4 overflow-scroll;
}

:deep(svg) {
  @apply mx-auto;
}

:deep(svg.viz) > .graph > path {
  @apply fill-white stroke-none dark:fill-zinc-800 dark:stroke-none;
}

:deep(svg.viz) path {
  @apply stroke-gray-800 dark:stroke-gray-200;
}

:deep(svg.viz) circle {
  @apply stroke-gray-800 dark:stroke-gray-200;
}

:deep(svg.viz) rect {
  @apply stroke-gray-800 dark:stroke-gray-200;
}

:deep(svg.viz) text {
  @apply fill-gray-800 dark:fill-gray-200;
}

:deep(svg.viz) .node circle:not([fill="none"]) {
  @apply fill-gray-800 dark:fill-gray-200;
}

:deep(svg.viz) .node rect:not([fill="none"]) {
  @apply fill-gray-800 dark:fill-gray-200;
}

:deep(svg.viz) .edge path:not([fill="none"]) {
  @apply fill-gray-800 dark:fill-gray-200;
}

:deep(svg.viz) .node path {
  @apply fill-gray-200 dark:fill-gray-700;
}

@media (prefers-color-scheme: dark) {
  :deep(svg.vega) {
    filter: invert(1) hue-rotate(180deg);
  }
}
</style>
