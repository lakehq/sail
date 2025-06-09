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
.diagram {
  @apply my-4 overflow-scroll;
}

:deep(svg) {
  @apply mx-auto;
}

:deep(svg) > .graph > path {
  @apply fill-white stroke-none dark:fill-zinc-800 dark:stroke-none;
}

:deep(svg) path {
  @apply stroke-gray-800 dark:stroke-gray-200;
}

:deep(svg) circle {
  @apply stroke-gray-800 dark:stroke-gray-200;
}

:deep(svg) rect {
  @apply stroke-gray-800 dark:stroke-gray-200;
}

:deep(svg) text {
  @apply fill-gray-800 dark:fill-gray-200;
}

:deep(svg) .node circle:not([fill="none"]) {
  @apply fill-gray-800 dark:fill-gray-200;
}

:deep(svg) .node rect:not([fill="none"]) {
  @apply fill-gray-800 dark:fill-gray-200;
}

:deep(svg) .edge path:not([fill="none"]) {
  @apply fill-gray-800 dark:fill-gray-200;
}

:deep(svg) .node path {
  @apply fill-gray-200 dark:fill-gray-700;
}
</style>
