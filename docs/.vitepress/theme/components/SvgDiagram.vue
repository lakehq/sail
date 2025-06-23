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
  margin-top: 1rem;
  margin-bottom: 1rem;
  overflow: scroll;
}

:deep(svg) {
  margin-left: auto;
  margin-right: auto;
}

:deep(svg.viz) > .graph > path {
  fill: white;
  stroke: none;
}

.dark :deep(svg.viz) > .graph > path {
  fill: rgb(39, 39, 42);
  stroke: none;
}

:deep(svg.viz) path {
  stroke: rgb(31, 41, 55);
}

.dark :deep(svg.viz) path {
  stroke: rgb(229, 231, 235);
}

:deep(svg.viz) circle {
  stroke: rgb(31, 41, 55);
}

.dark :deep(svg.viz) circle {
  stroke: rgb(229, 231, 235);
}

:deep(svg.viz) rect {
  stroke: rgb(31, 41, 55);
}

.dark :deep(svg.viz) rect {
  stroke: rgb(229, 231, 235);
}

:deep(svg.viz) text {
  fill: rgb(31, 41, 55);
}

.dark :deep(svg.viz) text {
  fill: rgb(229, 231, 235);
}

:deep(svg.viz) .node circle:not([fill="none"]) {
  fill: rgb(31, 41, 55);
}

.dark :deep(svg.viz) .node circle:not([fill="none"]) {
  fill: rgb(229, 231, 235);
}

:deep(svg.viz) .node rect:not([fill="none"]) {
  fill: rgb(31, 41, 55);
}

.dark :deep(svg.viz) .node rect:not([fill="none"]) {
  fill: rgb(229, 231, 235);
}

:deep(svg.viz) .edge path:not([fill="none"]) {
  fill: rgb(31, 41, 55);
}

.dark :deep(svg.viz) .edge path:not([fill="none"]) {
  fill: rgb(229, 231, 235);
}

:deep(svg.viz) .node path {
  fill: rgb(229, 231, 235);
}

.dark :deep(svg.viz) .node path {
  fill: rgb(55, 65, 81);
}

@media (prefers-color-scheme: dark) {
  :deep(svg.vega) {
    filter: invert(1) hue-rotate(180deg);
  }
}
</style>
