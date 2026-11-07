<template>
  <div v-for="group in props.groups" :key="group.id">
    <h2 :id="group.id">
      {{ group.title }}
      <a
        class="header-anchor"
        :href="`#${group.id}`"
        :aria-label="`Permalink to &quot;${group.title}&quot;`"
      ></a>
    </h2>
    <div class="metric-list">
      <div
        v-for="metric in group.metrics"
        :key="metric.name"
        class="metric-item"
      >
        <div class="property-row">
          <span class="property-name">Name:</span>
          <code class="property-value metric-name">{{ metric.name }}</code>
        </div>

        <div class="property-row">
          <span class="property-name">Type:</span>
          <span class="property-label metric-type">{{ metric.type }}</span>
        </div>

        <div v-if="metric.unit" class="property-row">
          <span class="property-name">Unit:</span>
          <span class="property-value">{{ metric.unit }}</span>
        </div>

        <div class="description">
          <MarkdownBlock :raw="metric.description" />
        </div>

        <div v-if="metric.attributes.length > 0" class="property-row">
          <span class="property-name">Attributes:</span>
          <span class="property-value">
            <template
              v-for="(attribute, index) in metric.attributes"
              :key="attribute"
            >
              <code>{{ attribute }}</code>
              <span v-if="index < metric.attributes.length - 1">, </span>
            </template>
          </span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { MetricGroup } from "@theme/utils/telemetry-metrics";
import MarkdownBlock from "./MarkdownBlock.vue";

const props = defineProps<{
  groups: MetricGroup[];
}>();
</script>

<style scoped>
@reference "../app.css";

.metric-list {
  @apply overflow-hidden rounded-md border border-gray-200 dark:border-gray-700;
}

.metric-item {
  @apply border-b border-gray-200 p-4 hover:bg-gray-50 dark:border-gray-700 dark:hover:bg-gray-800;
}

.metric-item:last-child {
  @apply border-b-0;
}

.property-row {
  @apply mb-2 flex flex-wrap items-baseline gap-x-2;
}

.property-name {
  @apply text-xs font-medium whitespace-nowrap text-gray-500 dark:text-gray-400;
}

.property-value {
  @apply break-all;
}

.property-label {
  @apply my-1 rounded px-2 py-0.5 text-xs font-semibold;
}

.metric-name {
  @apply font-bold;
}

.metric-type {
  @apply bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200;
}

.description {
  @apply mb-2;
}
</style>
