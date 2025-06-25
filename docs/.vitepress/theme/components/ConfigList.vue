<template>
  <div class="config-list">
    <div v-for="item in props.items" :key="item.key" class="config-item">
      <div class="property-row">
        <span class="property-name">Key:</span>
        <code class="property-value config-key">{{ item.key }}</code>
      </div>

      <div class="property-row">
        <span class="property-name">Environment Variable:</span>
        <code class="property-value">{{ configEnvVar(item.key) }}</code>
      </div>

      <div class="property-row">
        <span class="property-name">Type:</span>
        <span :class="`property-label config-type-${item.type}`">{{
          item.type
        }}</span>
      </div>

      <div class="property-row">
        <span class="property-name">Default Value:</span>
        <code v-if="item.default" class="property-value">{{
          item.default
        }}</code>
        <span v-else class="property-value">(empty)</span>
      </div>

      <div class="description">
        <MarkdownBlock :raw="item.description" />
      </div>

      <div v-if="item.experimental" class="experimental-note">
        <span>This option may change in future versions without notice.</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { ConfigItem } from "../utils/config";
import MarkdownBlock from "./MarkdownBlock.vue";

const props = defineProps<{
  items: ConfigItem[];
}>();

function configEnvVar(key: string): string {
  return `SAIL_${key.toUpperCase().replace(/\./g, "__")}`;
}
</script>

<style scoped>
@reference "../app.css";

.config-list {
  @apply overflow-hidden rounded-md border border-gray-200 dark:border-gray-700;
}

.config-item {
  @apply border-b border-gray-200 p-4 hover:bg-gray-50 dark:border-gray-700 dark:hover:bg-gray-800;
}

.config-item:last-child {
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

.config-key {
  @apply font-bold;
}

.config-type-string {
  @apply bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200;
}

.config-type-number {
  @apply bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200;
}

.config-type-boolean {
  @apply bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200;
}

.config-type-array {
  @apply bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200;
}

.config-type-map {
  @apply bg-pink-100 text-pink-800 dark:bg-pink-900 dark:text-pink-200;
}

.description {
  @apply mb-2;
}

.experimental-note {
  @apply mb-2 text-sm text-orange-600 dark:text-orange-200;
  @apply before:mr-1 before:content-['⚠'];
}
</style>
