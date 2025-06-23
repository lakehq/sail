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
.config-list {
  overflow: hidden;
  border-radius: 0.375rem;
  border: 1px solid;
  border-color: rgb(229, 231, 235);
}

.dark .config-list {
  border-color: rgb(55, 65, 81);
}

.config-item {
  border-bottom: 1px solid;
  border-color: rgb(229, 231, 235);
  padding: 1rem;
}

.config-item:hover {
  background-color: rgb(249, 250, 251);
}

.dark .config-item {
  border-color: rgb(55, 65, 81);
}

.dark .config-item:hover {
  background-color: rgb(31, 41, 55);
}

.config-item:last-child {
  border-bottom: none;
}

.property-row {
  margin-bottom: 0.5rem;
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  gap: 0.5rem;
}

.property-name {
  font-size: 0.75rem;
  font-weight: 500;
  white-space: nowrap;
  color: rgb(107, 114, 128);
}

.dark .property-name {
  color: rgb(156, 163, 175);
}

.property-value {
  word-break: break-all;
}

.property-label {
  margin-top: 0.25rem;
  margin-bottom: 0.25rem;
  border-radius: 0.25rem;
  padding-left: 0.5rem;
  padding-right: 0.5rem;
  padding-top: 0.125rem;
  padding-bottom: 0.125rem;
  font-size: 0.75rem;
  font-weight: 600;
}

.config-key {
  font-weight: bold;
}

.config-type-string {
  background-color: rgb(219, 234, 254);
  color: rgb(30, 64, 175);
}

.dark .config-type-string {
  background-color: rgb(30, 58, 138);
  color: rgb(191, 219, 254);
}

.config-type-number {
  background-color: rgb(220, 252, 231);
  color: rgb(22, 101, 52);
}

.dark .config-type-number {
  background-color: rgb(20, 83, 45);
  color: rgb(187, 247, 208);
}

.config-type-boolean {
  background-color: rgb(254, 249, 195);
  color: rgb(113, 63, 18);
}

.dark .config-type-boolean {
  background-color: rgb(113, 63, 18);
  color: rgb(254, 240, 138);
}

.config-type-array {
  background-color: rgb(243, 232, 255);
  color: rgb(107, 33, 168);
}

.dark .config-type-array {
  background-color: rgb(88, 28, 135);
  color: rgb(233, 213, 255);
}

.config-type-map {
  background-color: rgb(252, 231, 243);
  color: rgb(157, 23, 77);
}

.dark .config-type-map {
  background-color: rgb(112, 26, 117);
  color: rgb(251, 207, 232);
}

.description {
  margin-bottom: 0.5rem;
}

.experimental-note {
  margin-bottom: 0.5rem;
  font-size: 0.875rem;
  color: rgb(194, 65, 12);
}

.dark .experimental-note {
  color: rgb(254, 215, 170);
}

.experimental-note::before {
  content: "⚠";
  margin-right: 0.25rem;
}
</style>
