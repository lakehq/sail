<template>
  <!-- eslint-disable-next-line vue/no-v-html -->
  <div v-if="props.raw" v-html="md.render(raw)"></div>
</template>

<script setup lang="ts">
import MarkdownIt from "markdown-it";

const props = defineProps<{
  raw?: string;
}>();

const md = new MarkdownIt();
</script>
