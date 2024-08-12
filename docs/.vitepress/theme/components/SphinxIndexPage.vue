<template>
  <ul>
    <li v-for="link in props.links" :key="link.link">
      <a :href="withBase(link.link)">{{ link.text }}</a>
    </li>
  </ul>
</template>

<script setup lang="ts">
import { withBase } from "vitepress";

const props = defineProps<{
  links: { link: string; text: string }[];
}>();
</script>
