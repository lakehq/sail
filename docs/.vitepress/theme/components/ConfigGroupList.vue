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
    <ConfigList :items="group.items" />
  </div>
</template>

<script setup lang="ts">
import type { ConfigGroup } from "@theme/utils/config";
import ConfigList from "./ConfigList.vue";

const props = defineProps<{
  groups: ConfigGroup[];
}>();
</script>
