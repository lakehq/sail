<template>
  <ul>
    <li v-for="tree in props.trees" :key="tree.data.url">
      <a :href="withBase(tree.data.url)">{{ tree.data.title }}</a>
      <PageTreeList v-if="tree.children.length > 0" :trees="tree.children" />
    </li>
  </ul>
</template>

<script setup lang="ts">
import { withBase } from "vitepress";

import type { PageLink } from "../utils/link";
import type { TreeNode } from "../utils/tree";

const props = defineProps<{
  trees: TreeNode<PageLink>[];
}>();
</script>
