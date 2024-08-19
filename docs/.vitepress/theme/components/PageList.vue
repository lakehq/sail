<template>
  <PageTreeList :trees="trees" />
</template>

<script setup lang="ts">
import type { ContentData } from "vitepress";
import { computed } from "vue";

import { PageLink } from "../utils/link";
import { TreeNode } from "../utils/tree";
import PageTreeList from "./PageTreeList.vue";

const props = defineProps<{
  data: ContentData[];
  prefix: string[];
}>();

const trees = computed(() => {
  const links = props.data.map((content) => {
    if (content.frontmatter?.title === undefined) {
      throw new Error(
        `page ${content.url} does not have a title in frontmatter`,
      );
    }
    return new PageLink(
      content.url,
      content.frontmatter.title,
      content.frontmatter.rank,
    );
  });
  return TreeNode.fromPaths(links, props.prefix).map((tree) =>
    tree.transform((name: string, page: PageLink | null) => {
      if (page === null) {
        throw new Error(`page with name '${name}' does not exist`);
      }
      return page;
    }),
  );
});
</script>
