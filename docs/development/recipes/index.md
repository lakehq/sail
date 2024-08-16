---
title: Recipes
rank: 4
---

# Recipes

<PageList :data="data" :prefix="['development', 'recipes']" />

<script setup>
import PageList from "@theme/components/PageList.vue";
import { data } from "./index.data.ts";
</script>