---
title: Recipes
rank: 5
---

# Recipes

<PageList :data="data" :prefix="['development', 'recipes']" />

<script setup>
import PageList from "@theme/components/PageList.vue";
import { data } from "./index.data.ts";
</script>
