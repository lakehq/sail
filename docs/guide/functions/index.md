---
title: Functions and Operators
rank: 3
---

# Built-in Spark Functions and Operators

Sail supports built-in Spark functions and operators that can be used in your data processing tasks.
Below is a list of these functions and operators, categorized by their functionality.
This list is updated as new functions are added and existing ones are improved.
If you don't see a function you need, please open an issue on
the [Sail GitHub repository](https://github.com/lakehq/sail).

## Categories

<PageList :data="data" :prefix="['guide', 'functions']" />

<script setup>
import PageList from "@theme/components/PageList.vue";
import { data } from "./index.data.ts";
</script>
