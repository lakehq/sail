---
title: Tables
rank: 2
---

# Tables

Here is the list of tables available in the system catalog.

<SystemTableList :databases="data" />

<script setup lang="ts">
import SystemTableList from "@theme/components/SystemTableList.vue";
import { data } from "./databases.data.ts";
</script>
