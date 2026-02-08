---
title: Databases
rank: 1
---

# Databases

Here is the list of databases available in the system catalog.

<SystemDatabaseList :databases="data" />

<script setup lang="ts">
import SystemDatabaseList from "@theme/components/SystemDatabaseList.vue";
import { data } from "./databases.data.ts";
</script>
