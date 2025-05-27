# Configuration

This page lists all the available configuration options for Sail.

::: warning
The Sail configuration system is not stable yet.

For options with the ⚠ note, breaking changes can happen across versions **without notice**.

For all other options, breaking changes can happen across **minor versions** for the 0.x releases. Such changes will be documented in the [changelog](/reference/changelog/).
:::

::: info
The default value of each option is shown as its **string representation** that can be used directly in environment variables.
:::

<ConfigGroupList :groups="data" />

<script setup lang="ts">
import ConfigGroupList from '@theme/components/ConfigGroupList.vue';
import { data } from './index.data.ts';
</script>
