---
outline: false
description: >-
  Sail: unifying batch processing, stream processing,
  and compute-intensive (AI) workloads.
---

# Welcome

**Sail** <img class="welcome-sail-logo" :src="sailLogo" alt="" /> is an open-source multimodal compute engine designed for both single-host and distributed settings. Our mission is to unify batch processing, stream processing, and compute-intensive AI workloads.

You can start with the [Introduction](/introduction/) to get an overview of Sail.

The [User Guide](/guide/) provides detailed information on how to use Sail.

The [Concepts](/concepts/) pages discuss topics around how Sail works.

The [Development](/development/) handbook covers the development process and Sail internals, if you are interested in contributing to Sail.

The [Reference](/reference/) can be handy when you want to look up the Sail API documentation.

<script setup>
import { withBase } from "vitepress";

const sailLogo = withBase("/sail.svg");
</script>

<style>
.vp-doc .welcome-sail-logo {
  display: inline-block;
  width: 1.5rem;
  height: 1.5rem;
  margin: 0;
  vertical-align: -0.25rem;
}

.dark .welcome-sail-logo {
  filter: brightness(0) invert(1);
}
</style>
