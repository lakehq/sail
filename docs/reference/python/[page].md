---
titleTemplate: Sail Python API Reference
---

<div v-if="params.children">

# Index

<SphinxIndexPage :links="params.children" />

</div>

<SphinxPage>
<!-- @content -->
</SphinxPage>

<script setup lang="ts">
import { useData } from "vitepress"

import SphinxIndexPage from "@theme/components/SphinxIndexPage.vue"
import SphinxPage from "@theme/components/SphinxPage.vue"

const { params } = useData()
</script>
