<template>
  <div class="language-text">
    <pre class="vp-code" tabindex="0"><code><slot></slot></code></pre>
  </div>
</template>

<script setup lang="ts"></script>

<style scoped>
@reference "../app.css";

:deep(.syntax-text) {
  @apply block;
}
</style>
