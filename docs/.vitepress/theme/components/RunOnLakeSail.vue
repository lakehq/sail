<template>
  <div class="platform-action">
    <a
      class="platform-button no-icon"
      href="https://platform.lakesail.com/signup"
      target="_blank"
      rel="noopener noreferrer"
      aria-label="Run on LakeSail: create an account (opens in a new tab)"
    >
      <span
        class="platform-mark"
        :style="{ maskImage: `url(${sailLogo})` }"
        aria-hidden="true"
      ></span>
      <span>Run on LakeSail</span>
      <span aria-hidden="true">↗</span>
    </a>
  </div>
</template>

<script setup lang="ts">
import { withBase } from "vitepress";

const sailLogo = withBase("/sail.svg");
</script>

<style scoped>
.platform-action {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  margin: 24px 0 28px;
}

.platform-button {
  display: inline-flex;
  flex-shrink: 0;
  align-items: center;
  justify-content: center;
  gap: 8px;
  min-height: 44px;
  padding: 10px 16px;
  border-radius: 10px;
  background: #1e40af;
  color: #ffffff;
  font-size: 14px;
  font-weight: 600;
  line-height: 24px;
  text-decoration: none;
}

.platform-button:hover {
  background: #1d4ed8;
}

.platform-button:focus-visible {
  outline: 2px solid var(--vp-c-brand-1);
  outline-offset: 4px;
}

.platform-mark {
  width: 24px;
  height: 24px;
  background: currentColor;
  mask-position: center;
  mask-size: contain;
  mask-repeat: no-repeat;
}

.dark .platform-button {
  background: #dce5f2;
  color: #172554;
}

.dark .platform-button:hover {
  background: #eff6ff;
}
</style>
