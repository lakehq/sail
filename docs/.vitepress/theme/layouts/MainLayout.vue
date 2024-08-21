<template>
  <Layout>
    <template #nav-bar-title-after>
      <div class="ml-2">
        <span class="text-sm font-normal text-gray-500">{{ libVersion }}</span>
      </div>
    </template>
    <template #doc-before>
      <div class="vp-doc external-link-icon-enabled">
        <div
          v-if="version !== 'latest' && !isDevelopment"
          class="warning custom-block py-4"
        >
          <p>
            This is
            {{
              version === "main" ? "an unreleased version" : "an old version"
            }}
            of Sail. Please visit
            <a :href="latestBase" target="_blank">here</a>
            for the documentation of the latest stable version.
          </p>
        </div>
        <div
          v-if="version !== 'main' && isDevelopment"
          class="warning custom-block py-4"
        >
          <p>
            This is a snapshot of the development guide for a released Sail
            version. For up-to-date information, please visit the development
            guide for the <code>main</code> branch
            <a :href="`${mainBase}development/`" target="_blank"> here</a>.
          </p>
        </div>
        <div
          v-if="version === 'main' && isDevelopment"
          class="info custom-block py-4"
        >
          <p>
            This guide is up-to-date with the
            <code>main</code> branch.
          </p>
        </div>
      </div>
    </template>
  </Layout>
</template>

<script setup lang="ts">
import { useData, useRoute } from "vitepress";
import DefaultTheme from "vitepress/theme";
import { computed } from "vue";

const { Layout } = DefaultTheme;
const { site } = useData();
const route = useRoute();

const version = computed(() => site.value.contentProps?.version);
const libVersion = computed(() => site.value.contentProps?.libVersion);
const isDevelopment = computed(() =>
  route.data.relativePath.startsWith("development/"),
);

const switchBase = (base: string, version: string): string => {
  return base.replace(/\/[^/]+\/$/, `/${version}/`);
};

const latestBase = computed(() => switchBase(site.value.base, "latest"));
const mainBase = computed(() => switchBase(site.value.base, "main"));
</script>
