<template>
  <a
    :href="`https://spark.apache.org/docs/latest/api/python/reference/${base(props.name)}/${props.name}.html`"
    target="_blank"
    ><code>{{ name }}</code></a
  >
</template>

<script setup lang="ts">
const props = defineProps<{
  name: string;
}>();

function base(name: string): string {
  if (name.startsWith("pyspark.sql.")) {
    return "pyspark.sql/api";
  } else if (name.startsWith("pyspark.pandas.")) {
    return "pyspark.pandas/api";
  } else if (name.startsWith("pyspark.streaming.")) {
    return "pyspark.ss/api";
  } else {
    return "api";
  }
}
</script>
