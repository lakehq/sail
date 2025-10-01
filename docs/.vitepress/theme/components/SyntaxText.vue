<template>
  <span class="syntax-text">
    <span v-for="(token, index) in tokens" :key="index" :class="token.class">{{
      token.text
    }}</span>
  </span>
</template>

<!--
This component displays syntax text that roughly uses the BNF notation.
The raw syntax text is passed as a string with the following patterns.
* `|` is used to separate alternatives.
* `[]` is used to denote optional parts.
* `()` is used to denote grouping.
* `*` is used to denote zero or more repeats.
* `+` is used to denote one or more repeats.
* `<name>` is used to denote a non-terminal.
  The surrounding `<` and `>` will not be displayed.
* `'text'` is used to denote terminal text, where a literal `'` can be escaped
  by repeating it twice.
  The surrounding `'` will not be displayed.
* All other character sequences are considered invalid.

The component identifies patterns from the raw text using regex,
and converts the pattern into HTML `<span>` with CSS classes.
It does not attempt to parse the text, so unmatched parentheses, for example,
will not be detected.
-->
<script setup lang="ts">
import { computed } from "vue";

const props = defineProps<{
  raw: string;
}>();

interface Token {
  text: string;
  class: string;
}

const tokens = computed<Token[]>(() => {
  const regex = /(\||\[|\]|\(|\)|\*|\+|<[^>]*>|'(?:[^']|'')*')/g;
  const matches = props.raw.match(regex) || [];
  if (props.raw.length !== matches.join("").length) {
    throw new Error(`invalid syntax text: ${props.raw}`);
  }

  return matches.map((match) => {
    if (match === "|") {
      return { text: match, class: "alternative" };
    } else if (match === "[" || match === "]") {
      return { text: match, class: "optional" };
    } else if (match === "(" || match === ")") {
      return { text: match, class: "grouping" };
    } else if (match === "*" || match === "+") {
      return { text: match, class: "repeat" };
    } else if (match.startsWith("<") && match.endsWith(">")) {
      const name = match.slice(1, -1);
      return { text: name, class: "non-terminal" };
    } else if (match.startsWith("'") && match.endsWith("'")) {
      // unescape doubled single quotes
      const unescaped = match.slice(1, -1).replace(/''/g, "'");
      return { text: unescaped, class: "terminal" };
    } else {
      throw new Error(`unexpected syntax text match: ${match}`);
    }
  });
});
</script>

<style scoped>
@reference "../app.css";

.syntax-text {
  @apply font-mono leading-relaxed;

  .alternative,
  .optional,
  .grouping,
  .repeat {
    @apply text-yellow-700 dark:text-yellow-600;
  }

  .non-terminal {
    @apply font-bold text-red-700 italic dark:text-red-300;
  }

  .terminal {
    color: var(--vp-code-color);
  }
}
</style>
