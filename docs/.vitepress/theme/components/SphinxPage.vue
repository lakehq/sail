<template>
  <slot></slot>
</template>

<script setup></script>

<style scoped>
@reference "../app.css";

/*
HTML such as `<span> </span>` is incorrectly parsed as `<span></span>` in Vue.
This is a workaround for this Vue issue.
Note that this only fixes the display issue, but the text selection is still broken.

See also: https://github.com/vuejs/core/issues/7789
*/
:slotted(span.w)::before {
  content: " ";
}

/* Sphinx styles translated to Tailwind CSS */
/*
Note that we define "scoped" styles here, so we must use "slotted"
to apply styles to the content inside the slot.
*/

:slotted(.rubric) {
  @apply text-xl font-bold;
}

:slotted(a.headerlink) {
  @apply invisible px-2 text-slate-200 no-underline hover:text-slate-600;
}

:slotted(h1:hover > a.headerlink),
:slotted(h2:hover > a.headerlink),
:slotted(h3:hover > a.headerlink),
:slotted(h4:hover > a.headerlink),
:slotted(h5:hover > a.headerlink),
:slotted(h6:hover > a.headerlink),
:slotted(dt:hover > a.headerlink),
:slotted(caption:hover > a.headerlink),
:slotted(p.caption:hover > a.headerlink),
:slotted(div.code-block-caption:hover > a.headerlink),
:slotted(div.math:hover > a.headerlink) {
  @apply visible;
}
</style>
