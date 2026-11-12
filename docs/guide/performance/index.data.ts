import { createContentLoader } from "vitepress";

export default createContentLoader([
  "guide/performance/!(index|_*/**|**/_*/**).md",
]);
