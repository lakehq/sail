import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/development/recipes/!(index|_*/**|**/_*/**).md",
]);
