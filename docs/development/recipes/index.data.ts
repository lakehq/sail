import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/development/recipes/!(index).md",
  "/development/recipes/*/**/*.md",
]);
