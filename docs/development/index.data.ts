import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/development/!(index).md",
  "/development/*/**/*.md",
]);
