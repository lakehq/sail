import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/guide/deployment/!(index).md",
  "/guide/deployment/*/**/*.md",
]);
