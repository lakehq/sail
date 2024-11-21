import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/guide/deployment/!(index|_*/**|**/_*/**).md",
]);
