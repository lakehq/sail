import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/guide/deployment/docker-images/!(index|_*/**|**/_*/**).md",
]);
