import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/guide/deployment/docker-images/!(index).md",
  "/guide/deployment/docker-images/*/**/*.md",
]);
