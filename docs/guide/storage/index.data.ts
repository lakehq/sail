import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/guide/storage/!(index|_*/**|**/_*/**).md",
]);
