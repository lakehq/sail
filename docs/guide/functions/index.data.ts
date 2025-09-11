import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/guide/functions/!(index|_*/**|**/_*/**).md",
]);
