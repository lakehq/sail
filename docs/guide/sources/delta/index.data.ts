import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/guide/sources/delta/!(index|_*/**|**/_*/**).md",
]);
