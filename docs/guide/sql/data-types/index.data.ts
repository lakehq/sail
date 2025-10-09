import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/guide/sql/data-types/!(index|_*/**|**/_*/**).md",
]);
