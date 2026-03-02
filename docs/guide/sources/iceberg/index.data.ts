import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/guide/sources/iceberg/!(index|_*/**|**/_*/**).md",
]);
