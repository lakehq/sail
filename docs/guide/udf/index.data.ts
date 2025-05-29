import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/guide/udf/!(index|_*/**|**/_*/**).md",
]);
