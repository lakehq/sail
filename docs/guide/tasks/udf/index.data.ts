import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/guide/tasks/udf/!(index|_*/**|**/_*/**).md",
]);
