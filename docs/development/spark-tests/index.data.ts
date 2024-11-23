import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/development/spark-tests/!(index|_*/**|**/_*/**).md",
]);
