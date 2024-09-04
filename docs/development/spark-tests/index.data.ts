import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/development/spark-tests/!(index).md",
  "/development/spark-tests/*/**/*.md",
]);
