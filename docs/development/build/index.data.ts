import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/development/build/!(index).md",
  "/development/build/*/**/*.md",
]);
