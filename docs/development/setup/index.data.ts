import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/development/setup/!(index).md",
  "/development/setup/*/**/*.md",
]);
