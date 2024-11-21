import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/development/setup/!(index|_*/**|**/_*/**).md",
]);
