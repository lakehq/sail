import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/development/build/!(index|_*/**|**/_*/**).md",
]);
