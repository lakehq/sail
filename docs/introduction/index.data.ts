import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/introduction/!(index|_*/**|**/_*/**).md",
]);
