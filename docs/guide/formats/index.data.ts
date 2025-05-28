import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/guide/formats/!(index|_*/**|**/_*/**).md",
]);
