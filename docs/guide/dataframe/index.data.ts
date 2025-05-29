import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/guide/dataframe/!(index|_*/**|**/_*/**).md",
]);
