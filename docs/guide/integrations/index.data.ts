import { createContentLoader } from "vitepress";

export default createContentLoader([
  "/guide/integrations/!(index|_*/**|**/_*/**).md",
]);
