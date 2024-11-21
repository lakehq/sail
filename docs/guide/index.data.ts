import { createContentLoader } from "vitepress";

export default createContentLoader(["/guide/!(index|_*/**|**/_*/**).md"]);
