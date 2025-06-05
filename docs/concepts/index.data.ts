import { createContentLoader } from "vitepress";

export default createContentLoader(["/concepts/!(index|_*/**|**/_*/**).md"]);
