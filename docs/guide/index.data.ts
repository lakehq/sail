import { createContentLoader } from "vitepress";

export default createContentLoader(["/guide/!(index).md", "/guide/*/**/*.md"]);
