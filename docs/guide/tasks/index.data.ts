import { createContentLoader } from "vitepress";

export default createContentLoader(["/guide/tasks/!(index).md", "/guide/tasks/*/**/*.md"]);
