import { createGraphvizLoader } from "../../.vitepress/theme/utils/graphviz";

export default createGraphvizLoader(["./*.dot"], __dirname, { scale: 0.5 });
