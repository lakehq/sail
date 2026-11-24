import { createGraphvizLoader } from "../../.vitepress/theme/utils/graphviz.ts";

export default createGraphvizLoader(["./*.dot"], import.meta.dirname, {
  scale: 0.5,
});
