import { createVegaLoader } from "../../.vitepress/theme/utils/vega.ts";

export default createVegaLoader(["./*.vega.json"], import.meta.dirname, {
  "resource-utilization.vega.json": {
    spark: [{ name: "engine", value: "Spark" }],
    sail: [{ name: "engine", value: "Sail" }],
  },
});
