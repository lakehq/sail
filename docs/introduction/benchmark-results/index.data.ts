import { createVegaLoader } from "../../.vitepress/theme/utils/vega.ts";

export default createVegaLoader(["./*.vega.json"], import.meta.dirname, {
  "resource-utilization.vega.json": {
    spark: [{ name: "engine", value: "Spark" }],
    sail: [{ name: "engine", value: "Sail" }],
  },
  "tpcds-query-time.vega.json": {
    "1tb": [{ name: "dataset", value: "1 TB" }],
    "3tb": [{ name: "dataset", value: "3 TB" }],
  },
});
