import {
  buildConfigGroups,
  loadConfigItems,
} from "../../.vitepress/theme/utils/config";

export default {
  async load() {
    const items = await loadConfigItems();
    const grouping = {
      groups: [
        {
          id: "core",
          title: "Core Options",
          pattern: /^(mode)$/,
        },
        {
          id: "cluster",
          title: "Cluster Options",
          pattern: /^cluster\./,
        },
        {
          id: "kubernetes",
          title: "Kubernetes Options",
          pattern: /^kubernetes\./,
        },
        {
          id: "runtime",
          title: "Runtime Options",
          pattern: /^runtime\./,
        },
        {
          id: "spark",
          title: "Spark Options",
          pattern: /^spark\./,
        },
        {
          id: "execution",
          title: "Execution Options",
          pattern: /^execution\./,
        },
        {
          id: "optimizer",
          title: "Optimizer Options",
          pattern: /^optimizer\./,
        },
        {
          id: "parquet",
          title: "Parquet Options",
          pattern: /^parquet\./,
        },
        {
          id: "catalog",
          title: "Catalog Options",
          pattern: /^catalog\./,
        },
        {
          id: "telemetry",
          title: "Telemetry Options",
          pattern: /^telemetry\./,
        },
      ],
      fallbackGroup: {
        id: "other",
        title: "Other Options",
      },
    };
    return buildConfigGroups(items, grouping)
      .map((group) => {
        return {
          id: group.id,
          title: group.title,
          items: group.items.filter((item) => !item.hidden),
        };
      })
      .filter((group) => group.items.length > 0);
  },
};
