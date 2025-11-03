import fs from "fs/promises";

import yaml from "js-yaml";

import type { ConfigItem } from "../../.vitepress/theme/utils/config";
import {
  APP_CONFIG_PATH,
  buildConfigGroups,
} from "../../.vitepress/theme/utils/config";

export default {
  async load() {
    const content = await fs.readFile(APP_CONFIG_PATH, "utf-8");
    const items = yaml.load(content) as ConfigItem[];
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
          id: "parquet",
          title: "Parquet Options",
          pattern: /^parquet\./,
        },
        {
          id: "catalog",
          title: "Catalog Options",
          pattern: /^catalog\./,
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
