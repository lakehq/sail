import {
  buildMetricGroups,
  loadMetrics,
} from "../../.vitepress/theme/utils/telemetry-metrics.ts";

export default {
  async load() {
    const metrics = await loadMetrics();
    return buildMetricGroups(metrics, {
      groups: [
        {
          id: "execution",
          title: "Execution Metrics",
          pattern: /^execution\./,
        },
        {
          id: "flight",
          title: "Arrow Flight SQL Metrics",
          pattern: /^flight\./,
        },
      ],
      fallbackGroup: {
        id: "other",
        title: "Other Metrics",
      },
    });
  },
};
