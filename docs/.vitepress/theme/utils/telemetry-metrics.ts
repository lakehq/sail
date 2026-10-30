import fs from "fs/promises";
import path from "path";

import { load } from "js-yaml";

const TELEMETRY_METRICS_PATH = path.join(
  import.meta.dirname,
  "../../../../crates/sail-telemetry/data/metrics/registry.yaml",
);

interface Metric {
  name: string;
  description: string;
  unit?: string;
  type: string;
  value_type: string;
  attributes: string[];
  debug?: boolean;
}

interface MetricGroup {
  id: string;
  title: string;
  metrics: Metric[];
}

interface MetricGrouping {
  groups: {
    id: string;
    title: string;
    pattern: RegExp;
  }[];
  fallbackGroup: {
    id: string;
    title: string;
  };
}

function buildMetricGroups(
  metrics: Metric[],
  grouping: MetricGrouping,
): MetricGroup[] {
  const groups = grouping.groups.map((group) => {
    return {
      id: group.id,
      title: group.title,
      pattern: group.pattern,
      metrics: [] as Metric[],
    };
  });
  const fallbackGroup = {
    id: grouping.fallbackGroup.id,
    title: grouping.fallbackGroup.title,
    metrics: [] as Metric[],
  };
  for (const metric of metrics) {
    const group = groups.find((group) => group.pattern.test(metric.name));
    if (group) {
      group.metrics.push(metric);
    } else {
      fallbackGroup.metrics.push(metric);
    }
  }
  const output = groups.map((group) => ({
    id: group.id,
    title: group.title,
    metrics: group.metrics,
  }));
  output.push(fallbackGroup);
  return output.filter((group) => group.metrics.length > 0);
}

// Debug metrics are only defined in debug builds, so they are excluded.
export async function loadMetrics(): Promise<Metric[]> {
  const content = await fs.readFile(TELEMETRY_METRICS_PATH, "utf-8");
  return (load(content) as Metric[]).filter((metric) => !metric.debug);
}

export {
  TELEMETRY_METRICS_PATH,
  Metric,
  MetricGroup,
  MetricGrouping,
  buildMetricGroups,
};
