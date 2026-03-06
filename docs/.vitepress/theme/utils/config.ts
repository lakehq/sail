import fs from "fs/promises";
import path from "path";

import yaml from "js-yaml";

const APP_CONFIG_PATH = path.join(
  __dirname,
  "../../../../crates/sail-common/src/config/application.yaml",
);

interface ConfigItem {
  key: string;
  type: string;
  default: string;
  description?: string;
  experimental?: boolean;
  hidden?: boolean;
}

interface ConfigGroup {
  id: string;
  title: string;
  items: ConfigItem[];
}

interface ConfigGrouping {
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

function buildConfigGroups(
  items: ConfigItem[],
  grouping: ConfigGrouping,
): ConfigGroup[] {
  const groups = grouping.groups.map((group) => {
    return {
      id: group.id,
      title: group.title,
      pattern: group.pattern,
      items: [] as ConfigItem[],
    };
  });
  const fallbackGroup = {
    id: grouping.fallbackGroup.id,
    title: grouping.fallbackGroup.title,
    items: [] as ConfigItem[],
  };
  for (const item of items) {
    const group = groups.find((group) => group.pattern.test(item.key));
    if (group) {
      group.items.push(item);
    } else {
      fallbackGroup.items.push(item);
    }
  }
  const output = groups.map((group) => ({
    id: group.id,
    title: group.title,
    items: group.items.sort((a, b) => a.key.localeCompare(b.key)),
  }));
  output.push({
    id: fallbackGroup.id,
    title: fallbackGroup.title,
    items: fallbackGroup.items.sort((a, b) => a.key.localeCompare(b.key)),
  });
  return output;
}

export async function loadConfigItems(): Promise<ConfigItem[]> {
  const content = await fs.readFile(APP_CONFIG_PATH, "utf-8");
  return yaml.load(content) as ConfigItem[];
}

export {
  APP_CONFIG_PATH,
  ConfigItem,
  ConfigGroup,
  ConfigGrouping,
  buildConfigGroups,
};
