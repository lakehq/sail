import fs from "fs/promises";
import path from "path";

import yaml from "js-yaml";

export const SYSTEM_CATALOG_DATA_PATH = path.join(
  __dirname,
  "../../../../crates/sail-common-datafusion/data/system/databases.yaml",
);

export interface SystemColumn {
  name: string;
  description: string;
  sql_type: string;
  nullable: boolean;
}

export interface SystemTable {
  name: string;
  description: string;
  columns: SystemColumn[];
}

export interface SystemDatabase {
  name: string;
  description: string;
  tables: SystemTable[];
}

export async function loadSystemCatalog(): Promise<SystemDatabase[]> {
  const content = await fs.readFile(SYSTEM_CATALOG_DATA_PATH, "utf-8");
  return yaml.load(content) as SystemDatabase[];
}
