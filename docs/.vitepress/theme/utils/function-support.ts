import fs from "fs/promises";
import path from "path";

export const FUNCTION_SUPPORT_DATA_PATH = path.join(
  __dirname,
  "../../../../python/pysail/data/compatibility/functions",
);

export interface FunctionInfo {
  module: string;
  function: string;
  status: "supported" | "not supported" | "planned" | "partially supported";
  note?: string;
}

export async function loadFunctionSupportData(
  dir: string = FUNCTION_SUPPORT_DATA_PATH,
  rootDir: string = dir,
): Promise<Record<string, FunctionInfo[]>> {
  const files = await fs.readdir(dir, { withFileTypes: true });
  const data: Record<string, FunctionInfo[]> = {};

  for (const file of files) {
    if (file.isDirectory()) {
      const nestedData = await loadFunctionSupportData(
        path.join(dir, file.name),
        rootDir,
      );
      Object.assign(data, nestedData);
    } else if (file.name.endsWith(".json")) {
      const name = path.basename(file.name, ".json");
      const relativeDir = path.relative(rootDir, dir);
      const normalizedRelativeDir = relativeDir.split(path.sep).join("/");
      const key = normalizedRelativeDir
        ? `${normalizedRelativeDir}/${name}`
        : name;

      data[key] = JSON.parse(
        await fs.readFile(path.join(dir, file.name), "utf-8"),
      );
    }
  }
  return data;
}
