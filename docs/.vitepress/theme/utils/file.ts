import fs from "fs/promises";
import path from "path";

/**
 * Ensure [dirName] exists
 * @param dirName
 */
export async function ensureDir(dirName: string) {
  const dirExists = !!(await fs.stat(dirName).catch(() => {
    return false;
  }));
  if (!dirExists) {
    await fs.mkdir(dirName);
  }
}

/**
 * Write data to file, while ensuring that dir exists
 * @param filePath Path to file
 * @param fileData Data to write
 */
export async function writeFile(filePath: string, fileData: string) {
  const dirName = path.dirname(filePath);
  await ensureDir(dirName);
  await fs.writeFile(filePath, fileData);
}
