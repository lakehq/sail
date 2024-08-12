import fs from "fs/promises";
import path from "path";

import glob from "fast-glob";
import matter from "gray-matter";
import { normalizePath } from "vite";

import { PageLink } from "./link";

/**
 * Load the Markdown pages matching the pattern.
 * This is similar to `createContentLoader` in VitePress,
 * but we cannot use it since it relies on the VitePress configuration,
 * which may not exist when this function is called.
 * @param pattern The glob pattern to match the Markdown files.
 * @param srcDir The source directory of the documentation.
 * @returns The list of page links.
 */
async function loadPages(
  pattern: string | string[],
  srcDir: string,
): Promise<PageLink[]> {
  if (typeof pattern === "string") {
    pattern = [pattern];
  }
  pattern = pattern.map((p) => path.join(srcDir, p));
  const files = await glob(pattern);
  return await Promise.all(
    files.map(async (file) => {
      if (!file.endsWith(".md")) {
        throw new Error(`file ${file} is not a Markdown file`);
      }
      const content = await fs.readFile(file, "utf-8");
      const { data: frontmatter } = matter(content);
      const url =
        "/" +
        normalizePath(path.relative(srcDir, file))
          .replace(/(^|\/)index\.md$/, "$1")
          .replace(/\.md$/, ".html");
      if (!frontmatter.title) {
        throw new Error(`file ${file} does not have a title in frontmatter`);
      }
      return new PageLink(url, frontmatter.title);
    }),
  );
}

export { loadPages };
