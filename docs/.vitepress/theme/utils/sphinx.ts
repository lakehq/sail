import * as fs from "fs/promises";
import * as path from "path";

import glob from "fast-glob";
import { normalizePath } from "vite";

import { NodeLike } from "./tree";

const SPHINX_BUILD_OUTPUT = path.join(
  __dirname,
  "../../../../python/pysail/docs/_build",
);

type SphinxLink = {
  link: string;
  title: string;
};

/**
 * The page data generated by Sphinx.
 * All links are relative to the current page.
 */
type SphinxData = {
  title: string;
  body: string;
  parents?: SphinxLink[] | null;
  prev?: SphinxLink | null;
  next?: SphinxLink | null;
};

/**
 * The processed Sphinx page data.
 * All links are absolute, where the root is the Sphinx base directory.
 */
type SphinxPageData = {
  current: SphinxLink;
  parent?: SphinxLink;
  prev?: SphinxLink;
  next?: SphinxLink;
  content: string;
};

class SphinxPage implements NodeLike {
  readonly inner: SphinxPageData;

  constructor(data: SphinxPageData) {
    this.inner = data;
  }

  name(): string {
    return this.inner.current.link;
  }

  parent(): string | undefined {
    return this.inner.parent?.link;
  }

  prev(): string | undefined {
    return this.inner.prev?.link;
  }

  next(): string | undefined {
    return this.inner.next?.link;
  }
}

function resolveLink(
  link: SphinxLink | undefined | null,
  base: string,
): SphinxLink | undefined {
  if (link === undefined || link === null) {
    return undefined;
  }
  // The path separator is always `/` in Sphinx,
  // so we use a URL to resolve the link relative to the base.
  const url = new URL(link.link, `http://localhost${base}`);
  return {
    link: url.pathname.replace(/\/index.html$/, "/"),
    title: link.title,
  };
}

/**
 * Load the Sphinx pages generated by the Sphinx JSON builder.
 * @returns The list of Sphinx pages.
 */
async function loadSphinxPages(): Promise<SphinxPage[]> {
  const files = await glob(
    path.join(SPHINX_BUILD_OUTPUT, "**/!(search|genindex).fjson"),
  );
  return await Promise.all(
    files.map(async (file): Promise<SphinxPage> => {
      const content = await fs.readFile(file, "utf-8");
      const data = JSON.parse(content) as SphinxData;
      const link =
        "/" +
        normalizePath(path.relative(SPHINX_BUILD_OUTPUT, file))
          .replace(/(^|\/)index\.fjson$/, "$1")
          .replace(/\.fjson$/, ".html");
      const parents = data.parents ?? [];
      const parent = parents.length > 0 ? parents[parents.length - 1] : null;
      return new SphinxPage({
        current: { link: link, title: data.title },
        parent: resolveLink(parent, link),
        prev: resolveLink(data.prev, link),
        next: resolveLink(data.next, link),
        content: data.body,
      });
    }),
  );
}

export { SphinxLink, SphinxPage, SphinxPageData, loadSphinxPages };
