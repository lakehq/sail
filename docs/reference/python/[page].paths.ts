import * as fs from "node:fs";
import * as path from "node:path";

const SPHINX_BUILD_OUTPUT = path.join(
  __dirname,
  "../../../python/pysail/docs/_build",
);

export default {
  async paths() {
    const entries = await fs.promises.readdir(SPHINX_BUILD_OUTPUT, {
      withFileTypes: true,
      recursive: true,
    });
    const files = entries.filter(
      (entry) => entry.isFile() && entry.name.endsWith(".fjson"),
    );

    const paths = await Promise.all(
      files.map(async (entry) => {
        if (entry.name === "search.fjson" || entry.name === "genindex.fjson") {
          return null;
        }
        const file = path.join(entry.parentPath, entry.name);
        const content = await fs.promises.readFile(file, "utf-8");
        const data = JSON.parse(content);
        const page = path
          .relative(SPHINX_BUILD_OUTPUT, file)
          .replace(/^index\.fjson$/, "/index")
          .replace(/[/\\]index\.fjson$/, "/index")
          // We must turn non-index pages into directories, since Sphinx handles URL in this way
          // in `JSONHTMLBuilder`, which may generate relative URLs containing `../` may appear in the HTML.
          .replace(/\.fjson$/, "/index");
        return {
          params: {
            sphinx: true,
            page,
            title: data.title,
            prev: data.prev
              ? { link: data.prev.link, text: data.prev.title }
              : false,
            next: data.next
              ? { link: data.next.link, text: data.next.title }
              : false,
          },
          content: data.body,
        };
      }),
    );
    return paths.filter((path) => path !== null);
  },
};
