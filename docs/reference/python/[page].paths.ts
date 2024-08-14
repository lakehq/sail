import {
  loadSphinxPages,
  SphinxPageData,
} from "../../.vitepress/theme/utils/sphinx";
import { PathLike, TreeNode } from "../../.vitepress/theme/utils/tree";

/**
 * The base URL for the Sphinx documentation.
 */
const BASE = "/reference/python";

class SphinxPagePath implements PathLike {
  readonly inner: SphinxPageData;

  constructor(data: SphinxPageData) {
    this.inner = data;
  }

  path(): string[] {
    return this.inner.current.link.split("/").filter(Boolean);
  }

  rank(): number | undefined {
    // This class is only used for generating the path tree, so we do not need to
    // define the rank for each page.
    return undefined;
  }
}

type SphinxPageConfig = {
  params: {
    sphinx: true;
    /**
     * The parameter that defines the Markdown file path.
     */
    page: string;
    current: { link: string; text: string };
    prev: { link: string; text: string } | false | null;
    next: { link: string; text: string } | false | null;
    /**
     * A list of direct child pages, or `undefined` if it is not a generated index page.
     */
    children?: { link: string; text: string }[];
  };
  content: string;
};

function transform(
  tree: TreeNode<SphinxPagePath>,
  prefix?: string,
): TreeNode<SphinxPageConfig> {
  prefix = prefix ?? "";
  if (tree.name !== "") {
    prefix = `${prefix}/${tree.name}`;
  }

  // Transform the child nodes first so that generated index pages are defined
  // for all subdirectories.
  const children = tree.children.map((child) => transform(child, prefix));

  function data(): SphinxPageConfig {
    if (tree.data !== null) {
      const page = tree.data.inner;
      return {
        params: {
          sphinx: true,
          page: page.current.link
            .replace(/\/$/, "/index")
            .replace(/\.html$/, "")
            .replace(/^\//, ""),
          current: {
            link: `${BASE}${page.current.link}`,
            text: page.current.title,
          },
          prev: page.prev
            ? { link: `${BASE}${page.prev.link}`, text: page.prev.title }
            : null,
          next: page.next
            ? { link: `${BASE}${page.next.link}`, text: page.next.title }
            : null,
        },
        content: page.content,
      };
    } else {
      return {
        params: {
          sphinx: true,
          page: `${prefix}/index`.replace(/^\//, ""),
          current: { link: `${BASE}${prefix}/`, text: "Index" },
          prev: false,
          next: false,
          children: children.map((child) => ({
            link: child.data.params.current.link,
            text: child.data.params.current.text,
          })),
        },
        content: "",
      };
    }
  }

  return new TreeNode(tree.name, data(), children);
}

export default {
  async paths() {
    const links = (await loadSphinxPages()).map(
      (page) => new SphinxPagePath(page.inner),
    );
    const root = links.find((link) => link.inner.current.link === "/");
    if (root === undefined) {
      throw new Error("root page not found");
    }
    // Generate the page tree from the paths so that we can generate any missing index pages
    // for intermediate directories.
    // Note that the page tree can be different from the page tree in the side bar.
    // The former is defined by the paths, while the latter is defined by parent/previous/next links.
    const tree = new TreeNode("", root, TreeNode.fromPaths(links));
    return transform(tree).collect();
  },
};
