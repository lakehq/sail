import path from "path";

import type { DefaultTheme, PageData } from "vitepress";
import { defineConfig, HeadConfig } from "vitepress";

import { PageLink } from "./theme/utils/link";
import { loadPages } from "./theme/utils/page";
import { loadSphinxPages } from "./theme/utils/sphinx";
import { TreeNode } from "./theme/utils/tree";

class Head {
  static readonly title = "Sail";
  static readonly description =
    "The computation framework with a mission to unify stream processing, batch processing, and compute-intensive (AI) workloads.";
  // TODO: create a dedicated logo for Sail
  static readonly image = "https://lakesail.com/logo.png";
}

class Site {
  static url(): string {
    return (
      process.env.SAIL_SITE_URL ?? "https://docs.lakesail.com/sail/latest/"
    );
  }

  static base(): string {
    const url = new URL(Site.url());
    return url.pathname;
  }
}

class Analytics {
  static head(): HeadConfig[] {
    if (process.env.SAIL_FATHOM_SITE_ID) {
      return [
        [
          "script",
          {
            src: "https://cdn.usefathom.com/script.js",
            "data-site": process.env.SAIL_FATHOM_SITE_ID,
            "data-spa": "auto",
            defer: "",
          },
        ],
      ];
    } else {
      return [];
    }
  }
}

class TransformPageData {
  static canonicalUrl(pageData: PageData): void {
    const canonicalUrl = `${Site.url()}${pageData.relativePath}`
      .replace(/\/index\.md$/, "/")
      .replace(/\.md$/, ".html");

    pageData.frontmatter.head ??= [];
    pageData.frontmatter.head.push([
      "link",
      { rel: "canonical", href: canonicalUrl },
    ]);
    pageData.frontmatter.head.push([
      "meta",
      { property: "og:url", content: canonicalUrl },
    ]);
  }

  static sphinx(pageData: PageData): void {
    if (pageData.params?.sphinx) {
      pageData.title = pageData.params.title;
      pageData.frontmatter.prev = pageData.params.prev ?? {
        link: "/reference/",
        text: "Reference",
      };
      pageData.frontmatter.next = pageData.params.next ?? false;
    }
  }
}

class Sidebar {
  /**
   * The source directory of the documentation.
   * This must be the same as `srcDir` in VitePress configuration,
   * but we cannot reference the configuration since it does not exist yet
   * when configuring the sidebar.
   */
  private static readonly srcDir = path.join(__dirname, "..");

  private static items(
    trees: TreeNode<PageLink>[],
    base?: string,
  ): DefaultTheme.SidebarItem[] {
    function transform(tree: TreeNode<PageLink>): DefaultTheme.SidebarItem {
      if (tree.data === null) {
        throw new Error(`page with name '${tree.name}' does not exist`);
      }
      return {
        text: tree.data.title,
        link: base ? base + tree.data.url : tree.data.url,
        items: tree.children.map(transform),
      };
    }
    return trees.map(transform);
  }

  static async userGuide(): Promise<DefaultTheme.SidebarItem[]> {
    const pages = await loadPages("/guide/**/*.md", this.srcDir);
    return Sidebar.items(TreeNode.fromPaths(pages));
  }

  static async development(): Promise<DefaultTheme.SidebarItem[]> {
    const pages = await loadPages("/development/**/*.md", this.srcDir);
    return Sidebar.items(TreeNode.fromPaths(pages));
  }

  static async reference(): Promise<DefaultTheme.SidebarItem[]> {
    return [
      {
        text: "Reference",
        link: "/reference/",
        items: [
          {
            text: "Python API Reference",
            link: "/reference/python/",
          },
        ],
      },
    ];
  }

  static async pythonReference(): Promise<DefaultTheme.SidebarItem[]> {
    const pages = await loadSphinxPages();
    const trees = TreeNode.fromNodes(pages)
      .filter((tree) => {
        return tree.name != "/";
      })
      .map((tree) =>
        tree.transform(
          (_name, page) =>
            new PageLink(page.inner.current.link, page.inner.current.title),
        ),
      );
    return [
      {
        text: "Python API Reference",
        link: "/reference/python/",
        items: Sidebar.items(trees, "/reference/python"),
      },
    ];
  }
}

export default async () => {
  return defineConfig({
    base: Site.base(),
    lang: "en-US",
    title: Head.title,
    description: Head.description,
    head: [
      ["meta", { property: "title", content: Head.title }],
      ["meta", { property: "description", content: Head.description }],
      ["meta", { property: "twitter:title", content: Head.title }],
      ["meta", { property: "twitter:description", content: Head.description }],
      ["meta", { property: "twitter:card", content: "summary" }],
      ["meta", { property: "twitter:image", content: Head.image }],
      ["meta", { property: "og:title", content: Head.title }],
      ["meta", { property: "og:description", content: Head.description }],
      ["meta", { property: "og:image", content: Head.image }],
      ["meta", { property: "og:type", content: "website" }],
      ["meta", { property: "og:site_name", content: "Sail documentation" }],
      [
        "link",
        { rel: "icon", type: "image/png", href: `${Site.base()}favicon.png` },
      ],
      ...Analytics.head(),
    ],
    transformPageData(pageData) {
      TransformPageData.canonicalUrl(pageData);
      TransformPageData.sphinx(pageData);
    },
    // Exclude directories starting with an underscore. Such directories are
    // internal (e.g. containing pages to be included in other pages).
    srcExclude: ["**/_*/**/*.md"],
    ignoreDeadLinks: [
      /^https?:\/\/localhost(:\d+)?(\/.*)?$/,
      // The Python documentation is generated dynamically.
      /^\/reference\/python\//,
    ],
    themeConfig: {
      logo: "/favicon.png",
      nav: [
        { text: "User Guide", link: "/guide/", activeMatch: "^/guide/" },
        {
          text: "Development",
          link: "/development/",
          activeMatch: "^/development/",
        },
        { text: "Reference", link: "/reference/", activeMatch: "^/reference/" },
      ],
      notFound: {
        quote: "The page does not exist.",
      },
      sidebar: {
        "/": [
          ...(await Sidebar.userGuide()),
          ...(await Sidebar.development()),
          ...(await Sidebar.reference()),
        ],
        "/reference/python/": await Sidebar.pythonReference(),
      },
      socialLinks: [
        {
          icon: "github",
          link: "https://github.com/lakehq/sail",
          ariaLabel: "GitHub",
        },
      ],
    },
    sitemap: {
      hostname: Site.url(),
    },
  });
};
