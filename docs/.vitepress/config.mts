import path from "path";

import type { DefaultTheme, MarkdownOptions, PageData } from "vitepress";
import markdownItDeflist from "markdown-it-deflist";
import markdownItFootnote from "markdown-it-footnote";
import { defineConfig, HeadConfig } from "vitepress";

import { PageLink } from "./theme/utils/link";
import { loadPages } from "./theme/utils/page";
import { loadSphinxPages } from "./theme/utils/sphinx";
import { TreeNode } from "./theme/utils/tree";

// The documentation build process can be configured using the following environment variables:
// - SAIL_SITE_URL: The URL of the documentation site.
//     The URL must end with "/" and contain at least one last path segment corresponding to the documentation version.
// - SAIL_VERSION: The version of the Sail library.
// - SAIL_FATHOM_SITE_ID: (Optional) The Fathom site ID for analytics.

class Site {
  static url(): string {
    return process.env.SAIL_SITE_URL || "https://localhost/sail/main/";
  }

  static base(): string {
    const url = new URL(Site.url());
    return url.pathname;
  }

  /**
   * The documentation version extracted from the last path segment of the base URL.
   * @returns The documentation version.
   */
  static version(): string {
    const base = Site.base();
    const match = base.match(/\/(?<version>[^/]+)\/$/);
    if (match?.groups) {
      return match.groups.version;
    }
    throw new Error(`missing documentation version in the base URL: ${base}`);
  }

  /**
   * The version of the Sail library.
   * @returns The library version.
   */
  static libVersion(): string {
    return process.env.SAIL_VERSION || "0.0.0";
  }

  /**
   * The glob patterns to exclude source files from the documentation.
   * @returns The list of glob patterns.
   */
  static srcExclude(): string[] {
    // Exclude directories starting with an underscore. Such directories are
    // internal (e.g. containing pages to be included in other pages).
    return ["**/_*/**/*.md"];
  }
}

class Analytics {
  static head(): HeadConfig[] {
    const siteId = process.env.SAIL_FATHOM_SITE_ID;
    if (siteId) {
      return [
        [
          "script",
          {
            src: "https://cdn.usefathom.com/script.js",
            "data-site": siteId,
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

class Markdown {
  static options(): MarkdownOptions {
    return {
      theme: { light: "min-light", dark: "min-dark" },
      config: (md) => {
        md.use(markdownItFootnote);
        md.use(markdownItDeflist);
      },
    };
  }
}

class TransformPageData {
  static meta(pageData: PageData): void {
    const canonicalUrl = `${Site.url()}${pageData.relativePath}`
      .replace(/\/index\.md$/, "/")
      .replace(/\.md$/, ".html");

    pageData.frontmatter.head ??= [];
    pageData.frontmatter.head.push([
      "link",
      { rel: "canonical", href: canonicalUrl },
    ]);
    pageData.frontmatter.head.push(
      ["meta", { property: "og:url", content: canonicalUrl }],
      ["meta", { property: "og:title", content: pageData.title }],
      ["meta", { property: "og:description", content: pageData.description }],
      ["meta", { property: "og:image", content: `${Site.url()}logo.png` }],
      ["meta", { property: "og:type", content: "article" }],
      ["meta", { property: "og:site_name", content: "Sail Documentation" }],
    );
  }

  static sphinx(pageData: PageData): void {
    if (pageData.params?.sphinx) {
      pageData.title = pageData.params.current.text;
      pageData.titleTemplate = ":title - Sail Python API Reference";
      pageData.frontmatter.prev = pageData.params.prev ?? {
        link: "/reference/",
        text: "Reference",
      };
      pageData.frontmatter.next = pageData.params.next ?? {
        link: "/reference/changelog/",
        text: "Changelog",
      };
    }
  }

  static robots(pageData: PageData): void {
    const isDevGuide = pageData.relativePath.startsWith("development/");
    if (
      (Site.version() === "latest" && !isDevGuide) ||
      (Site.version() === "main" && isDevGuide)
    ) {
      return;
    }
    pageData.frontmatter.head ??= [];
    pageData.frontmatter.head.push([
      "meta",
      { name: "robots", content: "noindex, nofollow" },
    ]);
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
    trees: TreeNode<PageLink | null>[],
    base?: string,
  ): DefaultTheme.SidebarItem[] {
    function transform(
      tree: TreeNode<PageLink | null>,
      level: number,
    ): DefaultTheme.SidebarItem {
      if (tree.data === null) {
        throw new Error(`page with name '${tree.name}' does not exist`);
      }
      return {
        text: tree.data.title,
        link: base ? base + tree.data.url : tree.data.url,
        items: tree.children.map((child) => transform(child, level + 1)),
        collapsed: level > 1 ? true : undefined,
      };
    }
    return trees.map((tree) => transform(tree, 1));
  }

  static async userGuide(): Promise<DefaultTheme.SidebarItem[]> {
    const pages = await loadPages(
      this.srcDir,
      "/guide/**/*.md",
      Site.srcExclude(),
    );
    return Sidebar.items(TreeNode.fromPaths(pages));
  }

  static async development(): Promise<DefaultTheme.SidebarItem[]> {
    const pages = await loadPages(
      this.srcDir,
      "/development/**/*.md",
      Site.srcExclude(),
    );
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
          {
            text: "Changelog",
            link: "/reference/changelog/",
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
    title: "Sail",
    titleTemplate: ":title - Sail Documentation",
    description: "The Sail documentation site",
    head: [
      [
        "link",
        { rel: "icon", type: "image/png", href: `${Site.base()}favicon.png` },
      ],
      ...Analytics.head(),
    ],
    transformPageData(pageData) {
      TransformPageData.robots(pageData);
      TransformPageData.sphinx(pageData);
      // Add common meta tags at the end of the transformation.
      TransformPageData.meta(pageData);
    },
    markdown: Markdown.options(),
    srcExclude: Site.srcExclude(),
    ignoreDeadLinks: [
      /^https?:\/\/localhost(:\d+)?(\/.*)?$/,
      // The Python documentation is generated dynamically.
      /^\/reference\/python\//,
    ],
    contentProps: {
      version: Site.version(),
      libVersion: Site.libVersion(),
    },
    themeConfig: {
      siteTitle: "Sail",
      logo: "/logo.png",
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
      externalLinkIcon: true,
      socialLinks: [
        {
          icon: "github",
          link: "https://github.com/lakehq/sail",
          ariaLabel: "GitHub",
        },
      ],
      search: {
        provider: "local",
      },
    },
    sitemap: {
      hostname: Site.url(),
    },
  });
};
