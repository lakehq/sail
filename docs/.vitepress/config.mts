import { defineConfig, HeadConfig } from "vitepress";

const headTitle = "Sail";
const headDescription =
  "The computation framework with a mission to unify stream processing, batch processing, and compute-intensive (AI) workloads.";
// TODO: create a dedicated logo for Sail
const headImage = "https://lakesail.com/logo.png";

function siteUrl(): string {
  return process.env.SAIL_SITE_URL ?? "https://docs.lakesail.com/sail/latest/";
}

function siteBase(): string {
  const url = new URL(siteUrl());
  return url.pathname;
}

function analyticsHead(): HeadConfig[] {
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

export default async () => {
  return defineConfig({
    base: siteBase(),
    lang: "en-US",
    title: headTitle,
    description: headDescription,
    head: [
      ["meta", { property: "title", content: headTitle }],
      ["meta", { property: "description", content: headDescription }],
      ["meta", { property: "twitter:title", content: headTitle }],
      ["meta", { property: "twitter:description", content: headDescription }],
      ["meta", { property: "twitter:card", content: "summary" }],
      ["meta", { property: "twitter:image", content: headImage }],
      ["meta", { property: "og:title", content: headTitle }],
      ["meta", { property: "og:description", content: headDescription }],
      ["meta", { property: "og:image", content: headImage }],
      ["meta", { property: "og:type", content: "website" }],
      ["meta", { property: "og:site_name", content: "Sail documentation" }],
      [
        "link",
        { rel: "icon", type: "image/png", href: `${siteBase()}favicon.png` },
      ],
      ...analyticsHead(),
    ],
    transformPageData(pageData) {
      const canonicalUrl = `${siteUrl()}${pageData.relativePath}`
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

      if (pageData.params?.sphinx) {
        pageData.title = pageData.params.title;
        pageData.frontmatter.prev = pageData.params.prev || {
          link: "/reference/",
          text: "Reference",
        };
        pageData.frontmatter.next = pageData.params.next ?? false;
      }
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
        { text: "User Guide", link: "/guide/" },
        { text: "Development", link: "/development/" },
        { text: "Reference", link: "/reference/" },
      ],
      notFound: {
        quote: "The page does not exist.",
      },
      sidebar: {
        "/": [
          {
            text: "User Guide",
            link: "/guide/",
            items: [
              {
                text: "Installation",
                link: "/guide/installation",
              },
            ],
          },
          {
            text: "Development",
            link: "/development/",
            items: [],
          },
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
        ],
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
      hostname: siteUrl(),
    },
  });
};
