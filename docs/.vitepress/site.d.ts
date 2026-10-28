declare module "markdown-it-deflist";
declare module "*.css";

declare module "*/.vitepress/cache/sphinx-pages.ts" {
  export const sphinxPages: import("./theme/utils/sphinx.ts").SphinxPageData[];
}
