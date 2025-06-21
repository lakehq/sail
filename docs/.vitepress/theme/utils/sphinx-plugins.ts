import { writeFile } from "fs/promises";
import path from "path";

import type { Plugin } from "vite";
import type { UserConfig } from "vitepress";

import type { SphinxPage } from "./sphinx";
import { loadSphinxPages, requireSphinxPages } from "./sphinx";

declare module "vitepress" {
  type VitePressSphinxConfig = {
    // glob path to placeholder page that's shown when sphinx files could not be loaded
    placeholderPage: string;
    // Hook which is called after the sphinx pages have been loaded
    pagesLoaded?: (this: UserConfig, pages: SphinxPage[]) => Promise<void>;
  } & (
    | {
        // output folder of sphinx build pages
        outputFolder?: string;
        // list of sphinx pages
        pages: SphinxPage[];
      }
    | {
        outputFolder: string;
        pages?: SphinxPage[];
      }
  );

  interface UserConfig {
    sphinx?: VitePressSphinxConfig;
  }
}

// Options for VitePluginSphinxPages
type VitePluginSphinxPagesOptions = {
  // Path of placeholderPage
  placeholderPage: string;
  // If any sphinx pages were found
  hasPages: boolean;
  // If some sphinx pages should exist
  requiresPages: boolean;
};

/**
 * VitePlugin to validate Sphinx Pages
 * @param pluginOptions VitePluginSphinxPagesOptions
 * @returns Plugin
 */
export function VitePluginSphinxPages(
  pluginOptions: VitePluginSphinxPagesOptions,
): Plugin {
  return {
    name: "SphinxPages",
    config(config) {
      if (pluginOptions?.hasPages) {
        config.server ??= {};
        config.server.fs ??= {};
        config.server.fs.deny ??= [];
        config.server.fs.deny.push(pluginOptions.placeholderPage);
      }
      return config;
    },
    options() {
      if (pluginOptions?.hasPages) {
        return;
      }
      if (pluginOptions?.requiresPages) {
        this.error("No Sphinx Pages found");
      } else {
        this.warn(
          "No Sphinx Pages found (this causes a build error in production)",
        );
      }
    },
  };
}

/**
 * VitePress plugin to include sphinx pages
 * @param config UserConfig
 * @returns UserConfig
 */
export const withSphinxPages = async (config: UserConfig) => {
  config.sphinx ??= { placeholderPage: "", pages: [] };
  config.sphinx.pages ??= [];

  const allSphinxPages = config.sphinx.pages;
  // Load sphinx pages if output folder configuration was provided
  if (config.sphinx.outputFolder) {
    const sphinxPages = await loadSphinxPages(config.sphinx.outputFolder);
    allSphinxPages.push(...sphinxPages);
  }

  // Cache all sphinx page data for use in *.paths.ts file
  const cacheFile = path.join(__dirname, "../../cache/sphinx-pages.ts");
  const cacheData = JSON.stringify(
    allSphinxPages.map((page) => page.inner),
    null,
    2,
  );
  const cacheContents = `
import type { SphinxPageData } from "../theme/utils/sphinx"
export const sphinxPages: SphinxPageData[] = ${cacheData}
`;
  // Write Cache File for use in docs/reference/python/[page].paths
  await writeFile(cacheFile, cacheContents).catch((err) => {
    throw new Error(
      "VitePressSphinxPlugin: Cannot write sphinx-pages cache file:\n" +
        err.message,
    );
  });

  // Call pagesLoaded hook, f.e. to create a sitemap
  config.sphinx.pagesLoaded?.call(config, allSphinxPages);

  // Were there any sphinx pages?
  const hasSphinxPages = allSphinxPages.length > 0;

  // Do not include placeholderPage when SphinxPages exists
  if (hasSphinxPages) {
    config.srcExclude ??= [];
    config.srcExclude.push(config.sphinx.placeholderPage);
  }

  // Add VitePluginSphinxPages plugin
  config.vite ??= {};
  config.vite.plugins ??= [];
  config.vite.plugins.push(
    VitePluginSphinxPages({
      hasPages: hasSphinxPages,
      requiresPages: requireSphinxPages(),
      placeholderPage: config.sphinx.placeholderPage,
    }),
  );

  return config;
};
