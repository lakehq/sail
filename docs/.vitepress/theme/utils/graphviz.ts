import fs from "fs/promises";
import path from "path";

import { instance } from "@viz-js/viz";
import { optimize } from "svgo";

import { scaleSize, setClass } from "./svgo-plugins";

function createGraphvizLoader(
  glob: string[],
  base: string,
  config?: { scale?: number },
) {
  return {
    watch: glob,
    async load(watchedFiles: string[]) {
      const viz = await instance();
      const svg = await Promise.all(
        watchedFiles.map(async (file) => {
          const source = await fs.readFile(file, "utf-8");
          const svg = viz.renderString(source, {
            format: "svg",
          });
          const key = path.relative(base, file);
          const value = optimize(svg, {
            plugins: [
              "preset-default",
              ...(config?.scale !== undefined ? [scaleSize(config.scale)] : []),
              setClass("viz"),
            ],
          }).data;
          return [key, value];
        }),
      );
      return Object.fromEntries(svg);
    },
  };
}

export { createGraphvizLoader };
