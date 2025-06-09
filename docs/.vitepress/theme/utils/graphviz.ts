import fs from "fs/promises";
import path from "path";

import { instance } from "@viz-js/viz";
import { CustomPlugin, optimize } from "svgo";

// An SVGO plugin to scale SVG size by a given factor.
const scaleSize = (scale: number): CustomPlugin => {
  if (scale <= 0) {
    throw new Error(`invalid scale factor: ${scale}`);
  }

  // Scales a value with an optional unit.
  function scaleValue(value?: string): string {
    if (value === undefined || value === null || value === "") {
      throw new Error("value must be a non-empty string");
    }
    return value.replace(/^([\d.]+)(.*)$/, (_, num, unit) => {
      return `${num * scale}${unit}`;
    });
  }

  return {
    name: "scaleSize",
    fn: () => {
      return {
        root: {
          exit: (node) => {
            for (const child of node.children) {
              if (child.type === "element" && child.name === "svg") {
                child.attributes.width = scaleValue(child.attributes.width);
                child.attributes.height = scaleValue(child.attributes.height);
              }
            }
          },
        },
      };
    },
  };
};

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
