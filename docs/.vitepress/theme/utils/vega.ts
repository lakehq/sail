import fs from "node:fs/promises";
import path from "node:path";

import { optimize } from "svgo";
import * as vega from "vega";

import { setClass } from "./svgo-plugins";

async function render(
  spec: vega.Spec,
  base: string,
  signals?: (vega.InitSignal | vega.NewSignal)[],
): Promise<string> {
  const runtime = vega.parse(spec, { signals });
  // All data URLs in the spec are assumed to be relative to `base`.
  const loader = vega.loader({ baseURL: base, mode: "file" });
  const view = new vega.View(runtime, { loader });
  const svg = await view.toSVG();
  return optimize(svg, {
    plugins: ["preset-default", setClass("vega")],
  }).data;
}

function createVegaLoader(
  glob: string[],
  base: string,
  signals: {
    [path: string]: { [key: string]: (vega.InitSignal | vega.NewSignal)[] };
  } = {},
) {
  return {
    watch: glob,
    async load(watchedFiles: string[]) {
      const svg = await Promise.all(
        watchedFiles.map(async (file) => {
          const source = await fs.readFile(file, "utf-8");
          const spec = JSON.parse(source);
          const key = path.relative(base, file);
          if (signals[key]) {
            const entries = await Promise.all(
              Object.entries(signals[key]).map(async ([name, signals]) => {
                const value = await render(spec, base, signals);
                return [name, value];
              }),
            );
            return [key, Object.fromEntries(entries)];
          } else {
            const value = await render(spec, base);
            return [key, value];
          }
        }),
      );
      return Object.fromEntries(svg);
    },
  };
}

export { createVegaLoader };
