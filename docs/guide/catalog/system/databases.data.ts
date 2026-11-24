import { loadSystemCatalog } from "../../../.vitepress/theme/utils/system-catalog.ts";

export default {
  async load() {
    return await loadSystemCatalog();
  },
};
