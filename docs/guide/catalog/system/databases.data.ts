import { loadSystemCatalog } from "../../../.vitepress/theme/utils/system-catalog";

export default {
  async load() {
    return await loadSystemCatalog();
  },
};
