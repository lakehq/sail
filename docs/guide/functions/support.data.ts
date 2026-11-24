import { loadFunctionSupportData } from "../../.vitepress/theme/utils/function-support.ts";

export default {
  async load() {
    return await loadFunctionSupportData();
  },
};
