import { loadFunctionSupportData } from "../../.vitepress/theme/utils/function-support";

export default {
  async load() {
    return await loadFunctionSupportData();
  },
};
