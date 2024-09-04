import js from "@eslint/js";
import eslintConfigPrettier from "eslint-config-prettier";
import eslintPluginVue from "eslint-plugin-vue";
import ts from "typescript-eslint";

export default ts.config(
  js.configs.recommended,
  ...ts.configs.recommended,
  ...eslintPluginVue.configs["flat/recommended"],
  eslintConfigPrettier,
  {
    ignores: [
      "docs/.vitepress/cache",
      "docs/.vitepress/dist",
      "docs/.vitepress/.temp",
    ],
  },
  {
    files: ["**/*.{ts,mts,js,mjs,vue}"],
    languageOptions: {
      parserOptions: {
        parser: "@typescript-eslint/parser",
      },
    },
  },
);
