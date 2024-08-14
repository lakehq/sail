import DefaultTheme from "vitepress/theme";

import MainLayout from "./layouts/MainLayout.vue";

import "./app.css";

export default {
  extends: DefaultTheme,
  Layout: MainLayout,
};
