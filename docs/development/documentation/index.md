---
title: Documentation
rank: 4
---

# Documentation

The Sail documentation is built using [VitePress](https://vitepress.dev/), a static site generator based on
[Vue.js](https://vuejs.org/).
You write documentation in Markdown files, and VitePress generates the documentation site from them.

The Sail Python API reference is generated using [Sphinx](https://www.sphinx-doc.org/).
Instead of producing the HTML files directly, we configure Sphinx to output the rendered HTML fragments in JSON format.
The HTML fragments are loaded during the build process of the VitePress site.
This makes the Python API reference an organic part of the single-page application (SPA) for the Sail documentation.

The source code for the documentation site is located in the `docs` directory.
The source code for the Python API reference is located in the `python/pysail/docs` directory.

## Environment Setup

You need a recent version of [Node.js](https://nodejs.org/) and [pnpm](https://pnpm.io/) to build the documentation.

Run the following command to install the dependencies in the project.

```bash
pnpm install
```

## Code Formatting

Run the following commands to format and lint the documentation source code.
Specifically, this formats the all Markdown and YAML files in the project,
so it is also helpful when modifying configuration files such as the GitHub Actions workflows.

```bash
pnpm run format
pnpm run lint
```

## Building the Documentation

Follow the steps below to build the documentation.

We need to build the Python package first. Sphinx will need to import the Python package
and extract documentation from Python docstrings.
The Python API reference is generated using the `docs` Hatch environment.
We install the package in editable mode in the environment.

```bash
hatch run docs:maturin develop
```

::: info
You only need to run the `maturin develop` command after you make Rust code changes.
All changes to the Python code are reflected in the environment immediately.
:::

Run the following command to generate the Python API reference using Sphinx.
You need to run this command when you make changes to the Python code,
or the reStructuredText (reST) files of the Sphinx documentation.

```bash
hatch run docs:build
```

Run the following command to build the documentation site using VitePress.

```bash
pnpm run docs:build
```

::: info
You can refer to `docs/.vitepress/config.mts` for a few environment variables that configure the VitePress site.
:::

You can preview the documentation site locally using the following command.
This command starts a local server to serve the documentation site.

```bash
pnpm run docs:preview
```

## Running the Development Server

You can use the following command to start the development server for VitePress.
The changes to Markdown files are automatically reflected in the browser.

```bash
pnpm run docs:dev
```
