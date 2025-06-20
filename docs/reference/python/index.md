::: info
This page acts as a placeholder during development and should not be included in the
production build. For production the build should fail if no Python API documentation
was found
:::

# Python API Documentation Not Found

Most likely this means that the Python API documentation has not (yet) been built.

To build the Python API docs, first [build the full project](../../development/build/)
then [install](https://hatch.pypa.io/latest/install) hatch and run

```bash
hatch env create docs
hatch run docs:install-pysail
hatch run docs:build
```
