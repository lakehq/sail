<slot>

<!-- Even if this page would be included in a production build, it would not show
anything as the pages under python/* would fill this slot content -->

::: info
This page acts as a placeholder during development and should not be included in the
production build. For production the build should fail if no Python API documentation
was found
:::

# Python API Documentation Not Found

Most likely this means that the Python API documentation has not (yet) been built.

> Make sure to [install hatch](https://hatch.pypa.io/latest/install) first

The easiest way to build the Python API docs is to build them from the current release:

```bash
hatch env create docs
hatch run docs:pip install pysail
hatch run docs:build
```

If instead you wish to build from a local build, then make sure to
[build the full project](../../development/build/) and then run

```bash
hatch env create docs
hatch run docs:install-pysail
hatch run docs:build
```

</slot>
