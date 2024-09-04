---
title: Building the Python Package
rank: 20
---

# Building the Python Package

## Running the Code Formatter and Linter

Run the following command to format the Python code. The command uses [Ruff](https://docs.astral.sh/ruff) as the
code formatter and linter.

```bash
hatch fmt
```

You may need to fix linter errors manually.

## Building and Installing the Python Package

Run the following command to build and install the Python package for local development.
The package is installed in the `default` Hatch environment.

```bash
hatch run maturin develop
```

The command installs the source code as an editable package in the Hatch environment, while
the built `.so` native library is stored in the source directory. You can then use `hatch shell`
to enter the Python environment and test the library. Any changes to the Python code will be reflected in the
environment immediately. But if you make changes to the Rust code, you need to run the `develop` command again.

Run the following command if you want to build the Python package without installing it.

```bash
hatch run maturin build
```

## Running Tests

Use the following command to run the Python tests.
This assumes that you have installed the package in the `default` Hatch environment.

```bash
hatch run pytest
```

You can pass additional `pytest` arguments as needed.
For example, to run tests from the _installed package_, use the following command.

```bash
hatch run pytest --pyargs pysail
```
