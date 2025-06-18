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

By default, a Sail Spark Connect server is launched in the same process as a pytest fixture.
To run the tests against a server launched externally, set the `SPARK_REMOTE` environment variable.

```bash
env SPARK_REMOTE="sc://localhost:50051" hatch run pytest
```

You can also run the tests against a JVM-based Spark Connect server
by specifying a `local` Spark remote URL.
This is useful to ensure that the tests are written correctly to reflect the Spark behavior.
Note that tests written for extended features of Sail will be skipped in this case.

```bash
env SPARK_REMOTE="local" \
  PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-connect_2.12:3.5.5 pyspark-shell" \
  hatch run pytest
```

::: info

- You can use any valid local Spark master URLs such as `local`, `local[2]`, or `local[*]`.
- If tests involving catalog operations fail, you may need to clean up the local Spark warehouse and metastore in the project directory.
  ```bash
  rm -rf metastore_db spark-warehouse
  ```

:::
