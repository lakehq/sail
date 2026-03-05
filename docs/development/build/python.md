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
The built package will be available in the `target/wheels` directory.

```bash
hatch run maturin build
```

You can install the built package in the `default` environment using the following command.

```bash
hatch run install-pysail
```

## Running Tests

Use the following command to run the Python tests.
This assumes that you have installed the **editable package** in the `default` Hatch environment.

```bash
hatch run pytest
```

You can pass additional `pytest` arguments as needed.
For example, to run tests from the **installed package**, use the following command.

```bash
hatch run pytest --pyargs pysail
```

### Testing with an External Spark Connect Server

By default, a Sail Spark Connect server is launched in the same process as the tests.
To run the tests against a server launched externally, set the `SPARK_REMOTE` environment variable.

```bash
env SPARK_REMOTE="sc://localhost:50051" hatch run pytest
```

### Testing with Different Spark Versions

The `test` matrix environments allow you to run tests against multiple Spark versions.
Suppose the package is available in the `target/wheels` directory,
you can install the package in all `test` environments using the following command.

```bash
hatch run test:install-pysail
```

Then you can run the tests against multiple Spark versions.
The tests are discovered from the installed package.

```bash
hatch run test:pytest --pyargs pysail
```

Alternatively, you can run the tests against a specific Spark version.
For example, you can use the following command to install the package in a specific `test` environment.

```bash
hatch run test.spark-3.5.7:install-pysail
```

Then you can run the tests against the specific Spark version.

```bash
hatch run test.spark-3.5.7:pytest --pyargs pysail
```

### Testing with JVM Spark

You can also run the tests against a JVM-based Spark Connect server
by specifying a `local` Spark remote URL.
This is useful to ensure that the tests are written correctly to reflect the Spark behavior.
Note that tests written for extended features of Sail will be skipped in this case.

```bash
env SPARK_REMOTE="local" \
  PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-connect_2.12:3.5.7 pyspark-shell" \
  hatch run test.spark-3.5.7:pytest --pyargs pysail
```

The `PYSPARK_SUBMIT_ARGS` environment variable is no longer needed since Spark 4.0.

```bash
env SPARK_REMOTE="local" \
  hatch run test.spark-4.1.1:pytest --pyargs pysail
```

::: info

- You can use any valid local Spark master URLs such as `local`, `local[2]`, or `local[*]`.
- If tests involving catalog operations fail, you may need to clean up the local Spark warehouse and metastore in the project directory.
  ```bash
  rm -rf metastore_db spark-warehouse
  ```

:::
