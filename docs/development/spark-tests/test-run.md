---
title: Running Spark Tests
rank: 20
---

# Running Spark Tests

## Preparing the Test Environment

Before running Spark tests, please create the `test` Hatch environment using the following commands.

```bash
hatch env create test
hatch run test:install-pyspark
```

::: info
The Spark test environment depends on the [patched PySpark package](./spark-setup).
The commands above install the patched PySpark package after environment creation.
:::

## Running the Spark Connect Server

Use the following commands to build and run the Spark Connect server powered by Sail.

```bash
scripts/spark-tests/run-server.sh
```

::: warning
The command above starts the server in the `default` Hatch environment.
There are a few PySpark UDF tests that would fail in this setup, since they import testing UDFs available only in the **patched** PySpark library (installed in the `test` Hatch environment).

To work with these tests, run `hatch shell test` to enter the `test` environment before running the server.
However, this is not recommended in the general development workflow, as it may [pollute the build cache](../recipes/reducing-build-time.md).

The GitHub Actions workflow runs the server in the `test` environment (using the pre-built package from an earlier step),
so this is not an issue.
:::

## Running the Tests

After running the Spark Connect server, start another terminal and use the following command to run the Spark tests.
The test logs will be written to `tmp/spark-tests/<name>` where `<name>` is defined by
the `TEST_RUN_NAME` environment variable whose default value is `latest`.

```bash
scripts/spark-tests/run-tests.sh
```

The command runs a default set of test suites for Spark Connect.
Each test suite will write its `<suite>.jsonl` and `<suite>.log` files to the log directory,
where `<suite>` is the test suite name.

### Running Selected Tests

You can pass arguments to the script, which will be forwarded to `pytest`.
You can also use `PYTEST_` environment variables to customize the test execution.
For example, `PYTEST_ADDOPTS="-k <expression>"` can be used to run specific tests matching `<expression>`.

```bash
# Write the test logs to a different directory (`tmp/spark-tests/selected`).
env TEST_RUN_NAME=selected \
  scripts/spark-tests/run-tests.sh --pyargs pyspark.sql.tests.connect -v -k test_sql
```

When you customize the test execution using the above command, a single test suite will be run,
and the test log files are always `test.jsonl` and `test.log` in the log directory.

### Running Tests against the Original Spark Implementation

As a comparison, you can run the tests against the original Spark implementation,
by setting the `SPARK_TESTING_REMOTE_PORT` environment variable to an empty string.

```bash
env SPARK_TESTING_REMOTE_PORT= scripts/spark-tests/run-tests.sh
```

This can be useful for discovering issues in the test setup.
