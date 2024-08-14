---
title: Running Spark Tests
rank: 20
---

# Running Spark Tests

## Preparing the Test Environment

Before running Spark tests, please create the `test` Hatch environment using the following commands.
Note that you do _not_ need to run `maturin develop` in the `test` environment again after you make code changes.
We only use the pytest plugins (pure Python code) from the project, which do not need to be rebuilt by Maturin.

```bash
hatch env create test
hatch run test:install-pyspark
hatch run test:maturin develop
```

::: info

The Spark test environment depends on the [patched PySpark package](./spark-setup).
The patched PySpark package will be installed automatically during the environment creation.

:::

## Running the Spark Connect Server

Use the following commands to build and run the Spark Connect server powered by Sail.

```bash
scripts/spark-tests/run-server.sh
```

## Running the Tests

After running the Spark Connect server, start another terminal and use the following command to run the Spark tests.
The test logs will be written to `tmp/spark-tests/<name>` where `<name>` is defined by
the `TEST_RUN_NAME` environment variable whose default value is `latest`.

```bash
scripts/spark-tests/run-tests.sh
```

The above command runs a default set of test suites for Spark Connect.
Each test suite will write its `<suite>.jsonl` and `<suite>.log` files to the log directory,
where `<suite>` is the test suite name.

You can pass arguments to the script, which will be forwarded to `pytest`.
You can also use `PYTEST_` environment variables to customize the test execution.
For example, `PYTEST_ADDOPTS="-k <expression>"` can be used to run specific tests matching `<expression>`.

```bash
# Write the test logs to a different directory (`tmp/spark-tests/selected`).
export TEST_RUN_NAME=selected

scripts/spark-tests/run-tests.sh --pyargs pyspark.sql.tests.connect -v -k test_sql
```

When you customize the test execution using the above command, a single test suite will be run,
and the test log files are always `test.jsonl` and `test.log` in the log directory.
