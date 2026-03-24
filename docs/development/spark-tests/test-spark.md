---
title: Running Spark Tests
rank: 20
---

# Running Spark Tests

The Spark tests are unit tests collected from the Spark project
and are used to measure Spark feature parity for Sail.

The Spark project itself uses the Python [unittest](https://docs.python.org/3/library/unittest.html) module to run the tests.
We have developed custom scripts to run the tests using [pytest](https://docs.pytest.org/) instead.

## Preparing the Test Environment

Before running Spark tests, you need to install the [patched PySpark package](./spark-setup) to the `test-spark` matrix environments that supports testing against multiple Spark versions.

```bash
hatch run test-spark.spark-4.1.1:install-pyspark
hatch run test-spark.spark-3.5.7:install-pyspark
```

You can choose to run any or all of the commands above, depending on whether you have built the patched PySpark package for the corresponding Spark version.

When you run the command, the environment will be created automatically if it does not exist.

## Running the Spark Connect Server

Use the following commands to build and run the Sail Spark Connect server.

```bash
hatch run scripts/spark-tests/run-server.sh
```

## Running the Tests

After running the Spark Connect server, start another terminal and use the following command to run the Spark tests.

```bash
hatch run test-spark.spark-4.1.1:scripts/spark-tests/run-tests.sh
```

The command runs a default set of test suites for Spark Connect.
Each test suite will write its `<suite>.jsonl` and `<suite>.log` files to the log directory,
where `<suite>` is the test suite name.

The test logs will be written to `tmp/spark-tests/<name>` where `<name>` is defined by
the `TEST_RUN_NAME` environment variable whose default value is `latest`.

### Test Selection

You can pass arguments to the script, which will be forwarded to `pytest`.
You can also use `PYTEST_` environment variables to customize the test execution.
For example, `PYTEST_ADDOPTS="-k <expression>"` can be used to run specific tests matching `<expression>`.

```bash
hatch run test-spark.spark-4.1.1:env \
  TEST_RUN_NAME=selected \
  scripts/spark-tests/run-tests.sh \
  --pyargs pyspark.sql.tests.connect -v -k "test_sql"
```

When you customize test execution using the above command, a single test suite will be run,
and the test log files are always `test.jsonl` and `test.log` in the log directory.

Note that for the above command, the test logs are written to the directory `tmp/spark-tests/selected`, due to the `TEST_RUN_NAME` environment variable.

## Running the Tests against JVM Spark

As a comparison, you can run the tests against the original JVM-based Spark library,
by setting the `SPARK_TESTING_REMOTE_PORT` environment variable to an empty string.

```bash
hatch run test-spark.spark-4.1.1:env \
  SPARK_TESTING_REMOTE_PORT= \
  scripts/spark-tests/run-tests.sh
```

This can be useful for discovering issues in the test setup.

## Caveat: Environment Mismatch

The steps above start the server in the `default` Hatch environment.
There are a few PySpark UDF tests that would fail in this setup, since they import testing UDFs available only in the **patched** PySpark library (installed in the `test-spark` Hatch environment).
There are also a few data-dependent tests that would fail, since the data files in the `python/test_support` directory are only available in the **patched** PySpark library.

Moreover, when the server is started in the `default` environment which has the PySpark 4.1.1 library installed, the tests for PySpark 3.5.7 does not work.

To use the same PySpark library for both the server and the tests, run the server and the tests in the same `test-spark` environment.

```bash
hatch run test-spark.spark-3.5.7:scripts/spark-tests/run-server.sh
```

```bash
hatch run test-spark.spark-3.5.7:scripts/spark-tests/run-tests.sh
```

However, running the server outside the `default` environment [pollutes the build cache](../recipes/reducing-build-time.md), so you may notice that the server takes longer to build and start.
