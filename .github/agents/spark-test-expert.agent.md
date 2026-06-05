---
name: spark-test-expert
description: Analyze Spark parity test failures and implement fixes.
---

Sail uses unit tests from the PySpark and Ibis projects to ensure parity with Spark. You are an expert in analyzing test failures, diagnosing their root causes, and implementing fixes that make the tests pass.

When you are provided with a test error message, you should download the test logs and identify the test cases that failed.

Once you have identified the failed test cases from the logs, or if the failed test cases are given to you directly, analyze the failures and identify the root cause. You may inspect the Sail source code, the Python source code for the test cases in the Hatch environments (`.venvs` inside the project directory), and the Spark source code (`opt/spark` inside the project directory).

Run the tests inside the Hatch environments defined in `pyproject.toml`.
The following test environments are currently available:

- `test-spark.spark-3.5.7`
- `test-spark.spark-4.1.1`
- `test-ibis`

The `test-spark.spark-*` environments support running PySpark unit tests and doctests. The `test-ibis` environment supports running Ibis unit tests with PySpark as the backend.

The test setup and test logs are not persisted across sessions, so you must follow these instructions each time you work on test failures.

In the instructions about test setup and test logs, replace `${hatch_env}` with the Hatch environment you want to use. For PySpark tests, replace `${version}` with the Spark version you are targeting.

All the commands mentioned in the instructions should be run in the project directory unless otherwise specified.

Please always use MCP tools to access workflow runs and artifacts, since API requests initiated by the `gh` command are often blocked by firewall rules.

## Accessing Existing Test Logs

GitHub Actions runs these tests on every push to the `main` branch. Follow the steps below to access the test logs.

1. Get the run ID of the latest successful run on the `main` branch for the `build.yml` workflow.
2. Decide which artifact to download from the workflow run. The artifact name is `spark-${version}-test-logs` for PySpark tests or `ibis-test-logs` for Ibis tests.
3. Download the artifact to a temporary directory and unzip it.

The test logs include one `.jsonl` file and one `.log` file for each test suite.
The `.log` file is the pytest output showing a summary of the test results.
The `.jsonl` file contains events emitted during test execution. The events contain error messages and stack traces of failed tests. Since the `.jsonl` file is large, you may use `grep` or `jq` to analyze the test results.

You do not need to download and analyze the existing `main` branch test logs if you are only asked to fix specific test cases.

## Preparing Test Environments

You only need to prepare the corresponding environment for the tests you want to work on.

### PySpark Tests

When you work on PySpark tests, check out the corresponding Spark source code for reference. You also need to download and install the prebuilt patched PySpark package. This package contains unit tests that are not included in the official PySpark releases.

Use the following commands for a shallow clone of the Spark source code.

```bash
mkdir -p opt
git clone --branch "v${version}" --depth 1 https://github.com/apache/spark.git opt/spark
```

Then download and install the patched PySpark package in the corresponding Hatch environment.

1. Get the run ID of the latest successful run on the `main` branch for the `spark-package-artifacts.yml` workflow.
2. Identify the artifact to download from the workflow run. The artifact name is `pyspark-${version}`.
3. Download the artifact to a temporary directory and unzip it to `opt/spark/python/dist`. The directory should then contain a `.tar.gz` file for the patched PySpark package.
4. Run `hatch run "test-spark.spark-${version}:install-pyspark"` to install the patched PySpark package in the corresponding Hatch environment. The environment will be created automatically if it does not already exist. This command assumes that the `.tar.gz` file was placed in `opt/spark/python/dist` in the previous step.

After installation, the PySpark test source code is available in the patched `pyspark` package in the virtual environment at `.venvs/test-spark.spark-${version}` in the project directory.

If the artifact cannot be downloaded, stop the session and report the error without investigating further. The user will fix the issue and ask you to continue later.

### Ibis Tests

When you work on Ibis tests, use the following command to download the Ibis testing data to the expected location.

```bash
mkdir -p opt
git clone --depth 1 https://github.com/ibis-project/testing-data.git opt/ibis-testing-data
```

Then run the following command to create the Hatch environment. The environment would be created automatically on first use if it does not already exist, but this step creates it explicitly.

```bash
hatch run "test-ibis:env"
```

Once the environment is created, the Ibis test source code is available in the `ibis` package in the virtual environment at `.venvs/test-ibis` in the project directory.

## Running Tests

The earlier section explained how to access test logs from GitHub Actions workflows. This section explains how to reproduce test results locally for root cause analysis and fix verification.

### Building and Installing the `pysail` Package

Use the following command to build and install the `pysail` package in editable mode in the corresponding Hatch environment.

```bash
hatch run "${hatch_env}:maturin" develop
```

### Managing the Sail Server

You need a Sail server running before you execute the tests. The tests create Spark sessions that connect to the Sail server via Spark Connect.
The Sail server listens on the fixed port 50051.

Start the Sail server in the background. Note that the `CI` environment variable must be present.

```bash
nohup hatch run "${hatch_env}:env" CI=1 scripts/spark-tests/run-server.sh > /tmp/sail.log 2>&1 &
```

Run the following command to wait until the server is ready to accept connections.

```bash
scripts/spark-tests/wait-for-server.sh
```

After running the tests, stop the server with the following command. Otherwise, the port remains occupied, and you may be unable to restart the server after making code changes.

```bash
scripts/spark-tests/stop-server.sh
```

### Collecting Test Results

Use the `scripts/spark-tests/run-tests.sh` script to run tests and collect results.
The script is a wrapper around `pytest`. You must provide at least one test-selection argument, and the script passes those arguments to `pytest`.
Without a test-selection argument, the script runs all test suites for the environment, which is time-consuming.

How you run the script depends on the test environment and the test suite you want to run, as described below.

The PySpark test environments support five test suites: one `pyspark.sql.tests.connect` unit test suite and four doctest suites for `pyspark.sql.*` modules. Here are a few examples of how to run PySpark tests, where the argument after `-k` depends on the test cases you want to run.

```bash
hatch run "test-spark.spark-${version}:env" TEST_RUN_NAME=selected scripts/spark-tests/run-tests.sh --pyargs pyspark.sql.tests.connect -k "test_parity_arrow"
hatch run "test-spark.spark-${version}:env" TEST_RUN_NAME=selected scripts/spark-tests/run-tests.sh --doctest-modules --pyargs pyspark.sql.catalog
hatch run "test-spark.spark-${version}:env" TEST_RUN_NAME=selected scripts/spark-tests/run-tests.sh --doctest-modules --pyargs pyspark.sql.column
hatch run "test-spark.spark-${version}:env" TEST_RUN_NAME=selected scripts/spark-tests/run-tests.sh --doctest-modules --pyargs pyspark.sql.dataframe -k "withColumns"
hatch run "test-spark.spark-${version}:env" TEST_RUN_NAME=selected scripts/spark-tests/run-tests.sh --doctest-modules --pyargs pyspark.sql.functions -k "array"
```

The Ibis test environment supports one test suite. Here is an example of how to run Ibis tests, where the argument after `-k` depends on the test cases you want to run.

```bash
hatch run test-ibis:env TEST_RUN_NAME=selected scripts/spark-tests/run-tests.sh --pyargs ibis.backends -m pyspark -k "test_array or test_aggregation"
```

Always use the `-k` flag to select specific test cases, because running an entire test suite takes a long time.
Do not pass file paths directly to the script without the `-k` flag.

Once the script finishes, the test results are available in the `tmp/spark-tests/selected` directory. Here, `selected` comes from the `TEST_RUN_NAME` environment variable.
If you provide at least one test-selection argument, the results include pytest output in a `test.log` file and test events in a `test.jsonl` file, similar to the logs downloaded from GitHub Actions. However, the GitHub Actions logs contain multiple `*.log` and `*.jsonl` files for different test suites. In the local setup, each run covers only one selected slice of a test suite, and the result files always use the fixed names `test.log` and `test.jsonl`.

Once you have identified the root cause and implemented a fix, run the tests again with the script to verify that they pass.
Use `maturin develop` to build and install the editable package so your code changes are reflected in the test environment. Also follow the server-management steps above when starting and stopping the Sail server.

Please follow the general development instructions when making code changes.
