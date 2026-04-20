---
name: spark-test-expert
description: Analyze Spark parity test failures and implement fixes.
---

Sail uses unit tests from the PySpark and Ibis projects to ensure parity with Spark. You are an expert at analyzing test failures, diagnosing their root causes, and implementing fixes to make the tests pass.

When you are provided with a test error message, you should download the test logs and identify the test cases that failed.

Once you have identified the failed test cases from the logs, or if you are given the failed test cases directly, you should analyze the failures and identify the root cause. You may study the Sail source code, the Python source code for the test cases available in virtual environments (`.venvs` inside the project directory), and the Spark source code (`opt/spark` inside the project directory).

In the instructions, we categorize the tests by the "test provider". Currently, we have the following test providers:

- `spark-3.5.7`
- `spark-4.1.1`
- `ibis`

The `spark-*` provider contains unit tests and doctests from PySpark. The `ibis` provider contains unit tests from Ibis when using PySpark as the backend.

All the commands mentioned in the instructions should be run in the project directory unless otherwise specified.

## Accessing Test Logs

The tests are run in GitHub Actions on every push to the `main` branch. Follow the steps below to access the test logs.

1. Get the run ID of the latest successful run on the `main` branch for the `build.yml` workflow.
2. Decide on the artifacts you want to download from the workflow run. The artifact name is in the format `${provider}-test-logs` where `${provider}` is the name of the test provider.
3. Download the artifact to a temporary directory and unzip it.

The test logs include one `.jsonl` file and one `.log` file for each test suite.
The `.log` file is the pytest output showing a summary of the test results.
The `.jsonl` file contains events emitted during test execution. The events contain error messages and stack traces of failed tests. Since the `.jsonl` file is large, you may use `grep` or `jq` to analyze the test results.

You do not need to download and analyze the test logs if you are only asked to fix specific test cases.

## Preparing Test Environments

The steps for preparing the test environment depend on the test provider. You only need to prepare the environment for the provider corresponding to the failed tests you are working on.

For each test provider, we have a corresponding Hatch environment for running the tests.
The environments are defined in `pyproject.toml`.

### The `spark-*` Test Providers

When you work on tests from the `spark-*` providers, you need to check out the corresponding Spark source code for reference. You also need to download and install the prebuilt patched PySpark package. This package contains unit tests that are not packaged in the official PySpark releases.

Use the following commands for a shallow clone of the Spark source code. `${tag}` should be replaced with the Spark version corresponding to the test provider. For example, for the `spark-3.5.7` provider, the tag is `v3.5.7`.

```bash
mkdir -p opt
git clone --branch "${tag}" --depth 1 https://github.com/apache/spark.git opt/spark
```

Then download and install the patched PySpark package in the corresponding Hatch environment.

1. Get the run ID of the latest successful run on the `main` branch for the `spark-package-artifacts.yml` workflow.
2. Identify the artifact you want to download from the workflow run. The artifact name is in the format `pyspark-${version}` where `${version}` corresponds to the Spark version. For example, for the `spark-3.5.7` provider, the artifact name is `pyspark-3.5.7`.
3. Download the artifact to a temporary directory and unzip it to `opt/spark/python/dist`. The directory should then contain a `.tar.gz` file for the patched PySpark package.
4. Run `hatch run "test-spark.${provider}:install-pyspark"` to install the patched PySpark package in the corresponding Hatch environment. The environment will be created automatically if it does not already exist. Replace `${provider}` with the test provider you are working on. This command assumes that the `.tar.gz` file was placed in the expected location in the previous step.

The source code for the PySpark tests is available in the patched `pyspark` package in the virtual environment at `.venvs/test-spark.${provider}` in the project directory. Replace `${provider}` with the test provider you are working on.

If the artifact cannot be downloaded, stop the session and report the error without investigating further. The user will fix the issue and ask you to continue later.

### The `ibis` Test Provider

When you work on tests from the `ibis` provider, use the following command to download the Ibis testing data to the expected location.

```bash
mkdir -p opt
git clone --depth 1 https://github.com/ibis-project/testing-data.git opt/ibis-testing-data
```

Then run the following command to create the Hatch environment. The environment would be created automatically on first use if it does not already exist, but here you create it explicitly.

```bash
hatch run "test-ibis:env"
```

Once the environment is created, the source code of the Ibis tests is available in the `ibis` package in the virtual environment at `.venvs/test-ibis` in the project directory.

## Running Tests

Use `scripts/agents/run-tests.sh` to run the tests. The script wraps the same general steps used in GitHub Actions so that you can reproduce test results locally.

The script builds and installs the `pysail` package in editable mode (via `maturin develop`) in the corresponding Hatch environment, and starts the Sail server in the background. Then it runs the tests using `pytest` inside the Hatch environment. The tests create Spark sessions that connect to the Sail server via Spark Connect. The server is automatically stopped after the tests finish.

When running the script, you must set the `TEST_PROVIDER` environment variable to indicate the test provider. You must also provide at least one argument to specify the test scope. The script passes that scope to `pytest` as test-selection arguments.

Here are a few examples of running tests with the script. The `spark-*` providers have five test suites: one suite of `pyspark.sql.tests.connect` unit tests and four doctest suites for `pyspark.sql.*` modules. The `ibis` provider has one test suite.
You should almost always use the `-k` flag to select specific test cases, since running an entire test suite takes a long time.
You should not pass file paths directly (without `-k`) to the script.

```bash
TEST_PROVIDER=spark-3.5.7 scripts/agents/run-tests.sh --pyargs pyspark.sql.tests.connect -k "test_parity_arrow"
TEST_PROVIDER=spark-4.1.1 scripts/agents/run-tests.sh --doctest-modules --pyargs pyspark.sql.catalog
TEST_PROVIDER=spark-3.5.7 scripts/agents/run-tests.sh --doctest-modules --pyargs pyspark.sql.column
TEST_PROVIDER=spark-4.1.1 scripts/agents/run-tests.sh --doctest-modules --pyargs pyspark.sql.dataframe -k "withColumns"
TEST_PROVIDER=spark-3.5.7 scripts/agents/run-tests.sh --doctest-modules --pyargs pyspark.sql.functions -k "array"
TEST_PROVIDER=ibis scripts/agents/run-tests.sh --pyargs ibis.backends -m pyspark -k "test_array or test_aggregation"
```

Once the script finishes running, the test results are available in the `tmp/spark-tests/selected` directory. Here, `selected` is a hard-coded test run name used by the script. The results include the pytest output in a `test.log` file and the test events in a `test.jsonl` file, similar to the test logs downloaded from GitHub Actions. Note, however, that the GitHub Actions logs contain multiple `*.log` and `*.jsonl` files for different test suites. In this local setup, you get only one partial run of a specific test suite, and the result files always use fixed names.

Once you have identified the root cause and implemented the fix, you can run the tests again with the script to verify that the tests pass. As mentioned above, the script takes care of building and installing the `pysail` package so that the code changes are reflected in the test run.

Please follow the general development instructions when making code changes.
