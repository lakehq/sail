# LakeSail

## Getting Started

### Prerequisites

Please install the protocol buffer compiler (`protoc`) and the Rust toolchain (stable and nightly).

### Building the Project

Run the following commands to verify the code before committing changes.

```bash
cargo +nightly fmt && cargo clippy --all-targets --all-features && cargo build && cargo test
```

The code can be built and tested using the stable toolchain,
while the nightly toolchain is required for formatting the code.

Please make sure there are no warnings in the output.
The GitHub Actions workflow runs `cargo clippy` with the `-D warnings` option,
so that the build will fail if there are any warnings from either the compiler or the linter.

If the test fails due to mismatched gold data, use the following command to update the gold data
and commit the changes.

```bash
env FRAMEWORK_UPDATE_GOLD_DATA=1 cargo test
```

## Development Notes

### Python Setup

It is recommended to install Python via [pyenv](https://github.com/pyenv/pyenv).

```bash
# Build and install Python.
pyenv install 3.11.9

# Set the global Python version.
# If you do not want to set the global Python version, you can use the `PYENV_VERSION` environment variable
# or the `pyenv shell` command to set the Python version for the current terminal session.
pyenv global 3.11.9

# Install required tools for the Python version.
pip install poetry
```

The same Python version should be used in all the following sections.
We will use this Python interpreter to create two virtual environments,
one for the Spark project and the other for the framework.

### Java Setup

Please install OpenJDK 17 on your host.
You can use any widely-used OpenJDK distribution, such as [Amazon Corretto](https://aws.amazon.com/corretto/).

It is recommended to set `JAVA_HOME` when following the instructions in the next sections.
If the `JAVA_HOME` environment variable is not set, the Spark build script will try to find the Java installation using
either
(1) the location of `javac` (for Linux), or (2) the output of `/usr/libexec/java_home` (for macOS).

### Spark Setup

Run the following command to clone the Spark project.

```bash
git clone git@github.com:apache/spark.git opt/spark
```

Run the following command to patch the Spark project and set up the Python virtual environment for Spark.
You need to make sure the Spark directory is clean before applying the patch.

```bash
git -C opt/spark checkout v3.5.1
git -C opt/spark apply ../../scripts/spark-tests/spark-3.5.1.patch
scripts/spark-tests/build-spark-jars.sh
scripts/spark-tests/setup-spark-env.sh
```

It may take a while to build the Spark project.
(On GitHub Actions, it takes about 40 minutes on the default GitHub-hosted runners.)

### Python Virtual Environment Setup

Run the following commands to set up a Python virtual environment for the project.

```bash
poetry -C python install
```

### Running the Spark Connect Server

Use the following commands to build and run the Spark Connect server powered by the framework.

```bash
scripts/spark-tests/run-server.sh
```

You can run the Python examples in another terminal using the following command.

```bash
poetry -C python run python -m app
```

### Running Spark Tests

After running the Spark Connect server, start another terminal and use the following command to run the Spark tests.
The test logs will be written to `opt/spark/logs/<name>` where `<name>` is defined by
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
# Write the test logs to a different directory (`opt/spark/logs/selected`).
export TEST_RUN_NAME=selected

scripts/spark-tests/run-tests.sh python/pyspark/sql/tests/connect/ -v -k test_something
```

When you customize the test execution using the above command, a single test suite will be run,
and the test log files are always `test.jsonl` and `test.log` in the log directory.

### Analyzing Spark Test Logs

Here are some useful commands to analyze Spark test logs.
You can replace `test.jsonl` with a different log file name if you are analyzing a different test suite.

(1) Get the error counts for failed tests.

```bash
# You can remove the `--slurpfile baseline opt/spark/logs/baseline/test.jsonl` arguments
# if you do not have baseline test logs.
jq -r -f scripts/spark-tests/count-errors.jq \
  --slurpfile baseline opt/spark/logs/baseline/test.jsonl \
  opt/spark/logs/latest/test.jsonl | less
```

(2) Show a sorted list of passed tests.

```bash
jq -r -f scripts/spark-tests/show-passed-tests.jq \
  opt/spark/logs/latest/test.jsonl | less
```

### Starting a Local PySpark Session

You can use the following commands to start a local PySpark session.

```bash
cd opt/spark
source venv/bin/activate

# Run the PySpark shell using the original Java implementation.
env SPARK_PREPEND_CLASSES=1 SPARK_LOCAL_IP=127.0.0.1 bin/pyspark

# Run the PySpark shell using the Spark Connect implementation.
# You can ignore the "sparkContext() is not implemented" error when the shell starts.
env SPARK_PREPEND_CLASSES=1 SPARK_REMOTE="sc://localhost:50051" bin/pyspark
```

### Running Spark Tests in GitHub Actions

The Spark tests are triggered in GitHub Actions for pull requests,
either when the pull request is opened or when the commit message contains `[spark tests]` (case-insensitive).

The Spark tests are always run when the pull request is merged into the `main` branch.

### Running the Rust Debugger in RustRover

Since we use PyO3 to support Python binding in Rust, we need some additional setup to run the Rust debugger in
RustRover.
In **Run** > **Edit Configurations**, add a new **Cargo** configuration with the following settings:

1. Name: **Run Spark Connect server** (You can use any name you like.)
2. Command: `run -p framework-spark-connect`
3. Environment Variables:
    - (required) `PYTHONPATH`: `python/.venv/lib/python<version>/site-packages` (Please replace `<version>` with the
      actual Python version, e.g. `3.11`.)
    - (required) `PYO3_PYTHON`: `<project>/python/.venv/bin/python` (Please replace `<project>` with the actual project
      path. **This must be an absolute path.**)
    - (required) `RUST_MIN_STACK`: `8388608`
    - (optional) `RUST_BACKTRACE`: `full`
    - (optional) `RUST_LOG`: `framework_spark_connect=debug`

When entering environment variables, you can click on the button on the right side of the input box to open the dialog
and add the environment variables one by one.

You can leave the other settings as default.

### Updating the Spark Patch

You can use the following commands to update the Spark patch with your local modification.

```bash
git -C opt/spark add .
git -C opt/spark diff --staged -p > scripts/spark-tests/spark-3.5.1.patch
```
