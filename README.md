# LakeSail

## Development Notes

### Python Setup

It is recommended to install Python via [pyenv](https://github.com/pyenv/pyenv).

```bash
# Build and install Python.
pyenv install 3.11.9

# Set the global Python version.
pyenv global 3.11.9

# Install required tools for the global Python version.
pip install poetry
```

The same Python version should be used in all the following sections.
At a high level, we will use this Python interpreter to create two virtual environments,
one for the Spark project and the other for the framework Python examples.

### Spark Setup

Run the following command to clone the Spark project.

```bash
git clone git@github.com:apache/spark.git
```

In the remainder of this document, `${SPARK_PROJECT_PATH}` refers to the absolute path of the Spark project,
while `${FRAMEWORK_PROJECT_PATH}` refers to the absolute path of the LakeSail framework project.

Run the following command to patch the Spark project and set up a virtual environment.

```bash
"${FRAMEWORK_PROJECT_PATH}"/scripts/spark-connect-server/setup-spark-env.sh
```

### Python Examples Setup

Run the following commands to set up a virtual environment for the Python examples.

```bash
cd "${FRAMEWORK_PROJECT_PATH}"/examples/python && poetry install
```

### Running the Spark Connect Server

Use the following commands to build and run the Spark Connect server powered by the framework.

```bash
"${FRAMEWORK_PROJECT_PATH}"/scripts/spark-connect-server/run-framework-server.sh
```

You can run the Python examples in another terminal.
Please refer to `examples/python/README.md` for more information.

### Running Spark Tests

After running the Spark Connect server, start another terminal and use the following commands to run the Spark tests.

```bash
cd "${SPARK_PROJECT_PATH}" && source venv/bin/activate

# Run the tests and write the output to a log file.
# It takes a few minutes to run the tests.
env SPARK_TESTING_REMOTE_PORT=50051 \
  SPARK_LOCAL_IP=127.0.0.1 \
  python/run-pytest.sh \
  python/pyspark/sql/tests/connect/ \
  --full-trace \
  > logs/test.log

# The following are a few useful commands for development.

# Get the distribution of error details for failed tests.
# This does not include tests failed due to assertion errors.
# You can replace the last `less` with `awk '{ sum += $1 } END { print sum }'`
# to get the total number of failed tests.
cat logs/test.log | sed -n -e 's/^E.*\tdetails = "\(.*\)"/\1/p' | sort | uniq -c | sort -k1,1r | less

# Compare failed tests between two runs.
diff -u <(cat logs/test.1.log | grep "^FAILED") <(cat logs/test.2.log | grep "^FAILED") | less

# Start an interactive console with a local PySpark session.
env SPARK_PREPEND_CLASSES=1 bin/pyspark
```
