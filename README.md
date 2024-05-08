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
git clone git@github.com:apache/spark.git opt/spark
```

Run the following command to patch the Spark project and set up the Spark environment.
You need to make sure your working directory is clean before applying the patch.

```bash
git -C opt/spark checkout v3.5.1
git -C opt/spark apply ../../scripts/spark-tests/spark-3.5.1.patch
scripts/spark-tests/build-spark-jars.sh
scripts/spark-tests/setup-spark-env.sh
```

### Python Examples Setup

Run the following commands to set up a virtual environment for the Python examples.

```bash
poetry -C examples/python install
```

### Running the Spark Connect Server

Use the following commands to build and run the Spark Connect server powered by the framework.

```bash
scripts/spark-tests/run-server.sh
```

You can run the Python examples in another terminal.
Please refer to `examples/python/README.md` for more information.

### Running Spark Tests

After running the Spark Connect server, start another terminal and use the following command to run the Spark tests.
The test logs will be written to `opt/spark/logs/<name>` where `<name>` is defined by
the `TEST_RUN_NAME` environment variable whose default value is `latest`.

```bash
scripts/spark-tests/run-tests.sh
```

The following are useful commands to analyze test logs.

(1) Get the error counts for failed tests.

```bash
# You can remove the `--slurpfile baseline logs/baseline/test.jsonl` arguments
# if you do not have baseline test logs.
jq -r -f scripts/spark-tests/count-errors.jq \
  --slurpfile baseline logs/baseline/test.jsonl \
  logs/latest/test.jsonl | less
```

(2) Show a sorted list of passed tests.

```bash
jq -r -f scripts/spark-tests/show-passed-tests.jq \
  logs/latest/test.jsonl | less
```

You can use the following commands to start a local PySpark session.

```bash
cd opt/spark
source venv/bin/activate
env SPARK_LOCAL_IP=127.0.0.1 SPARK_PREPEND_CLASSES=1 bin/pyspark
```

The Spark tests are also triggered in GitHub Actions for pull requests,
either when the pull request is opened or when the commit message contains `[spark tests]` (case-insensitive).
