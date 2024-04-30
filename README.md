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

Run the following commands to patch the Spark project and set up a virtual environment.

```bash
cd "${SPARK_PROJECT_PATH}"

git checkout v3.5.1
git apply "${FRAMEWORK_PROJECT_PATH}"/scripts/spark-3.5.1.patch

# Create a directory for test logs. This directory is in `.gitignore`.
mkdir -p logs

python -m venv venv
echo '*' > venv/.gitignore
source venv/bin/activate
pip install --upgrade pip
pip install --upgrade -r python/requirements.txt
```

### Python Examples Setup

Run the following commands to set up a virtual environment for the Python examples.

```bash
cd "${FRAMEWORK_PROJECT_PATH}"/examples/python

poetry install
```

### Running the Spark Connect Server

Use the following commands to build and run the Spark Connect server powered by the framework.

```bash
cd "${FRAMEWORK_PROJECT_PATH}"

source examples/python/.venv/bin/activate

export RUST_LOG=spark_connect_server=debug
export RUST_BACKTRACE=full
# We have to set `PYTHONPATH` even if we are using the virtual environment.
# This is because the Python executable is the Rust program itself, and there is
# no `pyvenv.cfg` at its required location (one directory above the executable).
export PYTHONPATH="examples/python/.venv/lib/python3.11/site-packages"
cargo run -p spark-connect-server
```

You can run the Python examples in another terminal.
Please refer to `examples/python/README.md` for more information.

### Running Spark Tests

After running the Spark Connect server, start another terminal and use the following commands to run the Spark tests.

```bash
cd "${SPARK_PROJECT_PATH}"
source venv/bin/activate

# Run the tests and write the output to a log file.
# It takes a few minutes to run the tests.
env SPARK_TESTING_REMOTE_PORT=50051 \
  SPARK_LOCAL_IP=127.0.0.1 \
  python/run-pytest.sh \
  python/pyspark/sql/tests/connect/ \
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
