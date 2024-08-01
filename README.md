# Sail

## Getting Started

### Prerequisites

You need the Rust toolchain (both stable and nightly) to build the project.
You can use [rustup](https://rustup.rs/) to manage the Rust toolchain in your local environment.

You also need the following tools when working on the project.

1. The [Protocol Buffers](https://protobuf.dev/) compiler (`protoc`).
2. [Hatch](https://hatch.pypa.io/latest/).
3. [Maturin](https://www.maturin.rs/).
4. [Zig](https://ziglang.org/).

On macOS, you can install these tools via Homebrew.

```bash
brew install protobuf hatch maturin
```

If Homebrew overrides your default Rust installation,
you can prioritize the rustup-managed Rust by adding the following line to your shell profile.

```bash
export PATH="$HOME/.cargo/bin:$PATH"
```

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

#### Updating Test Gold Data

If the test fails due to mismatched gold data, use the following command to update the gold data
and commit the changes.

```bash
env SAIL_UPDATE_GOLD_DATA=1 cargo test
```

#### Building the Python Package

Run the following command to build the Python package using Maturin. The command builds the package inside the default
Hatch environment.

```bash
hatch run maturin build
```

If you want to build and install the Python package for local development, run the following command.

```bash
hatch run maturin develop
```

The command installs the source code as an editable package in the Hatch environment, while
The built `.so` native library is stored in the source directory. You can then use `hatch shell`
to enter the Python environment and test the library. Any changes to the Python code will be reflected in the
environment immediately. But if you make changes to the Rust code, you need to run the `develop` command again.

### Cross-compilation of the Python Package

Cross-compilation of the Python package is straightforward with Maturin and Zig.
For example, to build the package for the `aarch64-unknown-linux-gnu` target, run the following command to install
the target toolchain.

```bash
rustup target add aarch64-unknown-linux-gnu
```

Then run the following command to build the Python package for the target.

```bash
hatch run maturin build --release --target aarch64-unknown-linux-gnu --zig
```

#### Additional Notes

1. To build the Windows target on Linux/macOS, set the following environment variable to work around the issue with the
   `blake3` crate.
   ```bash
   export CARGO_FEATURE_PURE=1
   ```
2. Cross-compilation of the binary executable is more complicated since it requires a cross-compiled Python interpreter
   library. Currently, we do not provide support for this. However, if you are interested in exploring this, you can
   look at the PyO3 [documentation](https://pyo3.rs/main/building-and-distribution) and the
   [cargo zigbuild](https://github.com/rust-cross/cargo-zigbuild) tool.

## Development Notes

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

Run the following command to build the Spark project.
The command creates a patched PySpark package containing Python code along with the JAR files.
Python tests are also included in the patched package.

```bash
scripts/spark-tests/build-pyspark.sh
```

#### Additional Notes

Here are some notes about the `build-pyspark.sh` script.

1. The script will fail with an error if the Spark directory is not clean. The script internally applies a patch
   to the repository, and the patch is reverted before the script exits (either successfully or with an error).
2. The script can work with an arbitrary Python 3 installation,
   since the `setup.py` script in the Spark project only uses the Python standard library.
3. The script takes a while to run.
   On GitHub Actions, it takes about 40 minutes on the default GitHub-hosted runners.
   Fortunately, you only need to run this script once, unless there is a change in the Spark patch file.
   The patch file is in the `scripts/spark-tests` directory.

### Python Setup

We use [Hatch](https://hatch.pypa.io/latest/) to manage Python environments.
The environments are defined in the `pyproject.toml` file.

When you run Hatch commands, environments are created in `.venvs/` in the project root directory.
You can also run `hatch env create` to create the `default` environment explicitly, and then configure your IDE
to use this environment (`.venvs/default`) for Python development.

#### Additional Notes

1. Some environments depend on the patched PySpark package created in the previous section.
   The patched PySpark package will be installed automatically during the environment creation.
2. For this project, all Hatch environments are configured to use `pip` as the package installer for local development,
   so pip environment variables such as `PIP_INDEX_URL` still work.
   However, it is recommended to also set `uv` environment variables such as `UV_INDEX_URL`, since Hatch
   uses `uv` as the package installer for internal environments (e.g. when doing static analysis
   via `hatch fmt`.)
3. Hatch will download prebuilt Python interpreters when the specified Python version for an environment
   is not installed on your host. Note that the prebuilt Python interpreters only track the **minor version** of Python.
   If downloading prebuilt Python interpreters fails (e.g. due to network issues), or if you want precise control
   over the **patch version** of Python being used in Hatch environments, you can install Python manually so that
   Hatch can pick up the Python installations. For example, you can use [pyenv](https://github.com/pyenv/pyenv) to
   install multiple Python versions and run the following command in the project root directory.
   ```bash
   pyenv local 3.8.19 3.9.19 3.10.14 3.11.9
   ```
   The above command creates a `.python-version` file (ignored by Git) in the project root directory, so that multiple
   Python versions are available on `PATH` due to the pyenv shim.
   These Python versions are then available to Hatch for the environment creation.

### Running the Spark Connect Server

Use the following commands to build and run the Spark Connect server powered by Sail.

```bash
scripts/spark-tests/run-server.sh
```

### Running Spark Tests

Before running Spark tests, please create the `test` Hatch environment using the following commands.
Note that you do *not* need to run `maturin develop` in the `test` environment again after you make code changes.
We only use the pytest plugins (pure Python code) from the project, which do not need to be rebuilt by Maturin.

```bash
hatch env create test
hatch run test:install-pyspark
hatch run test:maturin develop
```

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

### Analyzing Spark Test Logs

Here are some useful commands to analyze Spark test logs.
You can replace `test.jsonl` with a different log file name if you are analyzing a different test suite.

(1) Get the error counts for failed tests.

```bash
# You can remove the `--slurpfile baseline tmp/spark-tests/baseline/test.jsonl` arguments
# if you do not have baseline test logs.
jq -r -f scripts/spark-tests/count-errors.jq \
  --slurpfile baseline tmp/spark-tests/baseline/test.jsonl \
  tmp/spark-tests/latest/test.jsonl | less
```

(2) Show a sorted list of passed tests.

```bash
jq -r -f scripts/spark-tests/show-passed-tests.jq \
  tmp/spark-tests/latest/test.jsonl | less
```

(3) Show the differences of passed tests between two runs.

```bash
diff -U 0 \
  <(jq -r -f scripts/spark-tests/show-passed-tests.jq tmp/spark-tests/baseline/test.jsonl) \
  <(jq -r -f scripts/spark-tests/show-passed-tests.jq tmp/spark-tests/latest/test.jsonl)
```

### Starting a Local PySpark Session

You can use the following commands to start a local PySpark session.

```bash
# Run the PySpark shell using the original Java implementation.
hatch run pyspark

# Run the PySpark shell using the Spark Connect implementation.
env SPARK_CONNECT_MODE_ENABLED=1 SPARK_REMOTE="sc://localhost:50051" hatch run pyspark
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
2. Command: `run -p sail-spark-connect --example server`
3. Environment Variables:
    - (required) `PYTHONPATH`: `.venvs/default/lib/python<version>/site-packages` (Please replace `<version>` with the
      actual Python version, e.g. `3.11`.)
    - (required) `PYO3_PYTHON`: `<project>/.venvs/default/bin/python` (Please replace `<project>` with the actual
      project
      path. **This must be an absolute path.**)
    - (required) `RUST_MIN_STACK`: `8388608`
    - (optional) `RUST_BACKTRACE`: `full`
    - (optional) `RUST_LOG`: `sail_spark_connect=debug`

When entering environment variables, you can click on the button on the right side of the input box to open the dialog
and add the environment variables one by one.

You can leave the other settings as default.

### Reducing Build Time

The PyO3 package will be rebuilt when the Python interpreter changes.
This will cause all downstream packages to be rebuilt, resulting in a long build time for development.
The issue gets more complicated when you use both command line tools and IDEs, which share the same Cargo build cache.
(For example, RustRover may run `cargo check` in the background.)

To reduce the build time, you need to make sure the Python interpreter used by PyO3 is configured in the same way
across environments. Please consider the following items.

1. Please always invoke Maturin via Hatch (e.g. `hatch run maturin develop` and `hatch run maturin build`). In this way,
   Maturin internally sets the `PYO3_PYTHON` environment variable to the absolute path of the Python interpreter of the
   project's default Hatch environment.
2. The `scripts/spark-tests/run-server.sh` script internally sets the `PYO3_PYTHON` environment variable to the
   same value as above.
3. The RustRover debugger configuration in the previous section sets the `PYO3_PYTHON` environment variable to the
   same value as above.
4. For RustRover, in "**Preferences**" > "**Rust**" > "**External Linters**", set the `PYO3_PYTHON` environment variable
   to the same value as above.
5. If you need to run Cargo commands such as `cargo build`, set the `PYO3_PYTHON` environment variable in the terminal
   session to the same value as above.
   ```bash
   # Run the following command in the project root directory.
   export PYO3_PYTHON="$(hatch env find)/bin/python"
   ```

Note that the `maturin` command and the `cargo` command enables different features for PyO3 (e.g. `extension-module`).
So if you alternate between the two build tools, the PyO3 library will still be rebuilt.

If you run `hatch build`, it uses Maturin as the build system, and the build happens in an isolated Python environment.
So the build does not interfere with the Cargo build cache in `target/`. However, it also means that a fresh build
is performed every time, which can be slow. Therefore, it is not recommended to use `hatch build` for local development.

### Working with the Spark Patch

Occasionally, you may need to patch the Spark source code further.
Here are the commands that can be helpful for this purpose.

```bash
# Apply the patch.
# You can now modify the Spark source code.
git -C opt/spark apply ../../scripts/spark-tests/spark-3.5.1.patch

# Update the Spark patch file with your local modification.
git -C opt/spark add .
git -C opt/spark diff --staged -p > scripts/spark-tests/spark-3.5.1.patch

# Revert the patch.
git -C opt/spark reset
git -C opt/spark apply -R ../../scripts/spark-tests/spark-3.5.1.patch
```

However, note that we should keep the patch minimal.
It is possible to alter many Spark test behaviors at runtime via monkey-patching using pytest fixtures.
