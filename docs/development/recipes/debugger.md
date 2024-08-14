---
title: Using the Rust Debugger in RustRover
rank: 10
---

# Using the Rust Debugger in RustRover

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
