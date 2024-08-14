---
title: Reducing Build Time
rank: 20
---

# Reducing Build Time

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
3. The [RustRover debugger configuration](./debugger) sets the `PYO3_PYTHON` environment variable to the
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
