---
title: Building the Python Package
rank: 20
---

# Building the Python Package

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
