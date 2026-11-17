---
title: Python Setup
rank: 20
---

# Python Setup

We use [Hatch](https://hatch.pypa.io/latest/) to manage Python environments.
The environments are defined in the `pyproject.toml` file.

When you run Hatch commands, environments are created in `.venvs/` in the project root directory.
You can also run `hatch env create` to create the `default` environment explicitly, and then configure your IDE
to use this environment (`.venvs/default`) for Python development.

::: info

1. Sail's Hatch environments use `uv` as the package installer for both local development and CI.
   For custom package indexes or mirrors, configure `uv` using `UV_INDEX_URL` or its
   [configuration files](https://docs.astral.sh/uv/configuration/files/).
   `uv` does not read pip-specific settings such as `PIP_INDEX_URL` or `pip.conf`.
   The environments also include `pip` for manual package installation and IDE support.
2. Hatch will download prebuilt Python interpreters when the specified Python version for an environment
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

:::
