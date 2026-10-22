---
title: Spark Version Update
rank: 35
---

# Updating Spark Version

::: info
This guide is for the project maintainers.
:::

This page outlines the work required to support a new Spark version in the codebase.
Each Spark release may introduce features or incompatibilities with our scripts, so updating the codebase may require manual changes.

This page assumes that the Spark source code is available in the `opt/spark` directory, as described in [Spark Setup](../spark-tests/spark-setup).

## Standard Procedures

### Project Settings

Update the PySpark version pins in `pyproject.toml`.

1. Update the `test` optional dependency when updating the latest supported Spark version.
2. Update the `default` environment when updating the latest supported Spark version.
3. Update the `test` environment when adding or updating a supported Spark minor version.
4. Update the `test-spark` environment when changing a Spark version used for parity tests.
5. Update the `test-ibis` environment only when changing the Spark patch version used for Ibis tests, unless the Ibis version is also being updated.

### Spark Configuration

The `scripts/spark-config/generate.py` script collects Spark configuration into data files in `crates/sail-spark-connect/data/config`.
The data files are used to generate the related Rust code.

In the project root directory, run a command like the following for each supported Spark minor version to update its configuration data file.

```bash
env SPARK_LOCAL_IP=127.0.0.1 \
  hatch run test.spark-3.5.9:scripts/spark-config/generate.py \
  -o crates/sail-spark-connect/data/config/spark-3.5.json
```

Update `crates/sail-spark-connect/build.rs` when adding support for a new Spark minor version.

### Documentation

Please search for all mentions of Spark versions in the documentation in the `docs` directory and update them accordingly.

### GitHub Actions Workflows

Please search for all mentions of Spark versions in the GitHub Actions workflows in the `.github` directory and update them accordingly.

### Spark Connect Protocol Buffers

When updating the latest supported Spark version, copy the `.proto` files from Spark to the `crates/sail-spark-connect/proto` directory.
You may need to adjust the Rust code to work with the generated Spark Connect gRPC service code.

### Parity Tests

Update the parity test patch whenever changing a Spark version in the `test-spark` environment.

We build patched PySpark packages to run parity tests for the supported Spark major versions.

First, update the patch file. Here are example steps for updating from Spark `3.5.8` to `3.5.9`.

1. Create a branch `patch-v3.5.8` from the `v3.5.8` tag.
2. Run `git -C opt/spark apply ../../scripts/spark-tests/spark-3.5.8.patch` to apply the current patch. Commit the changes.
3. Create another branch `patch-v3.5.9` from the `v3.5.9` tag.
4. Merge branch `patch-v3.5.8` into branch `patch-v3.5.9` and resolve any merge conflicts.
5. Run `git -C opt/spark diff -p v3.5.9 patch-v3.5.9 > scripts/spark-tests/spark-3.5.9.patch` to generate the new patch.
6. Remove the old patch `scripts/spark-tests/spark-3.5.8.patch`.

The patch must include all PySpark tests in the built package. Make sure the patched `setup.py` includes all test subpackages in the corresponding Spark version.

The following command verifies that the patched PySpark package can be built successfully for the new Spark version. Note that the command may take a long time to run. For example, in GitHub Actions, it takes about 30 minutes.

```bash
env SPARK_VERSION=3.5.9 scripts/spark-tests/build-pyspark.sh
```

### Gold Data

Update the gold data patch whenever changing the Spark version used by `scripts/spark-gold-data/bootstrap.sh`.

First, update the patch file. Here are example steps for updating from Spark `3.5.8` to `3.5.9`.

1. Create a branch `gold-patch-v3.5.8` from the `v3.5.8` tag.
2. Run `git -C opt/spark apply ../../scripts/spark-gold-data/spark-3.5.8.patch` to apply the current patch. Commit the changes.
3. Create another branch `gold-patch-v3.5.9` from the `v3.5.9` tag.
4. Merge branch `gold-patch-v3.5.8` into branch `gold-patch-v3.5.9` and resolve any merge conflicts.
5. Run `git -C opt/spark diff -p v3.5.9 gold-patch-v3.5.9 > scripts/spark-gold-data/spark-3.5.9.patch` to generate the new patch.
6. Remove the old patch `scripts/spark-gold-data/spark-3.5.8.patch`.

Then update the Spark version mentioned in `scripts/spark-gold-data/bootstrap.sh`.

In the project root directory, run the following command to collect test cases from the Spark project.
This rewrites the gold data files with the new test input only.

```bash
scripts/spark-gold-data/bootstrap.sh
```

The command requires the `JAVA_HOME` environment variable to be set. On macOS, you can specify the Java installation path in the following way.

```bash
env JAVA_HOME="$(/usr/libexec/java_home)" scripts/spark-gold-data/bootstrap.sh
```

Then generate the Sail test output using the following command.

```bash
env SAIL_UPDATE_GOLD_DATA=1 cargo test
```

## Non-standard Procedures

Review changes to the PySpark UDF and UDTF code in `worker.py` and related Spark files. Identify version-specific logic and update the codebase accordingly.

Review the patch files closely, especially if they are edited directly after being generated.
Patch files with ad-hoc edits can have incorrect line numbers or file hashes even if the patch applies cleanly.
It is recommended to generate the patch again using only `git` commands (`git apply` and `git diff`) to ensure the patch is correct.
