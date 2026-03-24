---
title: Spark Setup
rank: 10
---

# Spark Setup

Run the following command to clone the projects required for the Spark tests.
All the projects are stored in the `opt` directory and ignored by Git.

```bash
git clone git@github.com:apache/spark.git opt/spark
git clone git@github.com:ibis-project/testing-data.git opt/ibis-testing-data
```

::: info

You may use options such as `--branch v4.1.1 --depth 1` to get a shallow copy of the Spark repository. But you would need to take care of fetching other branches if you need to work with multiple Spark versions.

:::

Run the following commands to build the Spark project.
The commands create a patched PySpark package containing Python code along with the JAR files.
Python tests are also included in the patched package.

You can choose to run any or all of the commands, depending on the Spark versions you want to test against.

```bash
env SPARK_VERSION=4.1.1 scripts/spark-tests/build-pyspark.sh
env SPARK_VERSION=3.5.7 scripts/spark-tests/build-pyspark.sh
```

::: info

If you have cloned the Spark repository a while ago and encounter the error `unknown revision or path not in the working tree` when running the `build-pyspark.sh` script, please run `git fetch` in the `opt/spark` directory to update the repository.

:::

::: info

You should install the required Java version according to the [Java Setup](../setup/java) instructions.

It is recommended to set the `JAVA_HOME` environment variable.
If the `JAVA_HOME` environment variable is not set, the Spark build script will try to find the Java installation
using the following heuristics.

- For Linux, the Java installation is assumed to be the location of `javac`.
- For macOS, the Java installation is retrieved from the output of the `/usr/libexec/java_home` command.

:::

::: info

Here are some notes about the `build-pyspark.sh` script.

1. The script will fail with an error if the Spark directory is not clean. The script internally applies a patch
   to the repository, and the patch is reverted before the script exits (either successfully or with an error).
2. The script can work with an arbitrary Python 3 installation,
   since the `setup.py` script in the Spark project only uses the Python standard library.
3. The script takes a while to run.
   On GitHub Actions, it takes about 30 minutes on the default GitHub-hosted runners.
   Fortunately, you only need to run this script once, unless there is a change in the Spark patch file.
   The patch file is in the `scripts/spark-tests` directory.

:::
