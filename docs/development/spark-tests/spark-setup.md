---
title: Spark Setup
rank: 10
---

# Spark Setup

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

::: info

Here are some notes about the `build-pyspark.sh` script.

1. The script will fail with an error if the Spark directory is not clean. The script internally applies a patch
   to the repository, and the patch is reverted before the script exits (either successfully or with an error).
2. The script can work with an arbitrary Python 3 installation,
   since the `setup.py` script in the Spark project only uses the Python standard library.
3. The script takes a while to run.
   On GitHub Actions, it takes about 40 minutes on the default GitHub-hosted runners.
   Fortunately, you only need to run this script once, unless there is a change in the Spark patch file.
   The patch file is in the `scripts/spark-tests` directory.

:::
