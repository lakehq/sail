# Spark Configuration Data Collection

This is not part of the daily developer workflow.

This directory contains the script to collect Spark SQL configuration information
into a data file.
The data file is used to generate the Rust code for the Spark configuration.

Run the following command **in the project root directory** for each supported
PySpark version to update its data file. Please commit the changes if any.

```bash
env SPARK_LOCAL_IP=127.0.0.1 \
  hatch run test.spark-${SPARK_VERSION}:scripts/spark-config/generate.py \
  -o crates/sail-spark-connect/data/config/spark-${SPARK_VERSION%.*}.json
```
