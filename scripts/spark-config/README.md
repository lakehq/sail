# Spark Configuration Data Collection

This is not part of the daily developer workflow.

This directory contains the script to collect Spark SQL configuration information
into a data file.
The data file is used to generate the Rust code for the Spark configuration.

Run the following command **in the project root directory** to update the data file.
Please commit the changes if any.

```bash
poetry -C python run scripts/spark-config/generate.py -o crates/framework-spark-connect/data/spark_config.json
```