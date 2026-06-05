---
title: Data Sources
rank: 5
---

# Data Sources

Sail supports various data sources for reading and writing.

You can use the `SparkSession.read`, `DataFrame.write`, and `DataFrame.writeTo()` API to load and save data in different
formats.
You can also use the `CREATE TABLE` SQL statement to create a table that refers to a specific data source.

Here is a summary of the supported (:white_check_mark:) and unsupported (:x:) data sources for reading and writing data.
There are also features that are planned in our roadmap (:construction:).

| Format                 | Read Support       | Write Support      |
| ---------------------- | ------------------ | ------------------ |
| [Delta Lake](./delta/) | :white_check_mark: | :white_check_mark: |
| [Iceberg](./iceberg/)  | :white_check_mark: | :white_check_mark: |
| Files (Parquet)        | :white_check_mark: | :white_check_mark: |
| Files (CSV)            | :white_check_mark: | :white_check_mark: |
| Files (JSON)           | :white_check_mark: | :white_check_mark: |
| Files (Binary)         | :white_check_mark: | :x:                |
| Files (Text)           | :white_check_mark: | :white_check_mark: |
| Files (Avro)           | :white_check_mark: | :white_check_mark: |
| [Python](./python/)    | :white_check_mark: | :white_check_mark: |
| [JDBC](./jdbc/)        | :white_check_mark: | :construction:     |
| [Vortex](./vortex/)    | :white_check_mark: | :construction:     |
| Hudi                   | :construction:     | :construction:     |
| Files (ORC)            | :construction:     | :construction:     |
