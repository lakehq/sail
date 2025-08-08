---
title: Data Formats
rank: 4
---

# Data Formats

Sail supports various data formats for reading and writing.

You can use the `SparkSession.read`, `DataFrame.write`, and `DataFrame.writeTo()` API to load and save data in different formats.
You can also use the `CREATE TABLE` SQL statement to create a table that refers to data stored in a specific format.

Here is a summary of the supported (:white_check_mark:) and unsupported (:x:) data formats for reading and writing data. There are also features that are planned in our roadmap (:construction:).

| Format                | Read Support       | Write Support      |
| --------------------- | ------------------ | ------------------ |
| CSV                   | :white_check_mark: | :white_check_mark: |
| JSON                  | :white_check_mark: | :white_check_mark: |
| Parquet               | :white_check_mark: | :white_check_mark: |
| Text                  | :construction:     | :construction:     |
| Binary                | :construction:     | :construction:     |
| Avro                  | :construction:     | :construction:     |
| Protocol Buffers      | :construction:     | :construction:     |
| ORC                   | :x:                | :x:                |
| [Delta Lake](./delta) | :white_check_mark: | :white_check_mark: |
| Iceberg               | :construction:     | :construction:     |
| Hudi                  | :construction:     | :construction:     |
