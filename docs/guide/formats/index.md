---
title: Data Formats
rank: 5
---

# Data Formats

Sail supports various data formats for reading and writing.

You can use the `SparkSession.read`, `DataFrame.write`, and `DataFrame.writeTo()` API to load and save data in different
formats.
You can also use the `CREATE TABLE` SQL statement to create a table that refers to data stored in a specific format.

Here is a summary of the supported (:white_check_mark:) and unsupported (:x:) data formats for reading and writing data.
There are also features that are planned in our roadmap (:construction:).

| Format                 | Read Support                 | Write Support                |
| ---------------------- | ---------------------------- | ---------------------------- |
| [Delta Lake](./delta)  | :white_check_mark: (partial) | :white_check_mark: (partial) |
| [Iceberg](./iceberg)   | :white_check_mark: (partial) | :white_check_mark: (partial) |
| Parquet                | :white_check_mark:           | :white_check_mark:           |
| Binary (any file type) | :white_check_mark:           | :x:                          |
| CSV                    | :white_check_mark:           | :white_check_mark:           |
| JSON                   | :white_check_mark:           | :white_check_mark:           |
| Text                   | :white_check_mark:           | :white_check_mark:           |
| Avro                   | :white_check_mark:           | :white_check_mark:           |
| Protocol Buffers       | :construction:               | :construction:               |
| Hudi                   | :construction:               | :construction:               |
| ORC                    | :x:                          | :x:                          |
