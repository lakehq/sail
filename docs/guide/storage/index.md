---
title: Storage
rank: 5
---

# Storage

Sail supports various storage solutions, including file systems and cloud storage services.

You can use the `SparkSession.read` and `SparkSession.write` API to load data from and write data to the storage.

You can also use the `CREATE TABLE` SQL statement to create a table that refers to data stored in the storage.

Here is a summary of the supported (:white_check_mark:) and unsupported (:x:) storage features for reading and writing data. There are also features that are planned in our roadmap (:construction:).

| Storage                        | Read Support       | Write Support      |
| ------------------------------ | ------------------ | ------------------ |
| [File Systems](./fs)           | :white_check_mark: | :white_check_mark: |
| [AWS S3](./s3)                 | :white_check_mark: | :white_check_mark: |
| [HDFS](./hdfs)                 | :white_check_mark: | :white_check_mark: |
| [Hugging Face](./hf)           | :white_check_mark: | :x:                |
| Azure Data Lake Storage (ADLS) | :construction:     | :construction:     |
| Azure Blob Storage             | :construction:     | :construction:     |
| Google Cloud Storage (GCS)     | :construction:     | :construction:     |
