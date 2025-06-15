---
title: Supported Features
rank: 1
---

# Supported Features

Here is a high-level overview of the Spark DataFrame API features supported by Sail.
The list covers the most common use cases of the DataFrame API and is not meant to be complete.

| Feature                                                                                            | Supported          |
| -------------------------------------------------------------------------------------------------- | ------------------ |
| I/O - Reading (`SparkSession.read`)                                                                | :white_check_mark: |
| I/O - Writing (`SparkSession.write`)                                                               | :white_check_mark: |
| Structured Streaming (`SparkSession.readStream`)                                                   | :construction:     |
| Result collection (`DataFrame.show`, `DataFrame.collect`, and `DataFrame.count`)                   | :white_check_mark: |
| Schema display (`DataFrame.printSchema`)                                                           | :white_check_mark: |
| Query - Projection (`DataFrame.select` and `DataFrame.selectExpr`)                                 | :white_check_mark: |
| Query - Column operations (e.g. `DataFrame.withColumn`, `DataFrame.replace`, and `DataFrame.drop`) | :white_check_mark: |
| Query - Filtering (`DataFrame.filter`)                                                             | :white_check_mark: |
| Query - Aggregation (`DataFrame.agg` and `DataFrame.groupBy`)                                      | :white_check_mark: |
| Query - Join (`DataFrame.join`)                                                                    | :white_check_mark: |
| Query - Set operations (e.g. `DataFrame.union`, `DataFrame.intersect`, and `DataFrame.exceptAll`)  | :white_check_mark: |
| Query - Limit (`DataFrame.offset` and `DataFrame.limit`)                                           | :white_check_mark: |
| Query - Sorting (`DataFrame.sort` and `DataFrame.orderBy`)                                         | :white_check_mark: |
| NA functions (`DataFrame.na`)                                                                      | :white_check_mark: |
| Statistics functions (`DataFrame.stat`)                                                            | :construction:     |
| View management (e.g. `DataFrame.createOrReplaceTempView`)                                         | :white_check_mark: |
| RDD Access (`DataFrame.rdd`)                                                                       | :x:                |
| PySpark UDFs                                                                                       | :white_check_mark: |
| PySpark UDTFs                                                                                      | :white_check_mark: |
