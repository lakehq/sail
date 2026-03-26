---
title: Supported Features
rank: 2
---

# Supported Features

## Core Table Operations

| Feature                                     | Supported          |
| ------------------------------------------- | ------------------ |
| Read                                        | :white_check_mark: |
| Write (append)                              | :white_check_mark: |
| Write (overwrite)                           | :white_check_mark: |
| Data skipping (partition pruning)           | :white_check_mark: |
| Data skipping (pruning via file statistics) | :white_check_mark: |
| Schema validation                           | :white_check_mark: |
| Schema evolution                            | :white_check_mark: |
| Time travel (by version)                    | :white_check_mark: |
| Time travel (by timestamp)                  | :white_check_mark: |

Both non-partitioned and partitioned tables are supported for reading and writing.

## DML Operations

| Feature                  | Supported          |
| ------------------------ | ------------------ |
| `DELETE` (copy-on-write) | :white_check_mark: |
| `MERGE` (copy-on-write)  | :white_check_mark: |
| `DELETE` (merge-on-read) | :construction:     |
| `MERGE` (merge-on-read)  | :construction:     |
| `UPDATE`                 | :construction:     |

The "merge-on-read" mode refers to updating the table with deletion vectors. This reduces the amount of data that needs to be rewritten during DML operations, but incurs additional read overhead when querying the table.

## Table Maintenance Operations

| Feature    | Supported      |
| ---------- | -------------- |
| `VACUUM`   | :construction: |
| `OPTIMIZE` | :construction: |
| `RESTORE`  | :construction: |

## Protocol Internals

| Feature                          | Supported          |
| -------------------------------- | ------------------ |
| Checkpointing                    | :white_check_mark: |
| Log clean-up                     | :white_check_mark: |
| Column mapping                   | :white_check_mark: |
| Deletion vectors                 | :construction:     |
| Constraints                      | :construction:     |
| Identity columns                 | :construction:     |
| Generated columns                | :construction:     |
| Transaction (conflict detection) | :construction:     |
| Change data feed                 | :construction:     |
