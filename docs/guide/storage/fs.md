---
title: File Systems
rank: 1
---

# File Systems

You can use relative or absolute file paths to read and write data.
The relative file paths are relative to the current working directory of the Sail server.

You can also use the `file://` URI to refer to absolute file paths on the file system. For example, the file URI `file:///path/to/file` is equivalent to the absolute file path `/path/to/file`.

::: info
Sail is agnostic whether the data is stored on a local disk or a network drive, as long as the data appears in a file system mounted on the host.
:::

When reading a dataset, the path or URI can refer to a file or a directory. If it refers to a directory, all files matching the specified file format will be read.

When writing a dataset, the path or URI refers to a directory, and partitioned files will be written to that directory.
