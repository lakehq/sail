---
title: Iceberg
rank: 2
---

# Iceberg

Sail reads and writes [Apache Iceberg](https://iceberg.apache.org/) tables through the `iceberg` format, using either the Spark DataFrame API or Spark SQL. It supports Parquet tables in format versions 1, 2, and 3, including snapshot reads, schema evolution, time travel, and DML statements. New tables use version 2 by default. Sail also supports partition and write-order evolution, branch/tag lifecycle DDL, metadata tables, and binpack compaction. See [supported features](./features) for details by version and operation.

## Path-table metadata publication

For path/versioned tables, `write.metadata.compression-codec` (`none` or `gzip`)
is persisted immediately but controls physical metadata compression **one publication
later**. The initial metadata file is always uncompressed. Every subsequent
publication uses the property in its captured base metadata, not the new properties
or the encoding of the base file. For example, creating a table with `gzip` produces
`v1.metadata.json`, then `v2.gz.metadata.json` on the next write. Both historical gzip
filename conventions remain readable. Catalog-authoritative UUID publications are
unchanged and use the candidate properties.

This gives participating Sail writers from the same base a single conditional-create
key. It does **not** guarantee concurrency safety with older Sail versions or arbitrary
external Hadoop writers that choose a different codec key. Sequential cross-engine
reads and writes are supported; coordinate external codec-changing writes. Metadata
files are immutable and the version hint is advisory. If multiple files claim one
version, Sail fails closed: stop writers and explicitly recover the authoritative
history rather than selecting by modification time or deleting files automatically.

## Topics

<PageList :data="data" :prefix="['guide', 'sources', 'iceberg']" />

<script setup>
import PageList from "@theme/components/PageList.vue";
import { data } from "./index.data.ts";
</script>
