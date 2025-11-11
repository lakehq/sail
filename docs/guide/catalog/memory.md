---
title: Memory
rank: 1
---

# Memory Catalog

The memory catalog provider in Sail stores table metadata in memory for the duration of your Spark session. This is a useful default choice if you do not need persistent table metadata across sessions.

A memory catalog can be configured using the following options.

- `name` (required): The name of the catalog.
- `type` (required): The string `memory`.
- `initial_database` (required): The initial database namespace as an array of strings.
- `initial_database_comment` (optional): The comment for the initial database.

## Examples

```bash
export SAIL_CATALOG__LIST='[{name="sail", type="memory", initial_database=["dev", "analytics"], initial_database_comment="Development analytics database"}]'
```
