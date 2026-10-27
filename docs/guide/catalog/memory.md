---
title: Memory
rank: 1
---

# Memory Catalog

The memory catalog provider in Sail stores table metadata in memory for the duration of your session. This is a useful default choice if you do not need persistent table metadata across sessions.

## Options

A memory catalog can be configured using the following options.

- `type` (required): The catalog provider. Set this option to `memory`.
- `name` (required): The catalog name.
- `initial_database` (required): The initial database namespace. Provide an array of strings.
- `initial_database_comment` (optional): The comment for the initial database.

## Examples

```bash
export SAIL_CATALOG__LIST='[{type="memory", name="sail", initial_database=["dev", "analytics"], initial_database_comment="Development analytics database"}]'
```
