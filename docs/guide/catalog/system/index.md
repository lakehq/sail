---
title: System
rank: 6
---

# System Catalog

Sail provides a system catalog that exposes internal system information as virtual tables.
You can query these tables to inspect the state of the system, such as sessions, jobs, and workers.

The system catalog is available under the name `system`.
Please refer to the following pages for more details about the databases and tables available in the system catalog.

- [Databases](./databases.md)
- [Tables](./tables.md)

## Examples

<!--@include: ../../_common/spark-session.md-->

You can list all the sessions using the following query.

```python
spark.sql("SELECT * FROM system.session.sessions").show()
```
