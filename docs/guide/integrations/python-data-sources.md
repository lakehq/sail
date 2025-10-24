---
title: Python Data Sources
---

# Python Data Sources

Lakesail can execute data sources implemented in Python through the `sail-python-datasource` bridge introduced in commit `07f1c88`. This bridge exposes a simple three-method interface (`infer_schema`, `plan_partitions`, and `read_partition`) while handling zero-copy Arrow transfer and distributed execution in Rust.

## Getting Started

- Use `spark.read.format("python")` with `python_module` and `python_class` options to load custom providers.
- The JDBC provider is pre-packaged; see the examples in `python/pysail/examples/python_datasource/`.
- ConnectorX is the recommended backend for JDBC workloads, with ADBC and a pure Python fallback available.

## Architecture Reference

Read the detailed design, lifecycle walk-through, and API contract in [Python Data Source Architecture](./python-data-source-architecture.md).

## Further Reading

- JDBC module details: `python/pysail/jdbc/README.md`
- Test coverage: unit suites under `python/pysail/tests/jdbc/` and Spark integration tests in `python/pysail/tests/spark/datasource/test_jdbc.py`
