Sail is a unified and distributed multimodal computation framework.
It is a drop-in replacement for Apache Spark via the Spark Connect protocol.
It aims to unify batch, streaming, and AI workloads, offering high performance and low infrastructure costs.
It is written in Rust and Python, and is built using technologies such as Apache Arrow, Apache DataFusion, Tokio, and PyO3.

## Project Layout

- `crates/`: All Rust crates.
  - `sail-build-scripts`: Rust code generation logic to be used in `build.rs`.
  - `sail-cache`: Caching implementations.
  - `sail-catalog`: Catalog interface and common utilities.
  - `sail-catalog-*`: Catalog implementations.
  - `sail-cli`: Command-line interface entry point.
  - `sail-common`: Sail configuration, query plan specification, and utilities.
  - `sail-common-datafusion`: DataFusion utilities.
  - `sail-data-source`: Data source implementations.
  - `sail-delta-lake`: Delta Lake integration.
  - `sail-execution`: Distributed execution implementation.
  - `sail-flight`: Arrow Flight SQL server implementation.
  - `sail-function`: Scalar and aggregate functions.
  - `sail-gold-test`: SQL gold tests.
  - `sail-iceberg`: Apache Iceberg integration.
  - `sail-logical-optimizer`: Custom logical optimization rules.
  - `sail-logical-plan`: Custom logical plan nodes.
  - `sail-object-store`: Object store implementations and utilities.
  - `sail-physical-optimizer`: Custom physical optimization rules.
  - `sail-physical-plan`: Custom physical plan nodes.
  - `sail-plan`: Logical plan resolver.
  - `sail-python`: Native module for the `pysail` Python package.
  - `sail-python-udf`: Python UDF support.
  - `sail-server`: gRPC server utilities and actor implementation.
  - `sail-session`: Session management.
  - `sail-spark-connect`: Spark Connect protocol implementation.
  - `sail-sql-*`: Sail SQL parser and analyzer.
  - `sail-telemetry`: OpenTelemetry integration.
- `docs/`: Documentation site built with VitePress.
- `python/`: Source code for the `pysail` Python package.
- `scripts/`: Various scripts for development and testing purposes.
