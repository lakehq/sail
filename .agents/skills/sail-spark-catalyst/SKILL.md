---
name: sail-spark-catalyst
description: Expert guidance for Spark Catalyst optimizer, Tungsten execution engine, and Structured Streaming in Sail. Use when implementing Spark-compatible optimization, understanding execution differences, or working with streaming. Triggers include comparing Sail versus Spark optimization strategies, understanding Tungsten versus Sail execution models, or implementing Structured Streaming features. Has access to Spark source at /Users/santosh/IdeaProjects/spark/.
---

# Spark Catalyst, Tungsten & Streaming

Understanding Spark's internals for Sail compatibility.

## Spark Codebase Access

Access Apache Spark source at `/Users/santosh/IdeaProjects/spark/`:
- `sql/catalyst/` - Catalyst optimizer
- `sql/core/execution/` - Tungsten execution engine
- `sql/streaming/` - Structured streaming

## Sail vs Spark Architecture

| Spark | Sail |
|-------|------|
| Catalyst Analyzer | `sail-sql-analyzer` |
| Catalyst Optimizer | `sail-logical-optimizer` |
| SparkPlanner | `sail-physical-optimizer` |
| WholeStageCodegen | Vectorized Arrow execution |
| Unsafe memory | Rust ownership |
| Row-based | Columnar |

## Key Differences

- **Spark**: Row-based with runtime codegen
- **Sail**: Columnar with compiled Rust plus SIMD

- **Spark**: JVM heap plus off-heap (Unsafe)
- **Sail**: Rust ownership plus Arena allocation

- **Spark**: Row-by-row serialization (slow)
- **Sail**: Zero-copy Arrow batches (fast)

## Structured Streaming

**Status**: Partial support, under development

### Output Modes

| Mode | Description | Spark | Sail |
|------|-------------|-------|------|
| Append | Only new rows | ✓ | Planned |
| Complete | All rows | ✓ | Planned |
| Update | Changed rows | ✓ | Planned |

## Resources

- **[catalyst.md](references/catalyst.md)**: Comprehensive Spark Catalyst/Tungsten/Streaming guide
- **[spark-connect.md](../sail-spark-connect/references/spark-connect.md)**: Spark Connect protocol
- **[datafusion.md](../sail-datafusion/references/datafusion.md)**: Sail's query engine
