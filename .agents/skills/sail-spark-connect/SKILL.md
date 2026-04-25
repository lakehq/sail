---
name: sail-spark-connect
description: Expert guidance for Spark Connect protocol and Spark API compatibility in Sail. Use when implementing Spark-compatible features, debugging protocol issues, or understanding version differences. Triggers include implementing Relation or Expression protobuf conversion, debugging Spark compatibility, understanding Spark DataFrame API mapping, or accessing Spark source code at /Users/santosh/IdeaProjects/spark/ for reference.
---

# Spark Connect & Spark Compatibility

Sail achieves Spark compatibility through the Spark Connect gRPC protocol.

## Spark Codebase Access

Access Apache Spark source at `/Users/santosh/IdeaProjects/spark/`:
- `connector/spark-connect/` - Spark Connect server reference
- `sql/core/src/main/scala/org/apache/spark/sql/` - DataFrame API
- `sql/catalyst/` - Query optimizer reference

## Protocol Flow

```
PySpark Client → Relation protobuf → Sail Server → Internal Plan → Results
```

## Relation → Plan Conversion

```rust
fn convert_relation(relation: &Relation) -> Result<SailPlan> {
    match relation {
        Relation::Read(read) => Ok(SailPlan::ReadTable { table: read.table_name }),
        Relation::Project(proj) => {
            let input = convert_relation(&proj.input)?;
            Ok(SailPlan::Project { input: Box::new(input), expressions: ... })
        }
    }
}
```

## DataFrame API Mapping

| PySpark | Sail Implementation |
|---------|---------------------|
| `df.select("col")` | ProjectionRelation |
| `df.filter(condition)` | FilterRelation |
| `df.join(other)` | JoinRelation |
| `df.groupBy("col")` | AggregateRelation |

## Resources

- **[spark-connect.md](references/spark-connect.md)**: Comprehensive Spark Connect protocol guide
- **[catalyst.md](../sail-spark-catalyst/references/catalyst.md)**: Spark Catalyst optimizer comparison
- **[datafusion.md](../sail-datafusion/references/datafusion.md)**: Sail's query engine
