# Apache Spark & Spark Connect Expert

Expert in Spark architecture and Spark Connect protocol for Sail compatibility. Guides protocol-level implementation, DataFrame API semantics, and version differences.

## Context

Sail achieves Spark compatibility through the Spark Connect protocol - a gRPC-based client-server architecture that replaces JDBC/ODBC.

## Spark Connect Protocol

### gRPC Service Definition

```protobuf
// proto/spark/connect/connect.proto

service SparkConnectService {
  rpc ExecutePlan(ExecutePlanRequest) returns (ExecutePlanResponse);
  rpc AnalyzeQuery(AnalyzeQueryRequest) returns (AnalyzeQueryResponse);
  rpc Config(ConfigRequest) returns (ConfigResponse);
  rpc Interrupt(InterruptRequest) returns (InterruptResponse);
  rpc Reattach(ReattachRequest) returns (ReattachResponse);
  // ... more methods
}
```

### Request/Response Flow

```
PySpark Client                      Sail Server
     │                                    │
     │ ── ExecutePlanRequest ────────────>│
     │    (Relation protobuf)             │
     │                                    │
     │                         Convert to internal plan
     │                         Execute with DataFusion
     │                                    │
     │ <───────── ArrowBatch ─────────────│
     │    (Results in Arrow format)        │
```

## Relation Protocol

Spark uses `Relation` messages to represent DataFrame operations:

```protobuf
message Relation {
  oneof rel_type {
    CommonCommonInfo common = 1;
    ReadRelation read = 2;
    ProjectionRelation project = 3;
    FilterRelation filter = 4;
    JoinRelation join = 5;
    // ... many more relation types
  }
}
```

### Example Relations

**Read Relation** (read from data source):
```protobuf
ReadRelation {
  table_name: "my_table"
  schema: [...]
  options: { "path": "/path/to/data" }
}
```

**Projection Relation** (SELECT):
```protobuf
ProjectionRelation {
  input: (ReadRelation {...})
  expressions: [
    Expression { UnresolvedAttribute { name: "col1" } }
  ]
}
```

**Filter Relation** (WHERE):
```protobuf
FilterRelation {
  input: (ReadRelation {...})
  condition: Expression {
    UnresolvedFunction {
      name: ">"
      args: [
        UnresolvedAttribute { name: "value" },
        Literal { long: 100 }
      ]
    }
  }
}
```

## Spark Codebase Access

This skill has access to Apache Spark source at `/Users/santosh/IdeaProjects/spark/`:

### Spark Connect Server Reference
```
/Users/santosh/IdeaProjects/spark/connector/spark/connect/
├── client/jvm/           # Java client implementation
├── server/               # Scala server implementation
└── common/src/main/protobuf/  # Protocol definitions
```

### SQL Execution Reference
```
/Users/santosh/IdeaProjects/spark/sql/core/src/main/scala/org/apache/spark/sql/
├── execution/            # Physical execution
├── catalyst/             # Logical planning
└── functions.scala       # DataFrame functions
```

## DataFrame API Mapping

| PySpark API | Sail Implementation | Protocol |
|-------------|---------------------|----------|
| `df.select("col")` | `sail-plan/src/resolver/` | ProjectionRelation |
| `df.filter(df.col > 0)` | `sail-plan/src/resolver/` | FilterRelation |
| `df.join(other)` | `sail-plan/src/resolver/` | JoinRelation |
| `df.groupBy("col")` | `sail-plan/src/resolver/` | AggregateRelation |
| `df.orderBy("col")` | `sail-plan/src/resolver/` | SortRelation |

## Expression Protocol

```protobuf
message Expression {
  oneif expr_type {
    Literal literal = 1;
    UnresolvedAttribute unresolved_attr = 2;
    UnresolvedFunction unresolved_function = 3;
    Alias alias = 4;
    Cast cast = 5;
    // ... many more expression types
  }
}
```

### Expression Examples

**Literal**:
```protobuf
Expression {
  Literal {
    long: 42
  }
}
```

**UnresolvedAttribute** (column reference):
```protobuf
Expression {
  UnresolvedAttribute {
    name: "column_name"
  }
}
```

**UnresolvedFunction** (function call):
```protobuf
Expression {
  UnresolvedFunction {
    name: "upper"
    args: [UnresolvedAttribute { name: "str" }]
  }
}
```

## Spark Versions

Sail maintains compatibility across Spark versions:

| Version | PySpark | Key Differences |
|---------|---------|-----------------|
| 3.5.x | pyspark 3.5.7 | Baseline for testing |
| 4.0.x | pyspark 4.0.1 | New VARIANT type, behavior changes |
| 4.1.x | pyspark 4.1.0 | Latest features |

### Version-Specific Behaviors

```python
# Spark 4.0+ has VARIANT type
df.select(parse_json(col))
```

## Plan Conversion

### Spark Relation → Sail Plan

```rust
// crates/sail-spark-connect/src/plan_converter.rs

fn convert_relation(relation: &Relation) -> Result<SailPlan> {
    match relation {
        Relation::Read(read) => {
            Ok(SailPlan::ReadTable {
                table: read.table_name.clone(),
                options: read.options.clone(),
            })
        }
        Relation::Project(project) => {
            let input = convert_relation(&project.input)?;
            Ok(SailPlan::Project {
                input: Box::new(input),
                expressions: convert_expressions(&project.expressions)?,
            })
        }
        // ... other relation types
    }
}
```

### Spark Expression → Sail Expression

```rust
fn convert_expression(expr: &Expression) -> Result<SailExpr> {
    match expr {
        Expression::Literal(literal) => {
            Ok(SailExpr::Literal(convert_literal(literal)?))
        }
        Expression::UnresolvedAttribute(attr) => {
            Ok(SailExpr::Column(attr.name.clone()))
        }
        Expression::UnresolvedFunction(fun) => {
            let args = convert_expressions(&fun.args)?;
            Ok(SailExpr::Function {
                name: fun.name.clone(),
                args,
            })
        }
        // ... other expression types
    }
}
```

## Session Management

### User Sessions
```rust
// crates/sail-spark-connect/src/session.rs

pub struct SessionManager {
    sessions: HashMap<String, Session>,
}

impl SessionManager {
    fn create_session(&mut self, user: String) -> String {
        let session_id = Uuid::new_v4().to_string();
        let session = Session::new(session_id.clone(), user);
        self.sessions.insert(session_id.clone(), session);
        session_id
    }

    fn get_session(&self, session_id: &str) -> Option<&Session> {
        self.sessions.get(session_id)
    }
}
```

### Session Configuration
```rust
// Spark config mapping
struct SessionConfig {
    // Map Spark config to Sail config
    configs: HashMap<String, String>,
}
```

## Type System Mapping

| Spark Type | Arrow Type | Sail Type |
|------------|------------|-----------|
| Boolean | BooleanType | DataType::Boolean |
| Byte | Int8 | DataType::Int8 |
| Short | Int16 | DataType::Int16 |
| Integer | Int32 | DataType::Int32 |
| Long | Int64 | DataType::Int64 |
| Float | Float32 | DataType::Float32 |
| Double | Float64 | DataType::Float64 |
| String | Utf8 | DataType::Utf8 |
| Binary | Binary | DataType::Binary |
| Timestamp | Timestamp | DataType::Timestamp |
| Date | Date32 | DataType::Date32 |
| Decimal | Decimal128 | DataType::Decimal128 |
| Array | List | DataType::List |
| Map | Map | DataType::Map |
| Struct | Struct | DataType::Struct |
| VARIANT (Spark 4.0+) | - | Not supported |

## Error Mapping

### Spark Error Classes

```rust
// crates/sail-spark-connect/src/error.rs

pub enum SparkErrorClass {
    ArithmeticError,
    ArrayIndexOutOfBounds,
    CannotCastDataType,
    DivideByZero,
    // ... many more
}

impl SailError {
    pub fn to_spark_throwable(&self) -> SparkThrowable {
        SparkThrowable {
            error_class: self.error_class().to_string(),
            message: self.message(),
            stack_trace: self.stack_trace(),
            query_context: self.query_context(),
        }
    }
}
```

## Critical Files

| File | Purpose |
|------|---------|
| `crates/sail-spark-connect/src/` | Spark Connect implementation |
| `crates/sail-spark-connect/proto/` | Protocol buffers |
| `crates/sail-spark-connect/src/server.rs` | gRPC server |
| `crates/sail-spark-connect/src/session.rs` | Session management |
| `/Users/santosh/IdeaProjects/spark/` | Apache Spark source reference |

## Debugging Protocol Issues

### Enable Wire Logging
```bash
# Log all gRPC messages
RUST_LOG=sail_spark_connect=wire,grpc=debug
```

### Inspect Protobuf Messages
```rust
// Print received Relation
println!("{:#?}", relation);
```

### Compare with Spark
```bash
# Run Spark server and compare responses
# See scripts/spark-tests/ for setup
```

## Common Compatibility Issues

### Missing Relations
- `CachedRelation` - Use in-memory caching instead
- `RDDRelation` - Not supported (by design)
- `DataSourceV2Relation` - Partial support

### Missing Expressions
- `LambdaFunction` - Lambda expressions not supported
- `NamedLambdaVariable` - Lambda variables not supported
- `WindowExpression` - Partial support (some window functions)

### Behavior Differences
- Null handling in functions
- Floating point precision
- Date/time edge cases
- Type coercion rules

## References

- Spark Connect: `connector/spark/connect/`
- Spark Functions: `sql/core/src/main/scala/org/apache/spark/sql/functions.scala`
- Spark Types: `sql/catalyst/src/main/scala/org/apache/spark/sql/types/`
- PySpark Tests: `python/pysail/tests/spark/`
