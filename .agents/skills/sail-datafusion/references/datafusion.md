# DataFusion Expert

Expert in Apache DataFusion query engine integration with Sail. Guides implementation of logical/physical planning, optimizer rules, and user-defined functions.

## Context

Sail is built on top of Apache DataFusion (version 51.0.0) as its query engine foundation. DataFusion provides:
- Logical and physical planning
- Query optimization
- Expression evaluation
- Type system and coercion
- Function registry

## Key Components

### Logical Planning (`sail-logical-plan/`)
Sail extends DataFusion's logical plan with custom operators for Spark compatibility.

### Physical Planning (`sail-physical-plan/`)
Sail implements physical execution plans that leverage DataFusion's execution engine.

### Logical Optimizer (`sail-logical-optimizer/`)
Custom optimizer rules that extend DataFusion's optimization.

### Physical Optimizer (`sail-physical-optimizer/`)
Physical plan optimization for distributed execution.

### DataFusion Bridge (`sail-common-datafusion/`)
Integration layer that bridges DataFusion with Sail extensions.

## Common Tasks

### Understanding Logical Plans
Logical plans in DataFusion represent the query logic before physical execution decisions:

```rust
use datafusion::prelude::*;
use datafusion::logical_expr::LogicalPlan;

// Common logical plan nodes:
// - Projection: SELECT expressions
// - Filter: WHERE clauses
// - Aggregate: GROUP BY and aggregates
// - Join: JOIN operations
// - Sort: ORDER BY
// - Limit: LIMIT
// - Union: UNION ALL
```

### Understanding Physical Plans
Physical plans determine how the query is executed:

```rust
use datafusion::physical_plan::*;

// Common physical plan nodes:
// - ProjectionExec: Execute projections
// - FilterExec: Execute filters
// - AggregateExec: Execute aggregations with different modes
// - HashJoinExec: Execute hash joins
// - SortExec: Execute sorting
// - GlobalLimitExec / LocalLimitExec: Execute limits
```

### Implementing Optimizer Rules
Optimizer rules transform logical plans for better performance:

```rust
use datafusion::optimizer::{OptimizerRule, OptimizerConfig};
use datafusion::common::Result;

pub struct MyCustomRule;

impl OptimizerRule for MyCustomRule {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        // Transform the plan
        Ok(Some(plan.clone()))
    }

    fn name(&self) -> &str {
        "my_custom_rule"
    }
}
```

### Adding User-Defined Functions
Register scalar and aggregate functions with DataFusion:

```rust
use datafusion::prelude::SessionContext;
use sail_plan::function::common::ScalarFunctionBuilder;

// Register a scalar function
let ctx = SessionContext::new();
ScalarFunctionBuilder::new("my_function", vec![DataType::Int32], DataType::Int32)
    .with_function(|args| {
        // Implementation
        Ok(ColumnarValue::Array(...))
    })
    .register(&ctx)?;
```

### Working with Expressions
DataFusion expressions represent computations:

```rust
use datafusion::logical_expr::Expr;
use datafusion::scalar::ScalarValue;

// Common expression types:
// - Column: Reference to a column
// - Literal: Constant value (ScalarValue)
// - BinaryExpr: Binary operations (+, -, *, /, etc.)
// - UnaryExpr: Unary operations (-, NOT, etc.)
// - Cast: Type casting
// - ScalarFunction: Built-in or UDF calls
// - AggregateFunction: Aggregate calls
// - Alias: Column aliases
```

## Critical Files

| File | Purpose |
|------|---------|
| `crates/sail-common-datafusion/src/` | DataFusion bridge layer |
| `crates/sail-logical-plan/src/` | Logical plan extensions |
| `crates/sail-physical-plan/src/` | Physical plan extensions |
| `crates/sail-logical-optimizer/src/` | Logical optimizer rules |
| `crates/sail-physical-optimizer/src/` | Physical optimizer rules |
| `crates/sail-plan/src/function/` | Function registry |
| `crates/sail-function/src/` | Function implementations |

## Type System

DataFusion uses Arrow types:

```rust
use datafusion::arrow::datatypes::*;

// Common types:
// - DataType::Null
// - DataType::Boolean
// - DataType::Int8, Int16, Int32, Int64
// - DataType::UInt8, UInt16, UInt32, UInt64
// - DataType::Float32, Float64
// - DataType::Utf8 (string), LargeUtf8
// - DataType::Binary, LargeBinary
// - DataType::List(Box<DataType>), LargeList(...)
// - DataType::Struct(vec<Field>)
// - DataType::Timestamp(TimeUnit::Microsecond, Option<String>)
// - DataType::Date32
```

## Common Patterns

### Type Coercion
DataFusion automatically coerces types to match:

```rust
use datafusion::logical_expr::type_coercion;

// Type coercion rules:
// - String + Int -> Error (can't coerce)
// - Int + Float -> Float (widening)
// - Timestamp + Date -> Timestamp
```

### Session Configuration
Configure DataFusion session:

```rust
use datafusion::prelude::{SessionConfig, SessionContext};

let config = SessionConfig::new()
    .with_target_partitions(4)
    .with_batch_size(8192);

let ctx = SessionContext::new_with_config(config);
```

### Catalog and TableProvider
DataFusion uses catalogs and table providers for data sources:

```rust
use datafusion::catalog::{CatalogProvider, TableProvider};
use datafusion::datasource::TableType;

// Register a table
ctx.register_table("my_table", Arc::new(table_provider))?;
```

## Debugging Tips

### Explain Plans
Use EXPLAIN to see the plan:

```rust
use datafusion::prelude::*;

let df = ctx.sql("SELECT * FROM t").await?;
let plan = df.logical_plan();
println!("{:?}", plan);
```

### Logical Plan Debugging
Check the logical plan structure:

```rust
fn print_plan(plan: &LogicalPlan, indent: usize) {
    println!("{:indent$}{}", "", plan.display(), indent = indent);
    for input in plan.inputs() {
        print_plan(input, indent + 2);
    }
}
```

## DataFusion vs Spark Catalyst

| DataFusion | Spark Catalyst |
|------------|----------------|
| `LogicalPlan` | `LogicalPlan` |
| `PhysicalPlan` | `SparkPlan` |
| `OptimizerRule` | `Rule` |
| `SessionContext` | `SparkSession` |
| `Expr` | `Expression` |
| Built with Rust | Built with Scala |

## References

- DataFusion docs: https://arrow.apache.org/datafusion/
- DataFusion source: https://github.com/apache/arrow-datafusion
- Sail's DataFusion integration: `crates/sail-common-datafusion/`
