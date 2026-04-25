---
name: sail-datafusion
description: Expert guidance for Apache DataFusion query engine integration with Sail. Use when working with logical/physical planning, optimizer rules, user-defined functions, type system integration, or extending DataFusion for Spark compatibility. Triggers include implementing custom operators, adding scalar/aggregate functions, debugging optimizer behavior, understanding DataFusion APIs, or bridging DataFusion with Sail extensions.
---

# DataFusion Integration

Sail uses Apache DataFusion 51.0.0 as its query engine foundation.

## Quick Reference

| Module | Location | Purpose |
|--------|----------|---------|
| Bridge | `sail-common-datafusion/` | DataFusion integration layer |
| Logical | `sail-logical-plan/` | Custom logical operators |
| Physical | `sail-physical-plan/` | Custom physical operators |
| Opt Rules | `sail-logical-optimizer/`, `sail-physical-optimizer/` | Optimizer rules |
| Functions | `sail-plan/function/`, `sail-function/` | Function registry |

## Common Tasks

### Implementing an Optimizer Rule

```rust
use datafusion::optimizer::{OptimizerRule, OptimizerConfig};

pub struct MyRule;
impl OptimizerRule for MyRule {
    fn try_optimize(&self, plan: &LogicalPlan, config: &dyn OptimizerConfig) -> Result<Option<LogicalPlan>> {
        Ok(Some(plan.clone()))
    }
    fn name(&self) -> &str { "my_rule" }
}
```

### Adding a Scalar Function

```rust
use sail_plan::function::common::ScalarFunctionBuilder;

ScalarFunctionBuilder::new("my_func", vec![DataType::Int32], DataType::Int32)
    .with_function(|args| Ok(ColumnarValue::Array(...)))
    .register(&ctx)?;
```

## Key Concepts

- **LogicalPlan**: Query representation before execution decisions
- **PhysicalPlan**: Determines how query executes (with execution strategies)
- **Expr**: Expression language for computations
- **FunctionRegistry**: Register custom functions
- **TableProvider**: Custom data source interface

## Resources

- **[datafusion.md](references/datafusion.md)**: Comprehensive DataFusion guide with examples, API patterns, and Sail-specific integrations
- **[arrow.md](../sail-arrow/references/arrow.md)**: Arrow array operations (DataFusion uses Arrow)
- **[catalyst.md](../sail-spark-catalyst/references/catalyst.md)**: Spark Catalyst comparison for compatibility
