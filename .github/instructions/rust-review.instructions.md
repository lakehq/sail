---
applyTo: "**/*.rs"
excludeAgent: "cloud-agent"
---

Do not try to identify compilation errors, formatting issues, or linting problems (e.g., unused or missing `use` declarations).
Such issues will be automatically detected by CI.

When adding functions (`ScalarUDF` or `AggregateUDF`), physical expressions (`PhysicalExpr`), or physical plan nodes (`ExecutionPlan`), `crates/sail-execution/src/codec.rs` must be updated accordingly so that the query plan can work in cluster mode.
