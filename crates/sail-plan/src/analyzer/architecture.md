# Query analysis architecture

The analysis boundary belongs to Sail. A parsed Sail specification becomes an
analysis tree, analysis passes resolve that tree, and checked lowering produces
an executable DataFusion logical plan. DataFusion optimization must not decide
Spark argument coercions after this boundary.

## Representation and invariants

`Deferred<T: Resolvable>` contains either a boxed request or a value. A request
describes work to perform; it is not a default value. In particular, Arrow
`Null` is a resolved data type, and `nullable = true` must never stand in for
unknown nullability. Unknown functions, attributes, types, or output schemas
remain requests until a responsible pass resolves them or reports an error.

The dependency-neutral `sail-common-plan` crate owns the tree and ID instantiation.
A stored relation is instantiated with fresh occurrence and attribute IDs through
subqueries and lambda scopes, without changing display names.

An `Expr` has an `ExprId`, a deferred `ExprKind`, and a deferred result
field. The result field contains data type, nullability, and Arrow metadata.
These properties resolve together so that a successful type inference cannot
accidentally leave nullability from an earlier expression. A `Query` has a
deferred `QueryKind` and output `QuerySchema`. Children and subqueries belong to
the analysis tree; subqueries must not disappear behind the backend's
expression-leaf convention.

An expression ID identifies an occurrence, not structural equivalence. Rewrites
preserve the occurrence ID at their root and allocate fresh IDs for inserted
expressions. Common-subexpression elimination is a separate operation and must
respect volatility. Duplicate occurrence IDs are rejected during lowering.

The binding model has a separate `AttributeId` for a
materialized output slot and an origin identity for identity projections. This
distinction matters for repeated attributes, self joins, repeated CTE references,
and Spark Connect plan-ID scoping. Display names, qualifiers, and plan-ID scopes
are lookup properties, not attribute identities. Outer joins change a slot's
nullability without changing which input attribute it originates from.

The resolver uses ordered input and outer scopes keyed by attribute
IDs. Name lookup happens when binding an unresolved attribute. Later expression
rewrites keep ID references, and do not rename columns. Lowering assigns backend
column names (`#` followed by the attribute ID) once and returns user-facing
output names separately. These names are constructed directly as backend columns,
so they do not need SQL identifier syntax. Catalog temporary views store intermediate
plans, so referencing a view allocates fresh IDs without importing or renaming a
backend plan. CTE definitions are analyzed once;
each reference receives fresh output slots, with origin and Connect binding rules
kept separate from occurrence allocation.

## Passes

`AnalyzerPipeline` is an ordered sequence of `AnalyzerLayer` implementations.
`PlanAnalyzer` installs `PlanResolver` as its first layer by default. Each layer
consumes and returns a `Plan`, whose `PlanKind` contains a `Query` or a `Command`
and whose `PlanState` carries the allocation domain, lexical bindings, source
binding metadata, and analysis configuration.
`PlanResolver` implements the single mutable `AnalyzerLayer::analyze` method.
Layers are asynchronous because catalog and function lookup can require I/O. They execute once in order; there is no implicit
fixed-point iteration or unbounded retry on unresolved requests.

The resolution layer binds tables, attributes, functions, lambdas, parameters,
and subqueries, expands stars and generators, determines relational outputs,
and resolves types. It also accepts an assembled intermediate plan: child requests resolve
in their input scopes, inferred fields propagate to parent expressions and schemas,
and new IDs come from the allocator retained in `PlanState`. Every layer uses
that allocator; repeated resolution never scans the tree to recover IDs. Imported
relations are instantiated into the destination allocation domain. Additional layers can perform coercion refinements or other
analysis transformations. Any transformation which changes a result must also
resolve its field and every affected output schema. Lowering rejects unresolved
metadata and checks that resolved metadata still matches the lowered operator.
It never inserts an implicit cast to make a partially analyzed tree executable.

Coercion is needed even for plans executed *during* analysis. Pivot-value
discovery, tail counts, statistics, time-travel scalar evaluation, and EXPLAIN
must cross a typing boundary before using the session's execution APIs.

## Function lookup and typing

`resolver` owns specification binding and retains the query, expression, command,
and tree module layout. `PlanState` retains semantic bindings across passes.
Scope nodes capture lexical environments for CTEs, parameters, named windows, and
relation references. Each resolver pass starts with fresh traversal stacks and
derives row, outer, and lambda scopes from the current tree. Configuration is
captured on the first resolution pass and reused on subsequent passes.
`function` owns scalar, aggregate, window, and higher-order function lookup and
builders. `ScalarFunctionInput` carries analyzer expressions, argument display
names, and `FunctionContextInput` (session, configuration, and a borrowed ID allocator).
Explicit `F::` registrations select function builders without extending `AnalyzerContext`.
Builders return analyzer expressions;
only `analyzer::lowering` constructs DataFusion expressions. Query, command, write,
merge, and row-operation lowering share that directory boundary.


`sail-function` owns function-specific return-field inference and argument
coercion in its DataFusion UDF implementations. Analysis calls these hooks
through the scalar, aggregate, window, and higher-order call resolvers in
`sail-plan::analyzer::typing`. This module also owns common expression and
relational type rules. Shared spec-to-Arrow conversion lives in
`sail-common-datafusion::data_type`, which both the analyzer and functions use.

The target function-analysis operation accepts the function identity and its
configuration, argument fields, and available literal arguments. It returns
target argument fields, explicit cast requirements, and a result field. Return
inference runs on the *coerced* argument fields. Successful literal casts become
typed constants during analysis, while failed casts remain expressions so lazy
evaluation still controls their errors. Comparison coercion is separate from
output-type unification for set operations and conditional expressions. Literal-dependent results (for
example decimal scale or a schema literal), nested field metadata, timezone
configuration, and ANSI cast behavior must be part of this contract. Scalar,
aggregate, window, and higher-order functions use their UDF typing hooks and retain
their distinct inputs, including lambda parameter/body fields and aggregate
filter/order expressions.

Execution kernels consume already typed arguments. DataFusion may query a
function's result field while planning or optimizing, but that hook must not
rewrite the query or select new coercions. Existing codec identities and kernel
configuration stay unchanged through analysis and lowering.

## Execution boundaries

`PlanAnalyzer` is the production entry point for SQL, Spark Connect, schema
inspection, EXPLAIN, and session commands. Both queries and commands enter
`AnalyzerPipeline` as deferred requests. `Command` contains a deferred
`CommandKind` whose request is a `spec::CommandPlan`, just as `QueryKind` uses a
`spec::QueryPlan`. The resolution layer resolves commands and their nested query
inputs in one identity domain. It also accepts assembled commands containing
resolved and unresolved children. Configured additional layers receive the entire
resolved `Plan` once, including commands and their query inputs, before lowering.
There is no separate command analysis entry point or backend-plan import path.

Analyzed command representations retain typed query inputs, predicates, assignments,
checks, and defaults. INSERT resolves DEFAULT slots against the target schema.
UPDATE and MERGE resolve store-assignment casts, generated values, and schema
alignment before lowering. MERGE expands star actions and schema evolution in
analysis; backend row-level expansion validates assignment types. Data-source
writers still choose their physical write and commit implementation.

Row-identity functions and scalar Python iterator calls become relational
operators in the intermediate plan. Projected EXISTS becomes a mark join or scalar count; grouped
correlation binds to grouping outputs before lowering. Computed sort keys are
materialized as attributes. Recovery of missing inputs carries hidden slots
through transparent operators and stops at relation boundaries. Explicit input
sorts required by order-sensitive aggregates are marked in the intermediate query tree
so backend optimization preserves them. Temporary views, CTE references,
subqueries, lambda scopes, and command inputs share the same identity rules.

`sail-plan::analyzer::typing` owns call resolution, cast metadata,
binary/common-type inference, and store-assignment compatibility. Spec type,
field, and schema conversion in `sail-common-datafusion::data_type` is independent
of resolver state. Literal conversion stays in analysis. Configured and
literal-dependent function inference stays in `sail-function`, where the same UDF
hooks serve analysis, physical planning, and distributed deserialization.

The session analyzer omits DataFusion type coercion. Lambda binding and iterator
extraction happen in analysis. Lowering rejects unresolved fields, stale
result types, duplicate expression occurrences, and references outside the scope.
It does not repair them by inserting casts or invoking another resolver.

## Extending analysis

Add a resolution handler when introducing a Sail request, and add its operator to
`sail-common-plan` only if existing nodes cannot express the behavior. Update exhaustive
traversal, relation instantiation, and checked lowering together. Function-specific
typing belongs in the UDF implementation in `sail-function`; the analyzer calls the
same typing hooks as DataFusion. Add an `AnalyzerLayer` through
`PlanAnalyzer::with_layer` for a subsequent pass. Such passes preserve IDs for existing occurrences, allocate IDs
for inserted nodes through `plan.state.ids`, and update affected result fields and
output schemas. A layer can introduce deferred requests and be followed by another
`PlanResolver` layer. Reuse the complete `Plan` when analyzing again; a bare query
or command does not carry its allocation domain.

Boundary tests check unresolved and stale metadata without executing queries.
SQL and DataFrame suites exercise values, public schemas, catalog behavior, and
Python functions through the production analyzer.
