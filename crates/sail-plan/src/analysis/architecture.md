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

The dependency-neutral `sail-query` crate owns the tree and ID instantiation.
A stored relation is instantiated with fresh occurrence and attribute IDs through
subqueries and lambda scopes, without changing display names.

An expression has an `ExpressionId`, a deferred node, and a deferred result
field. The result field contains data type, nullability, and Arrow metadata.
These properties resolve together so that a successful type inference cannot
accidentally leave nullability from an earlier expression. A plan has a deferred
node and output schema. Children and subqueries belong to the analysis tree;
subqueries must not disappear behind the backend's expression-leaf convention.

An expression ID identifies an occurrence, not structural equivalence. Rewrites
preserve the occurrence ID at their root and allocate fresh IDs for inserted
expressions. Common-subexpression elimination is a separate operation and must
respect volatility. Duplicate occurrence IDs are rejected during lowering.

The native binding model has a separate `AttributeId` for a
materialized output slot and an origin identity for identity projections. This
distinction matters for repeated attributes, self joins, repeated CTE references,
and Spark Connect plan-ID scoping. Display names, qualifiers, and plan-ID scopes
are lookup properties, not attribute identities. Outer joins change a slot's
nullability without changing which input attribute it originates from.

The native resolver uses ordered input and outer scopes keyed by attribute
IDs. Name lookup happens when binding an unresolved attribute. Later expression
rewrites keep ID references, and do not rename columns. Lowering assigns backend
column names once and returns user-facing output names separately. Catalog
temporary views store native plans, so referencing a view allocates fresh IDs
without importing or renaming a backend plan. CTE definitions are analyzed once;
each reference receives fresh output slots, with origin and Connect binding rules
kept separate from occurrence allocation.

## Passes

`AnalysisPipeline` requires a resolution layer first, followed by explicitly ordered
`AnalysisLayer` implementations. Each layer consumes and returns the same
`LogicalQuery` representation. Layers are asynchronous because catalog and
function lookup can require I/O. They execute once in order; there is no implicit
fixed-point iteration or unbounded retry on unresolved requests.

The resolution layer binds tables, attributes, functions, lambdas, parameters,
and subqueries, expands stars and generators, determines relational outputs,
and resolves types. It also accepts an assembled native tree: child requests resolve
in their input scopes, inferred fields propagate to parent expressions and schemas,
and existing IDs are reserved before new nodes are allocated. Additional layers can perform coercion refinements or other
analysis transformations. Any transformation which changes a result must also
resolve its field and every affected output schema. Lowering rejects unresolved
metadata and checks that resolved metadata still matches the lowered operator.
It never inserts an implicit cast to make a partially analyzed tree executable.

Coercion is needed even for plans executed *during* analysis. Pivot-value
discovery, tail counts, statistics, time-travel scalar evaluation, and EXPLAIN
must cross a typing boundary before using the session's execution APIs.

## Function typing

`sail-type-system` is below both `sail-plan` and `sail-function` in the dependency
graph. It owns shared typing policies and does not depend on execution kernels
or resolver state. Function implementations retain the DataFusion-required
typing methods as adapters to those policies. Moving the policies into
`sail-plan` would create a dependency cycle and leave physical planning and
distributed deserialization without a usable typing contract.

The target function-analysis operation accepts the function identity and its
configuration, argument fields, and available literal arguments. It returns
target argument fields, explicit cast requirements, and a result field. Return
inference runs on the *coerced* argument fields. Successful literal casts become
typed constants during analysis, while failed casts remain expressions so lazy
evaluation still controls their errors. Comparison coercion is separate from
output-type unification for set operations and conditional expressions. Literal-dependent results (for
example decimal scale or a schema literal), nested field metadata, timezone
configuration, and ANSI cast behavior must be part of this contract. Scalar,
aggregate, window, and higher-order functions share the policy layer but retain
their distinct inputs, including lambda parameter/body fields and aggregate
filter/order expressions.

Execution kernels consume already typed arguments. DataFusion may query an
adapter's result field while planning or optimizing, but the adapter must not
rewrite the query or select new coercions. Existing codec identities and kernel
configuration stay unchanged when their typing policies move.

## Execution boundaries

`QueryAnalyzer` is the production entry point for SQL, Spark Connect, schema
inspection, EXPLAIN, and session commands. Query analysis runs through
`AnalysisPipeline`; command query inputs use the same resolver and receive the
configured additional layers before lowering. There is no legacy resolver or
backend-plan import path.

Native command representations retain typed query inputs, predicates, assignments,
checks, and defaults. INSERT resolves DEFAULT slots against the target schema.
UPDATE and MERGE resolve store-assignment casts, generated values, and schema
alignment before lowering. MERGE expands star actions and schema evolution in
analysis; backend row-level expansion validates assignment types. Data-source
writers still choose their physical write and commit implementation.

Row-identity functions and scalar Python iterator calls become native relational
operators. Projected EXISTS becomes a native mark join or scalar count; grouped
correlation binds to grouping outputs before lowering. Computed sort keys are
materialized as native attributes. Recovery of missing inputs carries hidden slots
through transparent operators and stops at relation boundaries. Explicit input sorts required by order-sensitive aggregates are marked
in the IR so backend optimization preserves them. Temporary views, CTE references,
subqueries, lambda scopes, and command inputs share the same identity rules.

`sail-type-system` owns scalar, aggregate, window, and higher-order call typing,
cast metadata, binary/common-type inference, store-assignment compatibility, and
configured or literal-dependent function policies. Spec type and literal conversion
are independent of resolver state. Kernel hooks delegate to the shared policies
when physical planning or distributed deserialization asks for a result field.

The session analyzer omits DataFusion type coercion. Lambda binding and iterator
extraction happen in native analysis. Lowering rejects unresolved fields, stale
result types, duplicate expression occurrences, and references outside the scope.
It does not repair them by inserting casts or invoking another resolver.

## Extending analysis

Add a native handler when introducing a Sail request, and add its operator to
`sail-query` only if existing nodes cannot express the behavior. Update exhaustive
traversal, relation instantiation, and checked lowering together. New function
policies belong in `sail-type-system`; execution kernels and the analyzer call the
same policy. Add an `AnalysisLayer` through `QueryAnalyzer::with_layer` for a
subsequent pass. Such passes preserve IDs for existing occurrences, allocate IDs
for inserted nodes, and update affected result fields and output schemas.

Boundary tests check unresolved and stale metadata without executing queries.
SQL and DataFrame suites exercise values, public schemas, catalog behavior, and
Python functions through the production analyzer.
