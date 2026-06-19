---
title: Graph Extension Architecture
rank: 8
---

# Graph Extension Architecture

This document proposes a Sail-native graph extension that accepts graph queries
over Spark Connect, supports Cypher through Sail's existing parser stack, and
reuses Grust and GrustFrames instead of introducing a second property graph
model.

## Goals

- Keep Spark Connect as the external transport.
- Add Cypher as a text query surface in Sail's parser and analyzer pipeline.
- Represent graph work in `sail-common::spec` before planning.
- Lower graph reads and analytics to DataFusion logical plans where possible.
- Reuse Grust's graph model, schema validation, Arrow staging, and Sail table
  conventions.
- Push reusable backend-independent graph features into Grust, and reusable
  Sail graph SQL/Arrow lowerings into `grust-sail`.

## Existing Sail Insertion Points

Sail already has the right convergence point for another query language:

```text
Spark Connect ExecutePlan / SqlCommand
        |
        v
sail-spark-connect
        |
        v
sail-sql-parser + sail-sql-analyzer, or protobuf conversion
        |
        v
sail-common::spec::Plan
        |
        v
sail-plan::PlanResolver
        |
        v
DataFusion LogicalPlan -> physical plan -> JobRunner -> Arrow IPC stream
```

The extension should preserve this shape. Spark Connect remains the transport.
Cypher text should be parsed before planning, converted to `spec::Plan`, and
then resolved by `sail-plan`. `sail-spark-connect` should not learn graph
semantics beyond routing text through the existing SQL command path.

## Grust Reuse

Grust already defines the durable property graph contract:

- `Node`, `Edge`, `NodeId`, `EdgeId`, `Label`, `Props`, and `Value`.
- `Graph` and `GraphIndex` for backend-neutral local graph layout and
  adjacency indexes.
- `GraphSchema` plus typed validation.
- `GraphStore`, `GraphAdminStore`, and `GraphMutationStore`.
- `SailGraphStore` in `grust-sail`, which persists graph data through Spark
  Connect into Sail tables.

The existing `grust-sail` table shape is the first Sail graph storage contract:

```text
grust_nodes
  id      string
  label   string
  props   string/json

grust_edges
  edge_key  string
  id        optional string
  src_id    string
  src_label string
  dst_id    string
  dst_label string
  edge_type string
  props     string/json
```

Typed graph schemas may also create label-specific node and edge tables. Those
typed tables are an optimization and validation layer, not the only logical
graph representation.

The Grust book frames this boundary explicitly: application code should build,
validate, traverse, and persist graphs without committing to a database query
language too early. Sail should follow that rule. Cypher is a Sail query
surface, not a replacement for Grust's `Graph`, `GraphStore`, or traversal IR.
When a graph query can be expressed as a backend-neutral traversal or pattern
primitive, define that primitive in Grust or `grust-sail` first and adapt Sail's
planner to it.

Two existing `grust-sail` implementation details are now especially relevant:

- `SailGraphStore::stage_arrow_ipc_view` and `query_arrow_ipc` already expose
  the Spark Connect Arrow boundary that external Grust consumers need.
- `traversal_sql`, `sail_out_degrees_sql`, `sail_in_degrees_sql`,
  `sail_degrees_sql`, and `sail_degree_pairs_sql` already lower portable graph
  operations over persisted `grust_nodes` and `grust_edges` tables.

Those lowerings are currently string-SQL helpers in `grust-sail`. Sail's native
planner should not call them directly because it already owns parsed `spec`
expressions and DataFusion logical plans. But the same concepts should be
factored into reusable `grust-sail` table-layout and property-access helpers so
the native Sail path and the external Grust backend do not diverge.

## GrustFrames Reuse

GrustFrames should remain above Grust. Its useful reusable pieces are:

- GraphFrames-compatible API concepts: vertices, edges, triplets, motifs,
  aggregate messages, Pregel-style iteration, PageRank, components, and BFS.
- Local algorithm behavior over `Graph` and `GraphIndex`.
- Planned distributed lowerings for filters, triplets, motifs, aggregate
  messages, and iterative Pregel state.

General lowerings that apply to every Grust backend belong in Grust. Lowerings
that are specifically Sail SQL or Spark Connect Arrow operations belong in
`grust-sail`. GrustFrames should call those lowerings rather than owning its own
Sail backend.

This matches the current GrustFrames architecture: GrustFrames owns the
GraphFrames-style API and algorithms, Grust owns the model and indexes,
`grust-sail` owns reusable Sail graph-table persistence/query lowering, and
Sail owns distributed execution. A Sail-native Cypher extension should therefore
be a peer to GrustFrames' future distributed lowerings, not a dependency of
GrustFrames and not a second graph model.

## Proposed Sail Crate Changes

### `sail-sql-parser`

Add a graph query AST alongside SQL AST nodes. The first target should be a
restricted, composable Cypher read subset:

```cypher
MATCH (a:Person)-[e:KNOWS]->(b:Person)
MATCH (a:Person)-->(b:Person)
WHERE a.age > 30
RETURN a.id, b.id, e.since
LIMIT 100
```

Parser work should use the existing `chumsky` parser-combinator style:

- Add Cypher keywords to `crates/sail-sql-parser/data/keywords.txt`.
- Add `ast/graph.rs` for graph patterns, labels, properties, path direction,
  `MATCH`, `WHERE`, `RETURN`, `ORDER BY`, `SKIP`, and `LIMIT`.
- Add a `Statement::GraphQuery` variant, or a sibling parser entrypoint if
  grammar conflicts require an explicit `cypher` mode.
- Keep expression reuse where semantics match SQL expressions. Do not duplicate
  the whole expression language for comparisons, literals, boolean operators,
  and scalar functions.

The initial parser should avoid full Cypher mutation and variable-length path
semantics until the read path has stable planning and tests.

### `sail-sql-analyzer`

Convert the graph AST into graph-specific `spec` nodes rather than directly
emitting SQL strings. The analyzer should be the boundary where Cypher syntax
becomes a language-neutral graph IR:

```text
GraphQuery
  match patterns
  predicates
  projections
  ordering
  offset/limit
```

This keeps future graph entry points from needing to reparse Cypher.

### `sail-common`

Add graph IR under `spec`, either in `spec::plan` or a new `spec::graph` module:

```rust
GraphQuery {
    input: GraphInput,
    matches: Vec<GraphPattern>,
    predicates: Vec<Expr>,
    returns: Vec<GraphReturnItem>,
    order: Vec<SortOrder>,
    skip: Option<Expr>,
    limit: Option<Expr>,
}
```

The IR should model nodes, edges, labels, directions, aliases, property
lookups, and result bindings. It should not depend on Grust types directly,
because `sail-common` is the bottom shared layer. Keep the shapes serializable
and backend-neutral; adapt to Grust at boundaries that already depend on graph
crates.

### `sail-plan`

Resolve graph IR to DataFusion plans over graph tables. The first lowering can
be relational:

- Node pattern: scan `grust_nodes` or a typed node table.
- Edge pattern: scan `grust_edges` or a typed edge table.
- Directed edge: join node alias to `src_id`, then edge to destination node
  alias by `dst_id`.
- Incoming edge: same shape with `dst_id` and `src_id` reversed.
- Undirected edge: join on either endpoint with a `CASE` projection where
  needed.
- Label predicates: `label = ...` or `edge_type = ...`.
- Property predicates: extract from JSON props initially, then prefer typed
  columns when a `GraphSchema` is present.
- `RETURN`: projection over bound node/edge aliases and properties.

For the first implementation, this can be a resolver module that produces
ordinary DataFusion `LogicalPlan` trees. A custom logical node is only needed
when a graph operation cannot be expressed cleanly as scans, joins, filters,
aggregates, and projections.

The current branch implements this first relational lowering for directed and
undirected patterns over the generic Grust table shape:

- node aliases scan `grust_nodes`;
- edge aliases scan `grust_edges`;
- outgoing edges join `source.id = edge.src_id` and
  `edge.dst_id = target.id`;
- incoming edges join `source.id = edge.dst_id` and
  `edge.src_id = target.id`;
- undirected edges join either endpoint and bind the target to the opposite
  endpoint;
- node labels lower to `alias.label = ...`;
- edge labels lower to `alias.edge_type = ...`;
- `edge.label` returns the physical `edge_type` value;
- non-physical properties lower through Spark-compatible JSON extraction over
  `alias.props`;
- Cypher `RETURN` names are preserved even when the physical expression is
  rewritten.

The implementation intentionally avoids depending on Grust Rust types inside
`sail-common` or `sail-plan`. The dependency direction should remain:
`grust-sail` may talk to Sail over Spark Connect; Sail's planner should only
share storage conventions and reusable helper semantics, not link to the Grust
backend crates.

### `sail-spark-connect`

Keep Spark Connect changes minimal:

- Continue accepting graph text through `SqlCommand`.
- Route graph statements through the same parse/analyze path as SQL.
- Return read queries as `SqlCommandResult` relations, preserving current
  client behavior.
- Return graph mutations later as command plans, matching existing DDL/DML
  behavior.

If a dedicated client API is added later, it should still produce either
`SqlCommand` text or a regular Spark Connect relation until the Spark protocol
has an explicit extension point Sail wants to support.

## Proposed Grust Changes

Push features to Grust when they are independent of Sail execution:

- A small backend-neutral graph pattern IR shared by GrustFrames and Sail
  lowering, if it can be kept independent of Sail parser types.
- Motif and path validation rules.
- Typed schema helpers for resolving labels and property fields.
- Arrow conversion helpers that are not Sail-specific.

Do not push Sail parser, `spec`, or DataFusion planning concerns into Grust.

## Proposed `grust-sail` Changes

Prefer `grust-sail` for reusable Sail-specific graph operations:

- SQL builders for Cypher-pattern relational lowerings.
- Table discovery helpers for generic vs typed graph tables.
- Property projection helpers that choose typed columns when available and JSON
  extraction otherwise.
- Result decoders for node, edge, path, degree, and triplet rows.
- Spark Connect client helpers only for external Grust consumers, not for
  Sail's in-process planner.

This keeps Sail's native extension and Grust's external Sail backend aligned
without making either one a wrapper around the other.

Concrete `grust-sail` helper work:

- Done in `grust-sail`: publish constants and helper accessors for the generic
  graph table contract:
  `grust_nodes`, `grust_edges`, `id`, `label`, `props`, `src_id`, `dst_id`,
  `edge_type`, and optional `src_label` / `dst_label` / edge `id`.
- Done in `grust-sail`: extract a small property projection model,
  `SailGraphFieldProjection::PhysicalColumn` versus `JsonProperty`, so
  `grust-sail` SQL builders and Sail's logical-plan lowering can make the same
  decision for `id`, `label`, `edge_type`, and arbitrary props.
- Done in `grust-sail`: expose typed table-name and typed-field compatibility
  helpers for `grust_node_<label>` and `grust_edge_<label>`, so typed-table
  naming and fallback rules are defined in the reusable Sail backend contract.
- Done in `grust-sail`: expose `GraphSchema`-derived typed table descriptors and
  typed column lists, so declared schema discovery can use the same reusable
  table contract as typed persistence.
- Promote traversal/pattern lowering tests that assert the shared table
  semantics independent of whether the final output is SQL text or DataFusion
  logical expressions.

## First Implementation Slice

1. Add a parser-only Cypher read subset with syntax gold tests:
   `MATCH`, node patterns, directed edge patterns, optional labels, `WHERE`,
   `RETURN`, `ORDER BY`, `SKIP`, and `LIMIT`.
2. Add `spec::GraphQuery` and analyzer conversion from the Cypher AST.
3. Add `PlanResolver` lowering for one-hop and multi-hop directed patterns over
   `grust_nodes` and `grust_edges`.
4. Add Spark Connect SQL command tests that execute a small graph query against
   local graph tables and verify Arrow results.
5. Move any table-name, property-access, or pattern-lowering helpers that are
   useful outside Sail into `grust-sail`.

### Current Branch Status

The branch has completed the in-process version of steps 1-4:

- `sail-sql-parser` parses a first Cypher read subset:
  `MATCH`, directed node-edge-node patterns, anonymous shorthand relationships
  such as `(a)-->(b)`, comma-separated patterns with shared aliases, labels,
  inline property maps, `WHERE`, `RETURN`, `ORDER BY`, `SKIP`, and `LIMIT`.
- `sail-sql-analyzer` converts the Cypher AST to a serializable
  `spec::GraphQuery` without stringifying back through SQL.
- `sail-plan` resolves `spec::GraphQuery` to ordinary DataFusion plans over
  `grust_nodes` and `grust_edges`.
- `sail-plan` now has an execution test over in-memory Grust-shaped
  `grust_nodes` / `grust_edges` temporary views. The test verifies label
  filters, outgoing joins, `e.label -> edge_type`, JSON property filtering,
  JSON property projection, ordering, limit, and user-facing return names.
- `sail-plan` also executes inline Cypher property maps on node and edge
  patterns by lowering them to the same typed-column or JSON-property filters
  used by explicit `WHERE` predicates.
- Additional parser, analyzer, and execution tests cover incoming
  `(b)<-[e]-(a)`, undirected `(a)-[e]-(b)`, and anonymous shorthand
  relationships such as `(a)-->(b)`.
- Sail planner execution coverage now asserts exact outgoing, incoming, and
  undirected `(left node, edge, right node)` semantics over the same
  Grust-shaped edge table contract used by the reusable `grust-sail` direction
  helpers.
- Additional analyzer, planner execution, and Spark Connect plan-gold tests
  cover comma-separated patterns that reuse aliases across pattern terms.
- `sail-plan` can opportunistically resolve labeled graph aliases through
  Grust/Sail typed tables such as `grust_node_person` and
  `grust_edge_knows`. Typed alias fields resolve to table columns, while
  `a.label` / `e.label` are synthesized from the pattern label. The generic
  `grust_nodes` / `grust_edges` fallback remains intact. Typed-table selection
  checks catalog column metadata first, including structural join columns
  required by the pattern (`id` for nodes and `src_id` / `dst_id` for edges), so
  a partial typed table does not break queries that can still be answered from
  generic JSON properties. Execution coverage includes label-only typed scans,
  structurally incomplete typed-edge fallback, typed multi-pattern queries that
  reuse aliases across pattern terms, and conflicting reused labels that
  correctly produce empty results instead of missing-column planning errors.
  Typed-table fallback paths now emit debug logs with the typed table and exact
  missing or incompatible fields.
- `sail-spark-connect` has SQL-to-plan gold coverage showing Cypher text
  entering through the Spark Connect SQL parser path and producing
  `spec::GraphQuery` for outgoing, incoming, and undirected patterns.
- `sail-spark-connect` also has service-level `SqlCommand` coverage that
  creates Grust-shaped temporary views, sends a Cypher query as Spark SQL,
  executes the returned relation through the Spark Connect relation path, and
  verifies returned Arrow rows for bracketed edge identity projection,
  shorthand traversal, incoming and undirected direction orientation, and
  multi-pattern shared-alias queries.
- `python/pysail/tests/spark/test_graph.py` adds an external PySpark
  `spark.sql(...)` smoke test against a running Sail Spark Connect server. The
  smoke creates Grust-shaped node/edge temporary views, verifies bracketed
  Cypher can project `e.id`, `e.edge_key`, `e.label`, verifies shorthand
  `MATCH (a)-->(b)` traversal, verifies inline property-map predicates, and
  asserts exact incoming and undirected endpoint orientation through
  `spark.sql(...)`. It runs through the existing PySpark fixture, which starts a
  local Sail server when `SPARK_REMOTE` is unset. The default Hatch environment
  now has a `graph-smoke` script for this test.
- `grust-sail` now exposes and tests typed-field compatibility helpers
  matching the Sail planner's typed-table fallback rules, so external Grust
  consumers and Sail's native planner share the same table/field contract.
  Those helpers include missing-field reporting for diagnostics when a typed
  table cannot satisfy a graph query.
- Sail's graph planner has a matching parity test for the same typed table
  names, physical-field aliases, JSON-property fallback, and structural edge
  field requirements. This keeps the in-process lowering pinned to the
  reusable `grust-sail` contract without adding a reverse dependency from Sail
  into the Grust backend crate.
- `grust-sail` also exposes `GraphSchema`-derived typed table descriptors,
  including `SailGraphTypedTable`, `SailGraphTypedTableKind`,
  `sail_graph_schema_typed_tables`, `sail_typed_node_columns`, and
  `sail_typed_edge_columns`, so declared schema metadata can be reused by
  external Grust consumers without duplicating table naming or column rules.
- `grust-sail` schema DDL now marks typed graph tables with
  `grust.graph.kind` and `grust.graph.label` table properties. Sail's native
  graph planner consumes those properties when they are present: matching
  metadata allows typed-table planning, missing metadata remains
  backward-compatible, and contradictory metadata produces a generic
  `grust_nodes` / `grust_edges` fallback with a debug diagnostic.
- `grust-sail` also exposes `SailGraphStore::triplets`, `SailTripletRow`,
  `SailGraphPatternDirection`, and direction-aware `sail_triplets_sql` /
  `SailGraphStore::triplets_for_direction` helpers; GrustFrames now delegates
  `GraphFrame::sail_triplets` and `sail_triplets_for_direction` to that shared
  backend primitive instead of owning a separate Sail triplet lowering.
- `grust-sail` now also has an ignored live Spark Connect integration test that
  loads a graph through Grust's external Sail backend, sends Cypher through
  `SailGraphStore::query_arrow_ipc`, and checks the same outgoing property-map,
  incoming, undirected, shorthand, and `SKIP ... LIMIT ALL` semantics asserted
  by Sail's native planner tests.
- Sail's native planner currently mirrors those `grust-sail` helper rules
  locally instead of depending on the external `grust-sail` crate. That keeps
  the dependency direction clean: `grust-sail` may call Sail over Spark
  Connect, while Sail's core planner stays independent of Grust backend crates.

Local verification notes:

- The graph PySpark smoke passes through the default Hatch environment with a
  healthy Hatch launcher:
  `uvx --python .venv/bin/python hatch run graph-smoke`.
- The Homebrew `hatch` executable on this checkout's machine still fails before
  project environment creation because its Python 3.14 runtime cannot import
  `pyexpat` (`_XML_SetAllocTrackerActivationThreshold` is missing from the
  system `libexpat`). That is a local Hatch launcher/toolchain issue, not a
  graph smoke or project Hatch environment failure.

The first branch now has concrete coverage for the parser/analyzer/planner
path, the Spark Connect SQL path, PySpark smoke behavior, the reusable
`grust-sail` table contract, GrustFrames delegation to `grust-sail`, external
Grust-backed Cypher traversal, and declared typed-table metadata diagnostics.

## Non-Goals For The First Slice

- Full Cypher compliance.
- Graph mutations such as `CREATE`, `MERGE`, `SET`, and `DELETE`.
- Variable-length paths.
- Cost-based graph-specific optimization.
- A new Spark Connect protobuf extension.
- A second Sail-owned property graph storage model.

## Completion Criteria

The first branch should be considered complete when:

- Cypher read syntax has parser gold coverage.
- Graph AST converts to `spec::Plan` without stringifying back through SQL.
- `PlanResolver` can execute directed `MATCH ... RETURN ...` over the generic
  Grust Sail tables.
- Spark Connect can run the query through `spark.sql(...)` or equivalent
  `SqlCommand` and return Arrow results.
- The Grust/grustframes reuse boundary is documented in code or docs, and any
  general helpers have been moved to Grust or `grust-sail`.
