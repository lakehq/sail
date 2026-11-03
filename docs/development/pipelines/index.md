---
title: Declarative Pipelines and Native Extensions
---

# Sail Declarative Pipelines and native extensions: the design record

This document records the design of three bodies of work built on a branch of
[Sail](https://github.com/lakehq/sail) and offered upstream as a series of
pull requests:

- **Sail Declarative Pipelines (SDP):** Sail's server-side implementation of
  Spark 4.2 Declarative Pipelines, served to the unchanged `pyspark.pipelines`
  client and `spark-pipelines` CLI over Spark Connect: batch pipelines,
  bounded streaming, and AutoCDC, with the catalogs and formats they need.
- **Native extensions:** a bounded host contract under which independently
  compiled native packages (a SedonaDB scalar library and a Nutmeg graph
  library are the two worked examples) run inside Sail without being Sail
  dependencies, with host-funded memory and full lifecycle coverage.
- **Measured performance work:** criterion benches for six hot sites and the
  changes that measurably improved them on the current tree.

It is written for Sail's maintainers, to be read alongside the pull requests in
[the upstream series](https://github.com/querygraph/grust/blob/work/sail-declarative-pipelines/docs/sail-declarative-pipelines/UPSTREAM-PR-SERIES.md). Every decision below names what
was chosen, what it replaced, and how it was measured. Where Sail deliberately
differs from Spark, the difference is stated as such; nothing is presented as
parity that was not measured against Spark 4.2.1.

- **Status:** current as of 2026-11-03.
- **Branch:** `work/spark-declarative-pipelines` on
  [querygraph/sail](https://github.com/querygraph/sail/tree/work/spark-declarative-pipelines),
  root `d65857340`, 427 commits over upstream `0ba425c03` (2026-10-29).
- **Longer reading:**
  - [_Sail Declarative Pipelines_, the manual](https://firstpair.org/read/sail-declarative-pipelines/):
    what a user can do, pipeline by pipeline.
  - [The technical report](https://firstpair.org/read/sail-declarative-pipelines/report/):
    the implementation and its evidence at milestone 1.
  - [_Sail Rust Book_](https://firstpair.org/read/sail-rust-book/): the guide to
    Sail's architecture that the pull requests assume; its pipelines chapter
    and the revised chapters on sessions, the resolver, catalogs, streaming,
    testing and extensions are on the book's `work/sdp-chapter` branch.
  - On the branch, until their own pull requests land:
    [the extension design review](https://github.com/querygraph/sail/blob/work/spark-declarative-pipelines/docs/development/extensions/design-review.md)
    and [ABI review](https://github.com/querygraph/sail/blob/work/spark-declarative-pipelines/docs/development/extensions/abi-review.md)
    (the extension contract and its evidence),
    [the performance record](https://github.com/querygraph/sail/blob/work/spark-declarative-pipelines/docs/development/benches.md), and
    [the manual and report sources](https://github.com/querygraph/sail/blob/work/spark-declarative-pipelines/docs/books/).

How to read the attributions: the work was built in two implementation lanes,
a **batch lane** (SQL, registration, analysis, catalogs, runtime, ORC,
integration) and a **CDC lane** (streaming, Iggy, AutoCDC), each a coding-agent
session supervised by the project owner, with a third lane for the extensions
port and a fourth for performance. A reference of the form **CF-NNNN** is an
entry in the lanes' append-only coordination log, kept in the
[Grust](https://github.com/querygraph/grust) repository beside this file; it is
cited for provenance and is not required reading. File names in capitals are
design documents in [the same directory of the Grust repository](https://github.com/querygraph/grust/blob/work/sail-declarative-pipelines/docs/sail-declarative-pipelines/). When a decision replaced an earlier
one, only the final state is given, with the superseded choice named. Open
questions are listed at the end and are not decisions.

## 0. Where this fits Sail's plan

**SDP is Spark Connect parity, which is Sail's core promise.** Spark 4.2 ships
Declarative Pipelines as a client (`pyspark.pipelines`, the `spark-pipelines`
CLI) that drives the server through six `PipelineCommand` messages and a
typed event stream. Without a server implementation every pipeline run
against Sail fails at the first command. SDP implements that server side on
Sail's existing session, resolver, catalog and execution layers, and is
measured test by test against Spark 4.2.1: frozen ports of Spark's public
Python pipeline suites (300 selected tests and 67 upstream tests) and 1,284
Sail pipeline tests pass on the root, on a single server, in local and cluster
mode, against the memory catalog, a Hive 4.0.1 metastore, a second catalog,
Unity Catalog and Iceberg. Upstream currently pins `pyspark[connect]==4.2.0`;
the series moves the pin to 4.2.1, where the pipelines client was measured.

**It exercises the roadmap's own items.** Sail's roadmap discussion
([#1001](https://github.com/lakehq/sail/issues/1001)) lists distributed
processing ([#989](https://github.com/lakehq/sail/issues/989)) and full
Iceberg integration ([#172](https://github.com/lakehq/sail/issues/172)) among
its priorities. SDP's streaming tables and AutoCDC write to Delta, Iceberg,
Hive and Unity targets; its runs are the first long-lived, multi-flow
workloads to be qualified on Sail's cluster mode, which is why the session
lifecycle, stop ordering and cleanup accounting in §7 and §8 exist. ORC, which
Sail did not read or write, is now a first-class format because Spark's
pipeline suites use it.

**The extensions work is the FFI proof of concept the maintainers asked for.**
The extensions epic ([#1068](https://github.com/lakehq/sail/issues/1068)) points
to two discussions: general extension ideas
([#2001](https://github.com/lakehq/sail/discussions/2001), opened by the Sedona
maintainers as a proposal for a `SailExtension` trait) and Spark Connect
extensions ([#2002](https://github.com/lakehq/sail/discussions/2002), opened by
a GraphFrames maintainer asking about `RelationPlugin`). The Sail
maintainers' stated position (in
[#1810](https://github.com/lakehq/sail/issues/1810), April 2026) is that the
extension API should not be a Rust trait but an FFI built on DataFusion's,
so that Sail and an extension release on their own schedules, with Python
packages as the distribution and discovery mechanism, and that they would like
to see an end-to-end proof of concept. That is what §15 describes: two
independently compiled wheels (Sedona scalars executing on workers, Nutmeg
stateful graph relations executing on the driver) bound through
`datafusion-ffi` and a small versioned C ABI for memory leases, discovered
through Python entry points, dispatched through the Connect `extension`
relation that #2002 asked about, with the loader refusing on declared
API/DataFusion/Arrow mismatches rather than on compiler identity, and with
measured reuse of unchanged wheels across Rust 1.97.1 and 1.98.1 and across
host revisions. It answers the three open questions in #1810 as far as the
evidence allows: breaking changes are caught by declared-version refusal
before binding, with an explicit compatibility negotiation proposed as the
next step; the Connect fit is the relation-plugin path, with command plugins
deferred; and planner ordering is not claimed, because the contract admits
bounded relations and scalar functions, not optimizer rules or physical
planner extensions from outside the workspace. The proposed discussion post
is `docs/development/extensions/maintainer-request.md`.

**The performance work is measured, not asserted.** The August 2026
performance branch did not survive upstream's rewrite of the Iceberg sites and
the parser. Its six hot sites were given criterion benches on the current tree
first, measured on both tips, and only the changes that still improved the
measurement were re-applied (§16). The one that did not, reuse of the Iceberg
manifest reader, is recorded as not re-applied, with the number.

**What is not proposed.** The branch carries material that stays out of the
series: the book sources and their First Pair publishing contract, the
vendored `orc-rust` copy (the series depends on a released `orc-rust` with the
three fixes upstreamed, §9), the extension review's audit JSON and the
Pecan/Nutmeg benchmark corpus beyond what the design review cites, and the
extensions branch's separate-process worker experiment, which the merged tree
does not carry.

## 1. Decisions by the project owner

These were made by the project owner directly and take precedence over anything below. Product decisions first; the process decisions that shaped the record are summarized after them.

1. **Shared memory catalog** (2026-11-02, CF-0608). `catalog.shared_memory` stays off by default and is opt-in. The `spark-pipelines` CLI tests run with it on in every gate. Turning it on by default broke 104 tests, because Sail's fixtures expect a fresh catalog per session.
2. **Cluster release with checkpoints** (2026-11-02). Releasing a cluster session that used checkpoints keeps failing, and keeps the files, when the driver cannot confirm that the writers stopped. The server then exits 1. Never delete files that a live writer may still be using.
3. **ORC timestamps** (2026-11-02). ORC timestamps keep Sail's stored type, TIMESTAMP_INSTANT. A write of an instant inside ORC-763's window, about one second before the epoch, fails. This is a documented difference: Spark writes that value and reads back a wrong one.
4. **Spark's internal Scala suites are excluded from the goal** (2026-11-02). They depend on Spark JVM internals. The frozen 300 selected public and 67 upstream ports stand in for them. This supersedes the "in-JVM native equivalence" acceptance route (CF-0457–0464) and the completion criterion in ORIGINAL-SUITE-EXECUTION-DESIGN.md.
5. **Docker- and S3-backed Hive tests are waived** (2026-11-02). The Derby-backed Hive 4.0.1 metastore profile runs in every gate instead.
6. **Finished scope** (2026-11-02). The 4 strict expected failures in §11 and the 37 wire-invisible partial audit entries are accepted as finished scope. Wire-invisible means a Spark assertion checks internal state that the Connect protocol does not expose, so only the client-visible part is ported.
7. **Control store constraints** (CF-0554, CF-0555): AutoCDC-only bounded admission; checkpoints at `warehouse/_checkpoints/<destination>/<flow>/<n>`, mirroring Spark; Iggy in the sail-iggy fork lineage, ported into the SDP workspace (CF-0551, CF-0573).
8. **BoundedSource in append flows** (CF-0594). Append flows read a BoundedSource, with Iggy first.
9. **Iceberg nested types** (CF-0612). Spark with Iceberg 1.13 refuses 3 nested-type AutoCDC cases. Sail keeps completing them.
10. **Iceberg runtime for the oracle** (2026-11-02, CF-0612). The Iceberg 1.13.0-SNAPSHOT nightly for Spark 4.2 (commit `2742348`) is the oracle runtime; it loads on Spark 4.2.1, the 1.12.0 runtime built for Spark 4.1 does not. `iceberg_on_spark` runs the AutoCDC files with it, with the catalog cache off.
11. **Unity Catalog built from source** (2026-11-02, CF-0607). Unity Catalog v0.4.0 is built from source and run locally; the Unity suite runs against it through `PYSAIL_UNITY_CATALOG_URL`.
12. **Remote warehouses tested against moto** (2026-11-02). `moto` is enough; real S3 is not required.
13. **Power loss accepted as a limit** (2026-11-02). Only process death (SIGKILL, `crash.py`) is qualified. Power loss and restore are not.
14. **Delta AutoCDC targets are policy** (2026-11-02). Sail accepts a Delta table as an AutoCDC target, as a deliberate extension beyond Spark, which refuses with AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE. The Delta path is fenced by the control store.
15. **The plain AutoCDC run path is Spark's own exposure** (2026-11-02). AutoCDC targets in Iceberg and Unity-managed Delta run without a lease or control-store slot; the catalog orders the commits. Two pipelines writing one such target would interleave, as they would on Spark. Milestone 1 documents one pipeline per target as the operator's responsibility; leasing plain targets and the single run path are the next milestone's design.
16. **Extensions and performance are ported, not merged** (2026-11-03, SDP-WITH-EXTENSIONS-AND-PERFORMANCE.md). The extensions branch and the performance branch are re-homed onto the milestone-1 root as granular units that become the upstream series, each gated like everything else, rather than merged as branches. The Linux ABI harness host is not used for the merged tree; the merged-tree host row is produced on the gate machine and labelled as such.
17. **Grust releases with the host's Arrow floor** (2026-11-03). Grust 0.24.1 was published with an Arrow floor of 59.2.0 so that the extension workspaces can pin exactly what the host resolves; the host is not moved to a newer Arrow to suit a library.

Process decisions, in brief: product code and tests go in Sail, plans, reviews, evidence and handoffs in Grust (the directory of this file); two lanes, batch and CDC, each posting scope once and then only shared-interface questions, blockers and gated commit offers, with overlapping shared-file changes declared before they are made; integration stays with the batch lane, which gates every offer on the combined tree; the owner is notified before each substantial design phase; and no push, PR, publication or release happened without the owner's explicit word. Decisions 1–15 and the lane split were recorded on 2026-11-02 (CF-0532 to CF-0616); 16 and 17 on 2026-11-03.

## 2. Scope and architecture

- **Target.** Spark 4.2.1 (tag `617618396`) on Sail base `0ba425c03`, with the upstream client and Spark Connect unchanged (DESIGN.md). The shipped client and server are matched first. Undispatched protocol messages, such as deferred query-function signals and `ExecuteOutputFlows`, are tracked separately.
- **Order.** Batch, then streaming and Iggy, then CDC, in small reviewable local commits (DESIGN.md "Delivery").
- **No generic SQL-parity prerequisites without an SDP dependency** (batch lane, CF-0424–0426; OPERAND-EVALUATION-DESIGN.md).
  - A general-SQL prerequisite is added only when an original SDP assertion depends on it.
  - The audit found none for IDENTIFIER, CASE/IF, nvl2, DIV, DECIMAL or ANSI.
  - Constant folding, the replay fast path, decimal routing, NaNvl and the legacy casters moved to the general Sail backlog.
  - No arithmetic deviation is accepted as final.
- **Classification.** Tests are classified per test body, not per folder (CF-0458, CF-0459). Suites stay whole, with no per-test deselection (CF-0466). Hive HQL materialized views are not SDP and are excluded.
- **Reuse Sail.** Use PlanResolver, RelationScope, catalogs, JobService, task contexts and the actor model. Do not rewrite frameworks or add a second scheduler (REVIEW.md "Constraints").
- **Module.** SDP starts as a `pipelines` module in sail-spark-connect. It is extracted only when a second frontend needs it. The planner must not depend on Spark protobufs (DESIGN.md).
- **Grust and LakeCat.**
  - Vendor only Grust's dependency-free topology kernel, plus the minimal column kernel and the ROOT lowercase helper.
  - Do not import Grust or LakeCat service crates.
  - Do not depend on `lakecat-sail`, which would create a cycle.
  - General catalog and SQL semantics belong in Sail, with LakeCat as one adapter (DESIGN.md; CF-0007).
- **Control state.** Pipeline control state stays out of Iceberg and lakehouse table metadata (DESIGN.md; STREAMING-REUSE.md).
- **Distribution.** Any new physical node or expression needs Sail distributed-codec support (REVIEW.md).
- **Errors stay in Sail.** Spark error precedence and phase policy stay in Sail. Reusable kernels hold only generic bookkeeping (CF-0007).

## 3. SQL and registration

- **Declarative registration.**
  - Registration never executes DDL or rewrites SQL strings.
  - Identifiers are parsed into components and never split on quoted dots.
  - Graph IDs are session-owned UUIDs, and nothing persistent uses them (DESIGN.md §1).
- **Persisted-view text.** It is the original source slice, taken from lexer spans. There is no regex and no reconstructed SQL. A statement's origin keeps trailing trivia up to the separator (SQL-SOURCE-DESIGN.md).
- **Atomic files.**
  - Each file is parsed whole, then lowered and registered in order.
  - Registration is all-or-nothing per file, and the namespace and config reset at each file.
  - **Deliberate difference, measured on Spark 2026-11-02:** Spark keeps the definitions registered before a failing statement. The standard client aborts the run anyway, and four Sail tests pin the all-or-nothing behavior.
- **USE.** Only `USE SCHEMA` and `USE CATALOG` are pipeline directives. A bare `USE x` is rejected as unsupported (SetCatalogAndNamespace), as on Spark 4.2.1 (batch lane, `2e73a0b87`, measured on Spark).
- **Declared view columns.** Declared column names on a view are ignored, as on Spark. This applies to temporary views and persisted views, and the view comment is kept (`2e73a0b87`, measured). It supersedes "private views honor declared column names".
- **SET CATALOG.**
  - It takes an expression, through a new `SetCurrentCatalogExpression` command. The direct Catalog API keeps `SetCurrentCatalog`.
  - One shared `resolve_catalog_name` is used, with a request-local constant mode (CATALOG-EXPRESSION-DESIGN.md).
  - Catalog folding uses the session SQL config, not file SET or graph defaults, matching Spark.
- **Three catalog constant policies** (the CDC lane found them, the batch lane accepted; CF-0321–0324). This supersedes a two-policy plan.
  - Bare `SET CATALOG` optimizes first.
  - A wrapped `IDENTIFIER` is evaluated during analysis.
  - The SDP bare directive runs analysis checks, then evaluates.
  - All three are exhaustive `ConstantContext` matches, and the foldability marker sits on the expression (CF-0312, CF-0314).
- **Preparation and the graph lock.** SQL preparation runs asynchronously outside the graph lock, then commits under the lock against the current definition. The blocking mutex is never held across an await, and it is not swapped for a Tokio mutex.
- **Unset namespace.** An unqualified persistent output with an unset namespace is rejected, without a panic.
- **Identifiers.**
  - Sail keeps its Unicode unquoted-identifier policy (SQL-CATALOG-DESIGN-HANDOFF.md).
  - **Deliberate difference:** Sail accepts dotted and hyphenated table names, such as `output.with.dot` and `hyphen-mv`, that Spark's session catalog refuses with INVALID_SCHEMA_OR_RELATION_NAME. This is engine-wide and not specific to pipelines.
- **Temporary views.**
  - `v`, `SESSION.v` and `SYSTEM.SESSION.v` are allowed. Any other qualifier gives INVALID_TEMP_OBJ_QUALIFIER (CF-0436, CF-0437).
  - DROP TEMPORARY VIEW was not added (withdrawn, CF-0433, CF-0434).
- **Registration errors** (REGISTRATION-ERROR-FIDELITY-DESIGN.md; CF-0474–0514).
  - A multipart flow name raises a raw MULTIPART_FLOW_NAME_NOT_SUPPORTED. Four or more name parts raise an unclassified error first. Unclassified errors omit metadata rather than inventing `[]`.
  - Clause errors are raw `_LEGACY_ERROR_TEMP_0035`, with no SQLSTATE and no file name. The file-name proposal was withdrawn after a Spark probe.
  - `INSERT … IF NOT EXISTS` is rejected as INVALID_STATEMENT_OR_CLAUSE, by a shared parser validator.
  - Flow INSERT checks run in this order: partition, columns, overwrite, by-name.
  - CTAS and RTAS are rejected with Spark's node names.
- **Direct dataset statements are rejected** (CF-0466–0471).
  - Direct CREATE MATERIALIZED VIEW or STREAMING TABLE gives UNSUPPORTED_FEATURE.CREATE_PIPELINE_DATASET_QUERY_EXECUTION.
  - CREATE FLOW gives CREATE_FLOW_QUERY_EXECUTION.
  - **Deliberate difference:** Spark writes the INSERT child before rejecting, while Sail rejects before any write (`test_rejected_flow_does_not_execute_insert`).
- **Parser diagnostics** (SQL-SYNTAX-DIAGNOSTICS-DESIGN.md; CF-0518–0531).
  - A generic variant-level TreeParser hook, with structured diagnostics.
  - Positions are UTF-16 columns.
  - The renderer is conservative: nothing is rendered when the expected set is empty or end of input is expected.
  - Rejected: post-parse filters, token regexes, and a global map from parser strings to Spark errors.
- **REPLACE** (CF-0538, CF-0541). REPLACE WHERE and BY NAME go through TruncateIf and ByName. Ordinary REPLACE USING execution stays unsupported, with no append approximation.
- **IDENTIFIER and parameters** (DYNAMIC-REFERENCE-BINDING-DESIGN.md).
  - Literal IDENTIFIER strings normalize to named reads.
  - A computed target is resolved late, through a live catalog lookup, and is never a graph dependency.
  - SQL parameters freeze input identity at registration.
- **Missing data source.** The generic message is "[DATA_SOURCE_NOT_FOUND] Failed to find the data source: X. Make sure the provider name is correct and the package is properly registered." (`2e73a0b87`, measured on Spark). On Spark the client sees this wrapped in TableMaterializationException, inside an UnknownException. That class difference is disclosed.
- **Query functions** (CF-0569).
  - Only `spark.sql()` is allowed inside a query function. AnalyzePlan is rejected.
  - A DDL-string schema cannot be passed, because that would be an analysis RPC.
  - **Rejected: `DefineFlow.once`.** Any `DefineFlow.once`, including `false`, is rejected with DEFINE_FLOW_ONCE_OPTION_NOT_SUPPORTED/0A000, before graph lookup, as in Spark's Connect handler.

## 4. Analysis and validation

- **Analyze before writing.**
  - All flows are analyzed before any write. There is no eager analysis at DefineFlow.
  - A dry run makes no output writes, no offset advances and no checkpoint resets.
  - A real run materializes metadata, then publishes views, then runs batch flows.
  - A failed write may leave tables behind, as in Spark (DESIGN.md §2–4).
- **Validation order** (CF-0082, CF-0091). First resolve all flows, then validate in Spark's order:
  1. user-specified schemas;
  2. topology;
  3. CDC multiflow, then MV multiflow;
  4. INVALID_TEMP_OBJ_REFERENCE;
  5. reset;
  6. streamingness and temporary-view CDC;
  7. the phase-9 raw schema merge.

  This supersedes in-loop placement (CF-0024, CF-0039) and first-direct-error (CF-0061, CF-0064).

- **No-flow check.** It covers tables and views only. Sinks are excluded, as in Spark (CF-0095–0097).
- **Multiple flows.** Among destinations with several flows, any non-streaming flow gives MATERIALIZED_VIEW_WITH_MULTIPLE_QUERIES (CF-0099–0101).
- **Unresolved flows** (CF-0066–0082).
  - They are reported through an `UnresolvedPipeline` wrapper, with typed direct and downstream maps. The client sees an AnalysisException.
  - A table with any failed producer is never installed.
  - Merge failures are attributed per reader.
- **Phase-9 merge error.** SparkException CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE (42825), with left and right in Spark's order (CF-0082, CF-0560). Client errors render `SQLSTATE: x` as Spark does (CF-0595).
- **Flow failure events** (CF-0108–0114).
  - They are emitted after the cancellation select: the downstream group first, then the direct group, each sorted.
  - **Disclosed divergence:** Scala's Map order is unspecified, and Sail sorts.
- **Configuration fusion** (CF-0251–0254; FLOW-CONFIGURATION-DESIGN.md). This follows Spark's `attemptResolveFlow` and supersedes CF-0227 and CF-0228.
  - A flow's config is unioned with the configs of the upstream views it uses. Conflicts give DUPLICATE_FLOW_SQL_CONF, and Sail reports the smallest conflicting key (CF-0282).
  - Tables contribute nothing.
  - Conflicts are checked after the first semantic pass, and the query is re-resolved if the union changed it.
  - Per-flow config is a PlanConfig override, never a session mutation (CF-0128, CF-0131).
- **CTEs.** All CTEs bind in definition order, superseding reference-site binding (CF-0259, CF-0260). Case-exact CTE matching is a general parity gap and is not copied silently.
- **Reanalysis** (CF-0227–0248; BATCH-PROVIDER-MIGRATION-DESIGN.md).
  - Only tables stop reanalysis; views are traversed through their flows.
  - Analysis exports only schemas and `CreateViewOptions` publications, never plans or providers.
  - Each table command resolves its own upstream map; the run-wide plan cache was removed.
  - Producer queries are not inlined.
  - Persisted views are resolved once, during analysis.
- **Native input binding** (NATIVE-INPUT-BINDING-DESIGN.md). Rejected: an upfront sorted merge, mutating config during lowering, a resolve-once discovery pass, reusing plan_id, overloading CachedRemoteRelation, AST pointer IDs, and parsing SQL twice.
- **Lazy virtual schemas** (CF-0553, CF-0559). No eager computation with a delayed error, no fallback schema, and no single collapsed error.
- **STREAM-read schema** (batch lane, CF-0566, measured on Spark). This supersedes the earlier handoff instruction to preserve metadata.
  - Field metadata is dropped at every depth.
  - Intervals and TIME are widened.
  - CHAR and VARCHAR raise an error.
  - One shared function serves both lanes.
- **Writes through `spark.sql()`.** A write returns no rows. CTAS returns an empty result, as Delta on Spark does.
- **Partition columns.** In file tables they move last (`ee193842f`).

## 5. Catalog, materialization and Hive

- **Publishing outputs.** Outputs are published through existing provider operations. A persisted view is never silently materialized as a table.
- **Execute guard.** The public `execute()` rejects Table and Sink outputs before any materialization or catalog write (batch lane CF-0440; the CDC lane withdrew a reorder, CF-0441). Metadata-only materialization uses a private native entry point. Incremental and sink pipelines stay rejected before writes until their effect path is qualified.
- **Metadata materialization** (METADATA-TABLE-MATERIALIZATION-DESIGN.md; CF-0442–0445).
  - Inferred schemas become nullable; declared schemas stay exact.
  - An ordinary refresh of an existing table merges with the existing schema on the left, with no truncate.
  - Materialized views keep replace and truncate.
  - A full refresh of a streaming table is refused before any effect, unless `pipelines.reset.allowed=false`. That whole-graph check is labelled a temporary Sail policy.
- **Caches.** REFRESH TABLE clears every file-listing and statistics cache entry, because those caches are not keyed by table. TRUNCATE invalidates the list-files cache (CF-0536, CF-0537).
- **CATALOG_NOT_FOUND.** It is raised only at explicit catalog loads, using the measured five-class hierarchy (CF-0333, CF-0334).
- **Column and ambiguity errors.** They are fixed in general Sail through `StructuredSparkError`, not by translating strings in the pipeline (CF-0289–0307).
- **Hive** (`5cc046a0c`; CF-0602, CF-0605).
  - `file:/path` is absolute.
  - Owned tables carry `external.table.purge`, so DROP deletes their data.
  - Table listing tries the newer request form, then falls back to the old call.
  - Map entries are compared by position.
  - Hive cannot REPLACE, so a fenced reset overwrites by path and a plain reset drops and creates.
- **Unity.** When the catalog refuses PURGE, the drop falls back to a non-PURGE drop (CDC lane, CF-0607).

## 6. Errors and expressions

- **Error framework** (EXECUTION-ERROR-DESIGN.md).
  - `StructuredSparkError` lives in sail-common-datafusion, with a structured `CommonErrorCause` carried through serde and RemoteError. There is no new error framework.
  - Rejected: Connect prefix matching, pipeline-only repair, replacing all casts, and a graph error framework.
- **Which errors are wrapped.** Catalog analysis failures get the registration wrapper (42000), and a structured AnalysisException is wrapped as an analysis failure. Execution, arithmetic and ANSI cast failures stay raw (CF-0329, CF-0331).
- **Arithmetic** (CF-0337–0350).
  - Lowering never evaluates.
  - Division and remainder by zero are typed UDFs, guarded as `dividend / spark_zero_divisor(divisor)`, raising DIVIDE_BY_ZERO or REMAINDER_BY_ZERO.
  - Withdrawn: a CASE dividend-duplication fix, because common-subexpression elimination skips volatile expressions.
- **Error row order.** Which row's error surfaces is observable and must match Spark (CF-0355).
- **Casts** (CF-0378–0398).
  - A legacy string-to-float cast goes through TryCast.
  - One `SparkStringToDecimal` UDF serves ANSI, legacy and TRY_CAST.
  - An ANSI string-to-numeric or string-to-boolean cast captures the failing value only on the failure path (codec field 41).
  - `allowNegativeScaleOfDecimal` is validated when it is SET.
- **Commit plus cleanup failure.** It gives a typed `DataFusionError::Collection` (CF-0017, CF-0021). Primary and cleanup errors are kept separately.
- **Unknown commit outcomes.** An unknown commit, or an interrupted mutation supervisor or observer, stays nonretryable.

## 7. Planning, sessions and shutdown

- **Preparation lease** (batch lane, CF-0125–0130; PRE-PLANNING-OWNERSHIP-DESIGN.md).
  - A move-only, runner-bound lease is used. Admission comes before any resolver or provider work, and planning is polled on the caller.
  - Each command or retry gets a fresh lease.
  - Rejected: cloned SessionContexts, merging state back, and swapping the runtime.
- **TaskContext capture and scoped sessions** (CF-0133, CF-0134, CF-0210, CF-0216–0222).
  - Late TaskContext capture is kept, so a SET is seen.
  - Cluster capture is unsupported and is reported explicitly.
  - Scoped sessions use an explicit `with_scoped_query_session`. A second capture is unsupported.
- **Session lifecycle** (SESSION-CLEANUP-LIFECYCLE-DESIGN.md).
  - Lifecycle policy lives in sail-session, with states Creating, Running, Deleting, DeletionFailed, Deleted and Failed. There is no resurrection.
  - A deleted session keeps a tombstone. Releasing a session that was never seen is a no-op.
  - One cleanup record serves every delete and shutdown path. The manager goes Open, then Closing, then terminal. Entrypoints always join the actor system.
  - The generic actor framework is not rewritten.
  - A released session returns `[INVALID_HANDLE.SESSION_CLOSED]` on every RPC, matching Spark's text, class and parameters.
- **Local stop** (CF-0014, CF-0015).
  - Stop seals admission, requests cancellation of every job before awaiting any, and returns a result that can fail.
  - Effects are joined before the namespace is deleted. A prefix is deleted only after the effects have settled, and stop failures are sticky.
- **Ownership** (CF-0088, CF-0090; PROVIDER-RUNTIME-OWNERSHIP-DESIGN.md; LOCAL-STREAM-OWNERSHIP-DESIGN.md).
  - Effects are owned by the operation.
  - One multipart supervisor owns create, parts, complete and abort. There is no blind abort after complete has been polled, and a failed complete stays typed as uncertain.
  - `TaskResourceScope`: `seal` does not cancel, `cancel` latches, and Drop only requests cancellation.
  - The consumer polls, with an exclusive poll lease, no forwarding task and no prefetch.
- **Settled failures at shutdown** (CF-0606, decided by the batch lane in the owner's session-shutdown design; `b1f41c6bf`).
  - A job failure already delivered to its query, with its resources drained, is logged and is not a cleanup failure.
  - An unknown commit outcome, or resources that did not drain, still fail shutdown.
- **Deliberate difference.** Interrupting a run, or releasing its session during the run, stops the flows. Spark cancels the operation but lets the flows finish. Otherwise nothing could stop such a run, and shutdown needs the flows stopped.

## 8. Cluster mode

- **Driver limit.** A stopping driver gives up on its workers after 30 seconds and aborts what remains (`a4dfce84a`).
- **Worker limit.** A worker's gRPC graceful stop is bounded at 5 seconds, then aborted (`2e73a0b87`). This is where a stuck worker was held. Which connection stays open was not identified.
- **Canceled jobs.** A job canceled by a stopping driver ends its output with an error, never a clean end (`2e73a0b87`). A refused retry reports the failure that caused it.
- **Checkpoint cleanup.** Clustered cleanup returns an explicit unconfirmed-cleanup failure and keeps the prefix. `ClusterJobRunner.stop` returning Ok does not authorize deletion. Owner decision 2 confirms this.

## 9. ORC

- **Dependency** (ORC-SOURCE-DESIGN.md). Sail uses orc-rust 0.9.0 core directly, with a small Sail adapter and `default-features=false`. It does not use the datafusion-orc adapter, and Sail's prost is not upgraded for it.
- **Location.** All ORC semantics live in Sail's shared data-source path. LakeCat needs no ORC API.
- **Reading.**
  - A stripe belongs to the half-open byte range that contains its start.
  - Tail and footer budgets are 64 MiB each, and schema depth is limited to 64.
  - Unsupported Hive and forced-positional read modes are rejected at the read boundary.
  - The source-read case-sensitivity seam applies to ORC only (CF-0286–0288).
- **Writing** (ORC-WRITER-DESIGN.md).
  - One stateful native ArrowWriter per file, with one footer, and a retained abortable upload handle.
  - No per-batch concatenation, Vec buffering, text serializer or gzip wrapper.
  - Unsupported schemas are rejected before any output.
- **Compression.** Precedence is `compression`, then `orc.compress`, then the session codec. The default is zstd, as in Spark (CF-0599).
- **Withdrawn.** Candidate `42c65ecf64` applied graph and flow ORC overrides as one blanket snapshot. Spark applies them phase by phase, so settings are resolved per phase. Do not bring it back.
- **Timestamps.** TIMESTAMP is stored as TIMESTAMP_INSTANT, because Spark's stored kind depends on the writer JVM's time zone, which a Sail worker cannot know. See owner decision 3 for the window before the epoch.
- **Accepted limits.** There is no hybrid-calendar rebase before the Gregorian cutover. Memory reservations are estimates and do not bound the heap.

## 10. TIME type (TIME-SEMANTICS.md)

- **Representation.** TIME is a shared type-system concern: `spec::DataType::Time{precision 0–6}`, defaulting to 6. It is stored as Arrow Time64(ns), with the precision in hidden `SAIL::spark::time` metadata.
- **Schema merge.** Merging rejects differing precisions, recursively, with no widening.
- **Casts.** A cast to a different precision truncates, even when widening, and recurses into nested types. A same-precision cast keeps the bits.
- **Scope.** There is no pipeline-specific patch and no LakeCat handling.

## 11. Deliberate differences from Spark (strict expected failures)

These four are pinned as strict expected failures. The project owner accepted them as finished scope (owner decision 6).

1. **Interrupting a run.** It stops the flows (§7).
2. **Releasing a session during a run.** It stops the flows (§7).
3. **Added columns.** They follow the target table's order. Spark's order comes from a Scala Map and becomes unstable past four fields.
4. **`writeStream` to `memory` outside a pipeline.** Sail does not support it.

A union of streaming and batch inputs was once a fifth difference. Since `609a6c251` Sail refuses it, as Spark does.

The other documented differences, recorded where each applies, are:

- file-atomic SQL registration (§3);
- rejecting a flow before its INSERT runs (§3);
- dotted and hyphenated names (§3);
- the DATA_SOURCE_NOT_FOUND class (§3);
- sorted failure events (§4);
- ORC timestamps (§9);
- Delta AutoCDC targets, accepted as policy (owner decision 14);
- the Iceberg nested types (owner decision 9).

## 12. Delta

- **One writer.** The existing DeltaCommitExec and ratification path is kept; there is no second CDC writer. Sail's `MaxByFunction` and the conflict tracking from `Txn` are reused.
- **Not a fence.** Delta transaction app IDs and commit-info tags are for audit and secondary deduplication only.
- **Enrolled tables** (CF-0573, CF-0589, CF-0591).
  - ALTER on an enrolled table is refused.
  - A relative location resolves against the working directory.
  - `action_digest` is the SHA-256 of the staged bytes.
- **AutoCDC targets.** Sail accepts Delta AutoCDC targets where Spark refuses with AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE (CF-0591, CF-0593). This is policy, by owner decision 14.
- **Test servers.** An external test server needs `SAIL_EXECUTION__DEFAULT_PARALLELISM=4` and a 16 MiB stack.

## 13. Streaming, Iggy and BoundedSource

- **Execution model.**
  - Streaming runs as bounded AvailableNow microbatches over Sail batch plans.
  - The append path admits a documented stateless subset, plus tumbling-window aggregates.
  - Extending the continuous `StreamSource` and `FlowMarker` path is deferred.
  - An unread streaming source read is refused, never treated as success (CF-0580–0584).
- **BoundedSource** (CF-0030–0082, CF-0573, CF-0583).
  - The trait is protocol-neutral. It was moved, unchanged, into `sail-common-datafusion/src/streaming/bounded.rs` to avoid a dependency cycle. The existing StreamSource is unchanged.
  - Cursors and incarnations are opaque, and the generic core does no numeric offset arithmetic. Progress is bound to the incarnation.
  - Errors use Sail's own conditions, causes are boxed, and "Confirmed" was renamed "Observed". `retains_origin` is required, with no default.
- **Iggy.**
  - Iggy is the first broker and a required integration. It is a Sail extension, since Spark has no Iggy source. The generic checkpoint machinery does not depend on Iggy or its schema registry.
  - AutoCommit is off. SDP acknowledges only after the sink commit and its own durable record. Acknowledgements are advisory.
  - The first-offset check is the only gap detector. A gap or an incarnation change fails closed.
  - The `iggy` source refuses batch scans.
  - The mirror's private SessionContext and planner, its RepartitionExec stripping, scalar acknowledgements and version-as-offset comparisons are not ported.
- **Unit 4 lowering** (CF-0039–0197).
  - BatchIntent is the only authority.
  - Rejection is exhaustive and happens before resolution. Joins follow UnsupportedOperationChecker, with no stream-stream joins in the append path.
  - Any non-immutable function makes the plan non-replayable.
  - Mixed read modes and dynamic Python UDTFs are refused for now.
- **Shared windowing code** (CDC lane; batch lane accepted, CF-0613). The changes to `append.rs` are shared:
  - `Windowed` gains `slide_micros` and `offset`, and carries `watermarks: Vec<Watermark>`.
  - `tumbling_window` became `event_window`.
  - `windowed_aggregate` finds watermarks through unions.
  - The append path keeps its refusals: a sliding window, more than one watermark, or a watermark not directly on a streaming read.
- **Late rows in the append path** (batch lane, `6f45d5d73`, measured on Spark 4.2.1 with Delta 4.4.1). A row is dropped only when its window has ended by the watermark the batch started with (`window.end <= watermark`, WatermarkSupport). The watermark is `floorDiv(max event µs, 1000)` minus the delay in whole milliseconds, starts at 0 and never moves back. Before this, a row behind the watermark was dropped even while its window was open.

## 14. AutoCDC and the control store

- **Control store** (CDC-CONTROL-STORE-DESIGN.md, batch lane CF-0556; the CDC lane accepted it as the build specification, CF-0557).
  - There is one transactional SQL control and catalog authority. It writes the target guard, the catalog-ratified sink head and the effect receipt in a single transaction.
  - Turso is the first backend, behind provider-neutral Sail interfaces. LakeCat is the first adapter, with a standalone adapter possible.
  - V1 is one durable service, with no HA claim.
  - Rejected: a separate compare-and-set or transaction ID as the fence, a separate Delta control table, an advisory lock with a local file, a file-based PutMode::Create JSON store, and Delta DomainMetadata or table properties as the store.
- **Store rules.**
  - One target-wide slot spans flows and pipelines, superseding a per-flow lease.
  - Auxiliary and target MERGEs get separate receipts, with no two-table atomicity claim.
  - Every enrolled mutation, including bootstrap, no-op, DDL and reset, needs a typed write context.
  - Unmanaged Delta tables are not upgraded transparently, and unfenced JSON records are never imported automatically.
- **Build order** (CF-0557, CF-0558; CDC-CONTROL-STORE-REVIEW-RESPONSE.md).
  - The pure model comes first. LocalUnfenced is an explicit constructor, never an automatic fallback.
  - The Turso dependency is qualified before its adapter.
  - B0 MemTable sealing fixes repeated evaluation only. It does not give a durable restart.
- **Rows.**
  - Input rows are validated before deduplication. Deduplication uses MaxBy, and a tie keeps the last row, where Spark leaves ties undefined.
  - Sealed content is the replay authority.
  - Scope is SCD1. SCD2 is rejected faithfully (AUTOCDC_SCD2_NOT_SUPPORTED).
  - Key drift is kept distinct from sequence evolution.
- **Definition parsing.**
  - The C1 name parser is CDC-only.
  - C2 lowercases with Java Locale.ROOT rules, ported from JDK 17 break tables.
  - C3 removes sink rejection from graph validation, because Spark rejects sinks only at execution.
  - C4 lookup uses a caller resolver and reproduces Scala's map order; a collision-rejection idea was rejected (CF-0007).
- **Runtime** (CDC lane, CF-0533, CF-0597–0612).
  - A full refresh drops the auxiliary table, as in Spark. `reset.allowed=false` keeps Spark's downgrade to an ordinary refresh.
  - Stateful queries before a merge keep real state between batches, as Spark's state store does. This supersedes evaluation by difference. It covers watermarked aggregates, deduplication, inner stream joins, sliding windows and window start times.
  - The query watermark is the minimum over the streams, with a stream that has no rows counting as 0. Only the query watermark survives a restart.
  - Join and deduplication state is append-only: one segment per batch, plus a manifest.
  - A recovery run waits out a dead owner's lease.
- **Withdrawn.** The claim that every SCD1 suite was ported (CF-0586).

## 15. Native extensions

The extensions were designed and qualified on their own branch first
(`sail-extensions`, review request 1.0.0), then re-homed onto the SDP session
layer in six units. The design review (`docs/development/extensions/design-review.md`)
is the full statement; these are the decisions.

- **A bounded host contract, not a plugin framework.** The host provides four
  things and no more: discovery and trusted registration of native packages
  (Python entry points; collisions and known incompatibilities rejected;
  matching scalar implementations resolved on workers by identity); dispatch
  of bounded Connect `extension` relations with ordinary child plans and pure
  planning; driver placement of driver-local stateful regions, with automatic
  replay of those regions disabled and an unacknowledged mutation reported as
  indeterminate; and a versioned, opaque, prepaid memory lease with
  final-owner accounting. The foreign region stays opaque to host
  optimization. Not claimed: general foreign-service propagation, sorted or
  co-partitioned input requirements, driver-only scalar scheduling, indexed
  spatial joins, distributed native CSR iteration, optimizer rules or physical
  planner extensions contributed from outside the workspace.
- **The boundary is DataFusion's FFI plus one small C ABI.** Relations,
  providers, execution plans and record-batch streams cross through
  `datafusion-ffi`; the memory lease crosses through
  `sail-native-resource-ffi`, a crate of 169 lines with a C-layout record
  (version, size, bytes, owner, retain, release) that a rejected header may be
  read but never dereferenced past its declared size. Rust ownership and
  allocator layouts never cross.
- **Version identity is declared, not compiled.** The loader compares the
  extension's declared API, DataFusion and Arrow versions with the host's,
  which `sail-session/build.rs` derives from `Cargo.lock` (DataFusion 55.2.0,
  Arrow 59.2.0 on this root; `datafusion-ffi` pinned to the same version in
  the root manifest). It does not compare Sail commit or Rust compiler
  identity. Unchanged wheels were run against hosts built with Rust 1.97.1 and
  1.98.1, an upstream advance, and an FFI-only patch move, and against three
  revisions of the merged tree (`abi-review.md`). This is evidence of reuse
  within the measured matrix, not an ABI promise; the next step is an explicit
  compatibility-version negotiation backed by unchanged-wheel CI, not relaxed
  checks.
- **Host-funded memory counts admissions, not clones** (A6 as found). The
  native resource tracker increments once per `reserve` and decrements when
  the last clone of that quota drops; a bound package keeps its admission
  lease in a capsule owned by the session's extensions for as long as it is
  registered. There is therefore no per-job "native producers released"
  signal today, only the session-wide one. Per-job drainage needs per-clone
  accounting on the lease, which changes the FFI capsule's contract, and is
  deferred with that reason recorded.
- **The session owns the admission pool.** A session manager explicitly owns
  and injects the pool into its sessions and in-process workers; equal
  configuration never implies shared ownership. On the SDP session layer this
  became two host tests: deleted status is reported only after the cleanup
  succeeded, and a lease owned by the session's own extensions does not block
  that session's cleanup (without the second, every real extension session
  would have deadlocked at deletion).
- **Lifecycle is covered by the host, not the extension.** Interrupt, early
  termination, session deletion and expiry, and graceful shutdown stop native
  regions through the same cleanup record as every other operation (§7); the
  extension matrix (client lifecycle, protocol, resources, distributed, graph
  tables, Nutmeg, Sedona, geometry composition) runs in local and actor-cluster
  mode on the merged tree, 67 and 68 passes with 3 and 2 explained skips.
- **Exact pins on both sides, from one source.** The extension workspaces
  (`examples/extensions/{sedona,nutmeg}`, the vendored `nutmeg-graph`) pin
  DataFusion, Arrow and the Grust crates exactly to the host's resolved
  versions; the Grust algorithms come from crates.io (0.24.1), not a path or
  git dependency, so the wheels are reproducible by a third party.
- **What the merged tree does not carry.** The extensions branch's
  separate-process worker launcher (`worker_manager/process.rs`) was not
  ported; the distributed tests that need it stay in the file, skipped with
  that reason, and the merged-tree evidence says which runs were in-process.
  The Linux x86_64 host row for the merged tree is not claimed; the Linux
  rows in `abi-review.md` are from the extensions branch's own hosts.

## 16. Performance

- **Benches before changes.** Six criterion benches (Iceberg metadata
  discovery, manifest parsing, delete index, snapshot lookup, single-statement
  SQL parsing, the object-store read path) were written against the current
  tree first, then run on the old performance branch's own tip and on the
  root with the same inputs on the same machine, so the two are comparable
  (`docs/development/benches.md`). No elapsed time from a gate or a test run
  is used as a performance number.
- **Re-apply only what still measures.** The parser's single-statement
  specialization, streamed Iceberg discovery with newest-first snapshot
  search and direct deserialization, and the shared delete-index descriptors
  with delete routing were re-applied to the rewritten crates because the
  benches still showed the gap. Reuse of the Iceberg manifest reader was
  measured slower on the rewritten crate and was not re-applied; the number
  and the reason are in the record.
- **The read cache is an opt-in layer in its own place.** The Foyer
  read-through cache sits beside SDP's owned-store and multipart layers, as an
  opt-in layer below them, wrapped from configuration
  (`sail-object-store/src/layers/cache.rs`, `registry.rs`); it never reaches
  the write path and adds `foyer-common` and `foyer-memory` as the only new
  root dependencies. The performance guide says where it sits and what it
  does not cache.

## 17. How the work was verified

- **One gate for every landing.** Each unit landed on the branch only after a
  full gate on an immutable, detached commit with its own target directory:
  `cargo fmt` (nightly, per crate), strict Clippy, the library tests of
  fifteen crates, a `sail-cli` build with `--locked`, the frozen Spark suite
  ports (300 selected public tests and 67 upstream tests, each tied to the
  commit it ran on), the Sail pipeline suite (1,284 passed, 25 skipped, 4
  expected failures on the root), the CLI tests with the shared memory
  catalog, the Hive 4.0.1 metastore profile, the second-catalog profile, and
  the Delta suite compared name by name against its recorded baseline. The
  toolchain is Rust 1.98.1 and a Python 3.12 environment with PySpark 4.2.1.
- **Spark is measured first.** A behavior is called a difference only after
  the same test ran on Spark 4.2.1 alone, in its own session with an empty
  warehouse (in the first such pass, 32 of 40 apparent failures were leftovers
  from earlier tests). A fix counts only when a control run with the fix
  reverted fails the new tests.
- **Honest records.** Failed runs are kept and never relabelled; a recorded
  incompatibility is never counted as a match; no upstream assertion is
  removed, no expected failure added, and no table swapped for a materialized
  view (or streaming for batch) to obtain a pass; no debug RPC or
  Spark-internal clone is added to Sail to satisfy a fixture. Changing a
  frozen test source requires a named alignment record in the harness.
- **Whole-tree comparison.** The tree is compared against upstream
  `0ba425c03` as a whole, not only against the previous root, so that a
  regression outside the pipeline suites is seen.
- **Process, briefly.** Two lanes built in parallel from one plan, posted
  scope once, declared shared-file changes before making them, and offered
  commits with a gate summary; the batch lane integrated every offer after
  gating it on the combined tree, never rewriting offered history. The
  landing scripts refuse a commit whose tree or parent differs from what was
  gated. This record exists because two early landings were faulty.

## 18. Open questions (not decided)

- **CDC representation.** The C0 representation (logical canonical encoding or sealed bytes), the B12 tie-break, and durable staging (CF-0053, CF-0054).
- **CDC activation.** Production durability, fencing and recovery: the activation gate, qualification on a real backend, and the recovery trace. Public Table, Sink, streaming and CDC effects stay disabled outside the admitted AutoCDC graph.
- **Performance work.** The replay fast path and where its check runs; constant folding; lazy NaNvl.
- **Dynamic references.** Frozen SQL inputs, and configured dynamic and WithRelations binding.
- **Session work.**
  - Fallible actor and gateway results, which need the common actor framework.
  - Routing non-checkpoint writers through the effect owner.
  - Cluster worker effects.
  - Effect-adapter candidates A and B are not selected.
- **ORC.** Transport of an unknown outcome, native accounting bounds, and real object stores.
- **Cluster mode.** A worker can wait up to 5 seconds to stop because the driver's output forwarder holds its Flight response open during the driver's own wait (DESIGN.md, review design change 3; fix proposed, not landed).
- **Append sources.** Delta and Iceberg append sources, rate limits, and exactly-once delivery for plain file tables.
- **Extensions.** Per-job drainage of native leases (per-clone accounting on the lease, an FFI contract change); an explicit compatibility-version negotiation and unchanged-wheel CI; command plugins and optimizer or planner contributions from outside the workspace, which #1810 inventories and this contract does not cover; the Linux host row for the merged tree; a pipeline dataset over an extension relation (in progress as the last unit of the branch).
- **Follow-ups.** named_struct nullability. The Hive fallback and S3 paths are unverified (owner decision 5 waives the S3 Hive tests).

## 19. The pull-request series

[UPSTREAM-PR-SERIES.md](https://github.com/querygraph/grust/blob/work/sail-declarative-pipelines/docs/sail-declarative-pipelines/UPSTREAM-PR-SERIES.md) breaks the branch into
self-contained pull requests in dependency order, each buildable and testable
on its own against upstream `main`, with the general Sail improvements first
(errors, casts, TIME, catalogs, session lifecycle, ORC, performance), then the
pipelines module in slices, then streaming and AutoCDC, then the extension
contract, with the documentation that each slice needs. Each PR cites the
sections of this record it implements.
