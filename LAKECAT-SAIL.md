# LakeCat ↔ Sail: Catalog & Iceberg Changes

This document summarizes the bug fixes and enabling changes we have made to
**Sail** in the course of building **LakeCat**, an Iceberg REST catalog designed
to sit as close to the engine as possible. It is the running record of what
lives on the `lakecat` branch of our Sail fork (`querygraph/sail`), why it is
kept on a branch rather than merged to `lakehq/sail` `main`, and which pieces
have been proposed upstream.

> **Status as of 2026-06-26.** Base = `lakehq/sail@7da486db` (the merged
> foundation, PR #2134). The `lakecat` branch carries four additional commits on
> top of that base.

## Why a branch

LakeCat needs Sail's Iceberg layer to be **correct** (so reads return the right
rows) and **extensible** (so a thin REST bridge can drive table commits without
forking Sail's generated models). Two of these needs overlap with an active
upstream redesign:

- **PR #2141 (@linhr), _"feat: improve catalog providers"_** is reworking the
  `CatalogProvider` surface in parallel. Our provider-seam additions
  (`commit_table` / `get_table_commits`) touch the same trait, so we hold them on
  `lakecat` to avoid churning against a moving target and to let that design
  land first.
- **Issue #982, _"[Catalog] Support Iceberg"_** is the umbrella for Iceberg
  catalog support; the metadata-evolution work (server-side `apply_table_updates`)
  depends on decisions still being settled there.

The correctness **bug fixes**, by contrast, are independent of that redesign and
have been proposed upstream as standalone PRs.

## Summary

| # | Change | Kind | Area | Upstream PR | Status |
|---|--------|------|------|-------------|--------|
| 1 | `TableUpdate`/`ViewUpdate` as discriminated enums | enablement | `sail-catalog-iceberg` models | **#2134** | ✅ merged (base) |
| 2 | Round-trip manifest lower/upper bounds through Avro | **bug fix** | `sail-iceberg` manifest serde | **#2139** | 🟢 open |
| 3 | Skip type-mismatched bound stats in pruning | **bug fix** | `sail-iceberg` datasource pruning | **#2139** | 🟢 open |
| 4 | `apply_table_updates` — evolve `TableMetadata` server-side | enablement | `sail-iceberg` metadata | **#2135** | 🔴 closed (held) |
| 5 | Expose planning helpers + commit-table provider seam | enablement | `sail-catalog`, `sail-catalog-iceberg` | **#2140** | 🟢 open |

`lakecat` = base + commits 2–5 (commit 1 is already in `main`).

---

## Bug fixes (Iceberg correctness)

These are pure correctness fixes in Sail's Iceberg reader; they benefit any Sail
user, not just LakeCat, and are offered upstream as such.

### 1. Manifest lower/upper bounds were silently lost on write — PR #2139
*Commit `dcc83743` · `crates/sail-iceberg/src/spec/manifest/_serde.rs`*

`IntBytesMapEntry.value` is a `Vec<u8>` typed as Avro `bytes` in the manifest
schema, but serde's default `Vec<u8>` serialization emits an Avro **array**. That
array fails to resolve against the `bytes` schema, so the nullable
`lower_bounds` / `upper_bounds` maps were written as **null** — every per-column
bound was lost on read.

**Fix:** force byte-string (de)serialization on the entry value, while still
tolerating the legacy array form on read for backward compatibility, so bounds
survive the write/parse round-trip. (`sail-iceberg`: 79 tests pass.)

**Impact:** without this, column bounds never reach the query planner, so
manifest-level pruning is a no-op — full scans where a pruned scan was expected.

### 2. Pruning could panic / mis-type on bound stats — PR #2139
*Commit `eba9090d` · `crates/sail-iceberg/src/datasource/pruning.rs`*

Once bounds round-trip correctly (fix #1), `IcebergPruningStats` feeds them to
DataFusion's `PruningPredicate`. An Iceberg bound can decode to a primitive whose
Arrow scalar type differs from how the predicate treats the column (the
`to_scalar` fallback yields e.g. an `Int32` for a value the predicate compares as
`Timestamp`/`Date`), tripping DataFusion's "same data type are comparable"
assertion on partition-transform reads.

**Fix:** wire up the previously-unused `arrow_schema` — `min_values` / `max_values`
now **skip** stats whose array type does not match the logical column type, so
pruning only applies with well-typed bounds and falls back to a full scan
otherwise. No false pruning, no wrong results. (`sail-iceberg`: 79 tests pass;
clippy + nightly fmt clean.)

> Fixes 1 and 2 are sequential: #1 makes the bounds *exist*, #2 makes consuming
> them *safe*. They ship together as PR #2139.

---

## Enabling changes (necessary updates for catalog features)

These add the surface LakeCat needs. They are intentionally **additive and
default-safe** — existing providers are unaffected — but because they extend the
catalog provider trait, they are coordinated with the upstream redesign (#2141)
rather than merged unilaterally.

### 3. `TableUpdate` / `ViewUpdate` as discriminated enums — PR #2134 (merged)
*Base `7da486db` · `crates/sail-catalog-iceberg/src/models/{table_update,view_update}.rs`, `lib.rs`*

The OpenAPI-generated models represented Iceberg REST `TableUpdate` /
`ViewUpdate` as flat structs, so the `action` discriminator could not be matched
exhaustively. We hand-patched them into proper **discriminated (tagged) enums**,
documenting the post-generation patch in
`recipes/iceberg-openapi-generation.md`. This is the foundation everything below
builds on — it is already merged into `main` and forms the `lakecat` base.

### 4. `apply_table_updates` — server-side metadata evolution — PR #2135 (held)
*Commit `cbccedf3` · `crates/sail-iceberg/src/spec/metadata/apply.rs`*

A reusable `apply_table_updates(&mut TableMetadata, &[TableUpdate], now_ms)` that
applies Iceberg REST metadata updates to produce new table metadata
**server-side**, so a catalog (LakeCat) can materialize new metadata on commit
instead of relying on the client to author it.

- **Handled:** properties, location, uuid, format-version, schema,
  current-schema, default-spec, sort-order, removals, statistics.
- **Deliberately `NotImplemented`** (rather than producing incorrect metadata):
  add-spec binding, add-snapshot sequencing, snapshot-ref state — the updates
  that need deeper machinery.

**Why held:** PR #2135 was opened as a draft and **closed** pending the Iceberg
catalog-support decisions tracked in #982. The code remains on `lakecat` and is
ready to re-propose once that direction is settled.

### 5. Planning helpers + commit-table provider seam — PR #2140
*Commit `a224c478` · `crates/sail-catalog-iceberg/src/{lib,planning,provider}.rs`, `crates/sail-catalog/src/provider/{mod,options}.rs`*

Surfaces the symbols a thin REST-catalog bridge needs **without forking the
generated models**:

- **`sail-catalog-iceberg`:** make `models` public; add planning result helpers
  (`completed_planning[_with_id]_result_from_values`,
  `fetch_scan_tasks_result_from_values`); re-export `LoadTableResult` /
  `TableMetadata`; expose `load_table_result_to_status` as a standalone
  `pub fn` (the existing method now delegates to it).
- **`sail-catalog`:** add `CommitTableOptions`, `GetTableCommitsOptions`,
  `TableCommitInfo`, `GetTableCommitsResponse`, and **default** trait methods
  `CatalogProvider::{commit_table, get_table_commits}` that return
  `NotSupported`. Existing providers compile unchanged.

**Why open but branch-tracked:** this is the piece that overlaps PR #2141's
provider rework. It is proposed (PR #2140) for visibility and review, but kept on
`lakecat` so LakeCat can build against it today regardless of when/how #2141
lands.

---

## Consuming the branch

LakeCat depends on these crates via git, pinned to the `lakecat` branch of the
fork:

```toml
[dependencies]
sail-catalog           = { git = "https://github.com/querygraph/sail", branch = "lakecat" }
sail-catalog-iceberg   = { git = "https://github.com/querygraph/sail", branch = "lakecat" }
sail-common-datafusion = { git = "https://github.com/querygraph/sail", branch = "lakecat" }
```

(`querygraph/sail` is our canonical fork; `lakecat` rebases onto `lakehq/sail`
`main` as upstream moves. The two bug-fix commits, #2139, are also offered
upstream and will drop out of the branch delta once merged.)

## Upstream coordination

- **Merge as they are:** #2139 (bug fixes 1–2) — independent of the redesign.
- **Coordinate with #2141:** #2140 (provider seam) — same trait surface.
- **Re-propose after #982 settles:** #2135 (`apply_table_updates`).

## Related earlier catalog work (merged, for context)

- **#2134** — `TableUpdate`/`ViewUpdate` discriminated enums (the base above).
- **#1924** — fix `create()` table for Nessie, with a regression test.
- **#1928** — improve the missing-JDBC-data-source error message.
