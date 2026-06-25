---
title: Fixing the Generated TableUpdate / ViewUpdate Models
rank: 80
---

# Fixing the Generated `TableUpdate` / `ViewUpdate` Models

This note documents a defect in the generated Iceberg REST catalog client models
in `sail-catalog-iceberg`, why it happens, and the fix.

## Symptom

Deserializing a standard Iceberg REST `updateTable` (commit) request into the
generated `models::CommitTableRequest` fails for **any** real metadata update.
For example, a `set-properties` commit:

```json
{
  "requirements": [{ "type": "assert-table-uuid", "uuid": "…" }],
  "updates": [{ "action": "set-properties", "updates": { "k": "v" } }]
}
```

fails with:

```text
missing field `uuid`
```

A commit with an empty `updates` array deserializes fine. The error therefore
comes from the `updates` element type, not the requirements or the envelope.

## Root cause

`models::CommitTableRequest.updates` is typed `Vec<models::TableUpdate>`, and the
generated `TableUpdate` is a **flat struct with every field required**:

```rust
pub struct TableUpdate {
    pub action: String,
    pub uuid: String,            // required
    pub format_version: i32,     // required
    pub schema: Box<models::Schema>, // required
    pub schema_id: i32,          // required
    // … every field of every update variant, all required …
}
```

So a `set-properties` update — which carries only `action` and `updates` — is
rejected because `uuid`, `format-version`, `schema`, … are absent.

This is a generation artifact, and it traces back to the **upstream** OpenAPI
spec, not a local mistake. `spec/iceberg-rest-catalog.yaml` is a pinned,
verbatim snapshot of Apache Iceberg's own spec (see the `SOURCE:` comment at
the top of the file) and is not edited locally — it must stay diffable against
upstream:

- `BaseUpdate` declares a **`discriminator`** on `action`, so the Rust
  openapi-generator emits a correct internally-tagged enum for it:

  ```rust
  #[serde(tag = "action")]
  pub enum BaseUpdate {
      #[serde(rename = "set-properties")]
      SetPropertiesUpdate {},
      // …
  }
  ```

- `TableUpdate` and `ViewUpdate`, by contrast, are declared upstream as bare
  **`anyOf`** lists with **no discriminator**. The generator cannot turn an
  undiscriminated `anyOf` into a tagged enum, so it merges all members into a
  single struct and marks every property required — the degenerate type above.
  This is a real gap in the upstream spec itself, shared by every consumer of
  it; fixing it would require an upstream PR to `apache/iceberg`, which is out
  of scope here.

## Fix

Rather than edit the vendored spec, hand-patch the two generated files to the
shape the generator *would* produce if `TableUpdate`/`ViewUpdate` carried the
same discriminator as `BaseUpdate`:

```rust
#[serde(tag = "action")]
pub enum TableUpdate {
    #[serde(rename = "assign-uuid")]
    AssignUuidUpdate(Box<models::AssignUuidUpdate>),
    #[serde(rename = "set-properties")]
    SetPropertiesUpdate(Box<models::SetPropertiesUpdate>),
    // … one variant per action …
}
```

This is the same fix conceptually (give the type a tag on `action`), applied to
the generated Rust output instead of the spec input, following the existing
**"Post-Generation Manual Steps"** convention in
[`iceberg-openapi-generation.md`](recipes/iceberg-openapi-generation.md) (which
already lists hand-patches required after every regeneration, e.g. for
`catalog_api_api.rs` and `schema.rs`). `table_update.rs` and `view_update.rs`
are now in that same category: **regenerating overwrites them, and the patch
below must be reapplied.**

The patched variant sets mirror the action set of each schema's original
`anyOf`: `TableUpdate` covers the 21 table updates, `ViewUpdate` the 8 view
updates (assign-uuid, upgrade-format-version, add-schema, set-location,
set-properties, remove-properties, add-view-version, set-current-view-version).

## Regenerating

The generated models are produced from the spec:

```sh
crates/sail-catalog-iceberg/spec/generate-client.sh
```

This requires `openapi-generator` (Java) and **always overwrites**
`table_update.rs` and `view_update.rs` back to the degenerate flat-struct form,
since the upstream spec is unchanged. After running it, manually reapply the
tagged-enum patch described above — see step 4 in the "Post-Generation Manual
Steps" list in `iceberg-openapi-generation.md`. Both files have no hand-written
consumers (the real, typed commit model used by the engine is the separate
`sail_iceberg::spec::catalog::TableUpdate` enum, which is unaffected), so the
patch is purely about making the generated REST envelope type deserialize
correctly.

## Blast radius

`models::TableUpdate` / `models::ViewUpdate` are referenced only by the generated
`CommitTableRequest` / `CommitViewRequest` (`updates: Vec<…>`). No Sail logic
constructs them or reads their fields, so converting them from a flat struct to
a tagged enum is source-compatible for all consumers.
