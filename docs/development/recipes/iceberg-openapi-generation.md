---
title: Generating Iceberg REST Catalog Client
rank: 51
---

# Generating Iceberg REST Catalog Client

This guide explains how to regenerate the Rust client code for the Iceberg REST Catalog from the OpenAPI specification.

## Prerequisites

To regenerate the REST client from the OpenAPI specification, you need to install the OpenAPI Generator CLI:

```bash
brew install openapi-generator
```

For other installation methods, see the [OpenAPI Generator CLI Installation Guide](https://openapi-generator.tech/docs/installation/).

## Configuration

Generator configuration is defined in `crates/sail-catalog-iceberg/spec/openapi-generator-config.yaml`.

## Generating the REST Client

The REST client is auto-generated from the [Iceberg REST Catalog OpenAPI spec](https://github.com/lakehq/sail/blob/main/crates/sail-catalog-iceberg/spec/iceberg-rest-catalog.yaml).

To regenerate the client code, run the generation script from the repository root:

```bash
./crates/sail-catalog-iceberg/spec/generate-client.sh
```

Or from the spec directory:

```bash
cd crates/sail-catalog-iceberg/spec
./generate-client.sh
```

The script will:

1. Generate Rust client code from the OpenAPI spec using `--schema-mappings` to use custom types from `src/types/`
2. Extract `apis/` and `models/` directories to `src/`
3. Format the generated code with `cargo fmt`

The generated code will be placed in `src/apis/` and `src/models/`.

## Schema Mappings

The generator uses custom type mappings to avoid problematic generated code:

- `Type`, `StructType`, `ListType`, `MapType`, `StructField` → `crate::types::{Type,StructType,ListType,MapType, NestedFieldRef}`

## Post-Generation Manual Steps

OpenAPI 3.1 support is still in beta when generating Rust clients with the OpenAPI Generator.
After running the generation script, you must manually fix the following:

1. In `src/apis/catalog_api_api.rs`:

   - Replace `"{}/v1/{prefix}/` with `"{}/v1{prefix}/`
   - Replace `crate::apis::urlencode(prefix.unwrap())` with `prefix.map(|p| format!("/{}", crate::apis::urlencode(p))).unwrap_or_default()`

2. In `src/apis/o_auth2_api_api.rs`:

   - Replace `models::models::TokenType` with `models::TokenType`

3. In `src/models/schema.rs`:

   - Replace `models::StructField` with `NestedFieldRef`

4. In `src/models/table_update.rs` and `src/models/view_update.rs`:

   - The upstream spec declares `TableUpdate`/`ViewUpdate` as a bare `anyOf`
     with no discriminator (unlike `BaseUpdate`, which has one), so the
     generator emits a degenerate flat struct with every field of every
     variant marked required. Replace each generated struct with a
     hand-written `#[serde(tag = "action")]` enum (mirroring `base_update.rs`),
     with one variant per action and with all payload fields preserved.

5. In `src/models/table_requirement.rs`, `src/models/view_requirement.rs`,
   `src/models/commit_view_request.rs`, and
   `src/models/assert_ref_snapshot_id.rs`:

   - Ensure `TableRequirement` is a hand-written `#[serde(tag = "type")]` enum
     whose variants preserve their payload fields. In particular,
     `AssertRefSnapshotId` must include `ref` and nullable `snapshot-id`; if it
     is generated as an empty variant, Iceberg REST commits will serialize as
     `{"type":"assert-ref-snapshot-id"}` and servers will reject the request
     because `ref` is missing.
   - Ensure `AssertRefSnapshotId.snapshot_id` is `Option<i64>` so create
     transactions can send `"snapshot-id": null`, which means the ref must not
     already exist.
   - If the generator omits `ViewRequirement` because it has only one variant,
     add it as a hand-written `#[serde(tag = "type")]` enum with
     `AssertViewUuid { uuid }`, and keep `CommitViewRequest.requirements` typed
     as `Option<Vec<ViewRequirement>>`.
