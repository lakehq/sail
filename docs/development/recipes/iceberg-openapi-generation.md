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

After running the generation script, you must manually fix the following:

1. In `src/apis/o_auth2_api_api.rs`:
   - Replace `models::models::TokenType` with `models::TokenType`

2. In `src/models/schema.rs`:
   - Replace `models::StructField` with `NestedFieldRef`
