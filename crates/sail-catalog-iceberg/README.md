# Sail Catalog - Iceberg REST Catalog

CHECK HERE: MOVE THIS TO SAIL DOCS

## Development

### Prerequisites

To regenerate the REST client from the OpenAPI specification, you need to install the OpenAPI Generator CLI:

```bash
brew install openapi-generator
```

For other installation methods, see the [OpenAPI Generator CLI Installation Guide](https://openapi-generator.tech/docs/installation/).

### Configuration

Generator configuration is defined in `crates/sail-catalog-iceberg/spec/openapi-generator-config.yaml`.

### Generating the REST Client

The REST client is auto-generated from the [Iceberg REST Catalog OpenAPI spec](spec/iceberg-rest-catalog.yaml).

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
1. Generate Rust client code from the OpenAPI spec
2. Extract `apis/` and `models/` directories to `src/`
3. Remove problematic `model_type.rs` file (see note below)
4. Format the generated code with `cargo fmt`

The generated code will be placed in `src/apis/` and `src/models/`.

**Known Issue:** The OpenAPI generator creates a problematic `model_type.rs` file with duplicate `Type` enum definitions. The generation script automatically removes this file to prevent compilation errors.