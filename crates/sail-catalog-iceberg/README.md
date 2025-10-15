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

Generator configuration is defined in `crates/sail-catalog-iceberg/openapi-generator-config.yaml`.

### Generating the REST Client

The REST client is auto-generated from the [Iceberg REST Catalog OpenAPI spec](spec/iceberg-rest-catalog.yaml).

To regenerate the client code from the repository root, run:

```bash
openapi-generator generate \
  --generator-name rust \
  --config crates/sail-catalog-iceberg/openapi-generator-config.yaml \
  --input-spec crates/sail-catalog-iceberg/spec/iceberg-rest-catalog.yaml \
  --output crates/sail-catalog-iceberg/src/generated_rest
```

The generator creates a nested structure. After running the command above, flatten it:

```bash
cd crates/sail-catalog-iceberg/src/generated_rest
mv src/* .
CHECK HERE
```