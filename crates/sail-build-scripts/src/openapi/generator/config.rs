use std::collections::BTreeSet;

/// Configuration for OpenAPI code generation.
#[derive(Clone, Debug, Default)]
pub struct OpenApiConfig {
    /// Schema names to omit from generated Rust type definitions.
    ///
    /// Use this when a schema needs a handwritten implementation in the crate that includes the
    /// generated code. References to excluded schemas are still emitted by name.
    pub excluded_schemas: BTreeSet<String>,
    /// Operation IDs to omit from generated `ApiClient` methods and operation error enums.
    ///
    /// This is useful for endpoints that are not needed by the target crate or that use currently
    /// unsupported OpenAPI features.
    pub excluded_operations: BTreeSet<String>,
}

impl OpenApiConfig {
    pub fn new() -> Self {
        Self::default()
    }
}
