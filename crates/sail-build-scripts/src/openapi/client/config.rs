/// Configuration for OpenAPI client code generation.
///
/// The generator supports a deliberately small OpenAPI subset used by Sail catalog clients.
/// These options let callers adapt that subset when generated code needs to coexist with
/// handwritten Rust types or when a specification contains endpoints outside the supported subset.
pub struct OpenApiConfig {
    /// Schema names to omit from generated Rust type definitions.
    ///
    /// Use this when a schema needs a handwritten implementation in the crate that includes the
    /// generated client. References to excluded schemas are still emitted by name.
    pub excluded_schemas: &'static [&'static str],
    /// Operation IDs to omit from generated `ApiClient` methods and operation error enums.
    ///
    /// This is useful for endpoints that are not needed by the target crate or that use currently
    /// unsupported OpenAPI features such as non-JSON request bodies or header/cookie parameters.
    pub excluded_operations: &'static [&'static str],
    /// Schema-specific serde conversion hints.
    ///
    /// Each entry is `(schema_name, from_type)` and emits `#[serde(try_from = "from_type")]` on
    /// the generated schema type.
    pub serde_types: &'static [(&'static str, &'static str)],
}

impl OpenApiConfig {
    pub const fn new() -> Self {
        Self {
            excluded_schemas: &[],
            excluded_operations: &[],
            serde_types: &[],
        }
    }
}

impl Default for OpenApiConfig {
    fn default() -> Self {
        Self::new()
    }
}
