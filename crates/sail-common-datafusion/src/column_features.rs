//! Engine-level contract for column-feature metadata.
//!
//! Column features (generation expressions, identity columns, invariants,
//! default values, internal markers) are carried on arrow `Field::metadata`
//! throughout the plan pipeline. This module defines the canonical key
//! namespace and provides a typed read/write API so callers do not reach for
//! raw string keys.
//!
//! # Separation from format-level metadata
//!
//! Table-format writers (Delta, Iceberg, …) are responsible for translating
//! these engine-level keys into format-specific on-disk keys at commit time,
//! and for translating them back at read time. The engine pipeline itself
//! never reasons about format-specific metadata directly.
//!
//! Some engine keys happen to coincide with a format's on-disk name today
//! (for example `delta.generationExpression` is both the engine key and the
//! Delta on-disk key). That coincidence is not load-bearing and future keys
//! should prefer the `sail.column.*` prefix to keep the layers distinct.

use std::collections::HashMap;

use datafusion::arrow::datatypes::Field;

/// Keys identifying column features carried on arrow field metadata.
///
/// This enum is `#[non_exhaustive]` so additional features (identity, default
/// value, invariants, …) can be added without breaking downstream matches.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnFeatureKey {
    /// SQL expression that deterministically produces the column's value.
    GenerationExpression,
}

impl ColumnFeatureKey {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GenerationExpression => "delta.generationExpression",
        }
    }
}

impl AsRef<str> for ColumnFeatureKey {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Typed read-only view over a column's engine-level feature metadata.
///
/// Construct via [`ColumnFeatures::from_field`] or [`ColumnFeatures::from_map`]
/// to read generation expressions, identity specs, invariants, defaults, etc.
/// without touching raw string keys.
#[derive(Debug, Clone, Copy)]
pub struct ColumnFeatures<'a> {
    metadata: &'a HashMap<String, String>,
}

impl<'a> ColumnFeatures<'a> {
    pub fn from_map(metadata: &'a HashMap<String, String>) -> Self {
        Self { metadata }
    }

    pub fn from_field(field: &'a Field) -> Self {
        Self {
            metadata: field.metadata(),
        }
    }

    /// Returns the generation expression SQL text, if this column is generated.
    ///
    /// External tables sometimes serialize the expression as a JSON string
    /// (i.e. wrapped in extra quotes). The decoded value is returned in that
    /// case, with the raw string as a fallback.
    pub fn generation_expression(&self) -> Option<String> {
        self.metadata
            .get(ColumnFeatureKey::GenerationExpression.as_str())
            .map(|v| serde_json::from_str::<String>(v).unwrap_or_else(|_| v.clone()))
    }

    /// Returns the raw stored value for a feature key, bypassing decoding.
    pub fn raw(&self, key: ColumnFeatureKey) -> Option<&str> {
        self.metadata.get(key.as_str()).map(String::as_str)
    }
}

/// Builder for column-feature metadata.
///
/// Emits a `HashMap<String, String>` suitable for attachment to either an
/// arrow `Field` or a DataFusion `FieldMetadata`.
#[derive(Debug, Default, Clone)]
pub struct ColumnFeaturesBuilder {
    entries: HashMap<String, String>,
}

impl ColumnFeaturesBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_generation_expression(mut self, expr: impl Into<String>) -> Self {
        self.entries.insert(
            ColumnFeatureKey::GenerationExpression.as_str().to_string(),
            expr.into(),
        );
        self
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn build(self) -> HashMap<String, String> {
        self.entries
    }
}
