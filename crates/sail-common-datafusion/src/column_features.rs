//! Engine-level contract for column-feature metadata.
//!
//! Column features (generation expressions, identity columns, default values,
//! internal markers) are carried on arrow `Field::metadata`
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

use crate::catalog::CatalogTableColumnIdentity;

/// Keys identifying column features carried on arrow field metadata.
///
/// This enum is `#[non_exhaustive]` so additional features (identity, default
/// value, constraints, …) can be added without breaking downstream matches.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnFeatureKey {
    /// SQL expression that deterministically produces the column's value.
    GenerationExpression,
    /// Sail-only marker used in the planning pipeline to preserve a Delta NOT NULL
    /// constraint when DataFusion expression rewrites make the output nullable.
    /// Delta/Spark stores this as `StructField(nullable = false)`, not as column metadata.
    NotNullConstraint,
    /// Starting value for a Delta identity column.
    IdentityStart,
    /// Increment between generated identity values.
    IdentityStep,
    /// Highest generated identity value recorded in the Delta log.
    IdentityHighWaterMark,
    /// Whether explicit inserts are allowed for the identity column.
    IdentityAllowExplicitInsert,
}

impl ColumnFeatureKey {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GenerationExpression => "delta.generationExpression",
            Self::NotNullConstraint => "sail.column.notNull",
            Self::IdentityStart => "delta.identity.start",
            Self::IdentityStep => "delta.identity.step",
            Self::IdentityHighWaterMark => "delta.identity.highWaterMark",
            Self::IdentityAllowExplicitInsert => "delta.identity.allowExplicitInsert",
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
/// to read generation expressions, identity specs, defaults, etc.
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

    pub fn is_not_null_constraint(&self) -> bool {
        self.metadata
            .get(ColumnFeatureKey::NotNullConstraint.as_str())
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
    }

    /// Returns identity column metadata if the column is an identity column.
    pub fn identity(&self) -> Option<CatalogTableColumnIdentity> {
        let start = self.parse_i64(ColumnFeatureKey::IdentityStart)?;
        let step = self.parse_i64(ColumnFeatureKey::IdentityStep)?;
        let allow_explicit_insert =
            self.parse_bool(ColumnFeatureKey::IdentityAllowExplicitInsert)?;
        let high_water_mark = self.parse_i64(ColumnFeatureKey::IdentityHighWaterMark);
        Some(CatalogTableColumnIdentity {
            start,
            step,
            allow_explicit_insert,
            high_water_mark,
        })
    }

    /// Returns the raw stored value for a feature key, bypassing decoding.
    pub fn raw(&self, key: ColumnFeatureKey) -> Option<&str> {
        self.metadata.get(key.as_str()).map(String::as_str)
    }

    fn parse_i64(&self, key: ColumnFeatureKey) -> Option<i64> {
        self.metadata.get(key.as_str()).and_then(|value| {
            serde_json::from_str::<i64>(value)
                .or_else(|_| value.parse())
                .ok()
        })
    }

    fn parse_bool(&self, key: ColumnFeatureKey) -> Option<bool> {
        self.metadata.get(key.as_str()).and_then(|value| {
            serde_json::from_str::<bool>(value)
                .or_else(|_| value.parse())
                .ok()
        })
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

    pub fn with_not_null_constraint(mut self) -> Self {
        self.entries.insert(
            ColumnFeatureKey::NotNullConstraint.as_str().to_string(),
            "true".to_string(),
        );
        self
    }

    pub fn with_identity(mut self, identity: &CatalogTableColumnIdentity) -> Self {
        self.entries.insert(
            ColumnFeatureKey::IdentityStart.as_str().to_string(),
            identity.start.to_string(),
        );
        self.entries.insert(
            ColumnFeatureKey::IdentityStep.as_str().to_string(),
            identity.step.to_string(),
        );
        self.entries.insert(
            ColumnFeatureKey::IdentityAllowExplicitInsert
                .as_str()
                .to_string(),
            identity.allow_explicit_insert.to_string(),
        );
        if let Some(high_water_mark) = identity.high_water_mark {
            self.entries.insert(
                ColumnFeatureKey::IdentityHighWaterMark.as_str().to_string(),
                high_water_mark.to_string(),
            );
        }
        self
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn build(self) -> HashMap<String, String> {
        self.entries
    }
}
