use serde::{Deserialize, Serialize};

pub(crate) mod timestamp;
pub(crate) mod type_coercion;
pub(crate) mod validation;

/// A recursively inferred schema before it is rendered as a Spark type string.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum InferredType {
    Null,
    Boolean,
    Long,
    Float,
    Decimal(u8, u8),
    Double,
    String,
    Binary,
    Date,
    Timestamp,
    TimestampNtz,
    Array(Box<InferredType>),
    Struct(Vec<(String, InferredType)>),
    Variant,
}

/// Defines how format-specific incompatible scalar types are merged.
pub(crate) trait TypeMerger {
    fn compare_field_names(&self, left: &str, right: &str) -> std::cmp::Ordering {
        left.cmp(right)
    }

    fn merge_atomic(&self, left: InferredType, right: InferredType) -> InferredType;
}

impl InferredType {
    /// Recursively merges two inferred types while delegating format-specific
    /// scalar coercion and incompatible-type handling to the merger.
    pub(crate) fn merge_with(self, other: InferredType, merger: &impl TypeMerger) -> InferredType {
        match (self, other) {
            (InferredType::Null, other) | (other, InferredType::Null) => other,
            (left, right) if left == right => left,
            (InferredType::Array(left), InferredType::Array(right)) => {
                InferredType::Array(Box::new(left.merge_with(*right, merger)))
            }
            (InferredType::Struct(left), InferredType::Struct(right)) => {
                InferredType::Struct(merge_fields(left, right, merger))
            }
            (left, right) => merger.merge_atomic(left, right),
        }
    }

    pub(crate) fn estimated_size(&self) -> usize {
        match self {
            InferredType::Array(element) => {
                std::mem::size_of::<InferredType>() + element.estimated_size()
            }
            InferredType::Struct(fields) => fields
                .iter()
                .map(|(name, ty)| {
                    name.len() + ty.estimated_size() + std::mem::size_of::<(String, InferredType)>()
                })
                .sum(),
            _ => 0,
        }
    }
}

fn merge_fields(
    left: Vec<(String, InferredType)>,
    right: Vec<(String, InferredType)>,
    merger: &impl TypeMerger,
) -> Vec<(String, InferredType)> {
    let mut fields = left;
    fields.extend(right);
    fields.sort_by(|(left, _), (right, _)| merger.compare_field_names(left, right));

    let mut merged: Vec<(String, InferredType)> = Vec::new();
    for (name, ty) in fields {
        match merged.last_mut() {
            Some((last_name, last_type))
                if merger.compare_field_names(last_name, &name).is_eq() =>
            {
                let previous = std::mem::replace(last_type, InferredType::Null);
                *last_type = previous.merge_with(ty, merger);
            }
            _ => merged.push((name, ty)),
        }
    }
    merged.sort_by(|a, b| a.0.cmp(&b.0));
    merged
}

/// Quotes a field name with backticks unless it is a valid identifier,
/// mirroring Spark's `quoteIfNeeded`.
fn quote_if_needed(name: &str) -> String {
    let mut chars = name.chars();
    let valid = match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {
            chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
        _ => false,
    };
    if valid {
        name.to_string()
    } else {
        format!("`{}`", name.replace('`', "``"))
    }
}

impl InferredType {
    pub(crate) fn canonicalize(&self) -> Option<Self> {
        match self {
            Self::Null => Some(Self::String),
            Self::Array(element) => element.canonicalize().map(|e| Self::Array(Box::new(e))),
            Self::Struct(fields) => {
                let fields: Vec<_> = fields
                    .iter()
                    .filter(|(name, _)| !name.is_empty())
                    .filter_map(|(name, ty)| ty.canonicalize().map(|ty| (name.clone(), ty)))
                    .collect();
                (!fields.is_empty()).then_some(Self::Struct(fields))
            }
            other => Some(other.clone()),
        }
    }

    /// Canonicalizes JSON and XML schemas while preserving an empty root struct.
    pub(crate) fn canonicalized_ddl(&self) -> String {
        let canonical = self.canonicalize().unwrap_or_else(|| match self {
            Self::Struct(_) => Self::Struct(vec![]),
            Self::Array(element) if matches!(**element, Self::Struct(_)) => {
                Self::Array(Box::new(Self::Struct(vec![])))
            }
            _ => Self::String,
        });
        canonical.to_ddl("STRUCT")
    }

    pub(crate) fn to_ddl(&self, struct_name: &str) -> String {
        match self {
            Self::Null => "VOID".to_string(),
            Self::Boolean => "BOOLEAN".to_string(),
            Self::Long => "BIGINT".to_string(),
            Self::Float => "FLOAT".to_string(),
            Self::Decimal(p, s) => format!("DECIMAL({p},{s})"),
            Self::Double => "DOUBLE".to_string(),
            Self::String => "STRING".to_string(),
            Self::Binary => "BINARY".to_string(),
            Self::Date => "DATE".to_string(),
            Self::Timestamp => "TIMESTAMP".to_string(),
            Self::TimestampNtz => "TIMESTAMP_NTZ".to_string(),
            Self::Variant => "VARIANT".to_string(),
            Self::Array(element) => format!("ARRAY<{}>", element.to_ddl(struct_name)),
            Self::Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|(name, ty)| {
                        format!("{}: {}", quote_if_needed(name), ty.to_ddl(struct_name))
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{struct_name}<{fields}>")
            }
        }
    }
}

/// Merges two decimal types by keeping enough integer digits and scale to
/// hold both, falling back to `DOUBLE` when the maximum precision is
/// exceeded, mirroring Spark's `JsonInferSchema.compatibleType`.
pub(crate) fn merge_decimals(p1: u8, s1: u8, p2: u8, s2: u8) -> InferredType {
    let integer_digits = (p1 - s1).max(p2 - s2);
    let scale = s1.max(s2);
    if (integer_digits + scale) as usize > 38 {
        InferredType::Double
    } else {
        InferredType::Decimal(integer_digits + scale, scale)
    }
}
