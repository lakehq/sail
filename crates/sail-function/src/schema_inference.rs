/// A recursively inferred schema before it is rendered as a Spark type string.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    fields.sort_by(|(left, _), (right, _)| left.cmp(right));

    let mut merged: Vec<(String, InferredType)> = Vec::new();
    for (name, ty) in fields {
        match merged.last_mut() {
            Some((last_name, last_type)) if *last_name == name => {
                let previous = std::mem::replace(last_type, InferredType::Null);
                *last_type = previous.merge_with(ty, merger);
            }
            _ => merged.push((name, ty)),
        }
    }
    merged
}
