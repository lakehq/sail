use std::collections::HashMap;

use crate::spec::schema::utils::visit_fields_bfs;
use crate::spec::{
    Datum, NestedFieldRef, PrimitiveLiteral, PrimitiveType, Schema, SortOrder, Type,
};

const DEFAULT_MODE: MetricsMode = MetricsMode::Truncate(16);
const DEFAULT_PROPERTY: &str = "write.metadata.metrics.default";
const COLUMN_PREFIX: &str = "write.metadata.metrics.column.";
const INFERRED_LIMIT_PROPERTY: &str = "write.metadata.metrics.max-inferred-column-defaults";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsMode {
    None,
    Counts,
    Full,
    Truncate(usize),
}

impl MetricsMode {
    fn parse(value: &str, fallback: Self) -> Self {
        let mode = value.to_ascii_lowercase();
        match mode.as_str() {
            "none" => return Self::None,
            "counts" => return Self::Counts,
            "full" => return Self::Full,
            _ => {}
        }
        if let Some(width) = mode
            .strip_prefix("truncate(")
            .and_then(|value| value.strip_suffix(')'))
            .filter(|value| !value.is_empty() && value.bytes().all(|c| c.is_ascii_digit()))
            .and_then(|value| value.parse::<i32>().ok())
            .filter(|width| *width > 0)
        {
            return Self::Truncate(width as usize);
        }
        log::warn!("Invalid Iceberg metrics mode '{value}'; using {fallback:?}");
        fallback
    }

    pub fn has_bounds(self) -> bool {
        matches!(self, Self::Full | Self::Truncate(_))
    }

    pub fn truncate_bound(self, mut bound: Datum, upper: bool) -> Option<Datum> {
        let Self::Truncate(width) = self else {
            return self.has_bounds().then_some(bound);
        };
        match (&bound.r#type, &mut bound.literal) {
            (PrimitiveType::String, PrimitiveLiteral::String(value)) => {
                if let Some((offset, _)) = value.char_indices().nth(width) {
                    value.truncate(offset);
                    if upper {
                        loop {
                            let last = value.pop()?;
                            let next = if last == '\u{d7ff}' {
                                Some('\u{e000}')
                            } else {
                                char::from_u32(last as u32 + 1)
                            };
                            if let Some(next) = next {
                                value.push(next);
                                break;
                            }
                        }
                    }
                }
            }
            (PrimitiveType::Binary, PrimitiveLiteral::Binary(value)) if value.len() > width => {
                value.truncate(width);
                if upper {
                    loop {
                        if let Some(next) = value.pop()?.checked_add(1) {
                            value.push(next);
                            break;
                        }
                    }
                }
            }
            _ => {}
        }
        Some(bound)
    }
}

#[derive(Debug, Clone)]
pub struct MetricsConfig {
    default_mode: MetricsMode,
    column_modes: HashMap<i32, MetricsMode>,
}

impl MetricsConfig {
    pub fn counts() -> Self {
        Self {
            default_mode: MetricsMode::Counts,
            column_modes: HashMap::new(),
        }
    }

    pub fn from_properties(
        schema: &Schema,
        order: &SortOrder,
        properties: &HashMap<String, String>,
    ) -> Result<Self, String> {
        let limit = properties
            .get(INFERRED_LIMIT_PROPERTY)
            .map(|value| {
                value
                    .parse::<i32>()
                    .map_err(|_| format!("Invalid Iceberg {INFERRED_LIMIT_PROPERTY}: {value}"))
            })
            .transpose()?
            .unwrap_or(100);
        let limit = if limit < 0 {
            log::warn!("Negative Iceberg {INFERRED_LIMIT_PROPERTY}: {limit}; using 100");
            100
        } else {
            limit as usize
        };
        let mut field_count = 0;
        visit_fields_bfs(schema, |_, _| field_count += 1);
        let mut column_modes = HashMap::new();
        let default_mode = if let Some(value) = properties.get(DEFAULT_PROPERTY) {
            MetricsMode::parse(value, DEFAULT_MODE)
        } else if field_count <= limit {
            DEFAULT_MODE
        } else {
            let mut ids = Vec::new();
            infer_field_ids(schema.fields(), limit, &mut ids);
            column_modes.extend(ids.into_iter().map(|id| (id, DEFAULT_MODE)));
            MetricsMode::None
        };
        let sorted_mode = if default_mode.has_bounds() {
            default_mode
        } else {
            DEFAULT_MODE
        };
        for field in &order.fields {
            if field.transform.preserves_order() {
                column_modes.insert(field.source_id, sorted_mode);
            }
        }
        for (key, value) in properties {
            if let Some(name) = key.strip_prefix(COLUMN_PREFIX)
                && let Some(field) = schema.field_by_name(name)
            {
                column_modes.insert(field.id, MetricsMode::parse(value, default_mode));
            }
        }
        Ok(Self {
            default_mode,
            column_modes,
        })
    }

    pub fn mode(&self, field_id: i32) -> MetricsMode {
        self.column_modes
            .get(&field_id)
            .copied()
            .unwrap_or(self.default_mode)
    }
}

fn infer_field_ids(fields: &[NestedFieldRef], limit: usize, ids: &mut Vec<i32>) {
    for field in fields {
        if ids.len() == limit {
            return;
        }
        if matches!(field.field_type.as_ref(), Type::Primitive(_)) {
            ids.push(field.id);
        }
    }
    for field in fields {
        if ids.len() == limit {
            return;
        }
        match field.field_type.as_ref() {
            Type::Struct(value) => infer_field_ids(value.fields(), limit, ids),
            Type::List(value) => {
                infer_field_ids(std::slice::from_ref(&value.element_field), limit, ids)
            }
            Type::Map(value) => infer_field_ids(
                &[value.key_field.clone(), value.value_field.clone()],
                limit,
                ids,
            ),
            Type::Primitive(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::spec::{NestedField, NullOrder, SortDirection, SortField, StructType, Transform};

    #[test]
    fn truncation_preserves_conservative_unicode_and_binary_bounds() {
        let string = |value: &str| {
            Datum::new(
                PrimitiveType::String,
                PrimitiveLiteral::String(value.into()),
            )
        };
        let binary = |value: &[u8]| {
            Datum::new(
                PrimitiveType::Binary,
                PrimitiveLiteral::Binary(value.into()),
            )
        };
        for (input, width, lower, upper) in [
            ("abcdef", 4, "abcd", Some("abce")),
            ("界😀abc", 2, "界😀", Some("界😁")),
            ("\u{d7ff}x", 1, "\u{d7ff}", Some("\u{e000}")),
            ("a\u{10ffff}x", 2, "a\u{10ffff}", Some("b")),
            ("\u{10ffff}x", 1, "\u{10ffff}", None),
            ("😀", 1, "😀", Some("😀")),
            ("", 1, "", Some("")),
        ] {
            let mode = MetricsMode::Truncate(width);
            assert_eq!(
                mode.truncate_bound(string(input), false),
                Some(string(lower))
            );
            assert_eq!(mode.truncate_bound(string(input), true), upper.map(string));
        }
        let mode = MetricsMode::Truncate(2);
        assert_eq!(
            mode.truncate_bound(binary(&[1, 255, 2]), true),
            Some(binary(&[2]))
        );
        assert_eq!(mode.truncate_bound(binary(&[255, 255, 0]), true), None);
        assert_eq!(
            mode.truncate_bound(binary(&[255, 255, 0]), false),
            Some(binary(&[255, 255]))
        );
        let fixed = Datum::new(
            PrimitiveType::Fixed(3),
            PrimitiveLiteral::Binary(vec![1, 2, 3]),
        );
        assert_eq!(mode.truncate_bound(fixed.clone(), true), Some(fixed));
    }

    #[test]
    fn inferred_defaults_prioritize_primitives_and_explicit_modes_override_limits()
    -> Result<(), String> {
        let field = |id, name| {
            Arc::new(NestedField::optional(
                id,
                name,
                Type::Primitive(PrimitiveType::String),
            ))
        };
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "nested",
                    Type::Struct(StructType::new(vec![field(2, "x"), field(3, "y")])),
                )),
                field(4, "a"),
                field(5, "b"),
            ])
            .build()?;
        let order = SortOrder::unsorted_order();
        let mut properties = HashMap::from([(INFERRED_LIMIT_PROPERTY.into(), "3".into())]);
        let config = MetricsConfig::from_properties(&schema, &order, &properties)?;
        assert_eq!(
            (config.mode(4), config.mode(5), config.mode(2)),
            (DEFAULT_MODE, DEFAULT_MODE, DEFAULT_MODE)
        );
        assert_eq!(config.mode(3), MetricsMode::None);
        properties.insert(DEFAULT_PROPERTY.into(), "counts".into());
        properties.insert(format!("{COLUMN_PREFIX}nested.y"), "full".into());
        let config = MetricsConfig::from_properties(&schema, &order, &properties)?;
        assert_eq!(config.mode(2), MetricsMode::Counts);
        assert_eq!(config.mode(3), MetricsMode::Full);
        Ok(())
    }

    #[test]
    fn sorted_columns_and_invalid_modes_follow_table_defaults() -> Result<(), String> {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "value",
                Type::Primitive(PrimitiveType::String),
            ))])
            .build()?;
        let order = SortOrder {
            order_id: 1,
            fields: vec![SortField {
                source_id: 1,
                source_ids: vec![],
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            }],
        };
        let mut properties = HashMap::from([(DEFAULT_PROPERTY.into(), "counts".into())]);
        assert_eq!(
            MetricsConfig::from_properties(&schema, &order, &properties)?.mode(1),
            DEFAULT_MODE
        );
        properties.insert(format!("{COLUMN_PREFIX}value"), "NONE".into());
        assert_eq!(
            MetricsConfig::from_properties(&schema, &order, &properties)?.mode(1),
            MetricsMode::None
        );
        properties.insert(format!("{COLUMN_PREFIX}value"), "truncate(0)".into());
        assert_eq!(
            MetricsConfig::from_properties(&schema, &order, &properties)?.mode(1),
            MetricsMode::Counts
        );
        properties.insert(DEFAULT_PROPERTY.into(), "invalid".into());
        assert_eq!(
            MetricsConfig::from_properties(&schema, &order, &properties)?.mode(1),
            DEFAULT_MODE
        );
        properties.insert(INFERRED_LIMIT_PROPERTY.into(), "invalid".into());
        assert!(MetricsConfig::from_properties(&schema, &order, &properties).is_err());
        Ok(())
    }
}
