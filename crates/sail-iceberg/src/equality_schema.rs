use std::sync::Arc;

use datafusion_common::{DataFusionError, Result};

use crate::spec::{NestedFieldRef, Schema, StructType, Type};

pub(crate) fn equality_field_path(
    fields: &[NestedFieldRef],
    id: i32,
) -> Option<Vec<NestedFieldRef>> {
    for field in fields {
        if field.id == id {
            return Some(vec![field.clone()]);
        }
        if let Type::Struct(children) = field.field_type.as_ref()
            && let Some(mut path) = equality_field_path(children.fields(), id)
        {
            path.insert(0, field.clone());
            return Some(path);
        }
    }
    None
}

/// Restore equality keys by ID without exposing dropped columns in the query schema.
pub(crate) fn equality_read_schema(
    current: &Schema,
    history: &[Schema],
    ids: impl IntoIterator<Item = i32>,
) -> Result<Schema> {
    fn restore_path(fields: &mut Vec<NestedFieldRef>, path: &[NestedFieldRef]) {
        let Some((source, children)) = path.split_first() else {
            return;
        };
        let index = fields.iter().position(|field| field.id == source.id);
        let mut field = index.map(|i| fields[i].as_ref()).unwrap_or(source).clone();
        if !children.is_empty() {
            let mut nested = if index.is_some() {
                match field.field_type.as_ref() {
                    Type::Struct(fields) => fields.fields().to_vec(),
                    _ => vec![],
                }
            } else {
                vec![]
            };
            restore_path(&mut nested, children);
            field.field_type = Box::new(Type::Struct(StructType::new(nested)));
        }
        match index {
            Some(i) => fields[i] = Arc::new(field),
            None => {
                // A newly added column may reuse a dropped column's name with a different ID.
                if fields.iter().any(|existing| existing.name == field.name) {
                    field.name = format!("__sail_iceberg_equality_{}", field.id);
                    while fields.iter().any(|existing| existing.name == field.name) {
                        field.name.push('_');
                    }
                }
                fields.push(Arc::new(field));
            }
        }
    }

    let mut fields = current.fields().to_vec();
    for id in ids {
        if current.field_by_id(id).is_some() {
            continue;
        }
        let path = history
            .iter()
            .rev()
            .find_map(|schema| equality_field_path(schema.fields(), id))
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Cannot resolve equality-delete field ID {id} in table schema history"
                ))
            })?;
        restore_path(&mut fields, &path);
    }
    Schema::builder()
        .with_schema_id(current.schema_id())
        .with_fields(fields)
        .build()
        .map_err(DataFusionError::Plan)
}
