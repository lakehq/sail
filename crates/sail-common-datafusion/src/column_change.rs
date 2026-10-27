use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema};
use datafusion_common::{Result, plan_datafusion_err, plan_err};
use sail_common::spec::{ColumnChange, ColumnPosition};

/// Apply changes to a copy, preserving field metadata and collection element identities.
pub fn alter_schema(
    schema: &Schema,
    changes: &[ColumnChange<DataType>],
    case_sensitive: bool,
) -> Result<Schema> {
    let mut fields = schema.fields().to_vec();
    for change in changes {
        alter_fields(&mut fields, change.path(), change, case_sensitive)?;
    }
    if fields.is_empty() {
        return plan_err!("Cannot drop all columns from a table");
    }
    validate_field_names(&fields, case_sensitive)?;
    Ok(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// Build the target schema for REPLACE COLUMNS without applying ADD COLUMN restrictions.
pub fn replacement_schema(columns: &[ColumnChange<DataType>]) -> Result<Schema> {
    let mut fields = Vec::new();
    for column in columns {
        let ColumnChange::Add {
            path,
            data_type,
            nullable,
            comment,
            default,
            position,
        } = column
        else {
            return plan_err!("REPLACE COLUMNS requires column definitions");
        };
        let [name] = path.as_slice() else {
            return plan_err!("REPLACE COLUMNS requires top-level column definitions");
        };
        if default.is_some() || position.is_some() {
            return plan_err!("REPLACE COLUMNS does not support DEFAULT or positional clauses");
        }
        let mut field = Field::new(name, data_type.clone(), *nullable);
        if let Some(comment) = comment {
            field
                .metadata_mut()
                .insert("comment".into(), comment.clone());
        }
        fields.push(Arc::new(field));
    }
    if fields.is_empty() {
        return plan_err!("Cannot replace a table schema with no columns");
    }
    validate_field_names(&fields, false)?;
    Ok(Schema::new(fields))
}

pub fn validate_field_names(fields: &[FieldRef], case_sensitive: bool) -> Result<()> {
    let mut names = std::collections::HashSet::new();
    for field in fields {
        let name = if case_sensitive {
            field.name().clone()
        } else {
            field.name().to_lowercase()
        };
        if !names.insert(name) {
            return plan_err!("Column '{}' already exists", field.name());
        }
        validate_type(field.data_type(), case_sensitive)?;
    }
    Ok(())
}

fn validate_type(ty: &DataType, case_sensitive: bool) -> Result<()> {
    match ty {
        DataType::Struct(fields) => validate_field_names(fields, case_sensitive),
        DataType::List(field) | DataType::LargeList(field) | DataType::Map(field, _) => {
            validate_type(field.data_type(), case_sensitive)
        }
        _ => Ok(()),
    }
}

fn names_match(left: &str, right: &str, case_sensitive: bool) -> bool {
    left == right || (!case_sensitive && left.eq_ignore_ascii_case(right))
}

fn index(fields: &[FieldRef], name: &str, case_sensitive: bool) -> Option<usize> {
    fields
        .iter()
        .position(|field| names_match(field.name(), name, case_sensitive))
}

fn move_field(
    fields: &mut Vec<FieldRef>,
    i: usize,
    position: &ColumnPosition,
    case_sensitive: bool,
) -> Result<()> {
    let target = match position {
        ColumnPosition::First => 0,
        ColumnPosition::After(name) => {
            let after = index(fields, name, case_sensitive)
                .ok_or_else(|| plan_datafusion_err!("Column '{name}' does not exist"))?;
            if after == i {
                return plan_err!("Cannot position a column after itself");
            }
            after + 1
        }
    };
    let field = fields.remove(i);
    fields.insert(if i < target { target - 1 } else { target }, field);
    Ok(())
}

fn alter_fields(
    fields: &mut Vec<FieldRef>,
    path: &[String],
    change: &ColumnChange<DataType>,
    case_sensitive: bool,
) -> Result<()> {
    let Some((name, rest)) = path.split_first() else {
        return plan_err!("Column name must not be empty");
    };
    let found = index(fields, name, case_sensitive);
    if found.is_none()
        && matches!(
            change,
            ColumnChange::Drop {
                if_exists: true,
                ..
            }
        )
    {
        return Ok(());
    }
    if rest.is_empty()
        && let ColumnChange::Add {
            data_type,
            nullable,
            comment,
            default,
            position,
            ..
        } = change
    {
        if found.is_some() {
            return plan_err!("Column '{name}' already exists");
        }
        if !nullable {
            return plan_err!("Cannot add a required column to an existing table");
        }
        if default.is_some() {
            return plan_err!(
                "Adding a column with a DEFAULT is not supported; add the column then set its default"
            );
        }
        let mut field = Field::new(name, data_type.clone(), *nullable);
        if let Some(comment) = comment {
            field
                .metadata_mut()
                .insert("comment".into(), comment.clone());
        }
        fields.push(Arc::new(field));
        if let Some(position) = position {
            move_field(fields, fields.len() - 1, position, case_sensitive)?;
        }
        return Ok(());
    }
    let i =
        found.ok_or_else(|| plan_datafusion_err!("Column '{}' does not exist", path.join(".")))?;
    if !rest.is_empty() {
        let ty = alter_nested(fields[i].data_type(), rest, change, case_sensitive)?;
        fields[i] = Arc::new(fields[i].as_ref().clone().with_data_type(ty));
        return Ok(());
    }
    match change {
        ColumnChange::Drop { .. } => {
            fields.remove(i);
        }
        ColumnChange::Rename { name, .. } => {
            if index(fields, name, case_sensitive).is_some_and(|other| other != i) {
                return plan_err!("Column '{name}' already exists");
            }
            fields[i] = Arc::new(fields[i].as_ref().clone().with_name(name));
        }
        ColumnChange::Comment { comment, .. } => {
            let field = Arc::make_mut(&mut fields[i]);
            // Iceberg also stores column documentation under "doc".
            field.metadata_mut().remove("doc");
            if let Some(comment) = comment {
                field
                    .metadata_mut()
                    .insert("comment".into(), comment.clone());
            } else {
                field.metadata_mut().remove("comment");
            }
        }
        ColumnChange::Nullability { nullable, .. } => {
            if !nullable && fields[i].is_nullable() {
                return plan_err!("Cannot tighten nullability of an existing column");
            }
            fields[i] = Arc::new(fields[i].as_ref().clone().with_nullable(*nullable));
        }
        ColumnChange::Position { position, .. } => move_field(fields, i, position, case_sensitive)?,
        ColumnChange::Add { .. } => unreachable!(),
    }
    if fields.is_empty() {
        return plan_err!("Cannot drop all fields from a struct");
    }
    Ok(())
}

fn alter_nested(
    ty: &DataType,
    path: &[String],
    change: &ColumnChange<DataType>,
    case_sensitive: bool,
) -> Result<DataType> {
    match ty {
        DataType::Struct(fields) => {
            let mut fields = fields.to_vec();
            alter_fields(&mut fields, path, change, case_sensitive)?;
            Ok(DataType::Struct(fields.into()))
        }
        DataType::List(element) | DataType::LargeList(element)
            if names_match(&path[0], "element", case_sensitive) =>
        {
            let is_large = matches!(ty, DataType::LargeList(_));
            let element = alter_collection_field(element, path, change, case_sensitive)?;
            Ok(if is_large {
                DataType::LargeList(element)
            } else {
                DataType::List(element)
            })
        }
        DataType::Map(entries, sorted) if names_match(&path[0], "value", case_sensitive) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return plan_err!("Invalid map entries");
            };
            let mut fields = fields.to_vec();
            fields[1] = alter_collection_field(&fields[1], path, change, case_sensitive)?;
            Ok(DataType::Map(
                Arc::new(
                    entries
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::Struct(Fields::from(fields))),
                ),
                *sorted,
            ))
        }
        _ => plan_err!(
            "Column path '{}' does not identify a struct field",
            path.join(".")
        ),
    }
}

fn alter_collection_field(
    field: &FieldRef,
    path: &[String],
    change: &ColumnChange<DataType>,
    case_sensitive: bool,
) -> Result<FieldRef> {
    if path.len() > 1 {
        let ty = alter_nested(field.data_type(), &path[1..], change, case_sensitive)?;
        return Ok(Arc::new(field.as_ref().clone().with_data_type(ty)));
    }
    if !matches!(
        change,
        ColumnChange::Comment { .. } | ColumnChange::Nullability { .. }
    ) {
        return plan_err!("Cannot add, drop, rename or reorder a collection element");
    }
    let mut fields = vec![Arc::new(field.as_ref().clone().with_name(&path[0]))];
    alter_fields(&mut fields, path, change, case_sensitive)?;
    Ok(Arc::new(fields[0].as_ref().clone().with_name(field.name())))
}
