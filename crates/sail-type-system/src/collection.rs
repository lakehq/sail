//! Collection type policies shared by query construction and kernel typing.
use std::sync::Arc;

use arrow_schema::{DataType, FieldRef};
use datafusion_expr::type_coercion::binary::comparison_coercion;

pub fn ansi_map_common_type(left: &DataType, right: &DataType) -> Option<DataType> {
    if left == right || right.is_null() {
        return Some(left.clone());
    }
    if left.is_null() {
        return Some(right.clone());
    }
    if left.is_string() != right.is_string() {
        let atomic = if left.is_string() { right } else { left };
        return if atomic.is_integer() {
            Some(DataType::Int64)
        } else if atomic.is_numeric() {
            Some(DataType::Float64)
        } else if atomic.is_nested()
            || matches!(atomic, DataType::Interval(_) | DataType::Duration(_))
        {
            None
        } else {
            Some(atomic.clone())
        };
    }
    if left.is_numeric() && right.is_numeric() && (left.is_floating() || right.is_floating()) {
        return Some(DataType::Float64);
    }
    comparison_coercion(left, right)
}

pub fn map_value_contains_null(data_type: &DataType) -> bool {
    let DataType::Map(entries, _) = data_type else {
        return true;
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return true;
    };
    fields
        .get(1)
        .map(|field| field.is_nullable())
        .unwrap_or(true)
}

pub fn map_entries_value_contains_null(data_type: &DataType) -> bool {
    let (DataType::List(field) | DataType::LargeList(field)) = data_type else {
        return true;
    };
    let DataType::Struct(fields) = field.data_type() else {
        return true;
    };
    fields
        .get(1)
        .map(|field| field.is_nullable())
        .unwrap_or(true)
}

pub fn list_nullability(data_type: &DataType, nullable: bool) -> DataType {
    match data_type {
        DataType::List(field) => DataType::List(with_nullable(field, nullable)),
        DataType::LargeList(field) => DataType::LargeList(with_nullable(field, nullable)),
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(with_nullable(field, nullable), *size)
        }
        _ => data_type.clone(),
    }
}

pub fn map_entries_nullability(data_type: &DataType, nullable: bool) -> DataType {
    match data_type {
        DataType::List(field) => DataType::List(entry_nullability(field, nullable)),
        DataType::LargeList(field) => DataType::LargeList(entry_nullability(field, nullable)),
        _ => data_type.clone(),
    }
}

pub fn map_nullability(data_type: &DataType, nullable: bool) -> DataType {
    match data_type {
        DataType::Map(field, sorted) => DataType::Map(entry_nullability(field, nullable), *sorted),
        _ => data_type.clone(),
    }
}

fn entry_nullability(field: &FieldRef, nullable: bool) -> FieldRef {
    let DataType::Struct(fields) = field.data_type() else {
        return field.clone();
    };
    if fields.len() != 2 || fields[1].is_nullable() == nullable {
        return field.clone();
    }
    Arc::new(field.as_ref().clone().with_data_type(DataType::Struct(
        vec![fields[0].clone(), with_nullable(&fields[1], nullable)].into(),
    )))
}

fn with_nullable(field: &FieldRef, nullable: bool) -> FieldRef {
    if field.is_nullable() == nullable {
        field.clone()
    } else {
        Arc::new(field.as_ref().clone().with_nullable(nullable))
    }
}

pub fn nullable_array_type(data_type: &DataType) -> DataType {
    fn field(field: &FieldRef) -> FieldRef {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_nullable(true)
                .with_data_type(nullable_array_type(field.data_type())),
        )
    }
    match data_type {
        DataType::List(value) => DataType::List(field(value)),
        DataType::LargeList(value) => DataType::LargeList(field(value)),
        DataType::FixedSizeList(value, size) => DataType::FixedSizeList(field(value), *size),
        _ => data_type.clone(),
    }
}
