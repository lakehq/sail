use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use sail_catalog::error::{CatalogError, CatalogResult};

pub fn arrow_to_hive_type(data_type: &DataType) -> CatalogResult<String> {
    match data_type {
        DataType::Null => Ok("void".to_string()),
        DataType::Boolean => Ok("boolean".to_string()),
        DataType::Int8 => Ok("tinyint".to_string()),
        DataType::Int16 => Ok("smallint".to_string()),
        DataType::Int32 => Ok("int".to_string()),
        DataType::Int64 => Ok("bigint".to_string()),
        // Note: Hive has no unsigned integer types. Unsigned Arrow types map to
        // the equivalent signed Hive type. Values exceeding the signed range will
        // be misinterpreted (lossy but intentional mapping).
        DataType::UInt8 => Ok("tinyint".to_string()),
        DataType::UInt16 => Ok("smallint".to_string()),
        DataType::UInt32 => Ok("int".to_string()),
        DataType::UInt64 => Ok("bigint".to_string()),
        DataType::Float16 | DataType::Float32 => Ok("float".to_string()),
        DataType::Float64 => Ok("double".to_string()),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            let precision = *precision;
            let scale = *scale;
            if precision > 38 {
                return Err(CatalogError::InvalidArgument(format!(
                    "Hive Metastore supports decimal precision up to 38, got {precision}"
                )));
            }
            if scale < 0 || scale as u8 > precision {
                return Err(CatalogError::InvalidArgument(format!(
                    "Invalid decimal scale {scale} for precision {precision}"
                )));
            }
            Ok(format!("decimal({precision},{scale})"))
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok("string".to_string()),
        DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::BinaryView => Ok("binary".to_string()),
        // HMS DATE is days since epoch. Arrow Date32 is days, Date64 is milliseconds.
        // Both map to the same HMS type since HMS has no sub-day date precision.
        DataType::Date32 | DataType::Date64 => Ok("date".to_string()),
        DataType::Timestamp(_, _) => Ok("timestamp".to_string()),
        DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_) => Ok("string".to_string()),
        DataType::List(field)
        | DataType::FixedSizeList(field, _)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => {
            Ok(format!("array<{}>", arrow_to_hive_type(field.data_type())?))
        }
        DataType::Struct(fields) => {
            let fields: CatalogResult<Vec<String>> = fields
                .iter()
                .map(|field| {
                    Ok(format!(
                        "{}:{}",
                        field.name(),
                        arrow_to_hive_type(field.data_type())?
                    ))
                })
                .collect();
            Ok(format!("struct<{}>", fields?.join(",")))
        }
        DataType::Map(field, _) => {
            if let DataType::Struct(fields) = field.data_type() {
                if fields.len() == 2 {
                    return Ok(format!(
                        "map<{},{}>",
                        arrow_to_hive_type(fields[0].data_type())?,
                        arrow_to_hive_type(fields[1].data_type())?
                    ));
                }
            }
            Err(CatalogError::InvalidArgument(
                "Map type must have key and value fields".to_string(),
            ))
        }
        DataType::Dictionary(_, value_type) => arrow_to_hive_type(value_type),
        DataType::Union(_, _) | DataType::RunEndEncoded(_, _) => Err(CatalogError::NotSupported(
            format!("Data type {data_type:?} is not supported by Hive Metastore catalog"),
        )),
    }
}

pub fn hive_type_to_arrow(type_str: &str) -> CatalogResult<DataType> {
    let type_str = type_str.trim().to_lowercase();

    if type_str.starts_with("decimal") {
        return parse_decimal_type(&type_str);
    }
    if type_str.starts_with("array<") {
        return parse_array_type(&type_str);
    }
    if type_str.starts_with("map<") {
        return parse_map_type(&type_str);
    }
    if type_str.starts_with("struct<") {
        return parse_struct_type(&type_str);
    }
    if type_str.starts_with("char(") || type_str.starts_with("varchar(") {
        return Ok(DataType::Utf8);
    }

    match type_str.as_str() {
        "void" | "null" => Ok(DataType::Null),
        "boolean" | "bool" => Ok(DataType::Boolean),
        "tinyint" | "byte" => Ok(DataType::Int8),
        "smallint" | "short" => Ok(DataType::Int16),
        "int" | "integer" => Ok(DataType::Int32),
        "bigint" | "long" => Ok(DataType::Int64),
        "float" | "real" => Ok(DataType::Float32),
        "double" => Ok(DataType::Float64),
        "string" | "varchar" | "char" => Ok(DataType::Utf8),
        "binary" => Ok(DataType::Binary),
        "date" => Ok(DataType::Date32),
        "timestamp" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        other => Err(CatalogError::InvalidArgument(format!(
            "Unknown Hive type: {other}"
        ))),
    }
}

fn parse_decimal_type(type_str: &str) -> CatalogResult<DataType> {
    if type_str == "decimal" {
        return Ok(DataType::Decimal128(38, 18));
    }

    let inner = type_str
        .strip_prefix("decimal(")
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| {
            CatalogError::InvalidArgument(format!("Invalid decimal type: {type_str}"))
        })?;

    let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
    match parts.as_slice() {
        [precision] => Ok(DataType::Decimal128(
            precision.parse().map_err(|_| {
                CatalogError::InvalidArgument(format!("Invalid decimal precision: {precision}"))
            })?,
            0,
        )),
        [precision, scale] => Ok(DataType::Decimal128(
            precision.parse().map_err(|_| {
                CatalogError::InvalidArgument(format!("Invalid decimal precision: {precision}"))
            })?,
            scale.parse().map_err(|_| {
                CatalogError::InvalidArgument(format!("Invalid decimal scale: {scale}"))
            })?,
        )),
        _ => Err(CatalogError::InvalidArgument(format!(
            "Invalid decimal type: {type_str}"
        ))),
    }
}

fn parse_array_type(type_str: &str) -> CatalogResult<DataType> {
    let inner = type_str
        .strip_prefix("array<")
        .and_then(|s| s.strip_suffix('>'))
        .ok_or_else(|| CatalogError::InvalidArgument(format!("Invalid array type: {type_str}")))?;
    Ok(DataType::List(Arc::new(Field::new_list_field(
        hive_type_to_arrow(inner)?,
        true,
    ))))
}

fn parse_map_type(type_str: &str) -> CatalogResult<DataType> {
    let inner = type_str
        .strip_prefix("map<")
        .and_then(|s| s.strip_suffix('>'))
        .ok_or_else(|| CatalogError::InvalidArgument(format!("Invalid map type: {type_str}")))?;
    let (key_type, value_type) = split_top_level_two(inner)?;
    Ok(DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", hive_type_to_arrow(key_type)?, false),
                Field::new("values", hive_type_to_arrow(value_type)?, true),
            ])),
            false,
        )),
        false,
    ))
}

fn parse_struct_type(type_str: &str) -> CatalogResult<DataType> {
    let inner = type_str
        .strip_prefix("struct<")
        .and_then(|s| s.strip_suffix('>'))
        .ok_or_else(|| CatalogError::InvalidArgument(format!("Invalid struct type: {type_str}")))?;

    let mut fields = Vec::new();
    for field_str in split_top_level(inner) {
        let (name, field_type) = field_str.split_once(':').ok_or_else(|| {
            CatalogError::InvalidArgument(format!("Invalid struct field: {field_str}"))
        })?;
        fields.push(Field::new(name, hive_type_to_arrow(field_type)?, true));
    }
    Ok(DataType::Struct(Fields::from(fields)))
}

fn split_top_level_two(input: &str) -> CatalogResult<(&str, &str)> {
    let parts = split_top_level(input);
    if parts.len() != 2 {
        return Err(CatalogError::InvalidArgument(format!(
            "Expected exactly two type parameters: {input}"
        )));
    }
    Ok((parts[0], parts[1]))
}

fn split_top_level(input: &str) -> Vec<&str> {
    let mut depth = 0;
    let mut start = 0;
    let mut parts = Vec::new();

    for (idx, ch) in input.char_indices() {
        match ch {
            '<' => depth += 1,
            '>' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(input[start..idx].trim());
                start = idx + 1;
            }
            _ => {}
        }
    }

    if start < input.len() {
        parts.push(input[start..].trim());
    }

    parts
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use arrow::datatypes::{DataType, Field, Fields};

    use super::{arrow_to_hive_type, hive_type_to_arrow};

    #[test]
    fn test_arrow_to_hive_struct() {
        let data_type = DataType::Struct(Fields::from(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        assert_eq!(
            arrow_to_hive_type(&data_type).unwrap(),
            "struct<id:bigint,name:string>"
        );
    }

    #[test]
    fn test_hive_to_arrow_nested_types() {
        let data_type = hive_type_to_arrow("array<struct<id:int,name:string>>").unwrap();
        assert!(matches!(data_type, DataType::List(_)));
    }
}
