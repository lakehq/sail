use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use sail_catalog::error::{CatalogError, CatalogResult};

/// Converts an Arrow DataType to a Glue/Hive type string.
/// See: https://docs.aws.amazon.com/glue/latest/dg/glue-types.html
pub fn arrow_to_glue_type(data_type: &DataType) -> CatalogResult<String> {
    match data_type {
        DataType::Null => Ok("void".to_string()),
        DataType::Boolean => Ok("boolean".to_string()),
        DataType::Int8 => Ok("tinyint".to_string()),
        DataType::Int16 => Ok("smallint".to_string()),
        DataType::Int32 => Ok("int".to_string()),
        DataType::Int64 => Ok("bigint".to_string()),
        DataType::UInt8 => Ok("tinyint".to_string()),
        DataType::UInt16 => Ok("smallint".to_string()),
        DataType::UInt32 => Ok("int".to_string()),
        DataType::UInt64 => Ok("bigint".to_string()),
        DataType::Float16 => Ok("float".to_string()),
        DataType::Float32 => Ok("float".to_string()),
        DataType::Float64 => Ok("double".to_string()),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            Ok(format!("decimal({precision},{scale})"))
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok("string".to_string()),
        DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::BinaryView => Ok("binary".to_string()),
        DataType::Date32 | DataType::Date64 => Ok("date".to_string()),
        DataType::Timestamp(_, _) => Ok("timestamp".to_string()),
        DataType::Time32(_) | DataType::Time64(_) => Ok("string".to_string()),
        DataType::Duration(_) | DataType::Interval(_) => Ok("string".to_string()),
        DataType::List(field)
        | DataType::FixedSizeList(field, _)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => {
            Ok(format!("array<{}>", arrow_to_glue_type(field.data_type())?))
        }
        DataType::Struct(fields) => {
            let field_strs: CatalogResult<Vec<String>> = fields
                .iter()
                .map(|f| {
                    let type_str = arrow_to_glue_type(f.data_type())?;
                    Ok(format!("{}:{}", f.name(), type_str))
                })
                .collect();
            Ok(format!("struct<{}>", field_strs?.join(",")))
        }
        DataType::Map(field, _) => {
            if let DataType::Struct(fields) = field.data_type() {
                if fields.len() == 2 {
                    let key_type = arrow_to_glue_type(fields[0].data_type())?;
                    let value_type = arrow_to_glue_type(fields[1].data_type())?;
                    return Ok(format!("map<{key_type},{value_type}>"));
                }
            }
            Err(CatalogError::InvalidArgument(
                "Map type must have key and value fields".to_string(),
            ))
        }
        DataType::Union(_, _) => Err(CatalogError::NotSupported(
            "Union types are not supported by Glue".to_string(),
        )),
        DataType::Dictionary(_, value_type) => arrow_to_glue_type(value_type),
        DataType::RunEndEncoded(_, _) | DataType::Decimal32(_, _) | DataType::Decimal64(_, _) => {
            Err(CatalogError::NotSupported(format!(
                "Data type {data_type:?} is not supported by Glue"
            )))
        }
    }
}

/// Converts a Glue/Hive type string to an Arrow DataType.
pub fn glue_type_to_arrow(type_str: &str) -> CatalogResult<DataType> {
    let type_str = type_str.trim().to_lowercase();

    // Handle parameterized types
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

    // Simple types
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
        _ => Err(CatalogError::InvalidArgument(format!(
            "Unknown Glue type: {type_str}"
        ))),
    }
}

fn parse_decimal_type(type_str: &str) -> CatalogResult<DataType> {
    // decimal(precision,scale) or decimal(precision) or just decimal
    if type_str == "decimal" {
        return Ok(DataType::Decimal128(38, 18));
    }

    let inner = type_str
        .strip_prefix("decimal(")
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| CatalogError::InvalidArgument(format!("Invalid decimal type: {type_str}")))?;

    let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
    match parts.as_slice() {
        [precision] => {
            let p: u8 = precision
                .parse()
                .map_err(|_| CatalogError::InvalidArgument(format!("Invalid precision: {precision}")))?;
            Ok(DataType::Decimal128(p, 0))
        }
        [precision, scale] => {
            let p: u8 = precision
                .parse()
                .map_err(|_| CatalogError::InvalidArgument(format!("Invalid precision: {precision}")))?;
            let s: i8 = scale
                .parse()
                .map_err(|_| CatalogError::InvalidArgument(format!("Invalid scale: {scale}")))?;
            Ok(DataType::Decimal128(p, s))
        }
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

    let element_type = glue_type_to_arrow(inner)?;
    Ok(DataType::List(Arc::new(Field::new(
        "item",
        element_type,
        true,
    ))))
}

fn parse_map_type(type_str: &str) -> CatalogResult<DataType> {
    let inner = type_str
        .strip_prefix("map<")
        .and_then(|s| s.strip_suffix('>'))
        .ok_or_else(|| CatalogError::InvalidArgument(format!("Invalid map type: {type_str}")))?;

    // Find the comma that separates key and value types (handling nested types)
    let split_pos = find_type_separator(inner)?;
    let key_str = &inner[..split_pos];
    let value_str = &inner[split_pos + 1..];

    let key_type = glue_type_to_arrow(key_str.trim())?;
    let value_type = glue_type_to_arrow(value_str.trim())?;

    let struct_field = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", key_type, false),
            Field::new("value", value_type, true),
        ])),
        false,
    );

    Ok(DataType::Map(Arc::new(struct_field), false))
}

fn parse_struct_type(type_str: &str) -> CatalogResult<DataType> {
    let inner = type_str
        .strip_prefix("struct<")
        .and_then(|s| s.strip_suffix('>'))
        .ok_or_else(|| CatalogError::InvalidArgument(format!("Invalid struct type: {type_str}")))?;

    if inner.is_empty() {
        return Ok(DataType::Struct(Fields::empty()));
    }

    let mut fields = Vec::new();
    let mut current_start = 0;
    let mut depth = 0;

    for (i, c) in inner.char_indices() {
        match c {
            '<' => depth += 1,
            '>' => depth -= 1,
            ',' if depth == 0 => {
                let field_str = &inner[current_start..i];
                fields.push(parse_struct_field(field_str.trim())?);
                current_start = i + 1;
            }
            _ => {}
        }
    }

    // Don't forget the last field
    let field_str = &inner[current_start..];
    if !field_str.trim().is_empty() {
        fields.push(parse_struct_field(field_str.trim())?);
    }

    Ok(DataType::Struct(Fields::from(fields)))
}

fn parse_struct_field(field_str: &str) -> CatalogResult<Field> {
    // Field format: name:type
    let colon_pos = field_str.find(':').ok_or_else(|| {
        CatalogError::InvalidArgument(format!("Invalid struct field (missing colon): {field_str}"))
    })?;

    let name = field_str[..colon_pos].trim();
    let type_str = field_str[colon_pos + 1..].trim();

    let data_type = glue_type_to_arrow(type_str)?;
    Ok(Field::new(name, data_type, true))
}

fn find_type_separator(s: &str) -> CatalogResult<usize> {
    let mut depth = 0;
    for (i, c) in s.char_indices() {
        match c {
            '<' => depth += 1,
            '>' => depth -= 1,
            ',' if depth == 0 => return Ok(i),
            _ => {}
        }
    }
    Err(CatalogError::InvalidArgument(format!(
        "Could not find type separator in: {s}"
    )))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_types() {
        assert_eq!(arrow_to_glue_type(&DataType::Boolean).unwrap(), "boolean");
        assert_eq!(arrow_to_glue_type(&DataType::Int32).unwrap(), "int");
        assert_eq!(arrow_to_glue_type(&DataType::Int64).unwrap(), "bigint");
        assert_eq!(arrow_to_glue_type(&DataType::Float64).unwrap(), "double");
        assert_eq!(arrow_to_glue_type(&DataType::Utf8).unwrap(), "string");
        assert_eq!(arrow_to_glue_type(&DataType::Binary).unwrap(), "binary");
        assert_eq!(arrow_to_glue_type(&DataType::Date32).unwrap(), "date");
    }

    #[test]
    fn test_decimal_type() {
        assert_eq!(
            arrow_to_glue_type(&DataType::Decimal128(10, 2)).unwrap(),
            "decimal(10,2)"
        );
    }

    #[test]
    fn test_array_type() {
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        assert_eq!(arrow_to_glue_type(&list_type).unwrap(), "array<int>");
    }

    #[test]
    fn test_struct_type() {
        let struct_type = DataType::Struct(Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
        ]));
        assert_eq!(
            arrow_to_glue_type(&struct_type).unwrap(),
            "struct<name:string,age:int>"
        );
    }

    #[test]
    fn test_glue_to_arrow_simple() {
        assert_eq!(glue_type_to_arrow("boolean").unwrap(), DataType::Boolean);
        assert_eq!(glue_type_to_arrow("int").unwrap(), DataType::Int32);
        assert_eq!(glue_type_to_arrow("bigint").unwrap(), DataType::Int64);
        assert_eq!(glue_type_to_arrow("double").unwrap(), DataType::Float64);
        assert_eq!(glue_type_to_arrow("string").unwrap(), DataType::Utf8);
    }

    #[test]
    fn test_glue_to_arrow_decimal() {
        assert_eq!(
            glue_type_to_arrow("decimal(10,2)").unwrap(),
            DataType::Decimal128(10, 2)
        );
    }
}
