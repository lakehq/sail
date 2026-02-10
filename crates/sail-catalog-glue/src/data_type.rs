use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use aws_smithy_types::Document;
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

/// Converts an Arrow DataType to an Iceberg type representation as Document.
pub fn arrow_to_iceberg_type(data_type: &DataType) -> CatalogResult<Document> {
    match data_type {
        DataType::Null => Ok(Document::String("void".to_string())),
        DataType::Boolean => Ok(Document::String("boolean".to_string())),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16 => {
            Ok(Document::String("int".to_string()))
        }
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => {
            Ok(Document::String("long".to_string()))
        }
        DataType::Float16 | DataType::Float32 => Ok(Document::String("float".to_string())),
        DataType::Float64 => Ok(Document::String("double".to_string())),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            Ok(Document::String(format!("decimal({precision},{scale})")))
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Ok(Document::String("string".to_string()))
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            Ok(Document::String("binary".to_string()))
        }
        DataType::FixedSizeBinary(16) => Ok(Document::String("uuid".to_string())),
        DataType::FixedSizeBinary(size) => Ok(Document::String(format!("fixed[{size}]"))),
        DataType::Date32 | DataType::Date64 => Ok(Document::String("date".to_string())),
        DataType::Timestamp(_, None) => Ok(Document::String("timestamp".to_string())),
        DataType::Timestamp(_, Some(_)) => Ok(Document::String("timestamptz".to_string())),
        DataType::Time32(_) | DataType::Time64(_) => Ok(Document::String("time".to_string())),
        DataType::Duration(_) | DataType::Interval(_) => Err(CatalogError::NotSupported(
            "Duration and Interval types are not supported by Iceberg".to_string(),
        )),
        DataType::List(field)
        | DataType::FixedSizeList(field, _)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => {
            let element_type = arrow_to_iceberg_type(field.data_type())?;
            Ok(Document::Object(
                vec![
                    ("type".to_string(), Document::String("list".to_string())),
                    (
                        "element-id".to_string(),
                        Document::Number(aws_smithy_types::Number::PosInt(1)),
                    ),
                    ("element".to_string(), element_type),
                    (
                        "element-required".to_string(),
                        Document::Bool(!field.is_nullable()),
                    ),
                ]
                .into_iter()
                .collect(),
            ))
        }
        DataType::Struct(fields) => {
            let iceberg_fields: CatalogResult<Vec<Document>> = fields
                .iter()
                .enumerate()
                .map(|(idx, f)| {
                    let field_type = arrow_to_iceberg_type(f.data_type())?;
                    Ok(Document::Object(
                        vec![
                            (
                                "id".to_string(),
                                Document::Number(aws_smithy_types::Number::PosInt(idx as u64 + 1)),
                            ),
                            ("name".to_string(), Document::String(f.name().to_string())),
                            ("required".to_string(), Document::Bool(!f.is_nullable())),
                            ("type".to_string(), field_type),
                        ]
                        .into_iter()
                        .collect(),
                    ))
                })
                .collect();
            Ok(Document::Object(
                vec![
                    ("type".to_string(), Document::String("struct".to_string())),
                    ("fields".to_string(), Document::Array(iceberg_fields?)),
                ]
                .into_iter()
                .collect(),
            ))
        }
        DataType::Map(field, _) => {
            if let DataType::Struct(fields) = field.data_type() {
                if fields.len() == 2 {
                    let key_type = arrow_to_iceberg_type(fields[0].data_type())?;
                    let value_type = arrow_to_iceberg_type(fields[1].data_type())?;
                    return Ok(Document::Object(
                        vec![
                            ("type".to_string(), Document::String("map".to_string())),
                            (
                                "key-id".to_string(),
                                Document::Number(aws_smithy_types::Number::PosInt(1)),
                            ),
                            ("key".to_string(), key_type),
                            (
                                "value-id".to_string(),
                                Document::Number(aws_smithy_types::Number::PosInt(2)),
                            ),
                            ("value".to_string(), value_type),
                            (
                                "value-required".to_string(),
                                Document::Bool(!fields[1].is_nullable()),
                            ),
                        ]
                        .into_iter()
                        .collect(),
                    ));
                }
            }
            Err(CatalogError::InvalidArgument(
                "Map type must have key and value fields".to_string(),
            ))
        }
        DataType::Union(_, _) => Err(CatalogError::NotSupported(
            "Union types are not supported by Iceberg".to_string(),
        )),
        DataType::Dictionary(_, value_type) => arrow_to_iceberg_type(value_type),
        DataType::RunEndEncoded(_, _) => Err(CatalogError::NotSupported(format!(
            "Data type {data_type:?} is not supported by Iceberg"
        ))),
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
        .ok_or_else(|| {
            CatalogError::InvalidArgument(format!("Invalid decimal type: {type_str}"))
        })?;

    let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
    match parts.as_slice() {
        [precision] => {
            let p: u8 = precision.parse().map_err(|_| {
                CatalogError::InvalidArgument(format!("Invalid precision: {precision}"))
            })?;
            Ok(DataType::Decimal128(p, 0))
        }
        [precision, scale] => {
            let p: u8 = precision.parse().map_err(|_| {
                CatalogError::InvalidArgument(format!("Invalid precision: {precision}"))
            })?;
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

    /// Tests round-trip conversion for all simple Arrow types.
    ///
    /// - Boolean, Int8, Int16, Int32, Int64, Float32, Float64
    /// - Utf8, Binary, Date32, Timestamp, Decimal128, Null
    /// - Verifies arrow → glue → arrow produces identical types
    #[test]
    fn test_roundtrip_simple_types() {
        let types = vec![
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::Binary,
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Decimal128(18, 5),
            DataType::Null,
        ];

        for original in types {
            let glue_str = arrow_to_glue_type(&original).unwrap();
            let roundtrip = glue_type_to_arrow(&glue_str).unwrap();
            assert_eq!(
                original, roundtrip,
                "Round-trip failed for {original:?} -> {glue_str} -> {roundtrip:?}"
            );
        }
    }

    /// Tests round-trip conversion for complex Arrow types.
    ///
    /// - List type with Int64 elements
    /// - Struct type with id and name fields
    /// - Map type with string keys and int values
    /// - Deeply nested: List<Struct<id, List<tags>>>
    #[test]
    fn test_roundtrip_complex_types() {
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let glue_str = arrow_to_glue_type(&list_type).unwrap();
        let roundtrip = glue_type_to_arrow(&glue_str).unwrap();
        if let (DataType::List(orig), DataType::List(rt)) = (&list_type, &roundtrip) {
            assert_eq!(orig.data_type(), rt.data_type());
        }

        let struct_type = DataType::Struct(Fields::from(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let glue_str = arrow_to_glue_type(&struct_type).unwrap();
        let roundtrip = glue_type_to_arrow(&glue_str).unwrap();
        assert_eq!(struct_type, roundtrip);

        let map_field = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
            ])),
            false,
        );
        let map_type = DataType::Map(Arc::new(map_field), false);
        let glue_str = arrow_to_glue_type(&map_type).unwrap();
        let roundtrip = glue_type_to_arrow(&glue_str).unwrap();
        if let (DataType::Map(orig, _), DataType::Map(rt, _)) = (&map_type, &roundtrip) {
            if let (DataType::Struct(of), DataType::Struct(rf)) = (orig.data_type(), rt.data_type())
            {
                assert_eq!(of[0].data_type(), rf[0].data_type());
                assert_eq!(of[1].data_type(), rf[1].data_type());
            }
        }

        let nested = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Field::new("id", DataType::Int64, true),
                Field::new(
                    "tags",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                ),
            ])),
            true,
        )));
        let glue_str = arrow_to_glue_type(&nested).unwrap();
        assert_eq!(glue_str, "array<struct<id:bigint,tags:array<string>>>");
        let roundtrip = glue_type_to_arrow(&glue_str).unwrap();
        if let (DataType::List(orig), DataType::List(rt)) = (&nested, &roundtrip) {
            assert_eq!(orig.data_type(), rt.data_type());
        }
    }

    /// Tests parsing complex Glue type strings to Arrow types.
    ///
    /// - `array<int>` and nested `array<array<string>>`
    /// - `map<string,int>`
    /// - `struct<name:string,age:int>` with field name verification
    /// - Deeply nested: `array<struct<field:map<string,array<int>>>>`
    #[test]
    fn test_glue_to_arrow_complex_types() {
        let result = glue_type_to_arrow("array<int>").unwrap();
        assert!(matches!(result, DataType::List(_)));

        let result = glue_type_to_arrow("array<array<string>>").unwrap();
        if let DataType::List(outer) = result {
            assert!(matches!(outer.data_type(), DataType::List(_)));
        }

        let result = glue_type_to_arrow("map<string,int>").unwrap();
        assert!(matches!(result, DataType::Map(_, _)));

        let result = glue_type_to_arrow("struct<name:string,age:int>").unwrap();
        if let DataType::Struct(fields) = result {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name(), "name");
            assert_eq!(fields[1].name(), "age");
        }

        let result = glue_type_to_arrow("array<struct<field:map<string,array<int>>>>").unwrap();
        assert!(matches!(result, DataType::List(_)));
    }

    /// Tests edge cases in Glue type string parsing.
    ///
    /// - Case insensitivity: INT, Int
    /// - Whitespace handling: "  int  "
    /// - Type aliases: integer/int, bool/boolean, long/bigint, short/smallint, byte/tinyint, real/float, varchar/char/string, void/null
    /// - Parameterized char/varchar: char(10), varchar(255)
    /// - Decimal with single param: decimal(10) defaults scale to 0
    #[test]
    fn test_glue_to_arrow_edge_cases() {
        assert_eq!(glue_type_to_arrow("INT").unwrap(), DataType::Int32);
        assert_eq!(glue_type_to_arrow("  int  ").unwrap(), DataType::Int32);
        assert_eq!(glue_type_to_arrow("integer").unwrap(), DataType::Int32);
        assert_eq!(glue_type_to_arrow("bool").unwrap(), DataType::Boolean);
        assert_eq!(glue_type_to_arrow("long").unwrap(), DataType::Int64);
        assert_eq!(glue_type_to_arrow("short").unwrap(), DataType::Int16);
        assert_eq!(glue_type_to_arrow("byte").unwrap(), DataType::Int8);
        assert_eq!(glue_type_to_arrow("real").unwrap(), DataType::Float32);
        assert_eq!(glue_type_to_arrow("varchar").unwrap(), DataType::Utf8);
        assert_eq!(glue_type_to_arrow("char(10)").unwrap(), DataType::Utf8);
        assert_eq!(glue_type_to_arrow("varchar(255)").unwrap(), DataType::Utf8);
        assert_eq!(glue_type_to_arrow("void").unwrap(), DataType::Null);
        assert_eq!(
            glue_type_to_arrow("decimal(10)").unwrap(),
            DataType::Decimal128(10, 0)
        );
    }

    /// Tests error handling for malformed Glue type strings.
    ///
    /// - Unknown type: "foobar"
    /// - Malformed array: "array<int", "array<>"
    /// - Malformed struct: "struct<name>", "struct<name:string"
    /// - Malformed decimal: "decimal()", "decimal(abc,def)", "decimal(10,2"
    /// - Malformed map: "map<int>", "map<string,int"
    #[test]
    fn test_glue_to_arrow_errors() {
        assert!(glue_type_to_arrow("foobar").is_err());
        assert!(glue_type_to_arrow("array<int").is_err());
        assert!(glue_type_to_arrow("array<>").is_err());
        assert!(glue_type_to_arrow("struct<name>").is_err());
        assert!(glue_type_to_arrow("struct<name:string").is_err());
        assert!(glue_type_to_arrow("decimal()").is_err());
        assert!(glue_type_to_arrow("decimal(abc,def)").is_err());
        assert!(glue_type_to_arrow("decimal(10,2").is_err());
        assert!(glue_type_to_arrow("map<int>").is_err());
        assert!(glue_type_to_arrow("map<string,int").is_err());
    }

    /// Tests error handling for unsupported Arrow types.
    ///
    /// - Union type is not supported by Glue and returns an error
    #[test]
    fn test_arrow_to_glue_errors() {
        let union_type = DataType::Union(
            arrow::datatypes::UnionFields::empty(),
            arrow::datatypes::UnionMode::Sparse,
        );
        assert!(arrow_to_glue_type(&union_type).is_err());
    }

    /// Tests Arrow to Iceberg type conversion for primitive types.
    #[test]
    fn test_arrow_to_iceberg_primitive_types() {
        assert!(matches!(
            arrow_to_iceberg_type(&DataType::Boolean).unwrap(),
            Document::String(s) if s == "boolean"
        ));
        assert!(matches!(
            arrow_to_iceberg_type(&DataType::Int32).unwrap(),
            Document::String(s) if s == "int"
        ));
        assert!(matches!(
            arrow_to_iceberg_type(&DataType::Int64).unwrap(),
            Document::String(s) if s == "long"
        ));
        assert!(matches!(
            arrow_to_iceberg_type(&DataType::Float32).unwrap(),
            Document::String(s) if s == "float"
        ));
        assert!(matches!(
            arrow_to_iceberg_type(&DataType::Float64).unwrap(),
            Document::String(s) if s == "double"
        ));
        assert!(matches!(
            arrow_to_iceberg_type(&DataType::Utf8).unwrap(),
            Document::String(s) if s == "string"
        ));
        assert!(matches!(
            arrow_to_iceberg_type(&DataType::Binary).unwrap(),
            Document::String(s) if s == "binary"
        ));
        assert!(matches!(
            arrow_to_iceberg_type(&DataType::Date32).unwrap(),
            Document::String(s) if s == "date"
        ));
        assert!(matches!(
            arrow_to_iceberg_type(&DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap(),
            Document::String(s) if s == "timestamp"
        ));
        assert!(matches!(
            arrow_to_iceberg_type(&DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))).unwrap(),
            Document::String(s) if s == "timestamptz"
        ));
        assert!(matches!(
            arrow_to_iceberg_type(&DataType::Decimal128(10, 2)).unwrap(),
            Document::String(s) if s == "decimal(10,2)"
        ));
        assert!(matches!(
            arrow_to_iceberg_type(&DataType::FixedSizeBinary(16)).unwrap(),
            Document::String(s) if s == "uuid"
        ));
    }

    /// Tests Arrow to Iceberg type conversion for complex types.
    #[test]
    fn test_arrow_to_iceberg_complex_types() {
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let result = arrow_to_iceberg_type(&list_type).unwrap();
        assert!(matches!(result, Document::Object(_)));

        let struct_type = DataType::Struct(Fields::from(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let result = arrow_to_iceberg_type(&struct_type).unwrap();
        assert!(matches!(result, Document::Object(_)));

        let map_field = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
            ])),
            false,
        );
        let map_type = DataType::Map(Arc::new(map_field), false);
        let result = arrow_to_iceberg_type(&map_type).unwrap();
        assert!(matches!(result, Document::Object(_)));
    }

    /// Tests Arrow to Iceberg type conversion errors for unsupported types.
    #[test]
    fn test_arrow_to_iceberg_errors() {
        let union_type = DataType::Union(
            arrow::datatypes::UnionFields::empty(),
            arrow::datatypes::UnionMode::Sparse,
        );
        assert!(arrow_to_iceberg_type(&union_type).is_err());
    }
}
