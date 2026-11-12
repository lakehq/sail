use std::sync::Arc;

use chumsky::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Fields, TimeUnit};

use crate::error::{CatalogError, CatalogResult};

/// Catalog-specific defaults for the shared Hive type syntax.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HiveTypeDialect {
    Hive,
    Glue,
}

impl HiveTypeDialect {
    fn timestamp_timezone(self) -> Option<Arc<str>> {
        match self {
            Self::Hive => Some(Arc::from("UTC")),
            Self::Glue => None,
        }
    }

    fn map_key_name(self) -> &'static str {
        match self {
            Self::Hive => "keys",
            Self::Glue => "key",
        }
    }

    fn map_value_name(self) -> &'static str {
        match self {
            Self::Hive => "values",
            Self::Glue => "value",
        }
    }

    pub fn format_arrow_type(self, data_type: &DataType) -> CatalogResult<String> {
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
                if self == Self::Glue
                    && matches!(
                        data_type,
                        DataType::Decimal32(_, _) | DataType::Decimal64(_, _)
                    )
                {
                    return Err(CatalogError::NotSupported(format!(
                        "Data type {data_type:?} is not supported by Glue"
                    )));
                }
                let precision = *precision;
                let scale = *scale;
                if self == Self::Hive && precision > 38 {
                    return Err(CatalogError::InvalidArgument(format!(
                        "Hive Metastore supports decimal precision up to 38, got {precision}"
                    )));
                }
                if self == Self::Hive && (scale < 0 || scale as u8 > precision) {
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
            | DataType::LargeListView(field) => Ok(format!(
                "array<{}>",
                self.format_arrow_type(field.data_type())?
            )),
            DataType::Struct(fields) => {
                let fields: CatalogResult<Vec<String>> = fields
                    .iter()
                    .map(|field| {
                        Ok(format!(
                            "{}:{}",
                            field.name(),
                            self.format_arrow_type(field.data_type())?
                        ))
                    })
                    .collect();
                Ok(format!("struct<{}>", fields?.join(",")))
            }
            DataType::Map(field, _) => {
                if let DataType::Struct(fields) = field.data_type()
                    && fields.len() == 2
                {
                    return Ok(format!(
                        "map<{},{}>",
                        self.format_arrow_type(fields[0].data_type())?,
                        self.format_arrow_type(fields[1].data_type())?
                    ));
                }
                Err(CatalogError::InvalidArgument(
                    "Map type must have key and value fields".to_string(),
                ))
            }
            DataType::Dictionary(_, value_type) => self.format_arrow_type(value_type),
            DataType::Union(_, _) | DataType::RunEndEncoded(_, _) => {
                Err(CatalogError::NotSupported(format!(
                    "Data type {data_type:?} is not supported by Hive Metastore catalog"
                )))
            }
        }
    }

    pub fn parse_arrow_type(self, raw: &str) -> CatalogResult<DataType> {
        self.create_data_type_parser()
            .then_ignore(end())
            .parse(raw)
            .into_result()
            .map_err(|errors| {
                CatalogError::InvalidArgument(format!(
                    "Invalid Hive type: {}",
                    errors
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })
    }

    fn create_data_type_parser<'a>(
        self,
    ) -> impl Parser<'a, &'a str, DataType, extra::Err<Rich<'a, char>>> {
        recursive(move |data_type| {
            let symbol = |c| just(c).padded();
            let keyword = |name: &'static str| {
                text::ascii::ident()
                    .filter(move |s: &&str| s.eq_ignore_ascii_case(name))
                    .ignored()
                    .padded()
            };

            let unsigned = text::digits(10).to_slice();
            let precision = unsigned
                .try_map(|s: &str, span| {
                    s.parse::<u8>()
                        .map_err(|_| Rich::custom(span, "invalid decimal precision"))
                })
                .padded();
            let scale = one_of("+-")
                .or_not()
                .then(unsigned)
                .to_slice()
                .try_map(|s: &str, span| {
                    s.parse::<i8>()
                        .map_err(|_| Rich::custom(span, "invalid decimal scale"))
                })
                .padded();
            let decimal = keyword("decimal")
                .ignore_then(
                    precision
                        .then(symbol(',').ignore_then(scale).or_not())
                        .delimited_by(symbol('('), symbol(')'))
                        .or_not(),
                )
                .map(move |parameters| {
                    let (precision, scale) = match parameters {
                        Some((precision, scale)) => (precision, scale.unwrap_or(0)),
                        None => match self {
                            Self::Hive => (10, 0),
                            Self::Glue => (38, 18),
                        },
                    };
                    DataType::Decimal128(precision, scale)
                });

            let character = keyword("char")
                .or(keyword("varchar"))
                .then(
                    unsigned
                        .padded()
                        .delimited_by(symbol('('), symbol(')'))
                        .or_not(),
                )
                .to(DataType::Utf8);
            let primitive = text::ascii::ident().try_map(move |s: &str, span| {
                match s.to_ascii_lowercase().as_str() {
                    "void" | "null" => Ok(DataType::Null),
                    "boolean" | "bool" => Ok(DataType::Boolean),
                    "tinyint" | "byte" => Ok(DataType::Int8),
                    "smallint" | "short" => Ok(DataType::Int16),
                    "int" | "integer" => Ok(DataType::Int32),
                    "bigint" | "long" => Ok(DataType::Int64),
                    "float" | "real" => Ok(DataType::Float32),
                    "double" => Ok(DataType::Float64),
                    "string" => Ok(DataType::Utf8),
                    "binary" => Ok(DataType::Binary),
                    "date" => Ok(DataType::Date32),
                    "timestamp" => Ok(DataType::Timestamp(
                        TimeUnit::Microsecond,
                        self.timestamp_timezone(),
                    )),
                    "timestamp_ntz" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
                    _ => Err(Rich::custom(span, format!("unknown Hive type: {s}"))),
                }
            });

            let array = keyword("array")
                .ignore_then(data_type.clone().delimited_by(symbol('<'), symbol('>')))
                .map(|element| DataType::List(Arc::new(Field::new_list_field(element, true))));
            let map = keyword("map")
                .ignore_then(
                    data_type
                        .clone()
                        .then_ignore(symbol(','))
                        .then(data_type.clone())
                        .delimited_by(symbol('<'), symbol('>')),
                )
                .map(move |(key, value)| {
                    DataType::Map(
                        Arc::new(Field::new(
                            "entries",
                            DataType::Struct(Fields::from(vec![
                                Field::new(self.map_key_name(), key, false),
                                Field::new(self.map_value_name(), value, true),
                            ])),
                            false,
                        )),
                        false,
                    )
                });

            let name = any()
                .filter(|c: &char| !":,<>()".contains(*c))
                .repeated()
                .at_least(1)
                .to_slice()
                .map(str::trim)
                .filter(|s: &&str| !s.is_empty());
            let field = name
                .then_ignore(symbol(':'))
                .then(data_type)
                .map(|(name, data_type)| Field::new(name, data_type, true));
            let structure = keyword("struct")
                .ignore_then(
                    field
                        .separated_by(symbol(','))
                        .collect::<Vec<_>>()
                        .delimited_by(symbol('<'), symbol('>')),
                )
                .map(|fields| DataType::Struct(Fields::from(fields)));
            choice((decimal, character, array, map, structure, primitive)).padded()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_malformed_type_syntax() {
        for dialect in [HiveTypeDialect::Hive, HiveTypeDialect::Glue] {
            for input in [
                "struct<a:int>>",
                "struct<a:int,<<b:int>>",
                "map<string,int",
                "map<int,int,int>",
                "struct<a:int,>",
                "array<int> garbage",
                "char(abc)",
                "varchar(10",
                "decimal(10,2,3)",
                "decimal(256,2)",
                "struct<:int>",
            ] {
                assert!(
                    dialect.parse_arrow_type(input).is_err(),
                    "{dialect:?}: {input}"
                );
            }
        }
    }

    #[test]
    fn parses_nested_types_with_whitespace() -> CatalogResult<()> {
        for dialect in [HiveTypeDialect::Hive, HiveTypeDialect::Glue] {
            let data_type = dialect.parse_arrow_type(
                " STRUCT <Amount: DECIMAL (10, -2), Tags: ARRAY < VARCHAR (20) >> ",
            )?;
            let expected = DataType::Struct(Fields::from(vec![
                Field::new("Amount", DataType::Decimal128(10, -2), true),
                Field::new(
                    "Tags",
                    DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                    true,
                ),
            ]));
            assert_eq!(data_type, expected);
        }
        Ok(())
    }
}
