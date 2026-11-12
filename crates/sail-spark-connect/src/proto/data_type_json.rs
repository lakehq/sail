use sail_common_spark::data_type::JsonDataType;

use crate::error::{SparkError, SparkResult};
use crate::spark::connect as sc;
use crate::spark::connect::data_type as dt;

pub(crate) fn parse_spark_json_data_type(schema: &str) -> SparkResult<sc::DataType> {
    let json_data_type: JsonDataType = serde_json::from_str(schema)?;
    json_data_type_to_proto(json_data_type)
}

fn json_data_type_to_proto(data_type: JsonDataType) -> SparkResult<sc::DataType> {
    let proto_data_type = match data_type {
        JsonDataType::Null => sc::DataType {
            kind: Some(dt::Kind::Null(dt::Null::default())),
        },
        JsonDataType::String => sc::DataType {
            kind: Some(dt::Kind::String(dt::String::default())),
        },
        JsonDataType::Char { length } => sc::DataType {
            kind: Some(dt::Kind::Char(dt::Char {
                length,
                type_variation_reference: 0,
            })),
        },
        JsonDataType::Varchar { length } => sc::DataType {
            kind: Some(dt::Kind::VarChar(dt::VarChar {
                length,
                type_variation_reference: 0,
            })),
        },
        JsonDataType::Binary => sc::DataType {
            kind: Some(dt::Kind::Binary(dt::Binary::default())),
        },
        JsonDataType::Boolean => sc::DataType {
            kind: Some(dt::Kind::Boolean(dt::Boolean::default())),
        },
        JsonDataType::Decimal => sc::DataType {
            kind: Some(dt::Kind::Decimal(dt::Decimal::default())),
        },
        JsonDataType::FixedDecimal { precision, scale } => sc::DataType {
            kind: Some(dt::Kind::Decimal(dt::Decimal {
                precision: Some(precision),
                scale: Some(scale),
                type_variation_reference: 0,
            })),
        },
        JsonDataType::Float => sc::DataType {
            kind: Some(dt::Kind::Float(dt::Float::default())),
        },
        JsonDataType::Double => sc::DataType {
            kind: Some(dt::Kind::Double(dt::Double::default())),
        },
        JsonDataType::Byte => sc::DataType {
            kind: Some(dt::Kind::Byte(dt::Byte::default())),
        },
        JsonDataType::Short => sc::DataType {
            kind: Some(dt::Kind::Short(dt::Short::default())),
        },
        JsonDataType::Integer => sc::DataType {
            kind: Some(dt::Kind::Integer(dt::Integer::default())),
        },
        JsonDataType::Long => sc::DataType {
            kind: Some(dt::Kind::Long(dt::Long::default())),
        },
        JsonDataType::Date => sc::DataType {
            kind: Some(dt::Kind::Date(dt::Date::default())),
        },
        JsonDataType::Time { precision } => sc::DataType {
            kind: Some(dt::Kind::Time(dt::Time {
                precision,
                type_variation_reference: 0,
            })),
        },
        JsonDataType::Timestamp => sc::DataType {
            kind: Some(dt::Kind::Timestamp(dt::Timestamp::default())),
        },
        JsonDataType::TimestampNtz => sc::DataType {
            kind: Some(dt::Kind::TimestampNtz(dt::TimestampNtz::default())),
        },
        JsonDataType::TimestampLtzNanos => sc::DataType {
            kind: Some(dt::Kind::TimestampLtzNanos(dt::TimestampLtzNanos::default())),
        },
        JsonDataType::TimestampNtzNanos => sc::DataType {
            kind: Some(dt::Kind::TimestampNtzNanos(dt::TimestampNtzNanos::default())),
        },
        JsonDataType::CalendarInterval => sc::DataType {
            kind: Some(dt::Kind::CalendarInterval(dt::CalendarInterval::default())),
        },
        JsonDataType::DayTimeInterval { start, end } => sc::DataType {
            kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                start_field: start.map(|f| f as i32),
                end_field: end.map(|f| f as i32),
                type_variation_reference: 0,
            })),
        },
        JsonDataType::YearMonthInterval { start, end } => sc::DataType {
            kind: Some(dt::Kind::YearMonthInterval(dt::YearMonthInterval {
                start_field: start.map(|f| f as i32),
                end_field: end.map(|f| f as i32),
                type_variation_reference: 0,
            })),
        },
        JsonDataType::Variant => sc::DataType {
            kind: Some(dt::Kind::Variant(dt::Variant::default())),
        },
        JsonDataType::Geometry { srid } => sc::DataType {
            kind: Some(dt::Kind::Geometry(dt::Geometry {
                srid,
                type_variation_reference: 0,
            })),
        },
        JsonDataType::Geography { srid, algorithm } => {
            // Validate algorithm: Spark 4.1 only supports "spherical" for Geography
            if algorithm != "spherical" {
                return Err(SparkError::invalid(format!(
                    "Algorithm '{algorithm}' is not valid for GEOGRAPHY. Only 'spherical' is supported in Spark 4.1"
                )));
            }
            sc::DataType {
                kind: Some(dt::Kind::Geography(dt::Geography {
                    srid,
                    type_variation_reference: 0,
                })),
            }
        }
        JsonDataType::Array {
            r#type: _,
            element_type,
            contains_null,
        } => sc::DataType {
            kind: Some(dt::Kind::Array(Box::new(dt::Array {
                element_type: Some(Box::new(json_data_type_to_proto(*element_type)?)),
                contains_null,
                type_variation_reference: 0,
            }))),
        },
        JsonDataType::Map {
            r#type: _,
            key_type,
            value_type,
            value_contains_null,
        } => sc::DataType {
            kind: Some(dt::Kind::Map(Box::new(dt::Map {
                key_type: Some(Box::new(json_data_type_to_proto(*key_type)?)),
                value_type: Some(Box::new(json_data_type_to_proto(*value_type)?)),
                value_contains_null,
                type_variation_reference: 0,
            }))),
        },
        JsonDataType::Struct { r#type: _, fields } => sc::DataType {
            kind: Some(dt::Kind::Struct(dt::Struct {
                fields: fields
                    .into_iter()
                    .map(|field| {
                        Ok(dt::StructField {
                            name: field.name,
                            data_type: Some(json_data_type_to_proto(field.r#type)?),
                            nullable: field.nullable,
                            metadata: field
                                .metadata
                                .map(|m| Ok(serde_json::to_string(&m)?) as SparkResult<_>)
                                .transpose()?,
                        })
                    })
                    .collect::<SparkResult<_>>()?,
                type_variation_reference: 0,
            })),
        },
        JsonDataType::Udt {
            r#type: _,
            class,
            py_class,
            serialized_class,
            sql_type,
        } => sc::DataType {
            kind: Some(dt::Kind::Udt(Box::new(dt::Udt {
                r#type: "udt".to_string(),
                jvm_class: class,
                python_class: py_class,
                serialized_python_class: serialized_class,
                sql_type: sql_type
                    .map(|sql_type| {
                        Ok(Box::new(json_data_type_to_proto(*sql_type)?)) as SparkResult<_>
                    })
                    .transpose()?,
            }))),
        },
    };
    Ok(proto_data_type)
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_spark_json_data_type() {
        let data = vec![
            (
                r#""null""#,
                sc::DataType {
                    kind: Some(dt::Kind::Null(dt::Null::default())),
                },
            ),
            (
                r#""void""#,
                sc::DataType {
                    kind: Some(dt::Kind::Null(dt::Null::default())),
                },
            ),
            (
                r#""string""#,
                sc::DataType {
                    kind: Some(dt::Kind::String(dt::String::default())),
                },
            ),
            (
                r#""char(10)""#,
                sc::DataType {
                    kind: Some(dt::Kind::Char(dt::Char {
                        length: 10,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""varchar(20)""#,
                sc::DataType {
                    kind: Some(dt::Kind::VarChar(dt::VarChar {
                        length: 20,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""binary""#,
                sc::DataType {
                    kind: Some(dt::Kind::Binary(dt::Binary::default())),
                },
            ),
            (
                r#""boolean""#,
                sc::DataType {
                    kind: Some(dt::Kind::Boolean(dt::Boolean::default())),
                },
            ),
            (
                r#""decimal""#,
                sc::DataType {
                    kind: Some(dt::Kind::Decimal(dt::Decimal::default())),
                },
            ),
            (
                r#""decimal(10, 2)""#,
                sc::DataType {
                    kind: Some(dt::Kind::Decimal(dt::Decimal {
                        precision: Some(10),
                        scale: Some(2),
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""float""#,
                sc::DataType {
                    kind: Some(dt::Kind::Float(dt::Float::default())),
                },
            ),
            (
                r#""double""#,
                sc::DataType {
                    kind: Some(dt::Kind::Double(dt::Double::default())),
                },
            ),
            (
                r#""byte""#,
                sc::DataType {
                    kind: Some(dt::Kind::Byte(dt::Byte::default())),
                },
            ),
            (
                r#""short""#,
                sc::DataType {
                    kind: Some(dt::Kind::Short(dt::Short::default())),
                },
            ),
            (
                r#""integer""#,
                sc::DataType {
                    kind: Some(dt::Kind::Integer(dt::Integer::default())),
                },
            ),
            (
                r#""long""#,
                sc::DataType {
                    kind: Some(dt::Kind::Long(dt::Long::default())),
                },
            ),
            (
                r#""date""#,
                sc::DataType {
                    kind: Some(dt::Kind::Date(dt::Date::default())),
                },
            ),
            (
                r#""timestamp""#,
                sc::DataType {
                    kind: Some(dt::Kind::Timestamp(dt::Timestamp::default())),
                },
            ),
            (
                r#""timestamp_ltz""#,
                sc::DataType {
                    kind: Some(dt::Kind::Timestamp(dt::Timestamp::default())),
                },
            ),
            (
                r#""timestamp_ntz""#,
                sc::DataType {
                    kind: Some(dt::Kind::TimestampNtz(dt::TimestampNtz::default())),
                },
            ),
            (
                r#""timestamp_ltz(9)""#,
                sc::DataType {
                    kind: Some(dt::Kind::TimestampLtzNanos(dt::TimestampLtzNanos::default())),
                },
            ),
            (
                r#""timestamp_ntz(9)""#,
                sc::DataType {
                    kind: Some(dt::Kind::TimestampNtzNanos(dt::TimestampNtzNanos::default())),
                },
            ),
            (
                r#""interval""#,
                sc::DataType {
                    kind: Some(dt::Kind::CalendarInterval(dt::CalendarInterval::default())),
                },
            ),
            (
                r#""interval day""#,
                sc::DataType {
                    kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                        start_field: Some(0),
                        end_field: None,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval day to hour""#,
                sc::DataType {
                    kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                        start_field: Some(0),
                        end_field: Some(1),
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval day to minute""#,
                sc::DataType {
                    kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                        start_field: Some(0),
                        end_field: Some(2),
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval day to second""#,
                sc::DataType {
                    kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                        start_field: Some(0),
                        end_field: Some(3),
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval hour""#,
                sc::DataType {
                    kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                        start_field: Some(1),
                        end_field: None,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval hour to minute""#,
                sc::DataType {
                    kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                        start_field: Some(1),
                        end_field: Some(2),
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval hour to second""#,
                sc::DataType {
                    kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                        start_field: Some(1),
                        end_field: Some(3),
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval minute""#,
                sc::DataType {
                    kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                        start_field: Some(2),
                        end_field: None,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval minute to second""#,
                sc::DataType {
                    kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                        start_field: Some(2),
                        end_field: Some(3),
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval second""#,
                sc::DataType {
                    kind: Some(dt::Kind::DayTimeInterval(dt::DayTimeInterval {
                        start_field: Some(3),
                        end_field: None,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval year""#,
                sc::DataType {
                    kind: Some(dt::Kind::YearMonthInterval(dt::YearMonthInterval {
                        start_field: Some(0),
                        end_field: None,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval year to month""#,
                sc::DataType {
                    kind: Some(dt::Kind::YearMonthInterval(dt::YearMonthInterval {
                        start_field: Some(0),
                        end_field: Some(1),
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""interval month""#,
                sc::DataType {
                    kind: Some(dt::Kind::YearMonthInterval(dt::YearMonthInterval {
                        start_field: Some(1),
                        end_field: None,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""time""#,
                sc::DataType {
                    kind: Some(dt::Kind::Time(dt::Time {
                        precision: None,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""time(0)""#,
                sc::DataType {
                    kind: Some(dt::Kind::Time(dt::Time {
                        precision: Some(0),
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""time(3)""#,
                sc::DataType {
                    kind: Some(dt::Kind::Time(dt::Time {
                        precision: Some(3),
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""time(6)""#,
                sc::DataType {
                    kind: Some(dt::Kind::Time(dt::Time {
                        precision: Some(6),
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""time(9)""#,
                sc::DataType {
                    kind: Some(dt::Kind::Time(dt::Time {
                        precision: Some(9),
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""variant""#,
                sc::DataType {
                    kind: Some(dt::Kind::Variant(dt::Variant::default())),
                },
            ),
            (
                r#"{
                    "type": "array",
                    "elementType": "integer",
                    "containsNull": true
                }"#,
                sc::DataType {
                    kind: Some(dt::Kind::Array(Box::new(dt::Array {
                        element_type: Some(Box::new(sc::DataType {
                            kind: Some(dt::Kind::Integer(dt::Integer::default())),
                        })),
                        contains_null: true,
                        type_variation_reference: 0,
                    }))),
                },
            ),
            (
                r#"{
                    "type": "map",
                    "keyType": "string",
                    "valueType": "integer",
                    "valueContainsNull": false
                }"#,
                sc::DataType {
                    kind: Some(dt::Kind::Map(Box::new(dt::Map {
                        key_type: Some(Box::new(sc::DataType {
                            kind: Some(dt::Kind::String(dt::String::default())),
                        })),
                        value_type: Some(Box::new(sc::DataType {
                            kind: Some(dt::Kind::Integer(dt::Integer::default())),
                        })),
                        value_contains_null: false,
                        type_variation_reference: 0,
                    }))),
                },
            ),
            (
                r#"{
                    "type": "struct",
                    "fields": [
                        {
                            "name": "name",
                            "type": "string",
                            "nullable": false,
                            "metadata": {}
                        },
                        {
                            "name": "age",
                            "type": {
                                "type": "array",
                                "elementType": "integer",
                                "containsNull": true
                            },
                            "nullable": true
                        }
                    ]
                }"#,
                sc::DataType {
                    kind: Some(dt::Kind::Struct(dt::Struct {
                        fields: vec![
                            dt::StructField {
                                name: "name".to_string(),
                                data_type: Some(sc::DataType {
                                    kind: Some(dt::Kind::String(dt::String::default())),
                                }),
                                nullable: false,
                                metadata: Some("{}".to_string()),
                            },
                            dt::StructField {
                                name: "age".to_string(),
                                data_type: Some(sc::DataType {
                                    kind: Some(dt::Kind::Array(Box::new(dt::Array {
                                        element_type: Some(Box::new(sc::DataType {
                                            kind: Some(dt::Kind::Integer(dt::Integer::default())),
                                        })),
                                        contains_null: true,
                                        type_variation_reference: 0,
                                    }))),
                                }),
                                nullable: true,
                                metadata: None,
                            },
                        ],
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#"{
                    "type": "udt",
                    "class": "SomeClass",
                    "pyClass": "app.udf",
                    "sqlType": "integer"
                }"#,
                sc::DataType {
                    kind: Some(dt::Kind::Udt(Box::new(dt::Udt {
                        r#type: "udt".to_string(),
                        jvm_class: Some("SomeClass".to_string()),
                        python_class: Some("app.udf".to_string()),
                        serialized_python_class: None,
                        sql_type: Some(Box::new(sc::DataType {
                            kind: Some(dt::Kind::Integer(dt::Integer::default())),
                        })),
                    }))),
                },
            ),
            (
                r#"{
                    "type": "udt",
                    "pyClass": "app.udf",
                    "serializedClass": "SomeSerializedClass",
                    "sqlType": "integer"
                }"#,
                sc::DataType {
                    kind: Some(dt::Kind::Udt(Box::new(dt::Udt {
                        r#type: "udt".to_string(),
                        jvm_class: None,
                        python_class: Some("app.udf".to_string()),
                        serialized_python_class: Some("SomeSerializedClass".to_string()),
                        sql_type: Some(Box::new(sc::DataType {
                            kind: Some(dt::Kind::Integer(dt::Integer::default())),
                        })),
                    }))),
                },
            ),
            (
                r#""geometry(4326)""#,
                sc::DataType {
                    kind: Some(dt::Kind::Geometry(dt::Geometry {
                        srid: 4326,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""geometry(3857)""#,
                sc::DataType {
                    kind: Some(dt::Kind::Geometry(dt::Geometry {
                        srid: 3857,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""geometry(0)""#,
                sc::DataType {
                    kind: Some(dt::Kind::Geometry(dt::Geometry {
                        srid: 0,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""geometry(ANY)""#,
                sc::DataType {
                    kind: Some(dt::Kind::Geometry(dt::Geometry {
                        srid: -1,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""geography(4326, spherical)""#,
                sc::DataType {
                    kind: Some(dt::Kind::Geography(dt::Geography {
                        srid: 4326,
                        type_variation_reference: 0,
                    })),
                },
            ),
            (
                r#""geography(ANY, spherical)""#,
                sc::DataType {
                    kind: Some(dt::Kind::Geography(dt::Geography {
                        srid: -1,
                        type_variation_reference: 0,
                    })),
                },
            ),
        ];
        for (schema, expected) in data {
            // test serde roundtrip
            let json_data_type: JsonDataType = serde_json::from_str(schema).unwrap();
            let serialized = serde_json::to_string(&json_data_type).unwrap();
            assert_eq!(json_data_type, serde_json::from_str(&serialized).unwrap());
            // test data type conversion
            let data_type = parse_spark_json_data_type(schema).unwrap();
            assert_eq!(data_type, expected);
            // Both input formats use the same conversion to the plan specification.
            let from_proto = sail_common::spec::DataType::try_from(data_type);
            let from_json = sail_common::spec::DataType::try_from(json_data_type);
            match from_proto {
                Ok(expected) => assert_eq!(from_json.unwrap(), expected),
                Err(_) => assert!(from_json.is_err()),
            }
        }
    }
}
