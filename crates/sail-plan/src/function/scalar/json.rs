use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Decimal32Builder, Float64Array, Int32Builder, Int64Array, Int64Builder, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{Field, Fields, SchemaBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{cast, expr, lit, when};
use datafusion_functions::unicode::expr_fn as unicode_fn;
use datafusion_functions_json::udfs;
use sail_sql_analyzer::data_type::from_ast_data_type;
use serde_json::Value;

use crate::error::{PlanError, PlanResult};
use crate::function::common::ScalarFunction;

use sail_sql_analyzer::parser::parse_data_type;
use sail_sql_parser::ast::data_type::DataType as AstDataType;

fn get_json_object(expr: expr::Expr, path: expr::Expr) -> PlanResult<expr::Expr> {
    let paths: Vec<expr::Expr> = match path {
        expr::Expr::Literal(ScalarValue::Utf8(Some(value)), _metadata)
            if value.starts_with("$.") =>
        {
            Ok::<_, DataFusionError>(value.replacen("$.", "", 1).split(".").map(lit).collect())
        }
        // FIXME: json_as_text_udf for array of paths with subpaths is not implemented, so only top level keys supported
        _ => Ok(vec![when(
            path.clone().like(lit("$.%")),
            unicode_fn::substr(path, lit(3)),
        )
        .when(lit(true), lit(""))
        .end()?]),
    }?;
    let mut args = Vec::with_capacity(1 + paths.len());
    args.push(expr);
    args.extend(paths);
    Ok(udfs::json_as_text_udf().call(args))
}

fn json_array_length(json_data: expr::Expr) -> expr::Expr {
    cast(
        udfs::json_length_udf().call(vec![json_data]),
        DataType::Int32,
    )
}

fn json_object_keys(json_data: expr::Expr) -> expr::Expr {
    udfs::json_object_keys_udf().call(vec![json_data])
}

fn from_json(json_expr: expr::Expr, schema_expr: expr::Expr) -> PlanResult<expr::Expr> {
    let json_str = match json_expr {
        expr::Expr::Literal(ScalarValue::Utf8(Some(utf8)), _) => utf8,
        _ => unimplemented!("Onwy utf8 avaiwable")
    };
    let schema_struct = match schema_expr {
        expr::Expr::Literal(ScalarValue::Utf8(Some(utf8)), _) => {
            let schema = utf8.as_str();
            if let Ok(dt) = parse_data_type(schema) {
                dt
            } else {
                parse_data_type(format!("struct<{schema}>").as_str())?
            }
        },
        _ => unimplemented!("Onwy utf8 avaiwable")
    };
    let value: Value = serde_json::from_str::<serde_json::Value>(json_str.as_str()).unwrap();
    let fields = schema_struct_datatype_to_fields(schema_struct)?;
    //let num_rows = value.as_array().unwrap().len();
    let num_rows = 1;
    let mut field_builders = create_field_builders(&fields, num_rows)?;
    let mut struct_nulls = vec![true; num_rows];
    if let serde_json::Value::Object(obj) = value {
        struct_nulls[0] = true;
        for (field, builder) in fields.iter().zip(field_builders.iter_mut()) {
            let field_value = obj.get(field.name());
            append_field_value(builder, &field, field_value)?;
        }
    } else {
        struct_nulls[0] = false;
        append_null_to_all_builders(&mut field_builders);
    }
    let arrays: Vec<ArrayRef> = field_builders
        .into_iter()
        .map(finish_builder)
        .collect::<PlanResult<Vec<_>>>()?;
    let null_buffer = NullBuffer::from(struct_nulls);
    let struct_array = Arc::new(StructArray::new(fields.clone(), arrays, Some(null_buffer)));
    dbg!(&struct_array);
    Ok(expr::Expr::Literal(ScalarValue::Struct(struct_array), None))
}

fn finish_builder(builder: FieldBuilder) -> PlanResult<ArrayRef> {
    Ok(
        match builder {
            FieldBuilder::Int32(mut b) => Arc::new(b.finish()),
            FieldBuilder::Int64(mut b) => Arc::new(b.finish()),
            FieldBuilder::Decimal32(mut b) => Arc::new(b.finish()),
            FieldBuilder::Struct {
                fields,
                builders,
                null_buffer
            } => {
                let nested_arrays: Vec<ArrayRef> = builders
                    .into_iter()
                    .map(finish_builder)
                    .collect::<PlanResult<Vec<_>>>()?;
                let null_buf = NullBuffer::from(null_buffer);
                Arc::new(StructArray::new(fields, nested_arrays, Some(null_buf)))
            }
        }
    )
}

fn append_field_value(
    builder: &mut FieldBuilder,
    field: &Field,
    serde_value: Option<&Value>
) -> PlanResult<()> {
    let value = match serde_value {
        Some(Value::Null) | None => {
            match builder {
                FieldBuilder::Int32(builder) => builder.append_null(),
                FieldBuilder::Int64(builder) => builder.append_null(),
                FieldBuilder::Decimal32(builder) => builder.append_null(),
                FieldBuilder::Struct {
                    builders: nested_builders,
                    null_buffer,
                    ..
                } => {
                    null_buffer.push(false);
                    append_null_to_all_builders(nested_builders)
                }
            }
            return Ok(());
        }
        Some(value) => value
    };

    match (builder, field.data_type()) {
        (FieldBuilder::Int32(b), DataType::Int32) => {
            if let Some(i) = value.as_i64() {
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    b.append_value(i as i32);
                } else {
                    b.append_null(); // Overflow
                }
            } else {
                b.append_null(); // Type mismatch
            }
        },
        (FieldBuilder::Int64(b), DataType::Int64) => {
            if let Some(i) = value.as_i64() {
                b.append_value(i);
            } else {
                b.append_null(); // Type mismatch
            }
        },
        (FieldBuilder::Decimal32(b), DataType::Decimal32(_, _)) => {
            if let Some(f) = value.as_i64() {
                b.append_value(f as i32);
            } else {
                b.append_null();
            }
        },
        (
            FieldBuilder::Struct {
                fields: nested_fields,
                builders: nested_builders,
                null_buffer,
            },
            DataType::Struct(_)
        ) => {
            if let Some(obj) = value.as_object() {
                null_buffer.push(true);
                for (nested_field, nested_builder) in nested_fields.iter().zip(nested_builders.iter_mut()) {
                    let nested_value = obj.get(nested_field.name());
                    append_field_value(nested_builder, nested_field, nested_value)?;
                }
            } else {
                null_buffer.push(false);
                append_null_to_all_builders(nested_builders);
            }
        },
        (_, other) => {
            return Err(PlanError::NotSupported(format!("Invalid type in json: {:?}", other)));
        }
    }
    Ok(())
}

fn append_null_to_all_builders(builders: &mut [FieldBuilder]) {
    for builder in builders {
        match builder {
            FieldBuilder::Int32(b) => b.append_null(),
            FieldBuilder::Int64(b) => b.append_null(),
            FieldBuilder::Decimal32(b) => b.append_null(),
            FieldBuilder::Struct {
                builders: nested_builder,
                null_buffer,
                ..
            } => {
                null_buffer.push(false);
                append_null_to_all_builders(nested_builder);
            }
        }
    }
}

fn schema_struct_datatype_to_fields(schema_struct: AstDataType) -> PlanResult<Fields> {
    let dtype = from_ast_data_type(schema_struct.clone());
    dbg!(dtype);
    let mut field_builder = SchemaBuilder::new();
    match schema_struct.clone() {
        AstDataType::Struct(_struct_identifier, _lt, Some(fields), _gt) => {
            for item in fields.items() {
                let arrow_data_type = match &item.data_type {
                    AstDataType::Struct(_struct_identifier, _lt, Some(_fields), _gt) => {
                        let fields = schema_struct_datatype_to_fields(item.data_type.clone())?;
                        DataType::Struct(Fields::from(fields))
                    },
                    other => ast_data_type_to_arrow(&other)?
                };
                let nullable = item.not_null.is_none();
                field_builder.push(Field::new(&item.identifier.value, arrow_data_type, nullable))
            }
        },
        other => {
            return Err(PlanError::invalid(format!("Expected struct got {:?}", other)));
        }
    }
    Ok(field_builder.finish().fields)
}

fn ast_data_type_to_arrow(ast_data_type: &AstDataType) -> PlanResult<DataType> {
    match ast_data_type {
        AstDataType::Int(_, _) => Ok(DataType::Int64),
        AstDataType::Double(_) => Ok(DataType::Decimal32(9, 2)),
        AstDataType::Struct(_, _, Some(struct_fields), _) => {
            let mut field_builder = SchemaBuilder::new();
            for struct_field in struct_fields.items() {
                let data_type = ast_data_type_to_arrow(&struct_field.data_type)?;
                field_builder.push(Field::new(
                    struct_field.identifier.value.clone(),
                    data_type,
                    struct_field.not_null.is_none()
                ));
            }
            Ok(DataType::Struct(field_builder.finish().fields))
        }
        _ => unimplemented!("Not hewe wet")
    }
}

enum FieldBuilder {
    Int32(Int32Builder),
    Int64(Int64Builder),
    Decimal32(Decimal32Builder),
    Struct {
        fields: Fields,
        builders: Vec<FieldBuilder>,
        null_buffer: Vec<bool>
    }
}

fn create_field_builders(fields: &Fields, capacity: usize) -> PlanResult<Vec<FieldBuilder>> {
    fields
        .iter()
        .map(|field| match field.data_type() {
            DataType::Int32 => Ok(FieldBuilder::Int32(Int32Builder::with_capacity(capacity))),
            DataType::Int64 => Ok(FieldBuilder::Int64(Int64Builder::with_capacity(capacity))),
            DataType::Decimal32(_, _) => Ok(FieldBuilder::Decimal32(Decimal32Builder::with_capacity(capacity))),
            DataType::Struct(fields) => {
                let builders = create_field_builders(fields, capacity)?;
                Ok(FieldBuilder::Struct {
                    fields: fields.clone(),
                    builders: builders,
                    null_buffer: Vec::with_capacity(capacity)
                })
            }
            other => {
                return Err(PlanError::unsupported(format!("Unsupported json type: {:?}", other)))
            }
        })
        .collect()
}

pub(super) fn list_built_in_json_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("from_json", F::binary(from_json)),
        ("get_json_object", F::binary(get_json_object)),
        ("json_array_length", F::unary(json_array_length)),
        ("json_object_keys", F::unary(json_object_keys)),
        ("json_tuple", F::unknown("json_tuple")),
        ("schema_of_json", F::unknown("schema_of_json")),
        ("to_json", F::unknown("to_json")),
    ]
}

#[cfg(test)]
mod tests {

    use datafusion::{prelude::Column};
    use datafusion_common::Spans;

    use super::*;

    #[test]
    fn test_utf8() {
        let s = r#"
            {
                "a": 1,
                "b": 0.8
            }
        "#;
        let expr_ = expr::Expr::Literal(ScalarValue::Utf8(Some(s.to_string())), None);
        let schema = r#"a int, b double"#;
        //let schema = r#"struct<a int, b int>"#;
        let schema_expr = expr::Expr::Literal(ScalarValue::Utf8(Some(schema.to_string())), None);
        from_json(expr_.clone(), schema_expr.clone()).unwrap();
    }

    #[test]
    fn test_column() {
        let col = Column {
            relation: None,
            name: "meh".to_string(),
            spans: Spans::new()
        };
        let expr_ = expr::Expr::Column(col);
        // meh not done with this - not sure how to get data out
        cast(expr_.clone(), DataType::Utf8);
        from_json(expr_.clone(), expr_.clone()).unwrap();
    }

    // TODO: remove
    #[test]
    fn test_parse_data_type() {
        let s = parse_data_type("struct<a INT, b DOUBLE>").unwrap();
        dbg!(s);
        let s = parse_data_type("a INT, b DOUBLE").unwrap();
        dbg!(s);
        let s = parse_data_type("STRUCT<teacher: STRING, student: ARRAY<STRUCT<name: STRING, rank: INT>>>").unwrap();
        dbg!(s);
    }

}
