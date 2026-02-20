use std::sync::Arc;

use arrow::array::{
	ArrayRef, Decimal32Builder, Float32Builder, Float64Builder, Int32Builder, Int64Builder, ListArray, StringBuilder, StructArray, TimestampMicrosecondBuilder
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{Field, Fields, TimeUnit};

use chrono::NaiveDate;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{cast, expr, lit, when};
use datafusion_functions::unicode::expr_fn as unicode_fn;
use sail_function::scalar::json::{
    json_as_text_udf, json_length_udf, json_object_keys_udf, to_json_udf,
};

use sail_sql_analyzer::data_type::from_ast_data_type;
use sail_sql_analyzer::parser::parse_data_type;

use serde_json::{Value};

use crate::error::{PlanError, PlanResult};
use crate::function::common::ScalarFunction;
use crate::resolver::data_type_helper::PlanResolver;

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
    Ok(json_as_text_udf().call(args))
}

fn json_array_length(json_data: expr::Expr) -> expr::Expr {
    cast(json_length_udf().call(vec![json_data]), DataType::Int32)
}

fn json_object_keys(json_data: expr::Expr) -> expr::Expr {
    json_object_keys_udf().call(vec![json_data])
}

fn to_json(args: Vec<expr::Expr>) -> PlanResult<expr::Expr> {
    // to_json accepts 1 or 2 arguments:
    // - to_json(expr) - convert expr to JSON string
    // - to_json(expr, options) - convert expr to JSON string with options
    match args.len() {
        1 | 2 => Ok(to_json_udf().call(args)),
        n => Err(PlanError::invalid(format!(
            "to_json expects 1 or 2 arguments, got {n}"
        ))),
    }
}

fn from_json(args: Vec<expr::Expr>) -> PlanResult<expr::Expr> {
    if args.len() < 2 && args.len() > 3 {
        return Err(PlanError::InvalidArgument(format!("Expected 2-3 args but got {}", args.len())));
    };
    let json_expr = args.get(0).unwrap().clone();
    let schema_expr = args.get(1).unwrap().clone();
    let _options = args.get(2).clone();
    let json_str = match json_expr {
        expr::Expr::Literal(ScalarValue::Utf8(Some(utf8)), _) => utf8,
        expr::Expr::Column(_) => return Err(PlanError::NotImplemented("Not yet implemented column support".to_string())),
        other => return Err(PlanError::NotSupported(format!("from_json doesn't support {other:?} types")))
    };
    let value: Value = serde_json::from_str::<serde_json::Value>(json_str.as_str()).unwrap();
    let fields = get_schema_expr_as_fields(schema_expr)?;
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
            FieldBuilder::Float32(mut b) => Arc::new(b.finish()),
            FieldBuilder::Float64(mut b) => Arc::new(b.finish()),
            FieldBuilder::Decimal32(mut b) => Arc::new(b.finish()),
            FieldBuilder::String(mut b) => Arc::new(b.finish()),
            FieldBuilder::TimestampMicrosecondBuilder(mut b) => Arc::new(b.finish()),
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
            },
            FieldBuilder::List {
                field,
                offsets,
                builder,
                null_buffer
            } => {
                let field_builder = *builder;
                let array_ref = finish_builder(field_builder)?;
                Arc::new(ListArray::new(
                    field,
                    OffsetBuffer::new(ScalarBuffer::from(offsets)),
                    array_ref,
                    Some(NullBuffer::from(null_buffer)),
                ))
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
                FieldBuilder::Float32(builder) => builder.append_null(),
                FieldBuilder::Float64(builder) => builder.append_null(),
                FieldBuilder::Decimal32(builder) => builder.append_null(),
                FieldBuilder::String(builder) => builder.append_null(),
                FieldBuilder::TimestampMicrosecondBuilder(builder) => builder.append_null(),
                FieldBuilder::Struct {
                    builders: nested_builders,
                    null_buffer,
                    ..
                } => {
                    null_buffer.push(false);
                    append_null_to_all_builders(nested_builders)
                },
                FieldBuilder::List {
					offsets,
					null_buffer,
                    ..
                } => {
                    null_buffer.push(false);
                    offsets.push(*offsets.last().unwrap());
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
        (FieldBuilder::Float32(b), DataType::Float32) => {
            if let Some(f) = value.as_f64() {
                if f >= f32::MIN as f64 && f <= f32::MAX as f64 {
                    b.append_value(f as f32);
                } else {
                    b.append_null(); // Overflow
                }
            } else {
                b.append_null(); // Type mismatch
            }
        },
        (FieldBuilder::Float64(b), DataType::Float64) => {
            if let Some(f) = value.as_f64() {
                b.append_value(f);
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
        (FieldBuilder::String(b), DataType::Utf8) => {
            if let Some(s) = value.as_str() {
                b.append_value(s);
            } else {
                b.append_null();
            }
        },
        (
            FieldBuilder::TimestampMicrosecondBuilder(b),
            DataType::Timestamp(TimeUnit::Microsecond, _)
        ) => {
            let s = value.as_str().unwrap();
            dbg!(s);
            let micro_seconds = NaiveDate::parse_from_str(s, "%d/%m/%Y")
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .timestamp_micros();
            dbg!(&micro_seconds);
            b.append_value(micro_seconds);
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
        (
            FieldBuilder::List {
                field,
                offsets,
                builder,
                null_buffer
            },
            DataType::List(_)
        ) => {
            if let Some(arr) = value.as_array() {
                null_buffer.push(true);
                for val in arr {
                    append_field_value(builder, field, Some(val))?;
                };
                let last = *offsets.last().unwrap();
                offsets.push(last + arr.len() as i32);
            } else {
                null_buffer.push(false);
                let last = *offsets.last().unwrap();
                offsets.push(last);
            }
        }
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
            FieldBuilder::Float32(b) => b.append_null(),
            FieldBuilder::Float64(b) => b.append_null(),
            FieldBuilder::Decimal32(b) => b.append_null(),
            FieldBuilder::String(b) => b.append_null(),
            FieldBuilder::TimestampMicrosecondBuilder(b) => b.append_null(),
            FieldBuilder::Struct {
                builders: nested_builder,
                null_buffer,
                ..
            } => {
                null_buffer.push(false);
                append_null_to_all_builders(nested_builder);
            },
            FieldBuilder::List {
				offsets,
				builder,
				null_buffer,
                ..
            } => {
                null_buffer.push(false);
                let last = *offsets.last().unwrap();
                offsets.push(last);
                append_null_to_all_builders(std::slice::from_mut(builder));
            }
        }
    }
}

fn get_schema_expr_as_fields(schema_expr: expr::Expr) -> PlanResult<Fields> {
    let schema_struct = match schema_expr {
        expr::Expr::Literal(ScalarValue::Utf8(Some(utf8)), _) => {
            let schema = utf8.as_str();
            if let Ok(dt) = parse_data_type(schema) {
                dt
            } else {
                parse_data_type(format!("struct<{schema}>").as_str())?
            }
        },
        expr::Expr::Column(_) => return Err(PlanError::NotImplemented("Not implemented cols yet".to_string())),
        other => return Err(PlanError::NotImplemented(format!("Not supported for type {other:?}"))),
    };
    let sail_dtype = from_ast_data_type(schema_struct.clone())?;
    let arrow_dtype = PlanResolver.resolve_data_type(&sail_dtype)?;
    match arrow_dtype {
        DataType::Struct(fields) => Ok(fields),
        other => Err(PlanError::NotImplemented(format!("Not implemented {other:?}")))
    }
}

enum FieldBuilder {
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Decimal32(Decimal32Builder),
    String(StringBuilder),
    TimestampMicrosecondBuilder(TimestampMicrosecondBuilder),
    Struct {
        fields: Fields,
        builders: Vec<FieldBuilder>,
        null_buffer: Vec<bool>,
    },
    List {
        field: Arc<Field>,
        offsets: Vec<i32>,
        builder: Box<FieldBuilder>,
        null_buffer: Vec<bool>,
    },
}

fn create_field_builders(fields: &Fields, capacity: usize) -> PlanResult<Vec<FieldBuilder>> {
    fields
        .iter()
        .map(|field| match field.data_type() {
            DataType::Int32 => Ok(FieldBuilder::Int32(Int32Builder::with_capacity(capacity))),
            DataType::Int64 => Ok(FieldBuilder::Int64(Int64Builder::with_capacity(capacity))),
            DataType::Float32 => Ok(FieldBuilder::Float32(Float32Builder::with_capacity(capacity))),
            DataType::Float64 => Ok(FieldBuilder::Float64(Float64Builder::with_capacity(capacity))),
            DataType::Decimal32(_, _) => Ok(FieldBuilder::Decimal32(Decimal32Builder::with_capacity(capacity))),
            DataType::Utf8 => Ok(FieldBuilder::String(StringBuilder::with_capacity(capacity, capacity*16))),
            DataType::Timestamp(TimeUnit::Microsecond, _) =>
                Ok(FieldBuilder::TimestampMicrosecondBuilder(TimestampMicrosecondBuilder::with_capacity(capacity))),
            DataType::Struct(fields) => {
                let builders = create_field_builders(fields, capacity)?;
                Ok(FieldBuilder::Struct {
                    fields: fields.clone(),
                    builders: builders,
                    null_buffer: Vec::with_capacity(capacity)

                })
            },
            DataType::List(field) => {
                // TODO: allow passing in one field rather than Fields
                let builder = create_field_builders(&Fields::from(vec![field.clone()]), capacity)?.pop().unwrap();
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);
                Ok(FieldBuilder::List {
                    field: field.clone(),
                    offsets: offsets,
                    builder: Box::new(builder),
                    null_buffer: Vec::with_capacity(capacity)
                })
            },
            other => {
                return Err(PlanError::unsupported(format!("Unsupported json type: {:?}", other)))
            },
        })
        .collect()
}

pub(super) fn list_built_in_json_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("from_json", F::var_arg(from_json)),
        ("get_json_object", F::binary(get_json_object)),
        ("json_array_length", F::unary(json_array_length)),
        ("json_object_keys", F::unary(json_object_keys)),
        ("json_tuple", F::unknown("json_tuple")),
        ("schema_of_json", F::unknown("schema_of_json")),
        ("to_json", F::var_arg(to_json)),
    ]
}

#[cfg(test)]
mod tests {

    use arrow::array::{MapBuilder};
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
        let schema_expr = expr::Expr::Literal(ScalarValue::Utf8(Some(schema.to_string())), None);
        from_json(vec![expr_.clone(), schema_expr.clone()]).unwrap();

        let s = r#"
            {"teacher": "Alice", "student": [{"name": "Bob", "rank": 1}, {"name": "Charlie", "rank": 2}]}
        "#;
        let expr_ = expr::Expr::Literal(ScalarValue::Utf8(Some(s.to_string())), None);
        let schema = r#"STRUCT<teacher: STRING, student: ARRAY<STRUCT<name: STRING, rank: INT>>>"#;
        let schema_expr = expr::Expr::Literal(ScalarValue::Utf8(Some(schema.to_string())), None);
        from_json(vec![expr_.clone(), schema_expr.clone()]).unwrap();

        let s = "{\"time\":\"26/08/2015\"}";
        let expr_ = expr::Expr::Literal(ScalarValue::Utf8(Some(s.to_string())), None);
        let schema = r#"time Timestamp"#;
        let schema_expr = expr::Expr::Literal(ScalarValue::Utf8(Some(schema.to_string())), None);
        //let opt = r#"map('timestampFormat', 'dd/MM/yyyy'))"#;
        let key_builder = StringBuilder::new();
        let value_builder = StringBuilder::new();
        let mut map_builder = MapBuilder::new(None, key_builder, value_builder);
        map_builder.keys().append_value("timestampFormat");
        map_builder.values().append_value("dd/MM/yyyy");
        map_builder.append(true).unwrap();
        let opt = expr::Expr::Literal(ScalarValue::Map(Arc::new(map_builder.finish())), None);
        from_json(vec![expr_.clone(), schema_expr.clone(), opt]).unwrap();
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
        from_json(vec![expr_.clone(), expr_.clone()]).unwrap();
    }
}
