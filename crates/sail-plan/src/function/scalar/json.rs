use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StructArray};
use arrow::datatypes::Field;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, ScalarValue, nested_struct};
use datafusion_expr::{ColumnarValue, cast, expr, lit, when};
use datafusion_functions::unicode::expr_fn as unicode_fn;
use datafusion_functions_json::udfs;
use serde_json::Value;

use crate::error::PlanResult;
use crate::function::common::ScalarFunction;

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

fn from_json(json_expr: expr::Expr, _schema_expr: expr::Expr) -> PlanResult<expr::Expr> {
    let json_str = match json_expr {
        expr::Expr::Literal(ScalarValue::Utf8(Some(utf8)), _) => utf8,
        _ => unimplemented!("Onwy utf8 avaiwable")
    };
    let value: Value = serde_json::from_str::<serde_json::Value>(json_str.as_str()).unwrap();
    let struct_ = match value {
        Value::Object(map) => {
            map_to_struct(map)
        },
        _ => unimplemented!("Onwy maps impwemented"),
    };
    let s = ScalarValue::Struct(Arc::new(struct_));
    dbg!(&s);
    Ok(expr::Expr::Literal(s, None))
}

fn map_to_struct(map: serde_json::Map<String, Value>) -> StructArray {
    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for (k, v) in map {
        match v {
            Value::Number(num) => {
                if num.is_i64() {
                    fields.push(Field::new(k, DataType::Int64, true));
                    arrays.push(Arc::new(Int64Array::from(vec![Some(num.as_i64().unwrap())])));
                } else {
                    fields.push(Field::new(k, DataType::Float64, true));
                    arrays.push(Arc::new(Float64Array::from(vec![Some(num.as_f64().unwrap())])));
                }
            },
            Value::Object(map) => {
                let nested_struct = map_to_struct(map);
                fields.push(Field::new(k, DataType::Struct(nested_struct.fields().clone()), nested_struct.is_nullable()));
                arrays.push(Arc::new(nested_struct));
            }
            _ => unimplemented!("not here yet")
        }
    };
    StructArray::new(fields.into(), arrays, None)
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

    use datafusion::prelude::Column;
    use datafusion_common::Spans;

    use super::*;

    #[test]
    fn test_utf8() {
        let s = r#"
            {
                "a": 1,
                "b": 2,
                "c": {"z": 1}
            }
        "#;
        let expr_ = expr::Expr::Literal(ScalarValue::Utf8(Some(s.to_string())), None);
        from_json(expr_.clone(), expr_.clone()).unwrap();
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

}
