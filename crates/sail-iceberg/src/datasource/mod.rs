pub mod expr_adapter;
pub mod expressions;
pub mod provider;
pub mod pruning;

use datafusion::common::scalar::ScalarValue;
pub use provider::*;

use crate::spec::types::values::{Literal, PrimitiveLiteral};

pub(crate) fn literal_to_scalar_value(literal: &Literal) -> ScalarValue {
    match literal {
        Literal::Primitive(primitive) => match primitive {
            PrimitiveLiteral::Boolean(v) => ScalarValue::Boolean(Some(*v)),
            PrimitiveLiteral::Int(v) => ScalarValue::Int32(Some(*v)),
            PrimitiveLiteral::Long(v) => ScalarValue::Int64(Some(*v)),
            PrimitiveLiteral::Float(v) => ScalarValue::Float32(Some(v.into_inner())),
            PrimitiveLiteral::Double(v) => ScalarValue::Float64(Some(v.into_inner())),
            PrimitiveLiteral::String(v) => ScalarValue::Utf8(Some(v.clone())),
            PrimitiveLiteral::Binary(v) => ScalarValue::Binary(Some(v.clone())),
            PrimitiveLiteral::Int128(v) => ScalarValue::Decimal128(Some(*v), 38, 0),
            PrimitiveLiteral::UInt128(v) => {
                if *v <= i128::MAX as u128 {
                    ScalarValue::Decimal128(Some(*v as i128), 38, 0)
                } else {
                    ScalarValue::Utf8(Some(v.to_string()))
                }
            }
        },
        Literal::Struct(fields) => {
            let json_repr = serde_json::to_string(fields).unwrap_or_default();
            ScalarValue::Utf8(Some(json_repr))
        }
        Literal::List(items) => {
            let json_repr = serde_json::to_string(items).unwrap_or_default();
            ScalarValue::Utf8(Some(json_repr))
        }
        Literal::Map(pairs) => {
            let json_repr = serde_json::to_string(pairs).unwrap_or_default();
            ScalarValue::Utf8(Some(json_repr))
        }
    }
}
