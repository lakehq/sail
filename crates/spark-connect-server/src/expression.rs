use crate::error::{ProtoFieldExt, SparkError};
use crate::schema::from_spark_data_type;
use crate::spark::connect as sc;
use crate::spark::connect::expression as sce;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::expr;

pub(crate) fn from_spark_expression(expr: &sc::Expression) -> Result<expr::Expr, SparkError> {
    let sc::Expression { expr_type } = expr;
    let expr_type = expr_type.as_ref().required("expression type")?;
    match expr_type {
        sce::ExprType::Literal(literal) => {
            Ok(expr::Expr::Literal(from_spark_literal_to_scalar(literal)?))
        }
        sce::ExprType::UnresolvedAttribute(_) => {
            todo!()
        }
        sce::ExprType::UnresolvedFunction(_) => {
            todo!()
        }
        sce::ExprType::ExpressionString(_) => {
            todo!()
        }
        sce::ExprType::UnresolvedStar(_) => {
            todo!()
        }
        sce::ExprType::Alias(alias) => {
            let expr = alias.expr.as_ref().required("expression for alias")?;
            if let [name] = alias.name.as_slice() {
                Ok(expr::Expr::Alias(expr::Alias {
                    expr: Box::new(from_spark_expression(expr)?),
                    relation: None,
                    name: name.to_string(),
                }))
            } else {
                Err(SparkError::invalid("one alias name must be specified"))
            }
        }
        sce::ExprType::Cast(cast) => {
            let expr = cast.expr.as_ref().required("expression for cast")?;
            let data_type = cast.cast_to_type.as_ref().required("data type for cast")?;
            let data_type = match data_type {
                sce::cast::CastToType::Type(t) => from_spark_data_type(&t)?,
                sce::cast::CastToType::TypeStr(_) => {
                    todo!()
                }
            };
            Ok(expr::Expr::Cast(expr::Cast {
                expr: Box::new(from_spark_expression(expr)?),
                data_type,
            }))
        }
        sce::ExprType::UnresolvedRegex(_) => {
            todo!()
        }
        sce::ExprType::SortOrder(_) => {
            todo!()
        }
        sce::ExprType::LambdaFunction(_) => {
            todo!()
        }
        sce::ExprType::Window(_) => {
            todo!()
        }
        sce::ExprType::UnresolvedExtractValue(_) => {
            todo!()
        }
        sce::ExprType::UpdateFields(_) => {
            todo!()
        }
        sce::ExprType::UnresolvedNamedLambdaVariable(_) => {
            todo!()
        }
        sce::ExprType::CommonInlineUserDefinedFunction(_) => {
            todo!()
        }
        sce::ExprType::CallFunction(_) => {
            todo!()
        }
        sce::ExprType::Extension(_) => {
            todo!()
        }
    }
}

pub(crate) fn from_spark_literal_to_scalar(
    literal: &sce::Literal,
) -> Result<ScalarValue, SparkError> {
    let sce::Literal { literal_type } = literal;
    let literal_type = literal_type.as_ref().required("literal type")?;
    match literal_type {
        sce::literal::LiteralType::Null(_) => Ok(ScalarValue::Null),
        sce::literal::LiteralType::Binary(x) => Ok(ScalarValue::Binary(Some(x.clone()))),
        sce::literal::LiteralType::Boolean(x) => Ok(ScalarValue::Boolean(Some(*x))),
        sce::literal::LiteralType::Byte(x) => Ok(ScalarValue::Int8(Some(*x as i8))),
        sce::literal::LiteralType::Short(x) => Ok(ScalarValue::Int16(Some(*x as i16))),
        sce::literal::LiteralType::Integer(x) => Ok(ScalarValue::Int32(Some(*x))),
        sce::literal::LiteralType::Long(x) => Ok(ScalarValue::Int64(Some(*x))),
        sce::literal::LiteralType::Float(x) => Ok(ScalarValue::Float32(Some(*x))),
        sce::literal::LiteralType::Double(x) => Ok(ScalarValue::Float64(Some(*x))),
        sce::literal::LiteralType::Decimal(_) => {
            todo!()
        }
        sce::literal::LiteralType::String(x) => Ok(ScalarValue::Utf8(Some(x.clone()))),
        sce::literal::LiteralType::Date(_) => {
            todo!()
        }
        sce::literal::LiteralType::Timestamp(_) => {
            todo!()
        }
        sce::literal::LiteralType::TimestampNtz(_) => {
            todo!()
        }
        sce::literal::LiteralType::CalendarInterval(_) => {
            todo!()
        }
        sce::literal::LiteralType::YearMonthInterval(_) => {
            todo!()
        }
        sce::literal::LiteralType::DayTimeInterval(_) => {
            todo!()
        }
        sce::literal::LiteralType::Array(_) => {
            todo!()
        }
        sce::literal::LiteralType::Map(_) => {
            todo!()
        }
        sce::literal::LiteralType::Struct(_) => {
            todo!()
        }
    }
}
