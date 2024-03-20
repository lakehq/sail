use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::TableReference;
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{expr, AggregateUDF, ScalarUDF, TableSource, WindowUDF};
use datafusion::sql::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::ast::ObjectName;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::{Parser, WildcardExpr};

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::schema::from_spark_data_type;
use crate::spark::connect as sc;

#[derive(Default)]
struct ExpressionContextProvider {
    options: ConfigOptions,
}

impl ContextProvider for ExpressionContextProvider {
    fn get_table_source(
        &self,
        name: TableReference,
    ) -> datafusion::common::Result<Arc<dyn TableSource>> {
        Err(datafusion::error::DataFusionError::NotImplemented(format!(
            "get table source: {:?}",
            name
        )))
    }

    fn get_function_meta(&self, _: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_window_meta(&self, _: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn get_variable_type(&self, _: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }
}

pub(crate) fn from_spark_sort_order(
    sort_order: &sc::expression::SortOrder,
    schema: &DFSchema,
) -> SparkResult<expr::Expr> {
    use sc::expression::sort_order::{NullOrdering, SortDirection};

    let expr = sort_order
        .child
        .as_ref()
        .required("expression for sort order")?;
    let asc = match SortDirection::try_from(sort_order.direction) {
        Ok(SortDirection::Ascending) => true,
        Ok(SortDirection::Descending) => false,
        Ok(SortDirection::Unspecified) => true,
        Err(_) => {
            return Err(SparkError::invalid("invalid sort direction"));
        }
    };
    let nulls_first = match NullOrdering::try_from(sort_order.null_ordering) {
        Ok(NullOrdering::SortNullsFirst) => true,
        Ok(NullOrdering::SortNullsLast) => false,
        Ok(NullOrdering::SortNullsUnspecified) => asc,
        Err(_) => {
            return Err(SparkError::invalid("invalid null ordering"));
        }
    };
    Ok(expr::Expr::Sort(expr::Sort {
        expr: Box::new(from_spark_expression(expr, schema)?),
        asc,
        nulls_first,
    }))
}

pub(crate) fn from_spark_expression(
    expr: &sc::Expression,
    schema: &DFSchema,
) -> SparkResult<expr::Expr> {
    use sc::expression::ExprType;

    let sc::Expression { expr_type } = expr;
    let expr_type = expr_type.as_ref().required("expression type")?;
    match expr_type {
        ExprType::Literal(literal) => {
            Ok(expr::Expr::Literal(from_spark_literal_to_scalar(literal)?))
        }
        ExprType::UnresolvedAttribute(attr) => {
            let col = Column::new_unqualified(&attr.unparsed_identifier);
            Ok(expr::Expr::Column(col))
        }
        ExprType::UnresolvedFunction(_) => Err(SparkError::todo("unresolved function")),
        ExprType::ExpressionString(expr) => {
            let dialect = GenericDialect {};
            let mut parser = Parser::new(&dialect).try_with_sql(&expr.expression)?;
            match parser.parse_wildcard_expr()? {
                WildcardExpr::Expr(expr) => {
                    let provider = ExpressionContextProvider::default();
                    let planner = SqlToRel::new(&provider);
                    let expr = planner.sql_to_expr(expr, schema, &mut PlannerContext::default())?;
                    Ok(expr)
                }
                WildcardExpr::QualifiedWildcard(ObjectName(name)) => {
                    let qualifier = name
                        .iter()
                        .map(|x| x.value.as_str())
                        .collect::<Vec<_>>()
                        .join(".");
                    Ok(expr::Expr::Wildcard {
                        qualifier: Some(qualifier),
                    })
                }
                WildcardExpr::Wildcard => Ok(expr::Expr::Wildcard { qualifier: None }),
            }
        }
        ExprType::UnresolvedStar(star) => {
            // FIXME: column reference is parsed as qualifier
            if let Some(target) = &star.unparsed_target {
                let dialect = GenericDialect {};
                let mut parser = Parser::new(&dialect).try_with_sql(target)?;
                let expr = parser.parse_wildcard_expr()?;
                match expr {
                    WildcardExpr::Expr(_) => Err(SparkError::todo("expression as wildcard target")),
                    WildcardExpr::QualifiedWildcard(ast::ObjectName(names)) => {
                        let qualifier = names
                            .iter()
                            .map(|x| match x {
                                ast::Ident {
                                    value,
                                    quote_style: None,
                                } => Ok(value.as_str()),
                                _ => Err(SparkError::todo("quoted identifier")),
                            })
                            .collect::<Result<Vec<_>, _>>()?
                            .join(".");
                        Ok(expr::Expr::Wildcard {
                            qualifier: Some(qualifier),
                        })
                    }
                    WildcardExpr::Wildcard => Ok(expr::Expr::Wildcard { qualifier: None }),
                }
            } else {
                Ok(expr::Expr::Wildcard { qualifier: None })
            }
        }
        ExprType::Alias(alias) => {
            let expr = alias.expr.as_ref().required("expression for alias")?;
            if let [name] = alias.name.as_slice() {
                Ok(expr::Expr::Alias(expr::Alias {
                    expr: Box::new(from_spark_expression(expr, schema)?),
                    relation: None,
                    name: name.to_string(),
                }))
            } else {
                Err(SparkError::invalid("one alias name must be specified"))
            }
        }
        ExprType::Cast(cast) => {
            use sc::expression::cast::CastToType;

            let expr = cast.expr.as_ref().required("expression for cast")?;
            let data_type = cast.cast_to_type.as_ref().required("data type for cast")?;
            let data_type = match data_type {
                CastToType::Type(t) => from_spark_data_type(&t)?,
                CastToType::TypeStr(_) => Err(SparkError::todo("cast type string"))?,
            };
            Ok(expr::Expr::Cast(expr::Cast {
                expr: Box::new(from_spark_expression(expr, schema)?),
                data_type,
            }))
        }
        ExprType::UnresolvedRegex(_) => Err(SparkError::todo("unresolved regex")),
        ExprType::SortOrder(sort) => from_spark_sort_order(sort, schema),
        ExprType::LambdaFunction(_) => Err(SparkError::todo("lambda function")),
        ExprType::Window(_) => Err(SparkError::todo("window")),
        ExprType::UnresolvedExtractValue(_) => Err(SparkError::todo("unresolved extract value")),
        ExprType::UpdateFields(_) => Err(SparkError::todo("update fields")),
        ExprType::UnresolvedNamedLambdaVariable(_) => {
            Err(SparkError::todo("unresolved named lambda variable"))
        }
        ExprType::CommonInlineUserDefinedFunction(_) => {
            Err(SparkError::todo("common inline user defined function"))
        }
        ExprType::CallFunction(_) => Err(SparkError::todo("call function")),
        ExprType::Extension(_) => Err(SparkError::unsupported("expression extension")),
    }
}

pub(crate) fn from_spark_literal_to_scalar(
    literal: &sc::expression::Literal,
) -> SparkResult<ScalarValue> {
    use sc::expression::literal::LiteralType;

    let sc::expression::Literal { literal_type } = literal;
    let literal_type = literal_type.as_ref().required("literal type")?;
    match literal_type {
        LiteralType::Null(_) => Ok(ScalarValue::Null),
        LiteralType::Binary(x) => Ok(ScalarValue::Binary(Some(x.clone()))),
        LiteralType::Boolean(x) => Ok(ScalarValue::Boolean(Some(*x))),
        LiteralType::Byte(x) => Ok(ScalarValue::Int8(Some(*x as i8))),
        LiteralType::Short(x) => Ok(ScalarValue::Int16(Some(*x as i16))),
        LiteralType::Integer(x) => Ok(ScalarValue::Int32(Some(*x))),
        LiteralType::Long(x) => Ok(ScalarValue::Int64(Some(*x))),
        LiteralType::Float(x) => Ok(ScalarValue::Float32(Some(*x))),
        LiteralType::Double(x) => Ok(ScalarValue::Float64(Some(*x))),
        LiteralType::Decimal(_) => Err(SparkError::todo("literal decimal")),
        LiteralType::String(x) => Ok(ScalarValue::Utf8(Some(x.clone()))),
        LiteralType::Date(_) => Err(SparkError::todo("literal date")),
        LiteralType::Timestamp(_) => Err(SparkError::todo("literal timestamp")),
        LiteralType::TimestampNtz(_) => Err(SparkError::todo("literal timestamp ntz")),
        LiteralType::CalendarInterval(_) => Err(SparkError::todo("literal calendar interval")),
        LiteralType::YearMonthInterval(_) => Err(SparkError::todo("literal year month interval")),
        LiteralType::DayTimeInterval(_) => Err(SparkError::todo("literal day time interval")),
        LiteralType::Array(_) => Err(SparkError::todo("literal array")),
        LiteralType::Map(_) => Err(SparkError::todo("literal map")),
        LiteralType::Struct(_) => Err(SparkError::todo("literal struct")),
    }
}
