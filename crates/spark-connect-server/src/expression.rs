use crate::error::{ProtoFieldExt, SparkError};
use crate::schema::from_spark_data_type;
use crate::spark::connect as sc;
use crate::spark::connect::expression as sce;
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::TableReference;
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{expr, AggregateUDF, ScalarUDF, TableSource, WindowUDF};
use datafusion::sql::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::{Parser, WildcardExpr};
use std::sync::Arc;

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
    sort_order: &sce::SortOrder,
    schema: &DFSchema,
) -> Result<expr::Expr, SparkError> {
    let expr = sort_order
        .child
        .as_ref()
        .required("expression for sort order")?;
    let asc = match sce::sort_order::SortDirection::try_from(sort_order.direction) {
        Ok(sce::sort_order::SortDirection::Ascending) => true,
        Ok(sce::sort_order::SortDirection::Descending) => false,
        Ok(sce::sort_order::SortDirection::Unspecified) => true,
        Err(_) => {
            return Err(SparkError::invalid("invalid sort direction"));
        }
    };
    let nulls_first = match sce::sort_order::NullOrdering::try_from(sort_order.null_ordering) {
        Ok(sce::sort_order::NullOrdering::SortNullsFirst) => true,
        Ok(sce::sort_order::NullOrdering::SortNullsLast) => false,
        Ok(sce::sort_order::NullOrdering::SortNullsUnspecified) => asc,
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
) -> Result<expr::Expr, SparkError> {
    let sc::Expression { expr_type } = expr;
    let expr_type = expr_type.as_ref().required("expression type")?;
    match expr_type {
        sce::ExprType::Literal(literal) => {
            Ok(expr::Expr::Literal(from_spark_literal_to_scalar(literal)?))
        }
        sce::ExprType::UnresolvedAttribute(attr) => {
            let col = Column::new_unqualified(&attr.unparsed_identifier);
            Ok(expr::Expr::Column(col))
        }
        sce::ExprType::UnresolvedFunction(_) => {
            todo!()
        }
        sce::ExprType::ExpressionString(expr) => {
            let dialect = GenericDialect {};
            let mut parser = Parser::new(&dialect).try_with_sql(&expr.expression)?;
            let provider = ExpressionContextProvider::default();
            let planner = SqlToRel::new(&provider);
            let expr = planner.sql_to_expr(
                parser.parse_expr()?,
                schema,
                &mut PlannerContext::default(),
            )?;
            Ok(expr)
        }
        sce::ExprType::UnresolvedStar(star) => {
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
        sce::ExprType::Alias(alias) => {
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
                expr: Box::new(from_spark_expression(expr, schema)?),
                data_type,
            }))
        }
        sce::ExprType::UnresolvedRegex(_) => {
            todo!()
        }
        sce::ExprType::SortOrder(sort) => from_spark_sort_order(sort, schema),
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
