use std::str::FromStr;
use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::datatypes::Date32Type;
use chrono::{NaiveTime, Timelike};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::catalog::Session;
use datafusion_common::{Column, DFSchema, DFSchemaRef, ScalarValue};
use datafusion_expr::expr::{self, BinaryExpr, ScalarFunction};
use datafusion_expr::{cast, try_cast, Expr, ExprSchemable, Operator};
use datafusion_functions::core::expr_ext::FieldAccessor;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::utils::datetime::localize_with_fallback;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_python_udf::udf::pyspark_unresolved_udf::PySparkUnresolvedUDF;
use sail_sql_analyzer::parser::{parse_date, parse_time, parse_timestamp};

use crate::config::{DefaultTimestampType, PlanConfig};
use crate::error::{PlanError, PlanResult};

pub fn resolve_row_local_sql_expression_without_session(
    sql: &str,
    schema: &Schema,
) -> PlanResult<expr::Expr> {
    let ast = sail_sql_analyzer::parser::parse_expression(sql)
        .map_err(|e| PlanError::invalid(format!("invalid SQL expression: {sql} ({e})")))?;
    let spec_expr = sail_sql_analyzer::expression::from_ast_expression(ast)
        .map_err(|e| PlanError::invalid(format!("invalid SQL expression: {sql} ({e})")))?;
    let df_schema = Arc::new(DFSchema::try_from(schema.clone())?);
    RowLocalExpressionResolver::new(df_schema, None).resolve(spec_expr)
}

pub fn resolve_row_local_sql_expression_with_session(
    session: &dyn Session,
    sql: &str,
    schema: &Schema,
) -> PlanResult<expr::Expr> {
    let ast = sail_sql_analyzer::parser::parse_expression(sql)
        .map_err(|e| PlanError::invalid(format!("invalid SQL expression: {sql} ({e})")))?;
    let spec_expr = sail_sql_analyzer::expression::from_ast_expression(ast)
        .map_err(|e| PlanError::invalid(format!("invalid SQL expression: {sql} ({e})")))?;
    let df_schema = Arc::new(DFSchema::try_from(schema.clone())?);
    RowLocalExpressionResolver::new(df_schema, Some(session)).resolve(spec_expr)
}

struct RowLocalExpressionResolver<'a> {
    df_schema: DFSchemaRef,
    session: Option<&'a dyn Session>,
    plan_config: Arc<PlanConfig>,
}

impl<'a> RowLocalExpressionResolver<'a> {
    fn new(df_schema: DFSchemaRef, session: Option<&'a dyn Session>) -> Self {
        let plan_config = if let Some(session) = session {
            let opts = session.config().options();
            let mut config = PlanConfig::default();
            if let Some(ref tz) = opts.execution.time_zone {
                config.session_timezone = Arc::from(tz.as_str());
            }
            Arc::new(config)
        } else {
            Arc::new(PlanConfig::default())
        };
        Self {
            df_schema,
            session,
            plan_config,
        }
    }

    fn resolve(&self, expr: spec::Expr) -> PlanResult<expr::Expr> {
        match expr {
            spec::Expr::Literal(literal) => {
                Ok(expr::Expr::Literal(self.resolve_literal(literal)?, None))
            }
            spec::Expr::UnresolvedDate { value } => self.resolve_date_literal(value),
            spec::Expr::UnresolvedTimestamp {
                value,
                timestamp_type,
            } => self.resolve_timestamp_literal(value, timestamp_type),
            spec::Expr::UnresolvedTime { value } => self.resolve_time_literal(value),
            spec::Expr::UnresolvedAttribute {
                name,
                plan_id: None,
                is_metadata_column: false,
            } => self.resolve_attribute(name),
            spec::Expr::UnresolvedAttribute {
                is_metadata_column: true,
                ..
            } => Err(PlanError::unsupported(
                "delta.invariants does not support metadata columns",
            )),
            spec::Expr::UnresolvedAttribute {
                plan_id: Some(_), ..
            } => Err(PlanError::unsupported(
                "delta.invariants does not support plan-qualified attributes",
            )),
            spec::Expr::UnresolvedExtractValue { child, extraction } => {
                self.resolve_extract_value(*child, *extraction)
            }
            spec::Expr::UnresolvedFunction(function) => self.resolve_function(function),
            spec::Expr::CallFunction {
                function_name,
                arguments,
            } => self.resolve_function(spec::UnresolvedFunction {
                function_name,
                arguments,
                named_arguments: vec![],
                is_distinct: false,
                is_user_defined_function: false,
                is_internal: None,
                ignore_nulls: None,
                filter: None,
                order_by: None,
            }),
            spec::Expr::Alias { expr, .. } => self.resolve(*expr),
            spec::Expr::Cast {
                expr,
                cast_to_type,
                rename: _,
                is_try,
            } => {
                let expr = self.resolve(*expr)?;
                let data_type = self.resolve_data_type(cast_to_type)?;
                Ok(if is_try {
                    try_cast(expr, data_type)
                } else {
                    cast(expr, data_type)
                })
            }
            spec::Expr::IsNull(expr) => Ok(expr::Expr::IsNull(Box::new(self.resolve(*expr)?))),
            spec::Expr::IsNotNull(expr) => {
                Ok(expr::Expr::IsNotNull(Box::new(self.resolve(*expr)?)))
            }
            spec::Expr::IsTrue(expr) => Ok(expr::Expr::IsTrue(Box::new(self.resolve(*expr)?))),
            spec::Expr::IsNotTrue(expr) => {
                Ok(expr::Expr::IsNotTrue(Box::new(self.resolve(*expr)?)))
            }
            spec::Expr::IsFalse(expr) => Ok(expr::Expr::IsFalse(Box::new(self.resolve(*expr)?))),
            spec::Expr::IsNotFalse(expr) => {
                Ok(expr::Expr::IsNotFalse(Box::new(self.resolve(*expr)?)))
            }
            spec::Expr::IsUnknown(expr) => {
                Ok(expr::Expr::IsUnknown(Box::new(self.resolve(*expr)?)))
            }
            spec::Expr::IsNotUnknown(expr) => {
                Ok(expr::Expr::IsNotUnknown(Box::new(self.resolve(*expr)?)))
            }
            spec::Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr = self.resolve(*expr)?;
                let low = self.resolve(*low)?;
                let high = self.resolve(*high)?;
                let greater_eq = expr::Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(expr.clone()),
                    Operator::GtEq,
                    Box::new(low),
                ));
                let less_eq = expr::Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(expr),
                    Operator::LtEq,
                    Box::new(high),
                ));
                let between = expr::Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(greater_eq),
                    Operator::And,
                    Box::new(less_eq),
                ));
                Ok(if negated {
                    expr::Expr::Not(Box::new(between))
                } else {
                    between
                })
            }
            spec::Expr::IsDistinctFrom { left, right } => {
                self.resolve_binary(*left, Operator::IsDistinctFrom, *right)
            }
            spec::Expr::IsNotDistinctFrom { left, right } => {
                self.resolve_binary(*left, Operator::IsNotDistinctFrom, *right)
            }
            spec::Expr::InList {
                expr,
                list,
                negated,
            } => Ok(expr::Expr::InList(expr::InList {
                expr: Box::new(self.resolve(*expr)?),
                list: list
                    .into_iter()
                    .map(|expr| self.resolve(expr))
                    .collect::<PlanResult<Vec<_>>>()?,
                negated,
            })),
            other => Err(PlanError::unsupported(format!(
                "delta.invariants does not support this expression in row-local resolution: {other:?}"
            ))),
        }
    }

    fn resolve_date_literal(&self, value: String) -> PlanResult<expr::Expr> {
        let date = parse_date(&value)?;
        Ok(expr::Expr::Literal(
            ScalarValue::Date32(Some(Date32Type::from_naive_date(date.try_into()?))),
            None,
        ))
    }

    fn resolve_timestamp_literal(
        &self,
        value: String,
        timestamp_type: spec::TimestampType,
    ) -> PlanResult<expr::Expr> {
        let (datetime, timezone) = parse_timestamp(&value).and_then(|x| x.into_naive())?;
        let timezone = if timezone.is_empty() {
            None
        } else {
            Some(timezone.parse::<Tz>()?)
        };
        let (datetime, timestamp_type) = match (
            timestamp_type,
            timezone,
            self.plan_config.default_timestamp_type,
        ) {
            (spec::TimestampType::Configured, None, DefaultTimestampType::TimestampLtz)
            | (spec::TimestampType::WithLocalTimeZone, None, _) => {
                let tz = Tz::from_str(&self.plan_config.session_timezone)?;
                let datetime = localize_with_fallback(&tz, &datetime)?;
                (datetime, spec::TimestampType::WithLocalTimeZone)
            }
            (spec::TimestampType::Configured, Some(tz), _)
            | (spec::TimestampType::WithLocalTimeZone, Some(tz), _) => {
                let datetime = localize_with_fallback(&tz, &datetime)?;
                (datetime, spec::TimestampType::WithLocalTimeZone)
            }
            (spec::TimestampType::Configured, None, DefaultTimestampType::TimestampNtz)
            | (spec::TimestampType::WithoutTimeZone, _, _) => {
                (datetime.and_utc(), spec::TimestampType::WithoutTimeZone)
            }
        };
        Ok(expr::Expr::Literal(
            ScalarValue::TimestampMicrosecond(
                Some(datetime.timestamp_micros()),
                self.resolve_timezone(&timestamp_type)?,
            ),
            None,
        ))
    }

    fn resolve_time_literal(&self, value: String) -> PlanResult<expr::Expr> {
        let time = parse_time(&value)?;
        let naive_time: NaiveTime = time.try_into()?;
        let seconds_from_midnight = naive_time.num_seconds_from_midnight() as i64;
        let nanoseconds = naive_time.nanosecond() as i64;
        let microseconds = seconds_from_midnight * 1_000_000 + nanoseconds / 1_000;
        Ok(expr::Expr::Literal(
            ScalarValue::Time64Microsecond(Some(microseconds)),
            None,
        ))
    }

    fn resolve_attribute(&self, name: spec::ObjectName) -> PlanResult<expr::Expr> {
        let parts: Vec<String> = name.into();
        let (base_name, mut data_type) =
            self.find_top_level_field(parts.first().ok_or_else(|| {
                PlanError::invalid("delta.invariants attribute must not be empty")
            })?)?;
        let mut expr = expr::Expr::Column(Column::new_unqualified(base_name));
        for segment in parts.iter().skip(1) {
            expr = self.resolve_struct_field(expr, &data_type, segment)?;
            data_type = expr.get_type(&self.df_schema)?;
        }
        Ok(expr)
    }

    fn resolve_extract_value(
        &self,
        child: spec::Expr,
        extraction: spec::Expr,
    ) -> PlanResult<expr::Expr> {
        let child = self.resolve(child)?;
        let data_type = child.get_type(&self.df_schema)?;
        match data_type {
            DataType::Struct(fields) => {
                let field_name = match extraction {
                    spec::Expr::Literal(spec::Literal::Utf8 { value: Some(name) })
                    | spec::Expr::Literal(spec::Literal::LargeUtf8 { value: Some(name) })
                    | spec::Expr::Literal(spec::Literal::Utf8View { value: Some(name) }) => name,
                    spec::Expr::UnresolvedAttribute {
                        name,
                        plan_id: None,
                        is_metadata_column: false,
                    } => {
                        let name: Vec<String> = name.into();
                        name.one()?
                    }
                    _ => {
                        return Err(PlanError::invalid(
                            "delta.invariants struct field extraction must be a field name",
                        ))
                    }
                };
                let field_name = self.find_struct_field_name(&fields, &field_name)?;
                Ok(child.field(field_name))
            }
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::Map(_, _) => Err(PlanError::unsupported(
                "delta.invariants does not support collection field extraction",
            )),
            _ => Err(PlanError::AnalysisError(format!(
                "cannot extract value from data type: {data_type}"
            ))),
        }
    }

    fn resolve_function(&self, function: spec::UnresolvedFunction) -> PlanResult<expr::Expr> {
        let spec::UnresolvedFunction {
            function_name,
            arguments,
            named_arguments,
            is_distinct,
            is_user_defined_function: _,
            is_internal,
            ignore_nulls,
            filter,
            order_by,
        } = function;
        if !named_arguments.is_empty()
            || is_distinct
            || is_internal.is_some()
            || ignore_nulls.is_some()
            || filter.is_some()
            || order_by.is_some()
        {
            return Err(PlanError::unsupported(
                "delta.invariants only supports simple row-local scalar function calls",
            ));
        }
        let function_name: Vec<String> = function_name.into();
        let function_name = function_name.one()?;
        let function_name = function_name.to_ascii_lowercase();
        let arguments = Self::convert_date_part_argument(&function_name, arguments);
        let arguments = arguments
            .into_iter()
            .map(|expr| self.resolve(expr))
            .collect::<PlanResult<Vec<_>>>()?;
        if let Some(session) = self.session {
            let task_ctx = session.task_ctx();
            if let Some(udf) = task_ctx.scalar_functions().get(&function_name) {
                return Ok(expr::Expr::ScalarFunction(ScalarFunction {
                    func: Arc::clone(udf),
                    args: arguments,
                }));
            }
            if let Ok(catalog_manager) = session.extension::<CatalogManager>() {
                if let Some(udf) = catalog_manager.get_function(&function_name)? {
                    if udf.inner().as_any().is::<PySparkUnresolvedUDF>() {
                        return Err(PlanError::unsupported(format!(
                            "delta.invariants does not support unresolved Python UDFs: {function_name}"
                        )));
                    }
                    return Ok(expr::Expr::ScalarFunction(ScalarFunction {
                        func: Arc::new(udf),
                        args: arguments,
                    }));
                }
            }
        }
        match (function_name.as_str(), arguments.len()) {
            ("!", 1) | ("not", 1) => Ok(expr::Expr::Not(Box::new(arguments.one()?))),
            ("+", 1) => Ok(arguments.one()?),
            ("-", 1) => Ok(expr::Expr::Negative(Box::new(arguments.one()?))),
            ("isnull", 1) => Ok(expr::Expr::IsNull(Box::new(arguments.one()?))),
            ("isnotnull", 1) => Ok(expr::Expr::IsNotNull(Box::new(arguments.one()?))),
            ("=", 2) | ("==", 2) => self.build_binary_expr(arguments, Operator::Eq),
            ("!=", 2) => self.build_binary_expr(arguments, Operator::NotEq),
            ("<", 2) => self.build_binary_expr(arguments, Operator::Lt),
            ("<=", 2) => self.build_binary_expr(arguments, Operator::LtEq),
            (">", 2) => self.build_binary_expr(arguments, Operator::Gt),
            (">=", 2) => self.build_binary_expr(arguments, Operator::GtEq),
            ("<=>", 2) => self.build_binary_expr(arguments, Operator::IsNotDistinctFrom),
            ("and", 2) => self.build_binary_expr(arguments, Operator::And),
            ("or", 2) => self.build_binary_expr(arguments, Operator::Or),
            ("+", 2) => self.build_binary_expr(arguments, Operator::Plus),
            ("-", 2) => self.build_binary_expr(arguments, Operator::Minus),
            ("*", 2) => self.build_binary_expr(arguments, Operator::Multiply),
            ("/", 2) => self.build_binary_expr(arguments, Operator::Divide),
            ("%", 2) => self.build_binary_expr(arguments, Operator::Modulo),
            _ => Err(PlanError::unsupported(format!(
                "delta.invariants function '{function_name}' is not supported in row-local resolution"
            ))),
        }
    }

    fn convert_date_part_argument(
        function_name: &str,
        mut arguments: Vec<spec::Expr>,
    ) -> Vec<spec::Expr> {
        const DATE_PART_FUNCTIONS: &[&str] = &["datediff", "date_diff", "timestampdiff"];
        if arguments.len() >= 3 && DATE_PART_FUNCTIONS.contains(&function_name) {
            if let spec::Expr::UnresolvedAttribute { ref name, .. } = arguments[0] {
                let parts: Vec<String> = name.clone().into();
                if parts.len() == 1 {
                    arguments[0] = spec::Expr::Literal(spec::Literal::Utf8 {
                        value: Some(parts.into_iter().next().unwrap_or_default()),
                    });
                }
            }
        }
        arguments
    }

    fn build_binary_expr(
        &self,
        arguments: Vec<Expr>,
        operator: Operator,
    ) -> PlanResult<expr::Expr> {
        let (left, right) = arguments.two()?;
        Ok(expr::Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            operator,
            Box::new(right),
        )))
    }

    fn resolve_binary(
        &self,
        left: spec::Expr,
        operator: Operator,
        right: spec::Expr,
    ) -> PlanResult<expr::Expr> {
        Ok(expr::Expr::BinaryExpr(BinaryExpr::new(
            Box::new(self.resolve(left)?),
            operator,
            Box::new(self.resolve(right)?),
        )))
    }

    fn find_top_level_field(&self, name: &str) -> PlanResult<(String, DataType)> {
        let matches = self
            .df_schema
            .as_arrow()
            .fields()
            .iter()
            .filter(|field| field.name().eq_ignore_ascii_case(name))
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [field] => Ok((field.name().clone(), field.data_type().clone())),
            [] => Err(PlanError::AnalysisError(format!(
                "cannot resolve attribute: {name}"
            ))),
            _ => Err(PlanError::AnalysisError(format!(
                "ambiguous attribute: {name}"
            ))),
        }
    }

    fn resolve_struct_field(
        &self,
        expr: expr::Expr,
        data_type: &DataType,
        name: &str,
    ) -> PlanResult<expr::Expr> {
        match data_type {
            DataType::Struct(fields) => {
                let field_name = self.find_struct_field_name(fields, name)?;
                Ok(expr.field(field_name))
            }
            other => Err(PlanError::AnalysisError(format!(
                "cannot resolve nested field '{name}' from data type: {other}"
            ))),
        }
    }

    fn find_struct_field_name(
        &self,
        fields: &datafusion::arrow::datatypes::Fields,
        name: &str,
    ) -> PlanResult<String> {
        let matches = fields
            .iter()
            .filter(|field| field.name().eq_ignore_ascii_case(name))
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [field] => Ok(field.name().clone()),
            [] => Err(PlanError::AnalysisError(format!("missing field: {name}"))),
            _ => Err(PlanError::AnalysisError(format!("ambiguous field: {name}"))),
        }
    }

    fn resolve_literal(&self, literal: spec::Literal) -> PlanResult<ScalarValue> {
        match literal {
            spec::Literal::Null => Ok(ScalarValue::Null),
            spec::Literal::Boolean { value } => Ok(ScalarValue::Boolean(value)),
            spec::Literal::Int8 { value } => Ok(ScalarValue::Int8(value)),
            spec::Literal::Int16 { value } => Ok(ScalarValue::Int16(value)),
            spec::Literal::Int32 { value } => Ok(ScalarValue::Int32(value)),
            spec::Literal::Int64 { value } => Ok(ScalarValue::Int64(value)),
            spec::Literal::UInt8 { value } => Ok(ScalarValue::UInt8(value)),
            spec::Literal::UInt16 { value } => Ok(ScalarValue::UInt16(value)),
            spec::Literal::UInt32 { value } => Ok(ScalarValue::UInt32(value)),
            spec::Literal::UInt64 { value } => Ok(ScalarValue::UInt64(value)),
            spec::Literal::Float16 { value } => Ok(ScalarValue::Float16(value)),
            spec::Literal::Float32 { value } => Ok(ScalarValue::Float32(value)),
            spec::Literal::Float64 { value } => Ok(ScalarValue::Float64(value)),
            spec::Literal::Date32 { days } => Ok(ScalarValue::Date32(days)),
            spec::Literal::Date64 { milliseconds } => Ok(ScalarValue::Date64(milliseconds)),
            spec::Literal::Binary { value } => Ok(ScalarValue::Binary(value)),
            spec::Literal::FixedSizeBinary { size, value } => {
                Ok(ScalarValue::FixedSizeBinary(size, value))
            }
            spec::Literal::LargeBinary { value } => Ok(ScalarValue::LargeBinary(value)),
            spec::Literal::BinaryView { value } => Ok(ScalarValue::BinaryView(value)),
            spec::Literal::Utf8 { value } => Ok(ScalarValue::Utf8(value)),
            spec::Literal::LargeUtf8 { value } => Ok(ScalarValue::LargeUtf8(value)),
            spec::Literal::Utf8View { value } => Ok(ScalarValue::Utf8View(value)),
            spec::Literal::Decimal128 {
                precision,
                scale,
                value,
            } => Ok(ScalarValue::Decimal128(value, precision, scale)),
            spec::Literal::Decimal256 {
                precision,
                scale,
                value,
            } => Ok(ScalarValue::Decimal256(value, precision, scale)),
            other => Err(PlanError::unsupported(format!(
                "delta.invariants literal is not supported in row-local resolution: {other:?}"
            ))),
        }
    }

    fn resolve_data_type(&self, data_type: spec::DataType) -> PlanResult<DataType> {
        match data_type {
            spec::DataType::Null => Ok(DataType::Null),
            spec::DataType::Boolean => Ok(DataType::Boolean),
            spec::DataType::Int8 => Ok(DataType::Int8),
            spec::DataType::Int16 => Ok(DataType::Int16),
            spec::DataType::Int32 => Ok(DataType::Int32),
            spec::DataType::Int64 => Ok(DataType::Int64),
            spec::DataType::UInt8 => Ok(DataType::UInt8),
            spec::DataType::UInt16 => Ok(DataType::UInt16),
            spec::DataType::UInt32 => Ok(DataType::UInt32),
            spec::DataType::UInt64 => Ok(DataType::UInt64),
            spec::DataType::Float16 => Ok(DataType::Float16),
            spec::DataType::Float32 => Ok(DataType::Float32),
            spec::DataType::Float64 => Ok(DataType::Float64),
            spec::DataType::Timestamp {
                time_unit,
                timestamp_type,
            } => Ok(DataType::Timestamp(
                Self::resolve_time_unit(&time_unit)?,
                self.resolve_timezone(&timestamp_type)?,
            )),
            spec::DataType::Binary => Ok(DataType::Binary),
            spec::DataType::FixedSizeBinary { size } => Ok(DataType::FixedSizeBinary(size)),
            spec::DataType::LargeBinary => Ok(DataType::LargeBinary),
            spec::DataType::BinaryView => Ok(DataType::BinaryView),
            spec::DataType::Utf8 => Ok(DataType::Utf8),
            spec::DataType::LargeUtf8 => Ok(DataType::LargeUtf8),
            spec::DataType::Utf8View => Ok(DataType::Utf8View),
            spec::DataType::Date32 => Ok(DataType::Date32),
            spec::DataType::Date64 => Ok(DataType::Date64),
            spec::DataType::Time32 { time_unit } => {
                Ok(DataType::Time32(Self::resolve_time_unit(&time_unit)?))
            }
            spec::DataType::Time64 { time_unit } => {
                Ok(DataType::Time64(Self::resolve_time_unit(&time_unit)?))
            }
            spec::DataType::Duration { time_unit } => {
                Ok(DataType::Duration(Self::resolve_time_unit(&time_unit)?))
            }
            spec::DataType::Decimal128 { precision, scale } => {
                Ok(DataType::Decimal128(precision, scale))
            }
            spec::DataType::Decimal256 { precision, scale } => {
                Ok(DataType::Decimal256(precision, scale))
            }
            other => Err(PlanError::unsupported(format!(
                "delta.invariants cast target is not supported in row-local resolution: {other:?}"
            ))),
        }
    }

    fn resolve_time_unit(
        time_unit: &spec::TimeUnit,
    ) -> PlanResult<datafusion::arrow::datatypes::TimeUnit> {
        match time_unit {
            spec::TimeUnit::Second => Ok(datafusion::arrow::datatypes::TimeUnit::Second),
            spec::TimeUnit::Millisecond => Ok(datafusion::arrow::datatypes::TimeUnit::Millisecond),
            spec::TimeUnit::Microsecond => Ok(datafusion::arrow::datatypes::TimeUnit::Microsecond),
            spec::TimeUnit::Nanosecond => Ok(datafusion::arrow::datatypes::TimeUnit::Nanosecond),
        }
    }

    fn resolve_timezone(
        &self,
        timestamp_type: &spec::TimestampType,
    ) -> PlanResult<Option<Arc<str>>> {
        match timestamp_type {
            spec::TimestampType::Configured => match self.plan_config.default_timestamp_type {
                DefaultTimestampType::TimestampLtz => {
                    Ok(Some(Arc::clone(&self.plan_config.session_timezone)))
                }
                DefaultTimestampType::TimestampNtz => Ok(None),
            },
            spec::TimestampType::WithLocalTimeZone => {
                Ok(Some(Arc::clone(&self.plan_config.session_timezone)))
            }
            spec::TimestampType::WithoutTimeZone => Ok(None),
        }
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use datafusion::arrow::datatypes::{Field, TimeUnit};
    use datafusion::execution::context::SessionContext;

    use super::*;

    #[test]
    fn resolves_builtin_row_local_function_with_session() {
        let schema = Schema::new(vec![Field::new("value", DataType::Int32, true)]);
        let ctx = SessionContext::new();
        let state = ctx.state();

        let expr = resolve_row_local_sql_expression_with_session(&state, "abs(value) > 0", &schema)
            .expect("row-local built-in should resolve with session");
        let df_schema = DFSchema::try_from(schema).expect("schema should convert");

        assert_eq!(
            expr.get_type(&df_schema).expect("expr type"),
            DataType::Boolean
        );
    }

    #[test]
    fn resolves_session_scalar_udf() {
        let schema = Schema::new(vec![Field::new("value", DataType::Float64, true)]);
        let ctx = SessionContext::new();
        let state = ctx.state();

        let expr =
            resolve_row_local_sql_expression_with_session(&state, "sqrt(value) > 1.0", &schema)
                .expect("session scalar function should resolve");
        let df_schema = DFSchema::try_from(schema).expect("schema should convert");

        assert_eq!(
            expr.get_type(&df_schema).expect("expr type"),
            DataType::Boolean
        );
    }

    #[test]
    fn resolves_temporal_literals_in_row_local_sql() {
        let schema = Schema::new(vec![
            Field::new("d", DataType::Date32, true),
            Field::new("t", DataType::Time64(TimeUnit::Microsecond), true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                true,
            ),
        ]);

        let expr = resolve_row_local_sql_expression_without_session(
            "d >= DATE '2026-01-01' AND t >= TIME '01:02:03' AND ts >= TIMESTAMP '2026-01-01 00:00:00'",
            &schema,
        )
        .expect("temporal literals should resolve");
        let df_schema = DFSchema::try_from(schema).expect("schema should convert");

        assert_eq!(
            expr.get_type(&df_schema).expect("expr type"),
            DataType::Boolean
        );
    }
}
