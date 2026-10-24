use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_cast_string_to_int32::SparkCastStringToInt32;
use sail_function::scalar::spark_to_string::SparkToUtf8;

use super::Resolver;
use crate::analysis::ir::*;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    pub(super) fn conditional_function(
        &mut self,
        name: &str,
        values: &mut Vec<LogicalExpression>,
    ) -> PlanResult<Option<LogicalExpression>> {
        if !matches!(
            name,
            "case" | "when" | "if" | "coalesce" | "nvl" | "ifnull" | "nullifzero" | "zeroifnull"
        ) {
            return Ok(None);
        }
        let values = std::mem::take(values);
        let result = match name {
            "case" | "when" => {
                let mut conditions = vec![];
                let mut branches = vec![];
                let mut values = values.into_iter();
                while let Some(condition) = values.next() {
                    if let Some(value) = values.next() {
                        conditions.push(condition);
                        branches.push(value);
                    } else {
                        conditions.push(self.literal(ScalarValue::Boolean(Some(true))));
                        branches.push(condition);
                    }
                }
                let branches = self.temporal_branches(branches)?;
                self.case(conditions.into_iter().zip(branches).collect(), None)?
            }
            "if" => {
                let (condition, yes, no) = values.three()?;
                let (yes, no) = self.temporal_branches(vec![yes, no])?.two()?;
                self.case(vec![(condition, yes)], Some(no))?
            }
            "coalesce" | "nvl" | "ifnull" => {
                if name != "coalesce" && values.len() != 2 {
                    return Err(PlanError::analysis(format!("{name} requires 2 parameters")));
                }
                let values = self.temporal_branches(values)?;
                self.scalar("coalesce", values)?
            }
            "nullifzero" => {
                let value = values.one()?;
                let data_type = &value.result_type.resolved("nullifzero input")?.data_type;
                let copy = self.copy_expression(&value)?;
                let condition = if data_type.is_string() {
                    let copy = if self.context.config.ansi_mode {
                        self.cast(copy, DataType::Int64)?
                    } else {
                        self.scalar_udf(
                            Arc::new(ScalarUDF::from(SparkCastStringToInt32::new())),
                            vec![copy],
                        )?
                    };
                    let zero = self.literal(ScalarValue::Int32(Some(0)));
                    self.binary(copy, Operator::Eq, zero)?
                } else if *data_type == DataType::Boolean && !self.context.config.ansi_mode {
                    let no = self.literal(ScalarValue::Boolean(Some(false)));
                    self.binary(copy, Operator::Eq, no)?
                } else if data_type.is_numeric() || data_type.is_null() {
                    let zero = self.literal(ScalarValue::Int32(Some(0)));
                    self.binary(copy, Operator::Eq, zero)?
                } else {
                    return Err(PlanError::invalid(format!(
                        "nullifzero cannot compare data type {data_type} with an integer zero"
                    )));
                };
                let null = self.literal(ScalarValue::Null);
                self.case(vec![(condition, null)], Some(value))?
            }
            "zeroifnull" => {
                let value = values.one()?;
                let data_type = value
                    .result_type
                    .resolved("zeroifnull input")?
                    .data_type
                    .clone();
                let ansi = self.context.config.ansi_mode;
                let mut zero = self.literal(ScalarValue::Int32(Some(0)));
                let value = match data_type {
                    DataType::Float32 if ansi => self.cast(value, DataType::Float64)?,
                    data_type if data_type.is_string() && ansi => {
                        self.cast(value, DataType::Int64)?
                    }
                    data_type if data_type.is_string() => {
                        zero = self.literal(ScalarValue::Utf8(Some("0".into())));
                        value
                    }
                    DataType::Decimal128(precision, scale) => {
                        let target = crate::analysis::typing::coercion::zeroifnull_decimal_type(
                            precision, scale,
                        );
                        if ansi {
                            self.cast(value, target)?
                        } else {
                            self.try_cast(value, target)?
                        }
                    }
                    data_type if data_type.is_numeric() || data_type.is_null() => value,
                    data_type => {
                        return Err(PlanError::invalid(format!(
                            "zeroifnull cannot combine data type {data_type} with an integer zero"
                        )));
                    }
                };
                self.scalar("coalesce", vec![value, zero])?
            }
            _ => return Err(PlanError::internal("unknown conditional function")),
        };
        Ok(Some(result))
    }

    pub(super) fn temporal_branches(
        &mut self,
        values: Vec<LogicalExpression>,
    ) -> PlanResult<Vec<LogicalExpression>> {
        let types = values
            .iter()
            .map(|v| {
                Ok(v.result_type
                    .resolved("conditional result")?
                    .data_type
                    .clone())
            })
            .collect::<PlanResult<Vec<_>>>()?;
        if !types.iter().any(DataType::is_string) {
            return Ok(values);
        }
        let Some(target) = crate::analysis::typing::temporal::common_temporal_type(
            &types,
            &self.context.config.session_timezone,
        ) else {
            return Ok(values);
        };
        values
            .into_iter()
            .zip(types)
            .map(|(value, source)| {
                let temporal = matches!(
                    source,
                    DataType::Date32 | DataType::Date64 | DataType::Timestamp(..)
                );
                if self.context.config.ansi_mode {
                    if source == target {
                        return Ok(value);
                    }
                    if source.is_string() {
                        let function = match &target {
                            DataType::Date32 => ScalarUDF::from(SparkDate::new(false)),
                            DataType::Timestamp(_, timezone) => ScalarUDF::from(
                                SparkTimestamp::try_new(timezone.clone(), true, false)?,
                            ),
                            _ => return self.cast(value, target.clone()),
                        };
                        self.scalar_udf(Arc::new(function), vec![value])
                    } else if temporal {
                        self.cast(value, target.clone())
                    } else {
                        Ok(value)
                    }
                } else if temporal {
                    self.scalar_udf(Arc::new(ScalarUDF::from(SparkToUtf8::new())), vec![value])
                } else {
                    Ok(value)
                }
            })
            .collect()
    }
}
