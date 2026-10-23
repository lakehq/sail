use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use sail_catalog::manager::CatalogManager;
use sail_catalog::utils::quote_namespace_if_needed;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::misc::hll_sketch::HllUnionFunction;
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_function::scalar::misc::theta_sketch::ThetaUnionFunction;
use sail_function::sketch::DEFAULT_THETA_LG_NOM_ENTRIES;

use super::Resolver;
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    pub(super) fn misc_function(
        &mut self,
        name: &str,
        values: &mut Vec<LogicalExpression>,
        names: &[String],
    ) -> PlanResult<Option<LogicalExpression>> {
        if !matches!(
            name,
            "assert_true"
                | "current_catalog"
                | "current_database"
                | "current_schema"
                | "current_user"
                | "session_user"
                | "user"
                | "input_file_name"
                | "typeof"
                | "bitmap_bit_position"
                | "bitmap_bucket_number"
                | "theta_union"
                | "hll_union"
                | "sha2"
                | "shiftrightunsigned"
                | ">>>"
                | "~"
        ) {
            return Ok(None);
        }
        let values = std::mem::take(values);
        let result = match name {
            "current_catalog" => {
                values.zero()?;
                let value = self
                    .context
                    .session
                    .extension::<CatalogManager>()?
                    .default_catalog()?
                    .to_string();
                self.literal(ScalarValue::Utf8(Some(value)))
            }
            "current_database" | "current_schema" => {
                values.zero()?;
                let value = quote_namespace_if_needed(
                    &self
                        .context
                        .session
                        .extension::<CatalogManager>()?
                        .default_database()?,
                );
                self.literal(ScalarValue::Utf8(Some(value)))
            }
            "current_user" | "session_user" | "user" => {
                values.zero()?;
                self.literal(ScalarValue::Utf8(Some(
                    self.context.config.session_user_id.clone(),
                )))
            }
            "input_file_name" => {
                values.zero()?;
                let value = self.scalar("input_file_name", vec![])?;
                let empty = self.literal(ScalarValue::Utf8(Some("".into())));
                self.scalar("coalesce", vec![value, empty])?
            }
            "typeof" => {
                let value = values.one()?;
                let data_type = &value.result_type.resolved("typeof argument")?.data_type;
                let name = self
                    .context
                    .session
                    .extension::<PlanService>()?
                    .plan_formatter()
                    .data_type_to_simple_string(data_type)?;
                self.literal(ScalarValue::Utf8(Some(name)))
            }
            "assert_true" => {
                let (value, error) = match values.len() {
                    1 => {
                        let message = format!(
                            "'{}' is not true!",
                            names.first().map(String::as_str).unwrap_or("")
                        );
                        (
                            values.one()?,
                            self.literal(ScalarValue::Utf8(Some(message))),
                        )
                    }
                    2 => values.two()?,
                    n => {
                        return Err(PlanError::invalid(format!(
                            "assert_true expects at most two arguments, got {n}"
                        )));
                    }
                };
                let condition = self.not(value)?;
                let error =
                    self.scalar_udf(Arc::new(ScalarUDF::from(RaiseError::new())), vec![error])?;
                let null = self.literal(ScalarValue::Null);
                self.case(vec![(condition, error)], Some(null))?
            }
            "hll_union" | "theta_union" => {
                let mut values = values;
                match values.len() {
                    2 => values.push(self.literal(if name == "hll_union" {
                        ScalarValue::Boolean(Some(false))
                    } else {
                        ScalarValue::Int32(Some(DEFAULT_THETA_LG_NOM_ENTRIES))
                    })),
                    3 => {
                        let value = values
                            .pop()
                            .ok_or_else(|| PlanError::internal("sketch option missing"))?;
                        values.push(self.cast(
                            value,
                            if name == "hll_union" {
                                DataType::Boolean
                            } else {
                                DataType::Int32
                            },
                        )?);
                    }
                    n => {
                        return Err(PlanError::invalid(format!(
                            "{name} requires 2 or 3 arguments, got {n}"
                        )));
                    }
                }
                let function = if name == "hll_union" {
                    ScalarUDF::from(HllUnionFunction::new())
                } else {
                    ScalarUDF::from(ThetaUnionFunction::new())
                };
                self.scalar_udf(Arc::new(function), values)?
            }
            "sha2" => {
                let (value, bits) = values.two()?;
                let function = match bits.node.resolved("sha2 bit length")? {
                    ExpressionNode::Literal(ScalarValue::Int32(Some(0 | 256))) => "sha256",
                    ExpressionNode::Literal(ScalarValue::Int32(Some(224))) => "sha224",
                    ExpressionNode::Literal(ScalarValue::Int32(Some(384))) => "sha384",
                    ExpressionNode::Literal(ScalarValue::Int32(Some(512))) => "sha512",
                    _ => {
                        return Err(PlanError::invalid(
                            "The second argument of sha2 must be a literal integer.",
                        ));
                    }
                };
                let value = self.scalar(function, vec![value])?;
                let value =
                    self.scalar_udf(datafusion_spark::function::math::hex(), vec![value])?;
                self.scalar("lower", vec![value])?
            }
            "~" => {
                let value = values.one()?;
                let negative = self.raw_negative(value);
                let one = self.literal(ScalarValue::Int32(Some(1)));
                self.binary(negative, Operator::Minus, one)?
            }
            "shiftrightunsigned" | ">>>" => {
                let (value, shift) = values.two()?;
                let target = value
                    .result_type
                    .resolved("unsigned shift input")?
                    .data_type
                    .clone();
                let (unsigned, max) = match target {
                    DataType::Int32 => (DataType::UInt32, u64::from(u32::MAX)),
                    DataType::Int64 => (DataType::UInt64, u64::MAX),
                    _ => {
                        return Err(PlanError::invalid(format!(
                            "`shiftrightunsigned`: unsupported input type {target:?}"
                        )));
                    }
                };
                let copy = self.copy_expression(&value)?;
                let zero = self.literal(ScalarValue::Int32(Some(0)));
                let negative = self.binary(copy, Operator::Lt, zero)?;
                let copy = self.copy_expression(&value)?;
                let absolute = self.scalar("abs", vec![copy])?;
                let one = self.literal(ScalarValue::Int32(Some(1)));
                let distance = self.binary(absolute, Operator::Minus, one)?;
                let maximum = self.literal(ScalarValue::UInt64(Some(max)));
                let wrapped = self.binary(maximum, Operator::Minus, distance)?;
                let value = self.case(vec![(negative, wrapped)], Some(value))?;
                let value = self.cast(value, unsigned)?;
                let value = self.binary(value, Operator::BitwiseShiftRight, shift)?;
                self.cast(value, target)?
            }
            "bitmap_bit_position" | "bitmap_bucket_number" => {
                let value = values.one()?;
                let copy = self.copy_expression(&value)?;
                let zero = self.literal(ScalarValue::Int32(Some(0)));
                let positive = self.binary(copy, Operator::Gt, zero)?;
                let copy = self.copy_expression(&value)?;
                let one = self.literal(ScalarValue::Int32(Some(1)));
                let offset = self.binary(copy, Operator::Minus, one)?;
                let bits = self.literal(ScalarValue::Int32(Some(32768)));
                let operator = if name == "bitmap_bit_position" {
                    Operator::Modulo
                } else {
                    Operator::Divide
                };
                let mut a = self.binary(offset, operator, bits)?;
                let b = if name == "bitmap_bit_position" {
                    self.raw_negative(value)
                } else {
                    let one = self.literal(ScalarValue::Int32(Some(1)));
                    a = self.binary(one, Operator::Plus, a)?;
                    value
                };
                let bits = self.literal(ScalarValue::Int32(Some(32768)));
                let b = self.binary(b, operator, bits)?;
                let yes = self.literal(ScalarValue::Boolean(Some(true)));
                self.case(vec![(positive, a), (yes, b)], None)?
            }
            _ => return Err(PlanError::internal("unknown miscellaneous function")),
        };
        Ok(Some(result))
    }

    fn raw_negative(&mut self, value: LogicalExpression) -> LogicalExpression {
        LogicalExpression {
            id: self.context.ids.expression(),
            result_type: value.result_type.clone(),
            node: Deferred::Resolved(ExpressionNode::Negative(Box::new(value))),
        }
    }
}
