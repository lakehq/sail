use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use rand::{RngExt, rng};
use sail_common::spec;
use sail_function::scalar::math::rand_poisson::RandPoisson;
use sail_function::scalar::math::random::Random;

use super::Resolver;
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::analysis::typing;
use crate::error::{PlanError, PlanResult};

const SAMPLE_ROUNDING_EPSILON: f64 = 1e-6;

/// Copied from `arrow_ord::rank::can_rank` (private in arrow-ord).
fn can_rank(data_type: &DataType) -> bool {
    data_type.is_primitive()
        || matches!(
            data_type,
            DataType::Boolean
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary
        )
}

/// Copied from `arrow_ord::sort::can_sort_to_indices` (private in arrow-ord).
fn can_sort_to_indices(data_type: &DataType) -> bool {
    data_type.is_primitive()
        || matches!(
            data_type,
            DataType::Boolean
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::FixedSizeBinary(_)
        )
        || match data_type {
            DataType::List(f) if can_rank(f.data_type()) => true,
            DataType::LargeList(f) if can_rank(f.data_type()) => true,
            DataType::FixedSizeList(f, _) if can_rank(f.data_type()) => true,
            DataType::Dictionary(_, values) if can_rank(values.as_ref()) => true,
            DataType::RunEndEncoded(_, f) if can_sort_to_indices(f.data_type()) => true,
            _ => false,
        }
}

impl Resolver<'_, '_> {
    #[async_recursion::async_recursion]
    pub(super) async fn sample(&mut self, sample: spec::Sample) -> PlanResult<LogicalPlan> {
        let mut input = self.query(*sample.input).await?;
        if sample.deterministic_order {
            let schema = input.schema.resolved("sample input")?;
            let mut order = vec![];
            for attribute in &schema.attributes {
                if !attribute.hidden
                    && can_sort_to_indices(
                        &attribute
                            .result_type
                            .resolved("sample sort type")?
                            .data_type,
                    )
                {
                    order.push(SortExpression {
                        expression: self.reference(attribute)?,
                        ascending: true,
                        nulls_first: true,
                    });
                }
            }
            if !order.is_empty() {
                input = LogicalPlan {
                    schema: input.schema.clone(),
                    node: Deferred::Resolved(PlanNode::Sort {
                        input: Box::new(input),
                        order,
                        global: false,
                        required: false,
                    }),
                };
            }
        }
        self.apply_sample(
            input,
            sample.lower_bound,
            sample.upper_bound,
            sample.with_replacement,
            sample.seed.unwrap_or_else(|| rng().random()),
        )
    }

    #[async_recursion::async_recursion]
    pub(super) async fn table_sample(
        &mut self,
        input: LogicalPlan,
        sample: spec::TableSample,
    ) -> PlanResult<LogicalPlan> {
        let fraction = match sample.method {
            spec::TableSampleMethod::Percent { value } => {
                let value = self
                    .expression(value, &PlanSchema::default())
                    .await?
                    .expression;
                let value = typing::coerce(value, &DataType::Float64, &mut self.context.ids)?;
                let ScalarValue::Float64(Some(value)) =
                    crate::analysis::lowering::evaluate_constant(&value)?
                else {
                    return Err(PlanError::invalid(
                        "TABLESAMPLE requires a numeric expression",
                    ));
                };
                value / 100.0
            }
            spec::TableSampleMethod::Bucket {
                numerator,
                denominator,
            } => numerator as f64 / denominator as f64,
            spec::TableSampleMethod::Rows { .. } => {
                return Err(PlanError::todo("TABLESAMPLE with ROWS"));
            }
        };
        self.apply_sample(
            input,
            0.0,
            fraction,
            false,
            sample.seed.unwrap_or_else(|| rng().random()),
        )
    }

    fn apply_sample(
        &mut self,
        input: LogicalPlan,
        lower: f64,
        upper: f64,
        replacement: bool,
        seed: i64,
    ) -> PlanResult<LogicalPlan> {
        let fraction = upper - lower;
        let valid_fraction = if replacement {
            fraction >= -SAMPLE_ROUNDING_EPSILON
        } else {
            (-SAMPLE_ROUNDING_EPSILON..=1.0 + SAMPLE_ROUNDING_EPSILON).contains(&fraction)
        };
        if !valid_fraction {
            let requirement = if replacement {
                "nonnegative with replacement"
            } else {
                "on interval [0, 1] without replacement"
            };
            return Err(PlanError::invalid(format!(
                "Sampling fraction ({fraction}) must be {requirement}"
            )));
        }

        // TODO: Move these individual-bound checks into the partition-scoped sampler
        // described below. Spark constructs `BernoulliCellSampler` during execution, so
        // schema-only analysis accepts fraction-valid bounds. Keep the eager checks until
        // that sampler exists; removing them now would silently execute invalid ranges.
        if !replacement {
            if lower > upper + SAMPLE_ROUNDING_EPSILON {
                return Err(PlanError::invalid(format!(
                    "Lower bound ({lower}) must be <= upper bound ({upper})"
                )));
            }
            if lower < -SAMPLE_ROUNDING_EPSILON {
                return Err(PlanError::invalid(format!(
                    "Lower bound ({lower}) must be >= 0.0"
                )));
            }
            if upper > 1.0 + SAMPLE_ROUNDING_EPSILON {
                return Err(PlanError::invalid(format!(
                    "Upper bound ({upper}) must be <= 1.0"
                )));
            }
        }

        let original = input.schema.resolved("sample input")?.attributes.clone();
        let seed = self.literal(ScalarValue::Int64(Some(seed)));
        let random = if replacement {
            let fraction = self.literal(ScalarValue::Float64(Some(fraction.max(0.0))));
            self.scalar_udf(
                Arc::new(ScalarUDF::from(RandPoisson::new())),
                vec![fraction, seed],
            )?
        } else {
            self.scalar_udf(Arc::new(ScalarUDF::from(Random::new())), vec![seed])?
        };
        let (input, random) = self.append(input, random, "sample_random".into())?;
        let input = if replacement {
            let one = self.literal(ScalarValue::Int64(Some(1)));
            let count = self.reference(&random)?;
            let repeat = self.scalar("array_repeat", vec![one, count])?;
            let (input, repeat) = self.append(input, repeat, "sample_repetitions".into())?;
            self.unnest(input, &repeat, false)?.0
        } else {
            let value = self.reference(&random)?;
            let bound = self.literal(ScalarValue::Float64(Some(upper)));
            let condition = self.binary(value, Operator::Lt, bound)?;
            let input = Self::filter(input, condition)?;
            let value = self.reference(&random)?;
            let bound = self.literal(ScalarValue::Float64(Some(lower)));
            let condition = self.binary(value, Operator::GtEq, bound)?;
            Self::filter(input, condition)?
        };
        self.select_attributes(input, original)
    }
}
