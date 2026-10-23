use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use sail_common::spec;
use sail_function::scalar::math::random::Random;

use super::{NamedExpression, Resolver};
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::analysis::scope::ResolutionScope;
use crate::analysis::typing;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    #[async_recursion::async_recursion]
    pub(super) async fn crosstab(
        &mut self,
        input: spec::QueryPlan,
        left: spec::Identifier,
        right: spec::Identifier,
    ) -> PlanResult<LogicalPlan> {
        let name = format!("{}_{}", left.as_ref(), right.as_ref());
        let input = self.query(input).await?;
        let schema = input.schema.resolved("crosstab input")?;
        let scope = ResolutionScope::new(schema, [])
            .with_case_sensitive(self.context.config.case_sensitive);
        let left = schema
            .attribute(scope.resolve(&[left.into()], None)?.id)?
            .clone();
        let right = schema
            .attribute(scope.resolve(&[right.into()], None)?.id)?
            .clone();
        let reference = self.reference(&right)?;
        let expression = typing::coerce(reference, &DataType::Utf8, &mut self.context.ids)?;
        let distinct = self.project(
            input.clone(),
            vec![NamedExpression {
                name: right.name.clone(),
                expression,
            }],
        )?;
        let distinct = LogicalPlan {
            schema: distinct.schema.clone(),
            node: Deferred::Resolved(PlanNode::Distinct {
                input: Box::new(distinct),
            }),
        };
        let mut values = vec![];
        for batch in self.execute(distinct).await? {
            for row in 0..batch.num_rows() {
                if let ScalarValue::Utf8(Some(value)) =
                    ScalarValue::try_from_array(batch.column(0), row)?
                {
                    values.push(value);
                }
            }
        }
        let grouping = NamedExpression {
            name: name.clone(),
            expression: self.reference(&left)?,
        };
        let mut output = vec![NamedExpression {
            name,
            expression: self.reference(&left)?,
        }];
        for value in values {
            let reference = self.reference(&right)?;
            let literal = self.literal(ScalarValue::Utf8(Some(value.clone())));
            let condition = self.binary(literal, Operator::Eq, reference)?;
            let one = self.literal(ScalarValue::Int32(Some(1)));
            let zero = self.literal(ScalarValue::Int32(Some(0)));
            let expression = typing::resolve_case(
                self.context.ids.expression(),
                None,
                vec![(condition, one)],
                Some(zero),
                &mut self.context.ids,
            )?;
            output.push(NamedExpression {
                name: value,
                expression: self.aggregate_call(self.aggregate_kernel("sum")?, vec![expression])?,
            });
        }
        let input = self.aggregate(input, vec![grouping], output, None)?;
        let attributes = input.schema.resolved("crosstab output")?.attributes.clone();
        let mut output = vec![];
        for (index, attribute) in attributes.iter().enumerate() {
            let mut expression = self.reference(attribute)?;
            if index == 0 {
                expression = typing::coerce(expression, &DataType::Utf8, &mut self.context.ids)?;
            }
            output.push(NamedExpression {
                name: attribute.name.clone(),
                expression,
            });
        }
        self.project(input, output)
    }

    #[async_recursion::async_recursion]
    pub(super) async fn tail(
        &mut self,
        input: spec::QueryPlan,
        limit: spec::Expr,
    ) -> PlanResult<LogicalPlan> {
        let input = self.query(input).await?;
        let limit = self
            .expression(limit, &PlanSchema::default())
            .await?
            .expression;
        let value = crate::analysis::lowering::evaluate_constant(&limit)?;
        if !value.data_type().is_integer() {
            return Err(PlanError::invalid("tail limit must be an integer"));
        }
        let ScalarValue::Int64(Some(limit)) = value.cast_to(&DataType::Int64)? else {
            return Err(PlanError::invalid("tail limit must be an integer"));
        };
        if limit < 0 {
            return Err(PlanError::invalid("tail limit must be nonnegative"));
        }
        let one = self.literal(ScalarValue::Int64(Some(1)));
        let count = self.aggregate_call(self.aggregate_kernel("count")?, vec![one])?;
        let count = self.aggregate(
            input.clone(),
            vec![],
            vec![NamedExpression {
                name: "count".into(),
                expression: count,
            }],
            None,
        )?;
        let batches = self.execute(count).await?;
        let value = batches
            .iter()
            .find(|batch| batch.num_rows() > 0)
            .ok_or_else(|| PlanError::internal("tail count returned no row"))?;
        let ScalarValue::Int64(Some(count)) = ScalarValue::try_from_array(value.column(0), 0)?
        else {
            return Err(PlanError::internal("tail count returned invalid value"));
        };
        Ok(self.limit(input, count.saturating_sub(limit).max(0), limit))
    }

    #[async_recursion::async_recursion]
    pub(super) async fn pair_statistic(
        &mut self,
        input: spec::QueryPlan,
        left: spec::Identifier,
        right: spec::Identifier,
        function: &str,
        name: &str,
    ) -> PlanResult<LogicalPlan> {
        let input = self.query(input).await?;
        let schema = input.schema.resolved("statistic input")?;
        let mut arguments = vec![];
        for name in [left, right] {
            let bound = ResolutionScope::new(schema, [])
                .with_case_sensitive(self.context.config.case_sensitive)
                .resolve(&[name.into()], None)?;
            arguments.push(self.reference(schema.attribute(bound.id)?)?);
        }
        let expression = self.aggregate_call(self.aggregate_kernel(function)?, arguments)?;
        self.aggregate(
            input,
            vec![],
            vec![NamedExpression {
                name: name.into(),
                expression,
            }],
            None,
        )
    }

    #[async_recursion::async_recursion]
    pub(super) async fn stat_sample_by(
        &mut self,
        input: spec::QueryPlan,
        column: spec::Expr,
        fractions: Vec<spec::Fraction>,
        seed: Option<i64>,
    ) -> PlanResult<LogicalPlan> {
        if fractions.iter().any(|f| !(0.0..=1.0).contains(&f.fraction)) {
            return Err(PlanError::invalid(
                "All fraction values must be >= 0.0 and <= 1.0",
            ));
        }
        let input = self.query(input).await?;
        let original = input
            .schema
            .resolved("stratified sample input")?
            .attributes
            .clone();
        if !matches!(&column, spec::Expr::UnresolvedAttribute { name, is_metadata_column: false, .. } if name.parts().len() == 1)
        {
            return Err(PlanError::invalid("expected simple column name"));
        }
        let seed = self.literal(ScalarValue::Int64(seed));
        let random = self.scalar_udf(Arc::new(ScalarUDF::from(Random::new())), vec![seed])?;
        let (input, random) = self.append(input, random, "sample_random".into())?;
        let mut predicate = self.literal(ScalarValue::Boolean(Some(false)));
        for fraction in fractions {
            let column = self
                .expression(
                    column.clone(),
                    input.schema.resolved("stratified sample input")?,
                )
                .await?
                .expression;
            let value = self.types().resolve_literal(fraction.stratum)?;
            let value = self.literal(value);
            let equal = self.binary(column, Operator::Eq, value)?;
            let value = self.reference(&random)?;
            let fraction = self.literal(ScalarValue::Float64(Some(fraction.fraction)));
            let sampled = self.binary(value, Operator::LtEq, fraction)?;
            let condition = self.binary(equal, Operator::And, sampled)?;
            predicate = self.binary(predicate, Operator::Or, condition)?;
        }
        let input = Self::filter(input, predicate)?;
        self.select_attributes(input, original)
    }

    #[async_recursion::async_recursion]
    pub(super) async fn summary(
        &mut self,
        input: spec::QueryPlan,
        columns: Vec<spec::Identifier>,
        statistics: Vec<String>,
    ) -> PlanResult<LogicalPlan> {
        let input = self.query(input).await?;
        let input = self.visible_output(input)?;
        let schema = input.schema.resolved("summary input")?;
        let mut positions = vec![];
        let chosen = if columns.is_empty() {
            schema.attributes.iter().map(|a| a.id).collect::<Vec<_>>()
        } else {
            columns
                .into_iter()
                .map(|name| {
                    Ok(ResolutionScope::new(schema, [])
                        .with_case_sensitive(self.context.config.case_sensitive)
                        .resolve(&[name.into()], None)?
                        .id)
                })
                .collect::<PlanResult<_>>()?
        };
        for (index, attribute) in schema.attributes.iter().enumerate() {
            let data_type = &attribute.result_type.resolved("summary column")?.data_type;
            if chosen.contains(&attribute.id)
                && (data_type.is_numeric()
                    || matches!(
                        data_type,
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                    ))
            {
                positions.push(index);
            }
        }
        let statistics = if statistics.is_empty() {
            ["count", "mean", "stddev", "min", "25%", "50%", "75%", "max"]
                .map(String::from)
                .to_vec()
        } else {
            statistics
        };
        let mut rows = vec![];
        let mut seen = std::collections::HashSet::new();
        for statistic in statistics {
            let statistic = statistic.trim().to_ascii_lowercase();
            if !matches!(
                statistic.as_str(),
                "count" | "mean" | "stddev" | "min" | "25%" | "50%" | "75%" | "max"
            ) || !seen.insert(statistic.clone())
            {
                continue;
            }
            let input = input.instantiate(&mut self.context.ids)?;
            let schema = input.schema.resolved("summary instance")?;
            let label = self.literal(ScalarValue::Utf8(Some(statistic.clone())));
            let mut output = vec![NamedExpression {
                name: "summary".into(),
                expression: label,
            }];
            for index in &positions {
                let attribute = &schema.attributes[*index];
                let original = &attribute
                    .result_type
                    .resolved("summary argument")?
                    .data_type;
                let mut value = self.reference(attribute)?;
                if matches!(
                    statistic.as_str(),
                    "mean" | "stddev" | "25%" | "50%" | "75%"
                ) && !original.is_numeric()
                {
                    value = self.try_cast(value, DataType::Float64)?;
                }
                let (function, arguments) = match statistic.as_str() {
                    "mean" => ("avg", vec![value]),
                    "25%" | "75%" => {
                        let percentile =
                            self.literal(ScalarValue::Float64(Some(if statistic == "25%" {
                                0.25
                            } else {
                                0.75
                            })));
                        ("approx_percentile_cont", vec![value, percentile])
                    }
                    "50%" => ("approx_median", vec![value]),
                    name => (name, vec![value]),
                };
                let mut value = self.aggregate_call(self.aggregate_kernel(function)?, arguments)?;
                if statistic.ends_with('%') && original.is_integer() {
                    value = typing::coerce(value, original, &mut self.context.ids)?;
                }
                value = typing::coerce(value, &DataType::Utf8, &mut self.context.ids)?;
                output.push(NamedExpression {
                    name: attribute.name.clone(),
                    expression: value,
                });
            }
            let row = if positions.is_empty() {
                self.project(Self::empty(true), output)?
            } else {
                self.aggregate(input, vec![], output, None)?
            };
            rows.push(row);
        }
        self.union(rows)
    }
}
