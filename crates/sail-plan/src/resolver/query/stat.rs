use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use sail_common::spec;
use sail_function::scalar::math::random::Random;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::analyzer::scope::ResolutionScope;
use crate::analyzer::typing;
use crate::error::{PlanError, PlanResult};
use crate::resolver::{NamedExpr, PlanResolver};

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn crosstab(
        &mut self,
        input: spec::QueryPlan,
        left: spec::Identifier,
        right: spec::Identifier,
    ) -> PlanResult<Query> {
        let name = format!("{}_{}", left.as_ref(), right.as_ref());
        let input = self.resolve_query_plan(input).await?;
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
            vec![NamedExpr {
                name: right.name.clone(),
                expression,
            }],
        )?;
        let distinct = Query {
            schema: distinct.schema.clone(),
            kind: Deferred::Resolved(QueryKind::Distinct {
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
        let grouping = NamedExpr {
            name: name.clone(),
            expression: self.reference(&left)?,
        };
        let mut output = vec![NamedExpr {
            name,
            expression: self.reference(&left)?,
        }];
        for value in values {
            let reference = self.reference(&right)?;
            let literal = self
                .context
                .function_context()
                .literal(ScalarValue::Utf8(Some(value.clone())));
            let condition =
                self.context
                    .function_context()
                    .binary(literal, Operator::Eq, reference)?;
            let one = self
                .context
                .function_context()
                .literal(ScalarValue::Int32(Some(1)));
            let zero = self
                .context
                .function_context()
                .literal(ScalarValue::Int32(Some(0)));
            let expression = typing::resolve_case(
                self.context.ids.expression(),
                None,
                vec![(condition, one)],
                Some(zero),
                &mut self.context.ids,
            )?;
            output.push(NamedExpr {
                name: value,
                expression: self
                    .context
                    .function_context()
                    .aggregate("sum", vec![expression])?,
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
            output.push(NamedExpr {
                name: attribute.name.clone(),
                expression,
            });
        }
        self.project(input, output)
    }

    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn tail(
        &mut self,
        input: spec::QueryPlan,
        limit: spec::Expr,
    ) -> PlanResult<Query> {
        let input = self.resolve_query_plan(input).await?;
        let limit = self
            .resolve_named_expression(limit, &QuerySchema::default())
            .await?
            .expression;
        let value = crate::analyzer::lowering::evaluate_constant(&limit)?;
        if !value.data_type().is_integer() {
            return Err(PlanError::invalid("tail limit must be an integer"));
        }
        let ScalarValue::Int64(Some(limit)) = value.cast_to(&DataType::Int64)? else {
            return Err(PlanError::invalid("tail limit must be an integer"));
        };
        if limit < 0 {
            return Err(PlanError::invalid("tail limit must be nonnegative"));
        }
        let one = self
            .context
            .function_context()
            .literal(ScalarValue::Int64(Some(1)));
        let count = self
            .context
            .function_context()
            .aggregate("count", vec![one])?;
        let count = self.aggregate(
            input.clone(),
            vec![],
            vec![NamedExpr {
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
    pub(in crate::resolver) async fn pair_statistic(
        &mut self,
        input: spec::QueryPlan,
        left: spec::Identifier,
        right: spec::Identifier,
        function: &str,
        name: &str,
    ) -> PlanResult<Query> {
        let input = self.resolve_query_plan(input).await?;
        let schema = input.schema.resolved("statistic input")?;
        let mut arguments = vec![];
        for name in [left, right] {
            let bound = ResolutionScope::new(schema, [])
                .with_case_sensitive(self.context.config.case_sensitive)
                .resolve(&[name.into()], None)?;
            arguments.push(self.reference(schema.attribute(bound.id)?)?);
        }
        let expression = self
            .context
            .function_context()
            .aggregate(function, arguments)?;
        self.aggregate(
            input,
            vec![],
            vec![NamedExpr {
                name: name.into(),
                expression,
            }],
            None,
        )
    }

    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn stat_sample_by(
        &mut self,
        input: spec::QueryPlan,
        column: spec::Expr,
        fractions: Vec<spec::Fraction>,
        seed: Option<i64>,
    ) -> PlanResult<Query> {
        if fractions.iter().any(|f| !(0.0..=1.0).contains(&f.fraction)) {
            return Err(PlanError::invalid(
                "All fraction values must be >= 0.0 and <= 1.0",
            ));
        }
        let input = self.resolve_query_plan(input).await?;
        let original = input
            .schema
            .resolved("stratified sample input")?
            .attributes
            .clone();
        if !matches!(&column, spec::Expr::UnresolvedAttribute { name, is_metadata_column: false, .. } if name.parts().len() == 1)
        {
            return Err(PlanError::invalid("expected simple column name"));
        }
        let seed = self
            .context
            .function_context()
            .literal(ScalarValue::Int64(seed));
        let random = self
            .context
            .function_context()
            .scalar_udf(Arc::new(ScalarUDF::from(Random::new())), vec![seed])?;
        let (input, random) = self.append(input, random, "sample_random".into())?;
        let mut predicate = self
            .context
            .function_context()
            .literal(ScalarValue::Boolean(Some(false)));
        for fraction in fractions {
            let column = self
                .resolve_named_expression(
                    column.clone(),
                    input.schema.resolved("stratified sample input")?,
                )
                .await?
                .expression;
            let value = self.types().resolve_literal(fraction.stratum)?;
            let value = self.context.function_context().literal(value);
            let equal = self
                .context
                .function_context()
                .binary(column, Operator::Eq, value)?;
            let value = self.reference(&random)?;
            let fraction = self
                .context
                .function_context()
                .literal(ScalarValue::Float64(Some(fraction.fraction)));
            let sampled =
                self.context
                    .function_context()
                    .binary(value, Operator::LtEq, fraction)?;
            let condition =
                self.context
                    .function_context()
                    .binary(equal, Operator::And, sampled)?;
            predicate =
                self.context
                    .function_context()
                    .binary(predicate, Operator::Or, condition)?;
        }
        let input = Self::filter(input, predicate)?;
        self.select_attributes(input, original)
    }

    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn summary(
        &mut self,
        input: spec::QueryPlan,
        columns: Vec<spec::Identifier>,
        statistics: Vec<String>,
    ) -> PlanResult<Query> {
        let input = self.resolve_query_plan(input).await?;
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
            let label = self
                .context
                .function_context()
                .literal(ScalarValue::Utf8(Some(statistic.clone())));
            let mut output = vec![NamedExpr {
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
                    value = self
                        .context
                        .function_context()
                        .try_cast(value, DataType::Float64)?;
                }
                let (function, arguments) = match statistic.as_str() {
                    "mean" => ("avg", vec![value]),
                    "25%" | "75%" => {
                        let percentile =
                            self.context
                                .function_context()
                                .literal(ScalarValue::Float64(Some(if statistic == "25%" {
                                    0.25
                                } else {
                                    0.75
                                })));
                        ("approx_percentile_cont", vec![value, percentile])
                    }
                    "50%" => ("approx_median", vec![value]),
                    name => (name, vec![value]),
                };
                let mut value = self
                    .context
                    .function_context()
                    .aggregate(function, arguments)?;
                if statistic.ends_with('%') && original.is_integer() {
                    value = typing::coerce(value, original, &mut self.context.ids)?;
                }
                value = typing::coerce(value, &DataType::Utf8, &mut self.context.ids)?;
                output.push(NamedExpr {
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
