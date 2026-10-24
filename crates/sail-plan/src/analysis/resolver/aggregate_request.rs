use sail_common::spec;

use super::{NamedExpression, Resolver};
use crate::analysis::deferred::Deferred;
use crate::analysis::model::*;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    #[async_recursion::async_recursion]
    pub(super) async fn aggregate_request(
        &mut self,
        aggregate: spec::Aggregate,
    ) -> PlanResult<QueryNode> {
        let aliases = std::mem::take(&mut self.expression_aliases);
        let replacements = std::mem::take(&mut self.expression_replacements);
        let result = self.aggregate_request_scoped(aggregate).await;
        self.expression_aliases = aliases;
        self.expression_replacements = replacements;
        result
    }

    #[async_recursion::async_recursion]
    async fn aggregate_request_scoped(
        &mut self,
        aggregate: spec::Aggregate,
    ) -> PlanResult<QueryNode> {
        let mut input = self.query(*aggregate.input).await?;
        let mut generators = vec![];
        for request in &aggregate.grouping {
            if super::generators::is_generator(request) {
                let (next, generated) = self.generator(input, request.clone()).await?;
                input = next;
                if generated.len() != 1 {
                    return Err(PlanError::analysis(
                        "grouping generator must produce one column",
                    ));
                }
                let value = generated
                    .into_iter()
                    .next()
                    .ok_or_else(|| PlanError::internal("missing generator output"))?;
                generators.push((request.clone(), value));
            }
        }
        self.expression_replacements = generators;
        let schema = input.schema.resolved("aggregate input")?.clone();
        let mut projections = vec![];
        for request in &aggregate.aggregate {
            match self.expression(request.clone(), &schema).await {
                Ok(value) => projections.push(Some(value)),
                Err(PlanError::AnalysisError(message))
                    if message.starts_with("unresolved attribute:") =>
                {
                    projections.push(None)
                }
                Err(error) => return Err(error),
            }
        }
        self.expression_aliases = projections
            .iter()
            .flatten()
            .map(|value| NamedExpression {
                name: value.name.clone(),
                expression: value.expression.clone(),
            })
            .collect();
        let (grouping, grouping_sets) = self
            .grouping_requests(aggregate.grouping, &projections, &schema)
            .await?;
        for (request, projection) in aggregate.aggregate.iter().zip(&projections) {
            if let Some(projection) = projection {
                for key in &grouping {
                    if projection.expression.equivalent(&key.expression)? {
                        self.expression_replacements.push((
                            request.clone(),
                            NamedExpression {
                                name: projection.name.clone(),
                                expression: key.expression.clone(),
                            },
                        ));
                        break;
                    }
                }
            }
        }
        self.expression_aliases = grouping
            .iter()
            .map(|value| NamedExpression {
                name: value.name.clone(),
                expression: value.expression.clone(),
            })
            .collect();
        let mut output = vec![];
        if aggregate.with_grouping_expressions {
            for value in &grouping {
                output.push(NamedExpression {
                    name: value.name.clone(),
                    expression: self.copy_expression(&value.expression)?,
                });
            }
        }
        for request in aggregate.aggregate {
            output.push(self.expression(request, &schema).await?);
        }
        self.expression_aliases
            .extend(output.iter().map(|value| NamedExpression {
                name: value.name.clone(),
                expression: value.expression.clone(),
            }));
        for value in &output {
            self.expression_replacements.push((
                spec::Expr::UnresolvedAttribute {
                    name: spec::ObjectName::bare(value.name.clone()),
                    plan_id: None,
                    is_metadata_column: false,
                },
                NamedExpression {
                    name: value.name.clone(),
                    expression: value.expression.clone(),
                },
            ));
        }
        let having = match aggregate.having {
            Some(request) => Some(self.expression(request, &schema).await?.expression),
            None => None,
        };
        self.validate_aggregate_functions(&output)?;
        self.aggregate_with_sets(input, grouping, output, having, grouping_sets)
    }

    fn validate_aggregate_functions(&self, output: &[NamedExpression]) -> PlanResult<()> {
        let mut python = false;
        let mut native = false;
        for named in output {
            named
                .expression
                .clone()
                .transform_up::<PlanError>(&mut |value| {
                    if let Deferred::Resolved(ExprKind::AggregateFunction { function, .. }) =
                        &value.kind
                    {
                        if function
                            .inner()
                            .is::<sail_python_udf::udf::pyspark_udaf::PySparkGroupAggregateUDF>()
                        {
                            python = true;
                        } else {
                            native = true;
                        }
                    }
                    Ok(value)
                })?;
        }
        if python && native {
            return Err(PlanError::analysis(
                "Grouped aggregate Python UDFs cannot be mixed with regular aggregate functions",
            ));
        }
        Ok(())
    }
}

pub(super) fn ordinal(request: &spec::Expr) -> Option<i64> {
    match request {
        spec::Expr::Literal(spec::Literal::Int32 { value: Some(value) }) => Some(i64::from(*value)),
        spec::Expr::Literal(spec::Literal::Int64 { value: Some(value) }) => Some(*value),
        _ => None,
    }
}
