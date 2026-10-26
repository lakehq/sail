use sail_common_datafusion::input_file::is_input_file_metadata_function;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::resolver::{NamedExpr, PlanResolver};

impl PlanResolver<'_> {
    pub(in crate::resolver) fn grouping_metadata(
        &mut self,
        input: &mut Query,
        grouping: &mut [NamedExpr],
        output: &mut [NamedExpr],
        having: &mut Option<Expr>,
    ) -> PlanResult<()> {
        let mut materialized: Vec<Projection> = vec![];
        for key in grouping.iter() {
            key.expression
                .clone()
                .transform_up::<PlanError>(&mut |expression| {
                    if let ExprKind::ScalarFunction { function, .. } =
                        expression.kind.resolved("grouping metadata")?
                        && is_input_file_metadata_function(function)
                        && !materialized
                            .iter()
                            .any(|p| p.expression.equivalent(&expression).unwrap_or(false))
                    {
                        let mut attribute = self.context.function_context().attribute(
                            function.name().to_string(),
                            expression
                                .result_type
                                .resolved("input metadata type")?
                                .clone(),
                        );
                        attribute.hidden = true;
                        materialized.push(Projection {
                            attribute,
                            expression: self
                                .context
                                .function_context()
                                .copy_expression(&expression)?,
                        });
                    }
                    Ok(expression)
                })?;
        }
        if materialized.is_empty() {
            return Ok(());
        }
        let mut rewrite = |value: Expr| {
            value.transform_up::<PlanError>(&mut |expression| {
                for projection in &materialized {
                    if projection.expression.equivalent(&expression)? {
                        return self.reference(&projection.attribute);
                    }
                }
                Ok(expression)
            })
        };
        for named in grouping.iter_mut().chain(output) {
            named.expression = rewrite(named.expression.clone())?;
        }
        *having = having.take().map(rewrite).transpose()?;
        let mut expressions = input
            .schema
            .resolved("metadata input")?
            .attributes
            .iter()
            .map(|attribute| {
                Ok(Projection {
                    attribute: attribute.clone(),
                    expression: self.reference(attribute)?,
                })
            })
            .collect::<PlanResult<Vec<_>>>()?;
        expressions.extend(materialized);
        let schema = QuerySchema {
            attributes: expressions.iter().map(|p| p.attribute.clone()).collect(),
        };
        *input = Query {
            kind: Deferred::Resolved(QueryKind::Project {
                input: Box::new(input.clone()),
                expressions,
            }),
            schema: Deferred::Resolved(schema),
        };
        Ok(())
    }
}
