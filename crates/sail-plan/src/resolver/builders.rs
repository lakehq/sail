use arrow_schema::DataType;
use datafusion_common::ScalarValue;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::analyzer::typing;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(in crate::resolver) fn limit(&mut self, input: Query, skip: i64, fetch: i64) -> Query {
        Query {
            schema: input.schema.clone(),
            kind: Deferred::Resolved(QueryKind::Limit {
                input: Box::new(input),
                skip: Some(Box::new(
                    self.context
                        .function_context()
                        .literal(ScalarValue::Int64(Some(skip))),
                )),
                fetch: Some(Box::new(
                    self.context
                        .function_context()
                        .literal(ScalarValue::Int64(Some(fetch))),
                )),
            }),
        }
    }

    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn execute(
        &self,
        plan: Query,
    ) -> PlanResult<Vec<arrow::record_batch::RecordBatch>> {
        let plan = crate::analyzer::lowering::lower_query(plan)?.plan;
        Ok(self
            .context
            .session
            .execute_logical_plan(plan)
            .await?
            .collect()
            .await?)
    }

    pub(in crate::resolver) fn union(&mut self, inputs: Vec<Query>) -> PlanResult<Query> {
        let first = inputs
            .first()
            .ok_or_else(|| PlanError::analysis("UNION requires an input"))?
            .schema
            .resolved("UNION input")?;
        let width = first.attributes.len();
        let mut attributes = vec![];
        for (index, column) in first.attributes.iter().enumerate() {
            let fields = inputs
                .iter()
                .map(|input| {
                    let schema = input.schema.resolved("UNION input")?;
                    if schema.attributes.len() != width {
                        return Err(PlanError::analysis("UNION input widths differ"));
                    }
                    Ok(schema.attributes[index]
                        .result_type
                        .resolved("UNION field")?)
                })
                .collect::<PlanResult<Vec<_>>>()?;
            let data_type = crate::analyzer::typing::coercion::common_type(
                &fields
                    .iter()
                    .map(|f| f.data_type.clone())
                    .collect::<Vec<_>>(),
            )?;
            let mut result_type = ExprType::new(data_type, fields.iter().any(|f| f.nullable));
            result_type
                .metadata
                .clone_from(&column.result_type.resolved("UNION first field")?.metadata);
            if fields.iter().any(|f| f.metadata != result_type.metadata) {
                return Err(PlanError::analysis("UNION field metadata differs"));
            }
            attributes.push(
                self.context
                    .function_context()
                    .attribute(column.name.clone(), result_type),
            );
        }
        let mut resolved = vec![];
        for input in inputs {
            let mut expressions = vec![];
            for (source, target) in input
                .schema
                .resolved("UNION input")?
                .attributes
                .iter()
                .zip(&attributes)
            {
                let expression = self.reference(source)?;
                let expression = typing::coerce(
                    expression,
                    &target.result_type.resolved("UNION target")?.data_type,
                    &mut self.context.ids,
                )?;
                expressions.push(crate::resolver::NamedExpression {
                    name: source.name.clone(),
                    expression,
                });
            }
            resolved.push(self.project(input, expressions)?);
        }
        Ok(Query {
            schema: Deferred::Resolved(QuerySchema { attributes }),
            kind: Deferred::Resolved(QueryKind::Union { inputs: resolved }),
        })
    }

    pub(in crate::resolver) fn filter(input: Query, condition: Expr) -> PlanResult<Query> {
        if condition
            .result_type
            .resolved("filter predicate")?
            .data_type
            != DataType::Boolean
        {
            return Err(PlanError::analysis("filter predicate must be boolean"));
        }
        Ok(Query {
            schema: input.schema.clone(),
            kind: Deferred::Resolved(QueryKind::Filter {
                input: Box::new(input),
                condition: Box::new(condition),
            }),
        })
    }

    /// Add a materialized value without changing the identities of existing slots.
    pub(in crate::resolver) fn append(
        &mut self,
        input: Query,
        expression: Expr,
        name: String,
    ) -> PlanResult<(Query, Attribute)> {
        let mut schema = input.schema.resolved("append input")?.clone();
        let mut expressions = schema
            .attributes
            .iter()
            .map(|attribute| {
                Ok(Projection {
                    expression: self.reference(attribute)?,
                    attribute: attribute.clone(),
                })
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let mut attribute = self.context.function_context().attribute(
            name,
            expression
                .result_type
                .resolved("append expression")?
                .clone(),
        );
        attribute.hidden = true;
        expressions.push(Projection {
            expression,
            attribute: attribute.clone(),
        });
        schema.attributes.push(attribute.clone());
        Ok((
            Query {
                schema: Deferred::Resolved(schema),
                kind: Deferred::Resolved(QueryKind::Project {
                    input: Box::new(input),
                    expressions,
                }),
            },
            attribute,
        ))
    }

    pub(in crate::resolver) fn select_attributes(
        &mut self,
        input: Query,
        attributes: Vec<Attribute>,
    ) -> PlanResult<Query> {
        let expressions = attributes
            .iter()
            .map(|attribute| {
                Ok(Projection {
                    expression: self.reference(attribute)?,
                    attribute: attribute.clone(),
                })
            })
            .collect::<PlanResult<_>>()?;
        Ok(Query {
            schema: Deferred::Resolved(QuerySchema { attributes }),
            kind: Deferred::Resolved(QueryKind::Project {
                input: Box::new(input),
                expressions,
            }),
        })
    }

    pub(in crate::resolver) fn unnest(
        &mut self,
        input: Query,
        column: &Attribute,
        outer: bool,
    ) -> PlanResult<(Query, Attribute)> {
        let field = match &column.result_type.resolved("unnest input")?.data_type {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _) => field,
            other => {
                return Err(PlanError::analysis(format!(
                    "UNNEST requires an array, got {other}"
                )));
            }
        };
        // DataFusion's physical UNNEST exposes nullable element slots even for
        // non-nullable element fields. Preserve that contract at this boundary.
        let mut result_type = ExprType::from_field(field);
        result_type.nullable = true;
        let attribute = self
            .context
            .function_context()
            .attribute(column.name.clone(), result_type);
        let mut schema = input.schema.resolved("unnest input")?.clone();
        let position = schema
            .attributes
            .iter()
            .position(|a| a.id == column.id)
            .ok_or_else(|| PlanError::internal("unnest column not in input"))?;
        schema.attributes[position] = attribute.clone();
        Ok((
            Query {
                schema: Deferred::Resolved(schema),
                kind: Deferred::Resolved(QueryKind::Unnest {
                    input: Box::new(input),
                    column: column.id,
                    outer,
                }),
            },
            attribute,
        ))
    }
}
