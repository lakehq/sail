use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{Operator, ScalarUDF};

use super::Resolver;
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::analysis::typing;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    pub(super) fn cast(
        &mut self,
        expression: LogicalExpression,
        target: DataType,
    ) -> PlanResult<LogicalExpression> {
        typing::coerce(expression, &target, &mut self.context.ids)
    }

    pub(super) fn case(
        &mut self,
        branches: Vec<(LogicalExpression, LogicalExpression)>,
        otherwise: Option<LogicalExpression>,
    ) -> PlanResult<LogicalExpression> {
        typing::resolve_case(
            self.context.ids.expression(),
            None,
            branches,
            otherwise,
            &mut self.context.ids,
        )
    }

    pub(super) fn not(&mut self, expression: LogicalExpression) -> PlanResult<LogicalExpression> {
        let expression = self.cast(expression, DataType::Boolean)?;
        Ok(LogicalExpression {
            id: self.context.ids.expression(),
            result_type: expression.result_type.clone(),
            node: Deferred::Resolved(ExpressionNode::Not(Box::new(expression))),
        })
    }

    pub(super) fn is_not_null(&mut self, expression: LogicalExpression) -> LogicalExpression {
        LogicalExpression {
            id: self.context.ids.expression(),
            node: Deferred::Resolved(ExpressionNode::IsNotNull(Box::new(expression))),
            result_type: Deferred::Resolved(ExpressionType::new(DataType::Boolean, false)),
        }
    }

    pub(super) fn is_null(&mut self, expression: LogicalExpression) -> LogicalExpression {
        LogicalExpression {
            id: self.context.ids.expression(),
            node: Deferred::Resolved(ExpressionNode::IsNull(Box::new(expression))),
            result_type: Deferred::Resolved(ExpressionType::new(DataType::Boolean, false)),
        }
    }

    pub(super) fn copy_expression(
        &mut self,
        expression: &LogicalExpression,
    ) -> PlanResult<LogicalExpression> {
        use sail_query::rewrite::{QueryRewriter, walk_expression};
        struct Copy<'a>(&'a mut IdGenerator);
        impl QueryRewriter for Copy<'_> {
            type Error = PlanError;
            fn rewrite_expression(
                &mut self,
                expression: LogicalExpression,
            ) -> PlanResult<LogicalExpression> {
                let mut expression = walk_expression(self, expression)?;
                expression.id = self.0.expression();
                if let Deferred::Resolved(ExpressionNode::HigherOrderFunction {
                    arguments, ..
                }) = &mut expression.node
                {
                    for argument in arguments {
                        if let FunctionArgument::Lambda { id, .. } = argument {
                            *id = self.0.expression();
                        }
                    }
                }
                Ok(expression)
            }
        }
        Copy(&mut self.context.ids).rewrite_expression(expression.clone())
    }

    pub(super) fn literal(&mut self, value: ScalarValue) -> LogicalExpression {
        LogicalExpression::literal(self.context.ids.expression(), value)
    }

    pub(super) fn binary(
        &mut self,
        left: LogicalExpression,
        operator: Operator,
        right: LogicalExpression,
    ) -> PlanResult<LogicalExpression> {
        let (left, right) = self.predicate_pair(left, operator, right)?;
        typing::resolve_binary(
            self.context.ids.expression(),
            left,
            operator,
            right,
            &mut self.context.ids,
        )
    }

    pub(super) fn scalar_kernel(&self, name: &str) -> PlanResult<Arc<ScalarUDF>> {
        self.context
            .session
            .udf(name)
            .ok()
            .or_else(|| super::functions::default_scalar_kernel(name))
            .ok_or_else(|| {
                crate::error::PlanError::unsupported(format!("unknown scalar kernel: {name}"))
            })
    }

    pub(super) fn aggregate_kernel(
        &self,
        name: &str,
    ) -> PlanResult<Arc<datafusion_expr::AggregateUDF>> {
        super::aggregate_functions::aggregate_kernel(name, self.context.config.ansi_mode)
            .or_else(|| self.context.session.udaf(name).ok())
            .ok_or_else(|| {
                crate::error::PlanError::unsupported(format!("unknown aggregate kernel: {name}"))
            })
    }

    pub(super) fn scalar(
        &mut self,
        name: &str,
        arguments: Vec<LogicalExpression>,
    ) -> PlanResult<LogicalExpression> {
        self.scalar_udf(self.scalar_kernel(name)?, arguments)
    }

    pub(super) fn scalar_udf(
        &mut self,
        function: Arc<ScalarUDF>,
        arguments: Vec<LogicalExpression>,
    ) -> PlanResult<LogicalExpression> {
        typing::resolve_scalar_call(
            self.context.ids.expression(),
            function,
            arguments,
            &mut self.context.ids,
        )
    }

    pub(super) fn aggregate_call(
        &mut self,
        function: Arc<datafusion_expr::AggregateUDF>,
        arguments: Vec<LogicalExpression>,
    ) -> PlanResult<LogicalExpression> {
        self.aggregate_call_with_options(
            function,
            arguments,
            super::aggregate_functions::AggregateOptions::default(),
        )
    }

    pub(super) fn try_cast(
        &mut self,
        expression: LogicalExpression,
        data_type: DataType,
    ) -> PlanResult<LogicalExpression> {
        let field = expression
            .result_type
            .resolved("try cast input")?
            .to_field("");
        if !arrow::compute::can_cast_types(field.data_type(), &data_type) {
            return Err(PlanError::analysis(format!(
                "cannot cast {} to {data_type}",
                field.data_type()
            )));
        }
        let mut target =
            ExpressionType::from_field(&sail_type_system::coercion::cast_field(&field, &data_type));
        target.nullable = true;
        Ok(LogicalExpression {
            id: self.context.ids.expression(),
            result_type: Deferred::Resolved(target.clone()),
            node: Deferred::Resolved(ExpressionNode::Cast {
                expression: Box::new(expression),
                target,
                safe: true,
            }),
        })
    }

    pub(super) fn limit(&mut self, input: LogicalPlan, skip: i64, fetch: i64) -> LogicalPlan {
        LogicalPlan {
            schema: input.schema.clone(),
            node: Deferred::Resolved(PlanNode::Limit {
                input: Box::new(input),
                skip: Some(Box::new(self.literal(ScalarValue::Int64(Some(skip))))),
                fetch: Some(Box::new(self.literal(ScalarValue::Int64(Some(fetch))))),
            }),
        }
    }

    #[async_recursion::async_recursion]
    pub(super) async fn execute(
        &self,
        plan: LogicalPlan,
    ) -> PlanResult<Vec<arrow::record_batch::RecordBatch>> {
        let plan = LogicalQuery { plan }.into_named_plan()?.plan;
        Ok(self
            .context
            .session
            .execute_logical_plan(plan)
            .await?
            .collect()
            .await?)
    }

    pub(super) fn union(&mut self, inputs: Vec<LogicalPlan>) -> PlanResult<LogicalPlan> {
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
            let data_type = sail_type_system::coercion::common_type(
                &fields
                    .iter()
                    .map(|f| f.data_type.clone())
                    .collect::<Vec<_>>(),
            )?;
            let mut result_type = ExpressionType::new(data_type, fields.iter().any(|f| f.nullable));
            result_type
                .metadata
                .clone_from(&column.result_type.resolved("UNION first field")?.metadata);
            if fields.iter().any(|f| f.metadata != result_type.metadata) {
                return Err(PlanError::analysis("UNION field metadata differs"));
            }
            attributes.push(self.attribute(column.name.clone(), result_type));
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
                expressions.push(super::NamedExpression {
                    name: source.name.clone(),
                    expression,
                });
            }
            resolved.push(self.project(input, expressions)?);
        }
        Ok(LogicalPlan {
            schema: Deferred::Resolved(PlanSchema { attributes }),
            node: Deferred::Resolved(PlanNode::Union { inputs: resolved }),
        })
    }

    pub(super) fn filter(
        input: LogicalPlan,
        condition: LogicalExpression,
    ) -> PlanResult<LogicalPlan> {
        if condition
            .result_type
            .resolved("filter predicate")?
            .data_type
            != DataType::Boolean
        {
            return Err(PlanError::analysis("filter predicate must be boolean"));
        }
        Ok(LogicalPlan {
            schema: input.schema.clone(),
            node: Deferred::Resolved(PlanNode::Filter {
                input: Box::new(input),
                condition: Box::new(condition),
            }),
        })
    }

    /// Add a materialized value without changing the identities of existing slots.
    pub(super) fn append(
        &mut self,
        input: LogicalPlan,
        expression: LogicalExpression,
        name: String,
    ) -> PlanResult<(LogicalPlan, Attribute)> {
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
        let mut attribute = self.attribute(
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
            LogicalPlan {
                schema: Deferred::Resolved(schema),
                node: Deferred::Resolved(PlanNode::Project {
                    input: Box::new(input),
                    expressions,
                }),
            },
            attribute,
        ))
    }

    pub(super) fn select_attributes(
        &mut self,
        input: LogicalPlan,
        attributes: Vec<Attribute>,
    ) -> PlanResult<LogicalPlan> {
        let expressions = attributes
            .iter()
            .map(|attribute| {
                Ok(Projection {
                    expression: self.reference(attribute)?,
                    attribute: attribute.clone(),
                })
            })
            .collect::<PlanResult<_>>()?;
        Ok(LogicalPlan {
            schema: Deferred::Resolved(PlanSchema { attributes }),
            node: Deferred::Resolved(PlanNode::Project {
                input: Box::new(input),
                expressions,
            }),
        })
    }

    pub(super) fn unnest(
        &mut self,
        input: LogicalPlan,
        column: &Attribute,
        outer: bool,
    ) -> PlanResult<(LogicalPlan, Attribute)> {
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
        let mut result_type = ExpressionType::from_field(field);
        result_type.nullable = true;
        let attribute = self.attribute(column.name.clone(), result_type);
        let mut schema = input.schema.resolved("unnest input")?.clone();
        let position = schema
            .attributes
            .iter()
            .position(|a| a.id == column.id)
            .ok_or_else(|| PlanError::internal("unnest column not in input"))?;
        schema.attributes[position] = attribute.clone();
        Ok((
            LogicalPlan {
                schema: Deferred::Resolved(schema),
                node: Deferred::Resolved(PlanNode::Unnest {
                    input: Box::new(input),
                    column: column.id,
                    outer,
                }),
            },
            attribute,
        ))
    }
}
