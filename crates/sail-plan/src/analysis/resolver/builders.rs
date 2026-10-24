use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{Operator, ScalarUDF};

use super::Resolver;
use crate::analysis::deferred::Deferred;
use crate::analysis::model::*;
use crate::analysis::typing;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    pub(super) fn cast(&mut self, expression: ExprNode, target: DataType) -> PlanResult<ExprNode> {
        typing::coerce(expression, &target, &mut self.context.ids)
    }

    pub(super) fn case(
        &mut self,
        branches: Vec<(ExprNode, ExprNode)>,
        otherwise: Option<ExprNode>,
    ) -> PlanResult<ExprNode> {
        typing::resolve_case(
            self.context.ids.expression(),
            None,
            branches,
            otherwise,
            &mut self.context.ids,
        )
    }

    pub(super) fn not(&mut self, expression: ExprNode) -> PlanResult<ExprNode> {
        let expression = self.cast(expression, DataType::Boolean)?;
        Ok(ExprNode {
            id: self.context.ids.expression(),
            result_type: expression.result_type.clone(),
            kind: Deferred::Resolved(ExprKind::Not(Box::new(expression))),
        })
    }

    pub(super) fn is_not_null(&mut self, expression: ExprNode) -> ExprNode {
        ExprNode {
            id: self.context.ids.expression(),
            kind: Deferred::Resolved(ExprKind::IsNotNull(Box::new(expression))),
            result_type: Deferred::Resolved(ExprType::new(DataType::Boolean, false)),
        }
    }

    pub(super) fn is_null(&mut self, expression: ExprNode) -> ExprNode {
        ExprNode {
            id: self.context.ids.expression(),
            kind: Deferred::Resolved(ExprKind::IsNull(Box::new(expression))),
            result_type: Deferred::Resolved(ExprType::new(DataType::Boolean, false)),
        }
    }

    pub(super) fn copy_expression(&mut self, expression: &ExprNode) -> PlanResult<ExprNode> {
        use sail_query::rewrite::{QueryRewriter, walk_expression};
        struct Copy<'a>(&'a mut IdGenerator);
        impl QueryRewriter for Copy<'_> {
            type Error = PlanError;
            fn rewrite_expression(&mut self, expression: ExprNode) -> PlanResult<ExprNode> {
                let mut expression = walk_expression(self, expression)?;
                expression.id = self.0.expression();
                if let Deferred::Resolved(ExprKind::HigherOrderFunction { arguments, .. }) =
                    &mut expression.kind
                {
                    for argument in arguments {
                        if let FunctionArg::Lambda { id, .. } = argument {
                            *id = self.0.expression();
                        }
                    }
                }
                Ok(expression)
            }
        }
        Copy(&mut self.context.ids).rewrite_expression(expression.clone())
    }

    pub(super) fn literal(&mut self, value: ScalarValue) -> ExprNode {
        ExprNode::literal(self.context.ids.expression(), value)
    }

    pub(super) fn binary(
        &mut self,
        left: ExprNode,
        operator: Operator,
        right: ExprNode,
    ) -> PlanResult<ExprNode> {
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

    pub(super) fn scalar(&mut self, name: &str, arguments: Vec<ExprNode>) -> PlanResult<ExprNode> {
        self.scalar_udf(self.scalar_kernel(name)?, arguments)
    }

    pub(super) fn scalar_udf(
        &mut self,
        function: Arc<ScalarUDF>,
        arguments: Vec<ExprNode>,
    ) -> PlanResult<ExprNode> {
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
        arguments: Vec<ExprNode>,
    ) -> PlanResult<ExprNode> {
        self.aggregate_call_with_options(
            function,
            arguments,
            super::aggregate_functions::AggregateOptions::default(),
        )
    }

    pub(super) fn try_cast(
        &mut self,
        expression: ExprNode,
        data_type: DataType,
    ) -> PlanResult<ExprNode> {
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
        let mut target = ExprType::from_field(&crate::analysis::typing::coercion::cast_field(
            &field, &data_type,
        ));
        target.nullable = true;
        Ok(ExprNode {
            id: self.context.ids.expression(),
            result_type: Deferred::Resolved(target.clone()),
            kind: Deferred::Resolved(ExprKind::Cast {
                expression: Box::new(expression),
                target,
                safe: true,
            }),
        })
    }

    pub(super) fn limit(&mut self, input: QueryNode, skip: i64, fetch: i64) -> QueryNode {
        QueryNode {
            schema: input.schema.clone(),
            kind: Deferred::Resolved(QueryKind::Limit {
                input: Box::new(input),
                skip: Some(Box::new(self.literal(ScalarValue::Int64(Some(skip))))),
                fetch: Some(Box::new(self.literal(ScalarValue::Int64(Some(fetch))))),
            }),
        }
    }

    #[async_recursion::async_recursion]
    pub(super) async fn execute(
        &self,
        plan: QueryNode,
    ) -> PlanResult<Vec<arrow::record_batch::RecordBatch>> {
        let plan = crate::analysis::lowering::lower_query(plan)?.plan;
        Ok(self
            .context
            .session
            .execute_logical_plan(plan)
            .await?
            .collect()
            .await?)
    }

    pub(super) fn union(&mut self, inputs: Vec<QueryNode>) -> PlanResult<QueryNode> {
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
            let data_type = crate::analysis::typing::coercion::common_type(
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
        Ok(QueryNode {
            schema: Deferred::Resolved(QuerySchema { attributes }),
            kind: Deferred::Resolved(QueryKind::Union { inputs: resolved }),
        })
    }

    pub(super) fn filter(input: QueryNode, condition: ExprNode) -> PlanResult<QueryNode> {
        if condition
            .result_type
            .resolved("filter predicate")?
            .data_type
            != DataType::Boolean
        {
            return Err(PlanError::analysis("filter predicate must be boolean"));
        }
        Ok(QueryNode {
            schema: input.schema.clone(),
            kind: Deferred::Resolved(QueryKind::Filter {
                input: Box::new(input),
                condition: Box::new(condition),
            }),
        })
    }

    /// Add a materialized value without changing the identities of existing slots.
    pub(super) fn append(
        &mut self,
        input: QueryNode,
        expression: ExprNode,
        name: String,
    ) -> PlanResult<(QueryNode, Attribute)> {
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
            QueryNode {
                schema: Deferred::Resolved(schema),
                kind: Deferred::Resolved(QueryKind::Project {
                    input: Box::new(input),
                    expressions,
                }),
            },
            attribute,
        ))
    }

    pub(super) fn select_attributes(
        &mut self,
        input: QueryNode,
        attributes: Vec<Attribute>,
    ) -> PlanResult<QueryNode> {
        let expressions = attributes
            .iter()
            .map(|attribute| {
                Ok(Projection {
                    expression: self.reference(attribute)?,
                    attribute: attribute.clone(),
                })
            })
            .collect::<PlanResult<_>>()?;
        Ok(QueryNode {
            schema: Deferred::Resolved(QuerySchema { attributes }),
            kind: Deferred::Resolved(QueryKind::Project {
                input: Box::new(input),
                expressions,
            }),
        })
    }

    pub(super) fn unnest(
        &mut self,
        input: QueryNode,
        column: &Attribute,
        outer: bool,
    ) -> PlanResult<(QueryNode, Attribute)> {
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
        let attribute = self.attribute(column.name.clone(), result_type);
        let mut schema = input.schema.resolved("unnest input")?.clone();
        let position = schema
            .attributes
            .iter()
            .position(|a| a.id == column.id)
            .ok_or_else(|| PlanError::internal("unnest column not in input"))?;
        schema.attributes[position] = attribute.clone();
        Ok((
            QueryNode {
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
