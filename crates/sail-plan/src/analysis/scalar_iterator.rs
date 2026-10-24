//! Scalar Python iterators consume a relation, not individual scalar batches.
//! Extract them while attribute identities and argument types are still native.
use std::sync::Arc;

use arrow_schema::Schema;
use datafusion_expr::JoinType;
use sail_python_udf::udf::pyspark_scalar_iter_udf::PySparkScalarPandasIterUDF;
use sail_python_udf::udf::pyspark_udf::{PySparkUDF, PySparkUdfKind};
use sail_query::rewrite::{QueryRewriter, walk_expression, walk_query};

use super::deferred::Deferred;
use super::model::*;
use crate::error::{PlanError, PlanResult};

pub(super) fn materialize(plan: QueryNode, ids: &mut IdGenerator) -> PlanResult<QueryNode> {
    Materialize { ids }.rewrite_query(plan)
}

struct Materialize<'a> {
    ids: &'a mut IdGenerator,
}

impl QueryRewriter for Materialize<'_> {
    type Error = PlanError;

    fn rewrite_query(&mut self, plan: QueryNode) -> PlanResult<QueryNode> {
        let plan = walk_query(self, plan)?;
        let output = plan.schema.into_resolved("iterator output")?;
        let mut node = plan.kind.into_resolved("iterator plan")?;
        if let QueryKind::Join {
            lateral,
            left,
            right,
            kind,
            condition: Some(condition),
        } = node
        {
            let mut predicates = vec![];
            split_conjunction(*condition, &mut predicates)?;
            let (outside, inside): (Vec<_>, Vec<_>) =
                predicates.into_iter().partition(|predicate| {
                    predicate
                        .any(&|node| {
                            let ExprKind::ScalarFunction {
                                function,
                                arguments,
                            } = node
                            else {
                                return false;
                            };
                            if !is_iterator(function) {
                                return false;
                            }
                            !arguments.iter().all(|e| in_scope(e, &left))
                                && !arguments.iter().all(|e| in_scope(e, &right))
                        })
                        .unwrap_or(false)
                });
            if !outside.is_empty() {
                if kind != JoinType::Inner {
                    return Err(PlanError::analysis(
                        "scalar iterator UDF referencing both join inputs requires an inner join",
                    ));
                }
                let input = QueryNode {
                    kind: Deferred::Resolved(QueryKind::Join {
                        lateral,
                        left,
                        right,
                        kind,
                        condition: conjunction(inside, self.ids)?,
                    }),
                    schema: Deferred::Resolved(output.clone()),
                };
                let condition = conjunction(outside, self.ids)?
                    .ok_or_else(|| PlanError::internal("missing iterator predicate"))?;
                return self.rewrite_query(QueryNode {
                    kind: Deferred::Resolved(QueryKind::Filter {
                        input: Box::new(input),
                        condition,
                    }),
                    schema: Deferred::Resolved(output),
                });
            }
            node = QueryKind::Join {
                lateral,
                left,
                right,
                kind,
                condition: conjunction(inside, self.ids)?,
            };
        }
        let mut extract = Extract {
            ids: self.ids,
            inputs: vec![],
            replacements: vec![],
        };
        let node = match node {
            QueryKind::Project { input, expressions } => {
                extract.inputs.push(*input);
                let expressions = extract.rewrite_projections(expressions)?;
                QueryKind::Project {
                    input: extract.input()?,
                    expressions,
                }
            }
            QueryKind::Aggregate {
                input,
                grouping,
                grouping_sets,
                grouping_id,
                aggregates,
            } => {
                extract.inputs.push(*input);
                let grouping = extract.rewrite_projections(grouping)?;
                let aggregates = extract.rewrite_projections(aggregates)?;
                QueryKind::Aggregate {
                    input: extract.input()?,
                    grouping,
                    grouping_sets,
                    grouping_id,
                    aggregates,
                }
            }
            QueryKind::Window { input, expressions } => {
                extract.inputs.push(*input);
                let expressions = extract.rewrite_projections(expressions)?;
                QueryKind::Window {
                    input: extract.input()?,
                    expressions,
                }
            }
            QueryKind::Filter { input, condition } => {
                extract.inputs.push(*input);
                let condition = Box::new(extract.rewrite_expression(*condition)?);
                QueryKind::Filter {
                    input: extract.input()?,
                    condition,
                }
            }
            QueryKind::Sort {
                input,
                order,
                global,
                required,
            } => {
                extract.inputs.push(*input);
                let order = extract.rewrite_sorts(order)?;
                QueryKind::Sort {
                    input: extract.input()?,
                    order,
                    global,
                    required,
                }
            }
            QueryKind::DistinctOn { input, keys } => {
                extract.inputs.push(*input);
                let keys = keys
                    .into_iter()
                    .map(|e| extract.rewrite_expression(e))
                    .collect::<PlanResult<_>>()?;
                QueryKind::DistinctOn {
                    input: extract.input()?,
                    keys,
                }
            }
            QueryKind::Repartition {
                input,
                partitioning,
            } => {
                extract.inputs.push(*input);
                let partitioning = match partitioning {
                    Partitioning::Hash(values, n) => Partitioning::Hash(
                        values
                            .into_iter()
                            .map(|e| extract.rewrite_expression(e))
                            .collect::<PlanResult<_>>()?,
                        n,
                    ),
                    Partitioning::Distribute(values) => Partitioning::Distribute(
                        values
                            .into_iter()
                            .map(|e| extract.rewrite_expression(e))
                            .collect::<PlanResult<_>>()?,
                    ),
                    value => value,
                };
                QueryKind::Repartition {
                    input: extract.input()?,
                    partitioning,
                }
            }
            QueryKind::Join {
                lateral,
                left,
                right,
                kind,
                condition,
            } => {
                extract.inputs.extend([*left, *right]);
                let condition = condition
                    .map(|e| extract.rewrite_expression(*e).map(Box::new))
                    .transpose()?;
                let right = extract.input()?;
                let left = extract.input()?;
                QueryKind::Join {
                    lateral,
                    left,
                    right,
                    kind,
                    condition,
                }
            }
            node => node,
        };
        let attributes = match &node {
            QueryKind::Window { input, expressions } => input
                .schema
                .resolved("iterator window")?
                .attributes
                .iter()
                .cloned()
                .chain(expressions.iter().map(|p| p.attribute.clone()))
                .collect(),
            QueryKind::Filter { input, .. }
            | QueryKind::Sort { input, .. }
            | QueryKind::Repartition { input, .. }
            | QueryKind::DistinctOn { input, .. } => {
                input.schema.resolved("iterator input")?.attributes.clone()
            }
            QueryKind::Join {
                left, right, kind, ..
            } => {
                let left = &left.schema.resolved("iterator join left")?.attributes;
                let right = &right.schema.resolved("iterator join right")?.attributes;
                let mut attributes = left
                    .iter()
                    .filter(|_| {
                        !matches!(
                            kind,
                            JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark
                        )
                    })
                    .chain(right.iter().filter(|_| {
                        !matches!(
                            kind,
                            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark
                        )
                    }))
                    .map(|attribute| {
                        output
                            .attributes
                            .iter()
                            .find(|a| a.id == attribute.id)
                            .unwrap_or(attribute)
                            .clone()
                    })
                    .collect::<Vec<_>>();
                for attribute in &output.attributes {
                    if !attributes.iter().any(|a| a.id == attribute.id) {
                        attributes.push(attribute.clone());
                    }
                }
                attributes
            }
            _ => output.attributes.clone(),
        };
        if attributes.len() == output.attributes.len() {
            return Ok(QueryNode {
                kind: Deferred::Resolved(node),
                schema: Deferred::Resolved(output),
            });
        }
        let expressions = output
            .attributes
            .iter()
            .map(|a| Projection {
                expression: reference(a, extract.ids),
                attribute: a.clone(),
            })
            .collect();
        let input = QueryNode {
            kind: Deferred::Resolved(node),
            schema: Deferred::Resolved(QuerySchema { attributes }),
        };
        Ok(QueryNode {
            kind: Deferred::Resolved(QueryKind::Project {
                input: Box::new(input),
                expressions,
            }),
            schema: Deferred::Resolved(output),
        })
    }
}

struct Extract<'a> {
    ids: &'a mut IdGenerator,
    inputs: Vec<QueryNode>,
    replacements: Vec<(ExprNode, Attribute)>,
}

impl Extract<'_> {
    fn input(&mut self) -> PlanResult<Box<QueryNode>> {
        self.inputs
            .pop()
            .map(Box::new)
            .ok_or_else(|| PlanError::internal("missing iterator input"))
    }
}

impl QueryRewriter for Extract<'_> {
    type Error = PlanError;

    fn rewrite_query(&mut self, plan: QueryNode) -> PlanResult<QueryNode> {
        Ok(plan)
    }

    fn rewrite_expression(&mut self, expression: ExprNode) -> PlanResult<ExprNode> {
        let expression = walk_expression(self, expression)?;
        let ExprKind::ScalarFunction {
            function,
            arguments,
        } = expression.kind.resolved("iterator expression")?
        else {
            return Ok(expression);
        };
        let Some(udf) = function.inner().downcast_ref::<PySparkUDF>() else {
            return Ok(expression);
        };
        if udf.kind() != PySparkUdfKind::ScalarPandasIter {
            return Ok(expression);
        }
        if udf.deterministic() {
            for (original, attribute) in &self.replacements {
                if original.equivalent(&expression)? {
                    return Ok(reference(attribute, self.ids));
                }
            }
        }
        let index = self
            .inputs
            .iter()
            .position(|input| arguments.iter().all(|e| in_scope(e, input)))
            .ok_or_else(|| {
                PlanError::analysis(
                    "scalar iterator UDF arguments must be evaluable on one relational input",
                )
            })?;
        let input = self.inputs[index].clone();
        let mut output = input.schema.resolved("iterator input")?.clone();
        let mut projections = output
            .attributes
            .iter()
            .map(|a| Projection {
                expression: reference(a, self.ids),
                attribute: a.clone(),
            })
            .collect::<Vec<_>>();
        for expression in arguments {
            let id = self.ids.attribute();
            projections.push(Projection {
                expression: expression.clone(),
                attribute: Attribute {
                    id,
                    origin: id,
                    name: format!("argument_{}", projections.len()),
                    qualifier: vec![],
                    qualified_position: 0,
                    plan_ids: vec![],
                    hidden: true,
                    result_type: expression.result_type.clone(),
                },
            });
        }
        let id = self.ids.attribute();
        let attribute = Attribute {
            id,
            origin: id,
            name: function.name().to_string(),
            qualifier: vec![],
            qualified_position: 0,
            plan_ids: vec![],
            hidden: true,
            result_type: expression.result_type.clone(),
        };
        output.attributes.push(attribute.clone());
        let fields = output
            .attributes
            .iter()
            .enumerate()
            .map(|(index, a)| {
                Ok(a.result_type
                    .resolved("iterator field")?
                    .to_field(format!("column_{index}")))
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let stream = PySparkScalarPandasIterUDF::try_new(
            function.name().to_string(),
            udf.payload().to_vec(),
            Arc::new(Schema::new(fields)),
            Arc::clone(udf.config()),
        )?;
        let schema = QuerySchema {
            attributes: projections.iter().map(|p| p.attribute.clone()).collect(),
        };
        let input = QueryNode {
            kind: Deferred::Resolved(QueryKind::Project {
                input: Box::new(input),
                expressions: projections,
            }),
            schema: Deferred::Resolved(schema),
        };
        self.inputs[index] = QueryNode {
            kind: Deferred::Resolved(QueryKind::MapPartitions {
                input: Box::new(input),
                function: Arc::new(stream),
            }),
            schema: Deferred::Resolved(output),
        };
        if udf.deterministic() {
            self.replacements.push((expression, attribute.clone()));
        }
        Ok(reference(&attribute, self.ids))
    }
}

fn reference(attribute: &Attribute, ids: &mut IdGenerator) -> ExprNode {
    ExprNode {
        id: ids.expression(),
        kind: Deferred::Resolved(ExprKind::Attribute(attribute.id)),
        result_type: attribute.result_type.clone(),
    }
}

fn is_iterator(function: &datafusion_expr::ScalarUDF) -> bool {
    function
        .inner()
        .downcast_ref::<PySparkUDF>()
        .is_some_and(|u| u.kind() == PySparkUdfKind::ScalarPandasIter)
}

fn in_scope(expression: &ExprNode, plan: &QueryNode) -> bool {
    let Ok(schema) = plan.schema.resolved("iterator scope") else {
        return false;
    };
    expression
        .any(&|node| matches!(node, ExprKind::Attribute(id) if schema.attribute(*id).is_err()))
        .is_ok_and(|missing| !missing)
}

fn split_conjunction(expression: ExprNode, output: &mut Vec<ExprNode>) -> PlanResult<()> {
    match expression.kind.resolved("join predicate")? {
        ExprKind::Binary {
            left,
            operator: datafusion_expr::Operator::And,
            right,
        } => {
            split_conjunction(*left.clone(), output)?;
            split_conjunction(*right.clone(), output)?;
        }
        _ => output.push(expression),
    }
    Ok(())
}

fn conjunction(
    expressions: Vec<ExprNode>,
    ids: &mut IdGenerator,
) -> PlanResult<Option<Box<ExprNode>>> {
    let mut expressions = expressions.into_iter();
    let Some(mut result) = expressions.next() else {
        return Ok(None);
    };
    for expression in expressions {
        result = super::typing::resolve_binary(
            ids.expression(),
            result,
            datafusion_expr::Operator::And,
            expression,
            ids,
        )?;
    }
    Ok(Some(Box::new(result)))
}
