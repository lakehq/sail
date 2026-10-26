use arrow_schema::DataType;
use sail_common::spec;

use super::IMPLICIT_CARTESIAN_PRODUCT_MSG;
use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::analyzer::typing;
use crate::error::{PlanError, PlanResult};
use crate::resolver::{NamedExpression, PlanResolver};

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn resolve_query_join(
        &mut self,
        join: spec::Join,
        lateral: bool,
    ) -> PlanResult<Query> {
        use datafusion_expr::JoinType as J;
        let explicit_cross = matches!(join.join_type, spec::JoinType::Cross);
        if explicit_cross && join.join_criteria.is_some() {
            return Err(PlanError::invalid("cross join with join criteria"));
        }
        if join.join_criteria.is_none() && join.join_data_type.is_some() {
            return Err(PlanError::invalid("cross join with join data type"));
        }
        if !lateral
            && !explicit_cross
            && join.join_criteria.is_none()
            && !self.context.config.cross_join_enabled
        {
            return Err(PlanError::analysis(IMPLICIT_CARTESIAN_PRODUCT_MSG));
        }
        let kind = if !lateral && join.join_criteria.is_none() {
            J::Inner
        } else {
            match join.join_type {
                spec::JoinType::Inner | spec::JoinType::Cross => J::Inner,
                spec::JoinType::LeftOuter => J::Left,
                spec::JoinType::RightOuter => J::Right,
                spec::JoinType::FullOuter => J::Full,
                spec::JoinType::LeftSemi => J::LeftSemi,
                spec::JoinType::RightSemi => J::RightSemi,
                spec::JoinType::LeftAnti => J::LeftAnti,
                spec::JoinType::RightAnti => J::RightAnti,
            }
        };
        let left = self.resolve_query_plan(*join.left).await?;
        if lateral {
            self.outer
                .push(left.schema.resolved("lateral left input")?.clone());
        }
        let right = self.resolve_query_plan(*join.right).await;
        if lateral {
            self.outer.pop();
        }
        let right = right?;
        let left_schema = left.schema.resolved("join left")?;
        let right_schema = right.schema.resolved("join right")?;
        let merged = QuerySchema {
            attributes: left_schema
                .attributes
                .iter()
                .chain(&right_schema.attributes)
                .cloned()
                .collect(),
        };
        let using = match &join.join_criteria {
            Some(spec::JoinCriteria::Using(names)) => Some(
                names
                    .iter()
                    .map(|name| name.as_ref().to_string())
                    .collect::<Vec<_>>(),
            ),
            Some(spec::JoinCriteria::Natural) => Some(
                left_schema
                    .attributes
                    .iter()
                    .filter(|left| {
                        !left.hidden
                            && right_schema.attributes.iter().any(|right| {
                                !right.hidden
                                    && self.name_key(&left.name) == self.name_key(&right.name)
                            })
                    })
                    .map(|attribute| attribute.name.clone())
                    .collect(),
            ),
            _ => None,
        };
        let mut keys = vec![];
        let condition = if let Some(names) = using {
            let mut condition = None;
            for name in names {
                let left_key = crate::analyzer::scope::ResolutionScope::new(left_schema, [])
                    .with_case_sensitive(self.context.config.case_sensitive)
                    .resolve(std::slice::from_ref(&name), None)?;
                let right_key = crate::analyzer::scope::ResolutionScope::new(right_schema, [])
                    .with_case_sensitive(self.context.config.case_sensitive)
                    .resolve(std::slice::from_ref(&name), None)?;
                let a = self.reference(left_schema.attribute(left_key.id)?)?;
                let b = self.reference(right_schema.attribute(right_key.id)?)?;
                let comparison = typing::resolve_binary(
                    self.context.ids.expression(),
                    a,
                    datafusion_expr::Operator::Eq,
                    b,
                    &mut self.context.ids,
                )?;
                let common = if let ExprKind::Binary { left, .. } =
                    comparison.kind.resolved("join key comparison")?
                {
                    left.result_type
                        .resolved("join key type")?
                        .data_type
                        .clone()
                } else {
                    return Err(PlanError::internal("join key comparison must be binary"));
                };
                condition = Some(match condition {
                    None => comparison,
                    Some(previous) => typing::resolve_binary(
                        self.context.ids.expression(),
                        previous,
                        datafusion_expr::Operator::And,
                        comparison,
                        &mut self.context.ids,
                    )?,
                });
                keys.push((name, left_key.id, right_key.id, common));
            }
            condition.map(Box::new)
        } else {
            match join.join_criteria {
                None => None,
                Some(spec::JoinCriteria::On(condition)) => {
                    let condition = self
                        .resolve_named_expression(condition, &merged)
                        .await?
                        .expression;
                    self.validate_join_condition(&condition, kind)?;
                    Some(Box::new(typing::coerce(
                        condition,
                        &DataType::Boolean,
                        &mut self.context.ids,
                    )?))
                }
                _ => return Err(PlanError::internal("join criteria dispatched twice")),
            }
        };
        let mut attributes = vec![];
        if !matches!(kind, J::RightSemi | J::RightAnti) {
            for attribute in &left_schema.attributes {
                attributes.push(Self::join_attribute(
                    attribute,
                    matches!(kind, J::Right | J::Full),
                )?);
            }
        }
        if !matches!(kind, J::LeftSemi | J::LeftAnti) {
            for attribute in &right_schema.attributes {
                attributes.push(Self::join_attribute(
                    attribute,
                    matches!(kind, J::Left | J::Full),
                )?);
            }
        }
        let joined = Query {
            kind: Deferred::Resolved(QueryKind::Join {
                lateral,
                left: Box::new(left),
                right: Box::new(right),
                kind,
                condition,
            }),
            schema: Deferred::Resolved(QuerySchema { attributes }),
        };
        if keys.is_empty() {
            return Ok(joined);
        }
        self.project_using_join(joined, keys, kind)
    }
}

impl PlanResolver<'_> {
    pub(in crate::resolver) fn project_using_join(
        &mut self,
        input: Query,
        keys: Vec<(String, AttributeId, AttributeId, DataType)>,
        kind: datafusion_expr::JoinType,
    ) -> PlanResult<Query> {
        use datafusion_expr::JoinType as J;
        let schema = input.schema.resolved("USING join input")?;
        let mut expressions = vec![];
        let key_ids = keys
            .iter()
            .flat_map(|(_, a, b, _)| [*a, *b])
            .collect::<std::collections::HashSet<_>>();
        for (name, left, right, _) in keys {
            let expression = if kind == J::Full {
                let left = self.reference(schema.attribute(left)?)?;
                let right = self.reference(schema.attribute(right)?)?;
                let arguments = crate::function::scalar::conditional::temporal_branches(
                    &mut self.context.function_context(),
                    vec![left, right],
                )?;
                typing::resolve_scalar_call(
                    self.context.ids.expression(),
                    self.context.function_context().scalar_kernel("coalesce")?,
                    arguments,
                    &mut self.context.ids,
                )?
            } else {
                let source = if matches!(kind, J::Right | J::RightSemi | J::RightAnti) {
                    right
                } else {
                    left
                };
                self.reference(schema.attribute(source)?)?
            };
            let attribute = self.context.function_context().attribute(
                name,
                expression
                    .result_type
                    .resolved("USING output type")?
                    .clone(),
            );
            expressions.push(Projection {
                expression,
                attribute,
            });
        }
        for source in &schema.attributes {
            if key_ids.contains(&source.id) {
                continue;
            }
            let expression = self.reference(source)?;
            let mut projection = self.projection(
                NamedExpression {
                    name: source.name.clone(),
                    expression,
                },
                schema,
            )?;
            projection.attribute.hidden = source.hidden;
            expressions.push(projection);
        }
        // Qualified references retain each side's original key. The merged key
        // is the only key exposed to unqualified lookup and wildcard expansion.
        for source in &schema.attributes {
            if !key_ids.contains(&source.id) {
                continue;
            }
            let expression = self.reference(source)?;
            let mut projection = self.projection(
                NamedExpression {
                    name: source.name.clone(),
                    expression,
                },
                schema,
            )?;
            projection.attribute.hidden = true;
            expressions.push(projection);
        }
        let schema = QuerySchema {
            attributes: expressions
                .iter()
                .map(|projection| projection.attribute.clone())
                .collect(),
        };
        Ok(Query {
            kind: Deferred::Resolved(QueryKind::Project {
                input: Box::new(input),
                expressions,
            }),
            schema: Deferred::Resolved(schema),
        })
    }
}

impl PlanResolver<'_> {
    pub(in crate::resolver) fn join_attribute(
        attribute: &Attribute,
        nullable: bool,
    ) -> PlanResult<Attribute> {
        let mut attribute = attribute.clone();
        if nullable {
            let mut field = attribute.result_type.into_resolved("join output type")?;
            field.nullable = true;
            attribute.result_type = Deferred::Resolved(field);
        }
        Ok(attribute)
    }
}

impl PlanResolver<'_> {
    pub(in crate::resolver) fn validate_join_condition(
        &self,
        expression: &Expr,
        kind: datafusion_expr::JoinType,
    ) -> PlanResult<()> {
        fn conjuncts(expression: &Expr, python: &mut bool, other: &mut bool) -> PlanResult<()> {
            match expression.kind.resolved("join conjunct")? {
                ExprKind::WithMetadata(inner) => conjuncts(inner, python, other)?,
                ExprKind::Binary {
                    left,
                    operator: datafusion_expr::Operator::And,
                    right,
                } => {
                    conjuncts(left, python, other)?;
                    conjuncts(right, python, other)?;
                }
                ExprKind::ScalarFunction { function, .. }
                    if function
                        .inner()
                        .is::<sail_python_udf::udf::pyspark_udf::PySparkUDF>() =>
                {
                    *python = true
                }
                _ => *other = true,
            }
            Ok(())
        }
        let (mut python, mut other) = (false, false);
        conjuncts(expression, &mut python, &mut other)?;
        if python {
            use datafusion_expr::JoinType as J;
            if kind == J::Inner {
                if !other && !self.context.config.cross_join_enabled {
                    return Err(PlanError::analysis(IMPLICIT_CARTESIAN_PRODUCT_MSG));
                }
            } else {
                let name = match kind {
                    J::Left => "LEFT OUTER",
                    J::Right => "RIGHT OUTER",
                    J::Full => "FULL OUTER",
                    J::LeftSemi => "LEFT SEMI",
                    J::RightSemi => "RIGHT SEMI",
                    J::LeftAnti => "LEFT ANTI",
                    J::RightAnti => "RIGHT ANTI",
                    J::LeftMark => "LEFT MARK",
                    J::RightMark => "RIGHT MARK",
                    J::Inner => "INNER",
                };
                return Err(PlanError::analysis(format!(
                    "Python UDF in the ON clause of a {name} JOIN."
                )));
            }
        }
        Ok(())
    }
}
