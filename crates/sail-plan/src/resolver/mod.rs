//! Resolution works on native nodes and typed attributes. Only the lowering
//! module is allowed to construct backend plans or expressions.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::prelude::SessionContext;
pub(crate) use expression::predicate::{coerce_timestamp_pair, in_arguments, in_list};
use sail_common::spec;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::analyzer::{AnalyzerContext, AnalyzerLayer};
use crate::config::PlanConfig;
use crate::error::PlanResult;

#[async_trait]
impl AnalyzerLayer for PlanResolver<'_> {
    async fn analyze(&mut self, mut plan: AnalyzerPlan) -> PlanResult<AnalyzerPlan> {
        *self = Self::new(self.context.session, Arc::clone(&self.context.config));
        plan.reserve_ids(&mut self.context.ids)?;
        self.resolve_plan(plan).await
    }
}

pub struct PlanResolver<'a> {
    context: AnalyzerContext<'a>,
    expression_aliases: Vec<NamedExpression>,
    expression_replacements: Vec<(spec::Expr, NamedExpression)>,
    outer: Vec<QuerySchema>,
    lambdas: Vec<QuerySchema>,
    // Definitions retain semantic origins; references allocate occurrence IDs.
    ctes: HashMap<String, Arc<CteDefinition>>,
    parameters: HashMap<String, datafusion_common::ScalarValue>,
    windows: HashMap<String, spec::Window>,
    relations: HashMap<i64, spec::QueryPlan>,
    recovery: Option<query::missing_input::RecoveryScope>,
    outer_recovery: HashMap<usize, query::missing_input::RecoveryScope>,
    discarded_predicate: bool,
    cte_origins: std::collections::HashSet<AttributeId>,
    parameter_origins: std::collections::HashSet<AttributeId>,
    bindings: HashMap<i64, Vec<QuerySchema>>,
}

struct CteDefinition {
    plan: Query,
    parameter: bool,
}

#[derive(Debug)]
struct NamedExpression {
    name: String,
    expression: Expr,
}

impl<'a> PlanResolver<'a> {
    pub fn new(session: &'a SessionContext, config: Arc<PlanConfig>) -> Self {
        Self::with_context(AnalyzerContext {
            session,
            config,
            ids: IdGenerator::default(),
        })
    }

    pub(crate) fn with_context(context: AnalyzerContext<'a>) -> Self {
        Self {
            context,
            outer: vec![],
            lambdas: vec![],
            ctes: HashMap::new(),
            parameters: HashMap::new(),
            windows: HashMap::new(),
            relations: HashMap::new(),
            recovery: None,
            outer_recovery: Default::default(),
            discarded_predicate: false,
            cte_origins: Default::default(),
            parameter_origins: Default::default(),
            bindings: Default::default(),
            expression_aliases: vec![],
            expression_replacements: vec![],
        }
    }
}

impl PlanResolver<'_> {
    fn name_key(&self, name: &str) -> String {
        if self.context.config.case_sensitive {
            name.to_string()
        } else {
            name.to_ascii_lowercase()
        }
    }

    fn reference(&mut self, attribute: &Attribute) -> PlanResult<Expr> {
        Ok(Expr {
            id: self.context.ids.expression(),
            kind: Deferred::Resolved(ExprKind::Attribute(attribute.id)),
            result_type: Deferred::Resolved(
                attribute.result_type.resolved("attribute type")?.clone(),
            ),
        })
    }

    fn projection(
        &mut self,
        named: NamedExpression,
        input: &QuerySchema,
    ) -> PlanResult<Projection> {
        let result_type = named
            .expression
            .result_type
            .resolved("projection expression type")?
            .clone();
        let mut attribute = self
            .context
            .function_context()
            .attribute(named.name, result_type);
        if let ExprKind::Attribute(id) = named.expression.kind.resolved("projection expression")? {
            let source = input.attribute(*id)?;
            attribute.origin = source.origin;
            attribute.qualifier.clone_from(&source.qualifier);
            attribute.qualified_position = source.qualified_position;
            attribute.plan_ids.clone_from(&source.plan_ids);
        }
        Ok(Projection {
            expression: named.expression,
            attribute,
        })
    }

    fn project(
        &mut self,
        mut input: Query,
        expressions: Vec<NamedExpression>,
    ) -> PlanResult<Query> {
        let mut windows: Vec<Projection> = vec![];
        let expressions = expressions
            .into_iter()
            .map(|named| {
                let expression = self.projected_exists(&mut input, named.expression)?;
                let expression =
                    expression.transform_up::<crate::error::PlanError>(&mut |expression| {
                        if matches!(
                            expression.kind.resolved("project expression")?,
                            ExprKind::WindowFunction { .. }
                        ) {
                            for window in &windows {
                                if window.expression.equivalent(&expression)? {
                                    return self.reference(&window.attribute);
                                }
                            }
                            let attribute = self.context.function_context().attribute(
                                format!("window_{}", windows.len()),
                                expression.result_type.resolved("window type")?.clone(),
                            );
                            let reference = self.reference(&attribute)?;
                            windows.push(Projection {
                                expression,
                                attribute,
                            });
                            Ok(reference)
                        } else {
                            Ok(expression)
                        }
                    })?;
                Ok(NamedExpression {
                    name: named.name,
                    expression,
                })
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let mut window_values = windows
            .iter()
            .map(|p| NamedExpression {
                name: p.attribute.name.clone(),
                expression: p.expression.clone(),
            })
            .collect::<Vec<_>>();
        self.grouping_metadata(&mut input, &mut window_values, &mut [], &mut None)?;
        for (window, value) in windows.iter_mut().zip(window_values) {
            window.expression = value.expression;
        }
        for window in windows {
            let mut schema = input.schema.resolved("window input")?.clone();
            schema.attributes.push(window.attribute.clone());
            input = Query {
                kind: Deferred::Resolved(QueryKind::Window {
                    input: Box::new(input),
                    expressions: vec![window],
                }),
                schema: Deferred::Resolved(schema),
            };
        }
        let schema = input.schema.resolved("projection input")?;
        let expressions = expressions
            .into_iter()
            .map(|expression| self.projection(expression, schema))
            .collect::<PlanResult<Vec<_>>>()?;
        let schema = QuerySchema {
            attributes: expressions.iter().map(|p| p.attribute.clone()).collect(),
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

#[cfg(test)]
mod tests;

pub(in crate::resolver) mod builders;
pub(in crate::resolver) mod command;
pub mod data_type;
pub(in crate::resolver) mod expression;
pub(in crate::resolver) mod lakehouse;
pub(in crate::resolver) mod literal;
pub(in crate::resolver) mod native;
pub(in crate::resolver) mod plan;
pub(in crate::resolver) mod query;
pub(in crate::resolver) mod retype;
pub(in crate::resolver) mod tree;
