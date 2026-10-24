//! Resolution works on native nodes and typed attributes. Only the lowering
//! module is allowed to construct backend plans or expressions.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use sail_common::spec;

use super::deferred::Deferred;
use super::model::*;
use super::{AnalysisContext, AnalysisLayer};
use crate::error::PlanResult;

mod aggregate;
mod aggregate_functions;
mod aggregate_request;
mod alter_table;
mod array;
mod builders;
mod cast;
mod collection;
mod columns;
mod command;
mod conditional;
mod data_functions;
mod dataframe;
mod datetime;
mod datetime_arithmetic;
mod exists;
mod expression;
mod functions;
mod generators;
mod grouping;
mod input_metadata;
mod insert;
mod lakehouse;
mod lambda;
mod math;
mod merge;
mod misc;
mod native;
mod pivot;
mod plan;
mod predicate;
mod procedure;
mod python;
mod python_groups;
mod python_table;
mod query;
mod recovery;
mod relations;
mod retype;
mod row_level;
mod sample;
mod selectors;
mod sequence;
mod set_operation;
mod source;
mod statistics;
mod string;
mod table_definition;
mod table_properties;
mod time_travel;
mod time_window;
mod timezone;
mod update;
mod window;
mod window_functions;
mod write;
mod write_constraints;
mod write_defaults;
mod write_input;
mod write_stream;
mod write_v1;
mod write_v2;

#[derive(Default)]
pub struct ResolvePlan;

#[async_trait]
impl AnalysisLayer for ResolvePlan {
    fn name(&self) -> &str {
        "resolve_plan"
    }

    async fn transform(
        &self,
        mut plan: Plan,
        context: &mut AnalysisContext<'_>,
    ) -> PlanResult<Plan> {
        plan.reserve_ids(&mut context.ids)?;
        Resolver::new(context).native_plan(plan).await
    }
}

struct Resolver<'a, 'b> {
    context: &'a mut AnalysisContext<'b>,
    expression_aliases: Vec<NamedExpression>,
    expression_replacements: Vec<(spec::Expr, NamedExpression)>,
    outer: Vec<QuerySchema>,
    lambdas: Vec<QuerySchema>,
    // Definitions retain semantic origins; references allocate occurrence IDs.
    ctes: HashMap<String, Arc<CteDefinition>>,
    parameters: HashMap<String, datafusion_common::ScalarValue>,
    windows: HashMap<String, spec::Window>,
    relations: HashMap<i64, spec::QueryPlan>,
    recovery: Option<recovery::RecoveryScope>,
    outer_recovery: HashMap<usize, recovery::RecoveryScope>,
    discarded_predicate: bool,
    cte_origins: std::collections::HashSet<AttributeId>,
    parameter_origins: std::collections::HashSet<AttributeId>,
    bindings: HashMap<i64, Vec<QuerySchema>>,
}

struct CteDefinition {
    plan: QueryNode,
    parameter: bool,
}

#[derive(Debug)]
struct NamedExpression {
    name: String,
    expression: ExprNode,
}

impl<'a, 'b> Resolver<'a, 'b> {
    fn new(context: &'a mut AnalysisContext<'b>) -> Self {
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

impl Resolver<'_, '_> {
    fn name_key(&self, name: &str) -> String {
        if self.context.config.case_sensitive {
            name.to_string()
        } else {
            name.to_ascii_lowercase()
        }
    }

    fn attribute(&mut self, name: String, result_type: ExprType) -> Attribute {
        let id = self.context.ids.attribute();
        Attribute {
            id,
            origin: id,
            name,
            qualifier: vec![],
            qualified_position: 0,
            plan_ids: vec![],
            hidden: false,
            result_type: Deferred::Resolved(result_type),
        }
    }

    fn reference(&mut self, attribute: &Attribute) -> PlanResult<ExprNode> {
        Ok(ExprNode {
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
        let mut attribute = self.attribute(named.name, result_type);
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
        mut input: QueryNode,
        expressions: Vec<NamedExpression>,
    ) -> PlanResult<QueryNode> {
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
                            let attribute = self.attribute(
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
            input = QueryNode {
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
        Ok(QueryNode {
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
