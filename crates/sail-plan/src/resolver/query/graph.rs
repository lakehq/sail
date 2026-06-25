use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion_common::tree_node::TreeNode;
use datafusion_common::JoinType;
use datafusion_expr::{
    build_join_schema, Filter, Limit, LogicalPlan, LogicalPlanBuilder, Projection, SubqueryAlias,
};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::TableColumnStatus;
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::explode::ExplodeRewriter;
use crate::resolver::tree::monotonic_id::MonotonicIdRewriter;
use crate::resolver::tree::spark_partition_id::SparkPartitionIdRewriter;
use crate::resolver::tree::window::WindowRewriter;
use crate::resolver::PlanResolver;

const GRAPH_NODES_TABLE: &str = "grust_nodes";
const GRAPH_EDGES_TABLE: &str = "grust_edges";
const GRAPH_TABLE_KIND_PROPERTY: &str = "grust.graph.kind";
const GRAPH_TABLE_LABEL_PROPERTY: &str = "grust.graph.label";
const GRAPH_TABLE_KIND_NODE: &str = "node";
const GRAPH_TABLE_KIND_EDGE: &str = "edge";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GraphAliasKind {
    Node,
    Edge,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum GraphAliasSource {
    Generic,
    Typed { label: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GraphAliasInfo {
    kind: GraphAliasKind,
    source: Option<GraphAliasSource>,
}

struct GraphAliasBinding {
    alias: String,
    is_new: bool,
}

#[derive(Default)]
struct GraphBindings {
    aliases: HashMap<String, GraphAliasInfo>,
    next_node: usize,
    next_edge: usize,
}

impl GraphBindings {
    fn bind_alias(&mut self, alias: String, kind: GraphAliasKind) -> PlanResult<GraphAliasBinding> {
        match self.aliases.get(&alias) {
            Some(existing) if existing.kind == kind => Ok(GraphAliasBinding {
                alias,
                is_new: false,
            }),
            Some(_) => Err(PlanError::invalid(format!(
                "graph variable `{alias}` is used with incompatible pattern element types"
            ))),
            None => {
                self.aliases
                    .insert(alias.clone(), GraphAliasInfo { kind, source: None });
                Ok(GraphAliasBinding {
                    alias,
                    is_new: true,
                })
            }
        }
    }

    fn bind_node(&mut self, node: &spec::GraphNodePattern) -> PlanResult<GraphAliasBinding> {
        let alias = match &node.variable {
            Some(variable) => variable.as_ref().to_string(),
            None => {
                let alias = format!("_graph_n{}", self.next_node);
                self.next_node += 1;
                alias
            }
        };
        self.bind_alias(alias, GraphAliasKind::Node)
    }

    fn bind_edge(&mut self, edge: &spec::GraphEdgePattern) -> PlanResult<GraphAliasBinding> {
        let alias = match &edge.variable {
            Some(variable) => variable.as_ref().to_string(),
            None => {
                let alias = format!("_graph_e{}", self.next_edge);
                self.next_edge += 1;
                alias
            }
        };
        self.bind_alias(alias, GraphAliasKind::Edge)
    }

    fn kind(&self, alias: &str) -> Option<GraphAliasKind> {
        self.aliases.get(alias).map(|info| info.kind)
    }

    fn source(&self, alias: &str) -> Option<&GraphAliasSource> {
        self.aliases
            .get(alias)
            .and_then(|info| info.source.as_ref())
    }

    fn set_source(&mut self, alias: &str, source: GraphAliasSource) -> PlanResult<()> {
        let info = self.aliases.get_mut(alias).ok_or_else(|| {
            PlanError::internal(format!("missing graph alias binding for `{alias}`"))
        })?;
        info.source = Some(source);
        Ok(())
    }
}

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_graph(
        &self,
        query: spec::GraphQuery,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::GraphQuery {
            patterns,
            predicates,
            returns,
            order,
            skip,
            limit,
        } = query;
        let field_usage = graph_field_usage(
            &patterns,
            &predicates,
            &returns,
            &order,
            skip.as_ref(),
            limit.as_ref(),
        );

        if patterns.is_empty() {
            return Err(PlanError::invalid(
                "graph query requires at least one pattern",
            ));
        }

        let mut bindings = GraphBindings::default();
        let mut plan = None;
        for pattern in patterns {
            plan = Some(
                self.resolve_graph_path_pattern(pattern, plan, &field_usage, &mut bindings, state)
                    .await?,
            );
        }
        let mut plan = plan.ok_or_else(|| PlanError::internal("missing graph plan"))?;

        for predicate in predicates {
            plan = self
                .apply_graph_filter(plan, rewrite_graph_expr(predicate, &bindings), state)
                .await?;
        }

        if !order.is_empty() {
            let order = order
                .into_iter()
                .map(|sort| rewrite_graph_sort_order(sort, &bindings))
                .collect();
            let sorts = self
                .resolve_sort_orders(order, true, plan.schema(), state)
                .await?;
            plan = LogicalPlanBuilder::from(plan).sort(sorts)?.build()?;
        }

        if skip.is_some() || limit.is_some() {
            let skip = if let Some(expr) = skip {
                Some(
                    self.resolve_expression(
                        rewrite_graph_expr(expr, &bindings),
                        plan.schema(),
                        state,
                    )
                    .await?,
                )
            } else {
                None
            };
            let limit = if let Some(expr) = limit {
                Some(
                    self.resolve_expression(
                        rewrite_graph_expr(expr, &bindings),
                        plan.schema(),
                        state,
                    )
                    .await?,
                )
            } else {
                None
            };
            plan = LogicalPlan::Limit(Limit {
                skip: skip.map(Box::new),
                fetch: limit.map(Box::new),
                input: Arc::new(plan),
            });
        }

        let returns = returns
            .into_iter()
            .map(|expr| rewrite_graph_return_expr(expr, &bindings))
            .collect();
        self.project_graph(plan, returns, state).await
    }

    async fn resolve_graph_path_pattern(
        &self,
        pattern: spec::GraphPathPattern,
        input: Option<LogicalPlan>,
        field_usage: &HashMap<String, HashSet<String>>,
        bindings: &mut GraphBindings,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::GraphPathPattern { start, steps } = pattern;
        let start_binding = bindings.bind_node(&start)?;
        let mut plan = if start_binding.is_new {
            let start_plan = self
                .resolve_graph_node_scan(&start_binding.alias, &start, field_usage, bindings, state)
                .await?;
            self.join_graph_plans(input, start_plan, None, state)
                .await?
        } else {
            let input = input.ok_or_else(|| {
                PlanError::internal("existing graph binding requires an input plan")
            })?;
            self.apply_existing_node_label(input, &start_binding.alias, &start, bindings, state)
                .await?
        };
        let mut previous_node_alias = start_binding.alias;

        for step in steps {
            let spec::GraphPatternStep {
                direction,
                edge,
                target,
            } = step;

            let edge_binding = bindings.bind_edge(&edge)?;
            let target_binding = bindings.bind_node(&target)?;
            let (source_condition, target_condition) = graph_step_conditions(
                direction,
                &previous_node_alias,
                &edge_binding.alias,
                &target_binding.alias,
            );
            plan = if edge_binding.is_new {
                let edge_plan = self
                    .resolve_graph_edge_scan(
                        &edge_binding.alias,
                        &edge,
                        field_usage,
                        bindings,
                        state,
                    )
                    .await?;
                self.join_graph_plans(Some(plan), edge_plan, Some(source_condition), state)
                    .await?
            } else {
                let plan = self
                    .apply_graph_filter(plan, source_condition, state)
                    .await?;
                self.apply_existing_edge_label(plan, &edge_binding.alias, &edge, bindings, state)
                    .await?
            };

            plan = if target_binding.is_new {
                let target_plan = self
                    .resolve_graph_node_scan(
                        &target_binding.alias,
                        &target,
                        field_usage,
                        bindings,
                        state,
                    )
                    .await?;
                self.join_graph_plans(Some(plan), target_plan, Some(target_condition), state)
                    .await?
            } else {
                let plan = self
                    .apply_graph_filter(plan, target_condition, state)
                    .await?;
                self.apply_existing_node_label(
                    plan,
                    &target_binding.alias,
                    &target,
                    bindings,
                    state,
                )
                .await?
            };
            previous_node_alias = target_binding.alias;
        }

        Ok(plan)
    }

    async fn resolve_graph_node_scan(
        &self,
        alias: &str,
        pattern: &spec::GraphNodePattern,
        field_usage: &HashMap<String, HashSet<String>>,
        bindings: &mut GraphBindings,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let scan_fields =
            graph_alias_scan_fields(field_usage.get(alias), &pattern.properties, &["id"]);
        if let Some(label) = pattern.label.as_ref() {
            if let Some(plan) = self
                .try_resolve_typed_graph_scan(
                    &typed_node_table(label.as_ref())?,
                    alias,
                    GRAPH_TABLE_KIND_NODE,
                    label.as_ref(),
                    scan_fields.as_ref(),
                    typed_node_table_missing_fields,
                    state,
                )
                .await?
            {
                bindings.set_source(
                    alias,
                    GraphAliasSource::Typed {
                        label: label.as_ref().to_string(),
                    },
                )?;
                return self
                    .apply_graph_properties(plan, alias, &pattern.properties, bindings, state)
                    .await;
            }
        }
        bindings.set_source(alias, GraphAliasSource::Generic)?;
        let plan = self
            .resolve_graph_scan(GRAPH_NODES_TABLE, alias, state)
            .await?;
        let plan = if let Some(label) = pattern.label.as_ref() {
            self.apply_graph_filter(
                plan,
                eq(attr(alias, "label"), string_lit(label.as_ref())),
                state,
            )
            .await?
        } else {
            plan
        };
        self.apply_graph_properties(plan, alias, &pattern.properties, bindings, state)
            .await
    }

    async fn apply_existing_node_label(
        &self,
        input: LogicalPlan,
        alias: &str,
        pattern: &spec::GraphNodePattern,
        bindings: &GraphBindings,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let plan = if let Some(label) = pattern.label.as_ref() {
            match graph_alias_label(bindings, alias) {
                Some(existing) if existing == label.as_ref() => input,
                Some(existing) => {
                    self.apply_graph_filter(
                        input,
                        eq(string_lit(existing), string_lit(label.as_ref())),
                        state,
                    )
                    .await?
                }
                None => {
                    self.apply_graph_filter(
                        input,
                        eq(attr(alias, "label"), string_lit(label.as_ref())),
                        state,
                    )
                    .await?
                }
            }
        } else {
            input
        };
        self.apply_graph_properties(plan, alias, &pattern.properties, bindings, state)
            .await
    }

    async fn apply_existing_edge_label(
        &self,
        input: LogicalPlan,
        alias: &str,
        pattern: &spec::GraphEdgePattern,
        bindings: &GraphBindings,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let plan = if let Some(label) = pattern.label.as_ref() {
            match graph_alias_label(bindings, alias) {
                Some(existing) if existing == label.as_ref() => input,
                Some(existing) => {
                    self.apply_graph_filter(
                        input,
                        eq(string_lit(existing), string_lit(label.as_ref())),
                        state,
                    )
                    .await?
                }
                None => {
                    self.apply_graph_filter(
                        input,
                        eq(attr(alias, "edge_type"), string_lit(label.as_ref())),
                        state,
                    )
                    .await?
                }
            }
        } else {
            input
        };
        self.apply_graph_properties(plan, alias, &pattern.properties, bindings, state)
            .await
    }

    async fn resolve_graph_edge_scan(
        &self,
        alias: &str,
        pattern: &spec::GraphEdgePattern,
        field_usage: &HashMap<String, HashSet<String>>,
        bindings: &mut GraphBindings,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let scan_fields = graph_alias_scan_fields(
            field_usage.get(alias),
            &pattern.properties,
            &["src_id", "dst_id"],
        );
        if let Some(label) = pattern.label.as_ref() {
            if let Some(plan) = self
                .try_resolve_typed_graph_scan(
                    &typed_edge_table(label.as_ref())?,
                    alias,
                    GRAPH_TABLE_KIND_EDGE,
                    label.as_ref(),
                    scan_fields.as_ref(),
                    typed_edge_table_missing_fields,
                    state,
                )
                .await?
            {
                bindings.set_source(
                    alias,
                    GraphAliasSource::Typed {
                        label: label.as_ref().to_string(),
                    },
                )?;
                return self
                    .apply_graph_properties(plan, alias, &pattern.properties, bindings, state)
                    .await;
            }
        }
        bindings.set_source(alias, GraphAliasSource::Generic)?;
        let plan = self
            .resolve_graph_scan(GRAPH_EDGES_TABLE, alias, state)
            .await?;
        let plan = if let Some(label) = pattern.label.as_ref() {
            self.apply_graph_filter(
                plan,
                eq(attr(alias, "edge_type"), string_lit(label.as_ref())),
                state,
            )
            .await?
        } else {
            plan
        };
        self.apply_graph_properties(plan, alias, &pattern.properties, bindings, state)
            .await
    }

    async fn try_resolve_typed_graph_scan(
        &self,
        table: &str,
        alias: &str,
        expected_kind: &str,
        expected_label: &str,
        fields: Option<&HashSet<String>>,
        table_missing_fields: impl Fn(&[TableColumnStatus], &HashSet<String>) -> Vec<String>,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<LogicalPlan>> {
        let Some(fields) = fields else {
            return Ok(None);
        };
        let status = match self
            .ctx
            .extension::<CatalogManager>()?
            .get_table_or_view(&[table.to_string()])
            .await
        {
            Ok(status) => status,
            Err(error) => {
                let error = PlanError::from(error);
                return if is_table_not_found(&error) {
                    log::debug!(
                        "falling back to generic graph table for alias `{alias}` because typed graph table `{table}` was not found"
                    );
                    Ok(None)
                } else {
                    Err(error)
                };
            }
        };
        let metadata_mismatches = graph_table_metadata_mismatches(
            status.kind.properties(),
            expected_kind,
            expected_label,
        );
        if !metadata_mismatches.is_empty() {
            log::debug!(
                "falling back to generic graph table for alias `{alias}` because typed graph table `{table}` has incompatible graph metadata: {}",
                metadata_mismatches.join(", ")
            );
            return Ok(None);
        }
        let missing = table_missing_fields(&status.kind.columns(), fields);
        if !missing.is_empty() {
            log::debug!(
                "falling back to generic graph table for alias `{alias}` because typed graph table `{table}` is missing required fields: {}",
                missing.join(", ")
            );
            return Ok(None);
        }
        match self.resolve_graph_scan(table, alias, state).await {
            Ok(plan) => Ok(Some(plan)),
            Err(error) if is_table_not_found(&error) => {
                log::debug!(
                    "falling back to generic graph table for alias `{alias}` because typed graph table `{table}` disappeared before planning"
                );
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    async fn resolve_graph_scan(
        &self,
        table: &str,
        alias: &str,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let plan = self
            .resolve_query_read_named_table(
                spec::ReadNamedTable {
                    name: spec::ObjectName::bare(table),
                    temporal: None,
                    sample: None,
                    options: vec![],
                },
                state,
            )
            .await?;
        Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(plan),
            self.resolve_table_reference(&spec::ObjectName::bare(alias))?,
        )?))
    }

    async fn join_graph_plans(
        &self,
        left: Option<LogicalPlan>,
        right: LogicalPlan,
        condition: Option<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let Some(left) = left else {
            return Ok(right);
        };
        let condition = if let Some(condition) = condition {
            let join_schema = Arc::new(build_join_schema(
                left.schema(),
                right.schema(),
                &JoinType::Inner,
            )?);
            Some(
                self.resolve_expression(condition, &join_schema, state)
                    .await?,
            )
        } else {
            None
        };
        Ok(LogicalPlanBuilder::from(left)
            .join_on(right, JoinType::Inner, condition)?
            .build()?)
    }

    async fn apply_graph_filter(
        &self,
        input: LogicalPlan,
        condition: spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let predicate = self
            .resolve_expression(condition, input.schema(), state)
            .await?;
        Ok(LogicalPlan::Filter(Filter::try_new(
            predicate,
            Arc::new(input),
        )?))
    }

    async fn apply_graph_properties(
        &self,
        mut input: LogicalPlan,
        alias: &str,
        properties: &[spec::GraphProperty],
        bindings: &GraphBindings,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        for property in properties {
            let condition = rewrite_graph_expr(
                eq(attr(alias, property.key.as_ref()), property.value.clone()),
                bindings,
            );
            input = self.apply_graph_filter(input, condition, state).await?;
        }
        Ok(input)
    }

    async fn project_graph(
        &self,
        input: LogicalPlan,
        expr: Vec<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let schema = input.schema();
        let expr = self.resolve_named_expressions(expr, schema, state).await?;
        let (input, expr) = self.rewrite_wildcard(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<MonotonicIdRewriter>(input, expr, state)?;
        let (input, expr) =
            self.rewrite_projection::<SparkPartitionIdRewriter>(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<ExplodeRewriter>(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<WindowRewriter>(input, expr, state)?;
        let expr = self.rewrite_multi_expr(expr)?;
        let has_aggregate = expr.iter().any(|e| {
            e.expr
                .exists(|e| match e {
                    datafusion_expr::Expr::AggregateFunction(_) => Ok(true),
                    _ => Ok(false),
                })
                .unwrap_or(false)
        });
        if has_aggregate {
            self.rewrite_aggregate(input, expr, vec![], None, false, state)
        } else {
            let expr = self.rewrite_named_expressions(expr, state)?;
            Ok(LogicalPlan::Projection(Projection::try_new(
                expr,
                Arc::new(input),
            )?))
        }
    }
}

fn graph_step_conditions(
    direction: spec::GraphDirection,
    previous_node: &str,
    edge: &str,
    target_node: &str,
) -> (spec::Expr, spec::Expr) {
    match direction {
        spec::GraphDirection::Outgoing => (
            eq(attr(previous_node, "id"), attr(edge, "src_id")),
            eq(attr(edge, "dst_id"), attr(target_node, "id")),
        ),
        spec::GraphDirection::Incoming => (
            eq(attr(previous_node, "id"), attr(edge, "dst_id")),
            eq(attr(edge, "src_id"), attr(target_node, "id")),
        ),
        spec::GraphDirection::Undirected => (
            or(
                eq(attr(previous_node, "id"), attr(edge, "src_id")),
                eq(attr(previous_node, "id"), attr(edge, "dst_id")),
            ),
            or(
                and(
                    eq(attr(previous_node, "id"), attr(edge, "src_id")),
                    eq(attr(edge, "dst_id"), attr(target_node, "id")),
                ),
                and(
                    eq(attr(previous_node, "id"), attr(edge, "dst_id")),
                    eq(attr(edge, "src_id"), attr(target_node, "id")),
                ),
            ),
        ),
    }
}

fn graph_alias_scan_fields(
    fields: Option<&HashSet<String>>,
    properties: &[spec::GraphProperty],
    required: &[&str],
) -> Option<HashSet<String>> {
    if fields.is_none() && properties.is_empty() && required.is_empty() {
        return None;
    }
    let mut result = fields.cloned().unwrap_or_default();
    for field in required {
        result.insert((*field).to_string());
    }
    for property in properties {
        result.insert(property.key.as_ref().to_string());
    }
    Some(result)
}

fn graph_field_usage(
    patterns: &[spec::GraphPathPattern],
    predicates: &[spec::Expr],
    returns: &[spec::Expr],
    order: &[spec::SortOrder],
    skip: Option<&spec::Expr>,
    limit: Option<&spec::Expr>,
) -> HashMap<String, HashSet<String>> {
    let mut usage = HashMap::new();
    for pattern in patterns {
        collect_graph_pattern_usage(pattern, &mut usage);
    }
    for expr in predicates.iter().chain(returns) {
        collect_graph_expr_usage(expr, &mut usage);
    }
    for sort in order {
        collect_graph_expr_usage(&sort.child, &mut usage);
    }
    if let Some(expr) = skip {
        collect_graph_expr_usage(expr, &mut usage);
    }
    if let Some(expr) = limit {
        collect_graph_expr_usage(expr, &mut usage);
    }
    usage
}

fn collect_graph_pattern_usage(
    pattern: &spec::GraphPathPattern,
    usage: &mut HashMap<String, HashSet<String>>,
) {
    collect_graph_node_pattern_usage(&pattern.start, usage);
    for step in &pattern.steps {
        collect_graph_edge_pattern_usage(&step.edge, usage);
        collect_graph_node_pattern_usage(&step.target, usage);
    }
}

fn collect_graph_node_pattern_usage(
    pattern: &spec::GraphNodePattern,
    usage: &mut HashMap<String, HashSet<String>>,
) {
    if let Some(alias) = &pattern.variable {
        collect_graph_property_usage(alias.as_ref(), &pattern.properties, usage);
    }
}

fn collect_graph_edge_pattern_usage(
    pattern: &spec::GraphEdgePattern,
    usage: &mut HashMap<String, HashSet<String>>,
) {
    if let Some(alias) = &pattern.variable {
        collect_graph_property_usage(alias.as_ref(), &pattern.properties, usage);
    }
}

fn collect_graph_property_usage(
    alias: &str,
    properties: &[spec::GraphProperty],
    usage: &mut HashMap<String, HashSet<String>>,
) {
    for property in properties {
        usage
            .entry(alias.to_string())
            .or_default()
            .insert(property.key.as_ref().to_string());
        collect_graph_expr_usage(&property.value, usage);
    }
}

fn collect_graph_expr_usage(expr: &spec::Expr, usage: &mut HashMap<String, HashSet<String>>) {
    use spec::Expr;

    match expr {
        Expr::UnresolvedAttribute { name, .. } => {
            let parts = name.parts();
            if parts.len() == 2 {
                usage
                    .entry(parts[0].as_ref().to_string())
                    .or_default()
                    .insert(parts[1].as_ref().to_string());
            }
        }
        Expr::UnresolvedFunction(function) => {
            for expr in &function.arguments {
                collect_graph_expr_usage(expr, usage);
            }
            for (_, expr) in &function.named_arguments {
                collect_graph_expr_usage(expr, usage);
            }
            if let Some(expr) = &function.filter {
                collect_graph_expr_usage(expr, usage);
            }
            if let Some(order) = &function.order_by {
                for sort in order {
                    collect_graph_expr_usage(&sort.child, usage);
                }
            }
        }
        Expr::Alias { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::SortOrder(spec::SortOrder { child: expr, .. })
        | Expr::Table { expr }
        | Expr::IdentifierClause { expr }
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr) => collect_graph_expr_usage(expr, usage),
        Expr::LambdaFunction { function, .. } => collect_graph_expr_usage(function, usage),
        Expr::Window {
            window_function,
            window,
        } => {
            collect_graph_expr_usage(window_function, usage);
            collect_graph_window_usage(window, usage);
        }
        Expr::UnresolvedExtractValue { child, extraction } => {
            collect_graph_expr_usage(child, usage);
            collect_graph_expr_usage(extraction, usage);
        }
        Expr::UpdateFields {
            struct_expression,
            value_expression,
            ..
        } => {
            collect_graph_expr_usage(struct_expression, usage);
            if let Some(expr) = value_expression {
                collect_graph_expr_usage(expr, usage);
            }
        }
        Expr::CommonInlineUserDefinedFunction(function) => {
            for expr in &function.arguments {
                collect_graph_expr_usage(expr, usage);
            }
        }
        Expr::CallFunction { arguments, .. } | Expr::Rollup(arguments) | Expr::Cube(arguments) => {
            for expr in arguments {
                collect_graph_expr_usage(expr, usage);
            }
        }
        Expr::NamedArgument { value, .. } => collect_graph_expr_usage(value, usage),
        Expr::GroupingSets(sets) => {
            for set in sets {
                for expr in set {
                    collect_graph_expr_usage(expr, usage);
                }
            }
        }
        Expr::InSubquery { expr, .. } | Expr::SimilarTo { expr, .. } => {
            collect_graph_expr_usage(expr, usage)
        }
        Expr::InList { expr, list, .. } => {
            collect_graph_expr_usage(expr, usage);
            for expr in list {
                collect_graph_expr_usage(expr, usage);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_graph_expr_usage(expr, usage);
            collect_graph_expr_usage(low, usage);
            collect_graph_expr_usage(high, usage);
        }
        Expr::IsDistinctFrom { left, right } | Expr::IsNotDistinctFrom { left, right } => {
            collect_graph_expr_usage(left, usage);
            collect_graph_expr_usage(right, usage);
        }
        Expr::Subquery {
            in_subquery_values, ..
        } => {
            for expr in in_subquery_values {
                collect_graph_expr_usage(expr, usage);
            }
        }
        Expr::Literal(_)
        | Expr::UnresolvedStar { .. }
        | Expr::UnresolvedRegex { .. }
        | Expr::UnresolvedNamedLambdaVariable(_)
        | Expr::DefaultColumnValue
        | Expr::Placeholder(_)
        | Expr::ScalarSubquery { .. }
        | Expr::Exists { .. }
        | Expr::UnresolvedDate { .. }
        | Expr::UnresolvedTime { .. }
        | Expr::UnresolvedTimestamp { .. } => {}
    }
}

fn typed_node_field_compatible(field: &str) -> bool {
    field != "props"
}

fn typed_edge_field_compatible(field: &str) -> bool {
    !matches!(field, "src_label" | "dst_label" | "props")
}

#[cfg(test)]
fn typed_node_table_has_fields(columns: &[TableColumnStatus], fields: &HashSet<String>) -> bool {
    typed_node_table_missing_fields(columns, fields).is_empty()
}

fn typed_node_table_missing_fields(
    columns: &[TableColumnStatus],
    fields: &HashSet<String>,
) -> Vec<String> {
    let mut missing = fields
        .iter()
        .filter_map(|field| {
            let available = field == "label"
                || (typed_node_field_compatible(field)
                    && columns
                        .iter()
                        .any(|column| column.name.eq_ignore_ascii_case(field.as_str())));
            (!available).then(|| field.to_string())
        })
        .collect::<Vec<_>>();
    missing.sort();
    missing
}

#[cfg(test)]
fn typed_edge_table_has_fields(columns: &[TableColumnStatus], fields: &HashSet<String>) -> bool {
    typed_edge_table_missing_fields(columns, fields).is_empty()
}

fn typed_edge_table_missing_fields(
    columns: &[TableColumnStatus],
    fields: &HashSet<String>,
) -> Vec<String> {
    let mut missing = fields
        .iter()
        .filter_map(|field| {
            let available = matches!(field.as_str(), "label" | "edge_type")
                || (typed_edge_field_compatible(field)
                    && columns
                        .iter()
                        .any(|column| column.name.eq_ignore_ascii_case(field.as_str())));
            (!available).then(|| field.to_string())
        })
        .collect::<Vec<_>>();
    missing.sort();
    missing
}

fn graph_table_metadata_mismatches(
    properties: &[(String, String)],
    expected_kind: &str,
    expected_label: &str,
) -> Vec<String> {
    let mut mismatches = Vec::new();
    for (key, value) in properties {
        match key.as_str() {
            GRAPH_TABLE_KIND_PROPERTY if value != expected_kind => mismatches.push(format!(
                "{GRAPH_TABLE_KIND_PROPERTY}={value:?}, expected {expected_kind:?}"
            )),
            GRAPH_TABLE_LABEL_PROPERTY if value != expected_label => mismatches.push(format!(
                "{GRAPH_TABLE_LABEL_PROPERTY}={value:?}, expected {expected_label:?}"
            )),
            _ => {}
        }
    }
    mismatches
}

fn graph_alias_label<'a>(bindings: &'a GraphBindings, alias: &str) -> Option<&'a str> {
    match bindings.source(alias) {
        Some(GraphAliasSource::Typed { label }) => Some(label.as_str()),
        Some(GraphAliasSource::Generic) | None => None,
    }
}

fn typed_node_table(label: &str) -> PlanResult<String> {
    Ok(format!("grust_node_{}", graph_schema_identifier(label)?))
}

fn typed_edge_table(label: &str) -> PlanResult<String> {
    Ok(format!("grust_edge_{}", graph_schema_identifier(label)?))
}

fn graph_schema_identifier(value: &str) -> PlanResult<String> {
    let identifier = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    if identifier.is_empty()
        || identifier
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_digit())
    {
        return Err(PlanError::invalid(format!(
            "invalid graph schema identifier `{value}`"
        )));
    }
    Ok(identifier)
}

fn is_table_not_found(error: &PlanError) -> bool {
    matches!(
        error,
        PlanError::AnalysisError(message) if message.contains("[TABLE_OR_VIEW_NOT_FOUND]")
    )
}

fn collect_graph_window_usage(window: &spec::Window, usage: &mut HashMap<String, HashSet<String>>) {
    match window {
        spec::Window::Named(_) => {}
        spec::Window::Unnamed {
            cluster_by,
            partition_by,
            order_by,
            frame,
        } => {
            for expr in cluster_by.iter().chain(partition_by) {
                collect_graph_expr_usage(expr, usage);
            }
            for sort in order_by {
                collect_graph_expr_usage(&sort.child, usage);
            }
            if let Some(frame) = frame {
                collect_graph_window_boundary_usage(&frame.lower, usage);
                collect_graph_window_boundary_usage(&frame.upper, usage);
            }
        }
    }
}

fn collect_graph_window_boundary_usage(
    boundary: &spec::WindowFrameBoundary,
    usage: &mut HashMap<String, HashSet<String>>,
) {
    match boundary {
        spec::WindowFrameBoundary::Preceding(expr)
        | spec::WindowFrameBoundary::Following(expr)
        | spec::WindowFrameBoundary::Value(expr) => collect_graph_expr_usage(expr, usage),
        spec::WindowFrameBoundary::CurrentRow
        | spec::WindowFrameBoundary::UnboundedPreceding
        | spec::WindowFrameBoundary::UnboundedFollowing => {}
    }
}

fn rewrite_graph_sort_order(sort: spec::SortOrder, bindings: &GraphBindings) -> spec::SortOrder {
    spec::SortOrder {
        child: Box::new(rewrite_graph_expr(*sort.child, bindings)),
        direction: sort.direction,
        null_ordering: sort.null_ordering,
    }
}

fn rewrite_graph_return_expr(expr: spec::Expr, bindings: &GraphBindings) -> spec::Expr {
    match &expr {
        spec::Expr::Alias { .. } => rewrite_graph_expr(expr, bindings),
        spec::Expr::UnresolvedAttribute { name, .. } if name.parts().len() == 2 => {
            let name = name
                .parts()
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<_>>()
                .join(".");
            spec::Expr::Alias {
                expr: Box::new(rewrite_graph_expr(expr, bindings)),
                name: vec![name.into()],
                metadata: None,
            }
        }
        _ => rewrite_graph_expr(expr, bindings),
    }
}

fn rewrite_graph_expr(expr: spec::Expr, bindings: &GraphBindings) -> spec::Expr {
    use spec::Expr;

    match expr {
        Expr::UnresolvedAttribute {
            name,
            plan_id,
            is_metadata_column,
        } => rewrite_graph_attribute(name, plan_id, is_metadata_column, bindings),
        Expr::UnresolvedFunction(function) => Expr::UnresolvedFunction(spec::UnresolvedFunction {
            function_name: function.function_name,
            arguments: function
                .arguments
                .into_iter()
                .map(|expr| rewrite_graph_expr(expr, bindings))
                .collect(),
            named_arguments: function
                .named_arguments
                .into_iter()
                .map(|(name, expr)| (name, rewrite_graph_expr(expr, bindings)))
                .collect(),
            is_distinct: function.is_distinct,
            is_user_defined_function: function.is_user_defined_function,
            is_internal: function.is_internal,
            ignore_nulls: function.ignore_nulls,
            filter: function
                .filter
                .map(|expr| Box::new(rewrite_graph_expr(*expr, bindings))),
            order_by: function.order_by.map(|order| {
                order
                    .into_iter()
                    .map(|sort| rewrite_graph_sort_order(sort, bindings))
                    .collect()
            }),
        }),
        Expr::Alias {
            expr,
            name,
            metadata,
        } => Expr::Alias {
            expr: Box::new(rewrite_graph_expr(*expr, bindings)),
            name,
            metadata,
        },
        Expr::Cast {
            expr,
            cast_to_type,
            rename,
            is_try,
        } => Expr::Cast {
            expr: Box::new(rewrite_graph_expr(*expr, bindings)),
            cast_to_type,
            rename,
            is_try,
        },
        Expr::SortOrder(sort) => Expr::SortOrder(rewrite_graph_sort_order(sort, bindings)),
        Expr::LambdaFunction {
            function,
            arguments,
        } => Expr::LambdaFunction {
            function: Box::new(rewrite_graph_expr(*function, bindings)),
            arguments,
        },
        Expr::Window {
            window_function,
            window,
        } => Expr::Window {
            window_function: Box::new(rewrite_graph_expr(*window_function, bindings)),
            window: rewrite_graph_window(window, bindings),
        },
        Expr::UnresolvedExtractValue { child, extraction } => Expr::UnresolvedExtractValue {
            child: Box::new(rewrite_graph_expr(*child, bindings)),
            extraction: Box::new(rewrite_graph_expr(*extraction, bindings)),
        },
        Expr::UpdateFields {
            struct_expression,
            field_name,
            value_expression,
        } => Expr::UpdateFields {
            struct_expression: Box::new(rewrite_graph_expr(*struct_expression, bindings)),
            field_name,
            value_expression: value_expression
                .map(|expr| Box::new(rewrite_graph_expr(*expr, bindings))),
        },
        Expr::CommonInlineUserDefinedFunction(mut function) => {
            function.arguments = function
                .arguments
                .into_iter()
                .map(|expr| rewrite_graph_expr(expr, bindings))
                .collect();
            Expr::CommonInlineUserDefinedFunction(function)
        }
        Expr::CallFunction {
            function_name,
            arguments,
        } => Expr::CallFunction {
            function_name,
            arguments: arguments
                .into_iter()
                .map(|expr| rewrite_graph_expr(expr, bindings))
                .collect(),
        },
        Expr::NamedArgument { key, value } => Expr::NamedArgument {
            key,
            value: Box::new(rewrite_graph_expr(*value, bindings)),
        },
        Expr::Rollup(exprs) => Expr::Rollup(
            exprs
                .into_iter()
                .map(|expr| rewrite_graph_expr(expr, bindings))
                .collect(),
        ),
        Expr::Cube(exprs) => Expr::Cube(
            exprs
                .into_iter()
                .map(|expr| rewrite_graph_expr(expr, bindings))
                .collect(),
        ),
        Expr::GroupingSets(sets) => Expr::GroupingSets(
            sets.into_iter()
                .map(|set| {
                    set.into_iter()
                        .map(|expr| rewrite_graph_expr(expr, bindings))
                        .collect()
                })
                .collect(),
        ),
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Expr::InSubquery {
            expr: Box::new(rewrite_graph_expr(*expr, bindings)),
            subquery,
            negated,
        },
        Expr::Subquery {
            plan_id,
            subquery_type,
            in_subquery_values,
            negated,
        } => Expr::Subquery {
            plan_id,
            subquery_type,
            in_subquery_values: in_subquery_values
                .into_iter()
                .map(|expr| rewrite_graph_expr(expr, bindings))
                .collect(),
            negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(rewrite_graph_expr(*expr, bindings)),
            list: list
                .into_iter()
                .map(|expr| rewrite_graph_expr(expr, bindings))
                .collect(),
            negated,
        },
        Expr::IsFalse(expr) => Expr::IsFalse(Box::new(rewrite_graph_expr(*expr, bindings))),
        Expr::IsNotFalse(expr) => Expr::IsNotFalse(Box::new(rewrite_graph_expr(*expr, bindings))),
        Expr::IsTrue(expr) => Expr::IsTrue(Box::new(rewrite_graph_expr(*expr, bindings))),
        Expr::IsNotTrue(expr) => Expr::IsNotTrue(Box::new(rewrite_graph_expr(*expr, bindings))),
        Expr::IsNull(expr) => Expr::IsNull(Box::new(rewrite_graph_expr(*expr, bindings))),
        Expr::IsNotNull(expr) => Expr::IsNotNull(Box::new(rewrite_graph_expr(*expr, bindings))),
        Expr::IsUnknown(expr) => Expr::IsUnknown(Box::new(rewrite_graph_expr(*expr, bindings))),
        Expr::IsNotUnknown(expr) => {
            Expr::IsNotUnknown(Box::new(rewrite_graph_expr(*expr, bindings)))
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Expr::Between {
            expr: Box::new(rewrite_graph_expr(*expr, bindings)),
            negated,
            low: Box::new(rewrite_graph_expr(*low, bindings)),
            high: Box::new(rewrite_graph_expr(*high, bindings)),
        },
        Expr::IsDistinctFrom { left, right } => Expr::IsDistinctFrom {
            left: Box::new(rewrite_graph_expr(*left, bindings)),
            right: Box::new(rewrite_graph_expr(*right, bindings)),
        },
        Expr::IsNotDistinctFrom { left, right } => Expr::IsNotDistinctFrom {
            left: Box::new(rewrite_graph_expr(*left, bindings)),
            right: Box::new(rewrite_graph_expr(*right, bindings)),
        },
        Expr::SimilarTo {
            expr,
            pattern,
            negated,
            escape_char,
            case_insensitive,
        } => Expr::SimilarTo {
            expr: Box::new(rewrite_graph_expr(*expr, bindings)),
            pattern: Box::new(rewrite_graph_expr(*pattern, bindings)),
            negated,
            escape_char,
            case_insensitive,
        },
        Expr::Table { expr } => Expr::Table {
            expr: Box::new(rewrite_graph_expr(*expr, bindings)),
        },
        Expr::IdentifierClause { expr } => Expr::IdentifierClause {
            expr: Box::new(rewrite_graph_expr(*expr, bindings)),
        },
        Expr::Literal(_)
        | Expr::UnresolvedStar { .. }
        | Expr::UnresolvedRegex { .. }
        | Expr::UnresolvedNamedLambdaVariable(_)
        | Expr::DefaultColumnValue
        | Expr::Placeholder(_)
        | Expr::ScalarSubquery { .. }
        | Expr::Exists { .. }
        | Expr::UnresolvedDate { .. }
        | Expr::UnresolvedTime { .. }
        | Expr::UnresolvedTimestamp { .. } => expr,
    }
}

fn rewrite_graph_window(window: spec::Window, bindings: &GraphBindings) -> spec::Window {
    match window {
        spec::Window::Named(name) => spec::Window::Named(name),
        spec::Window::Unnamed {
            cluster_by,
            partition_by,
            order_by,
            frame,
        } => spec::Window::Unnamed {
            cluster_by: cluster_by
                .into_iter()
                .map(|expr| rewrite_graph_expr(expr, bindings))
                .collect(),
            partition_by: partition_by
                .into_iter()
                .map(|expr| rewrite_graph_expr(expr, bindings))
                .collect(),
            order_by: order_by
                .into_iter()
                .map(|sort| rewrite_graph_sort_order(sort, bindings))
                .collect(),
            frame: frame.map(|frame| rewrite_graph_window_frame(frame, bindings)),
        },
    }
}

fn rewrite_graph_window_frame(
    frame: spec::WindowFrame,
    bindings: &GraphBindings,
) -> spec::WindowFrame {
    spec::WindowFrame {
        frame_type: frame.frame_type,
        lower: rewrite_graph_window_frame_boundary(frame.lower, bindings),
        upper: rewrite_graph_window_frame_boundary(frame.upper, bindings),
    }
}

fn rewrite_graph_window_frame_boundary(
    boundary: spec::WindowFrameBoundary,
    bindings: &GraphBindings,
) -> spec::WindowFrameBoundary {
    match boundary {
        spec::WindowFrameBoundary::Preceding(expr) => {
            spec::WindowFrameBoundary::Preceding(Box::new(rewrite_graph_expr(*expr, bindings)))
        }
        spec::WindowFrameBoundary::Following(expr) => {
            spec::WindowFrameBoundary::Following(Box::new(rewrite_graph_expr(*expr, bindings)))
        }
        spec::WindowFrameBoundary::Value(expr) => {
            spec::WindowFrameBoundary::Value(Box::new(rewrite_graph_expr(*expr, bindings)))
        }
        spec::WindowFrameBoundary::CurrentRow
        | spec::WindowFrameBoundary::UnboundedPreceding
        | spec::WindowFrameBoundary::UnboundedFollowing => boundary,
    }
}

fn rewrite_graph_attribute(
    name: spec::ObjectName,
    plan_id: Option<i64>,
    is_metadata_column: bool,
    bindings: &GraphBindings,
) -> spec::Expr {
    let parts = name.parts();
    if parts.len() != 2 {
        return spec::Expr::UnresolvedAttribute {
            name,
            plan_id,
            is_metadata_column,
        };
    }

    let alias = parts[0].as_ref();
    let field = parts[1].as_ref();
    match (bindings.kind(alias), bindings.source(alias)) {
        (Some(GraphAliasKind::Node), Some(GraphAliasSource::Typed { label }))
            if field == "label" =>
        {
            string_lit(label.as_str())
        }
        (Some(GraphAliasKind::Edge), Some(GraphAliasSource::Typed { label }))
            if field == "label" || field == "edge_type" =>
        {
            string_lit(label.as_str())
        }
        (Some(GraphAliasKind::Node), Some(GraphAliasSource::Typed { .. })) if field != "props" => {
            attr(alias, field)
        }
        (Some(GraphAliasKind::Edge), Some(GraphAliasSource::Typed { .. }))
            if !matches!(field, "src_label" | "dst_label" | "props") =>
        {
            attr(alias, edge_physical_field(field))
        }
        (Some(GraphAliasKind::Node), _) if is_node_physical_field(field) => attr(alias, field),
        (Some(GraphAliasKind::Edge), _) if is_edge_physical_field(field) => {
            attr(alias, edge_physical_field(field))
        }
        (Some(GraphAliasKind::Node), _) | (Some(GraphAliasKind::Edge), _) => {
            json_prop(alias, field)
        }
        (None, _) => spec::Expr::UnresolvedAttribute {
            name,
            plan_id,
            is_metadata_column,
        },
    }
}

fn is_node_physical_field(field: &str) -> bool {
    matches!(field, "id" | "label" | "props")
}

fn is_edge_physical_field(field: &str) -> bool {
    matches!(
        field,
        "id" | "edge_key"
            | "src_id"
            | "src_label"
            | "dst_id"
            | "dst_label"
            | "edge_type"
            | "label"
            | "props"
    )
}

fn edge_physical_field(field: &str) -> &str {
    if field == "label" {
        "edge_type"
    } else {
        field
    }
}

fn attr(alias: impl Into<String>, field: &str) -> spec::Expr {
    spec::Expr::UnresolvedAttribute {
        name: spec::ObjectName::from([alias.into(), field.to_string()]),
        plan_id: None,
        is_metadata_column: false,
    }
}

fn json_prop(alias: &str, field: &str) -> spec::Expr {
    spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
        function_name: spec::ObjectName::bare("get_json_object"),
        arguments: vec![attr(alias, "props"), string_lit(format!("$.{field}"))],
        named_arguments: vec![],
        is_distinct: false,
        is_user_defined_function: false,
        is_internal: None,
        ignore_nulls: None,
        filter: None,
        order_by: None,
    })
}

fn eq(left: spec::Expr, right: spec::Expr) -> spec::Expr {
    graph_binary_function("==", left, right)
}

fn and(left: spec::Expr, right: spec::Expr) -> spec::Expr {
    graph_binary_function("and", left, right)
}

fn or(left: spec::Expr, right: spec::Expr) -> spec::Expr {
    graph_binary_function("or", left, right)
}

fn graph_binary_function(name: &str, left: spec::Expr, right: spec::Expr) -> spec::Expr {
    spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
        function_name: spec::ObjectName::bare(name),
        arguments: vec![left, right],
        named_arguments: vec![],
        is_distinct: false,
        is_user_defined_function: false,
        is_internal: None,
        ignore_nulls: None,
        filter: None,
        order_by: None,
    })
}

fn string_lit(value: impl Into<String>) -> spec::Expr {
    spec::Expr::Literal(spec::Literal::Utf8 {
        value: Some(value.into()),
    })
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use datafusion::arrow::array::{ArrayRef, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::MemTable;
    use datafusion::datasource::provider_as_source;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use datafusion_common::{DFSchema, DFSchemaRef};
    use datafusion_expr::{EmptyRelation, LogicalPlan, TableScan, UNNAMED_TABLE};
    use sail_catalog::manager::{CatalogManager, CatalogManagerOptions};
    use sail_catalog::provider::{
        CatalogProvider, CreateTemporaryViewColumnOptions, CreateTemporaryViewOptions,
    };
    use sail_catalog_memory::MemoryCatalogProvider;
    use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
    use sail_common_datafusion::extension::SessionExtensionAccessor;
    use sail_common_datafusion::session::plan::PlanService;
    use sail_sql_analyzer::parser::parse_one_statement;
    use sail_sql_analyzer::statement::from_ast_statement;

    use super::*;
    use crate::catalog::SparkCatalogObjectDisplay;
    use crate::config::PlanConfig;
    use crate::formatter::SparkPlanFormatter;

    fn create_session() -> PlanResult<SessionContext> {
        let mut state = SessionStateBuilder::new().build();
        let catalog_manager = CatalogManager::try_new(CatalogManagerOptions {
            catalogs: HashMap::from([(
                "sail".to_string(),
                Arc::new(MemoryCatalogProvider::new(
                    "sail".to_string(),
                    vec![Arc::from("default")].try_into()?,
                    None,
                )) as Arc<dyn CatalogProvider>,
            )]),
            default_catalog: "sail".to_string(),
            default_database: vec!["default".to_string()],
            global_temporary_database: vec!["global_temp".to_string()],
        })?;
        let plan_service = PlanService::new(
            Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
            Box::new(SparkPlanFormatter),
        );
        state.config_mut().set_extension(Arc::new(catalog_manager));
        state.config_mut().set_extension(Arc::new(plan_service));
        Ok(SessionContext::new_with_state(state))
    }

    fn empty_plan(schema: Schema) -> PlanResult<Arc<LogicalPlan>> {
        Ok(Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: DFSchemaRef::new(DFSchema::try_from(schema)?),
        })))
    }

    fn record_batch(schema: Arc<Schema>, columns: Vec<Vec<&str>>) -> PlanResult<RecordBatch> {
        let columns = columns
            .into_iter()
            .map(|values| Arc::new(StringArray::from(values)) as ArrayRef)
            .collect();
        Ok(RecordBatch::try_new(schema, columns)?)
    }

    fn table_columns(names: &[&str]) -> Vec<TableColumnStatus> {
        names
            .iter()
            .map(|name| TableColumnStatus {
                name: (*name).to_string(),
                data_type: DataType::Utf8,
                nullable: true,
                comment: None,
                default: None,
                generated_always_as: None,
                identity: None,
                is_partition: false,
                is_bucket: false,
                is_cluster: false,
            })
            .collect()
    }

    fn fields(names: &[&str]) -> HashSet<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    fn graph_table_properties(kind: &str, label: &str) -> Vec<(String, String)> {
        vec![
            (GRAPH_TABLE_KIND_PROPERTY.to_string(), kind.to_string()),
            (GRAPH_TABLE_LABEL_PROPERTY.to_string(), label.to_string()),
        ]
    }

    async fn collect_graph_query_rows(
        ctx: &SessionContext,
        resolver: &PlanResolver<'_>,
        sql: &str,
        columns: usize,
    ) -> PlanResult<Vec<Vec<String>>> {
        let plan = from_ast_statement(parse_one_statement(sql)?)?;
        let named = resolver.resolve_named_plan(plan).await?;
        let batches = ctx
            .execute_logical_plan(named.plan)
            .await?
            .collect()
            .await?;
        let mut rows = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                let mut values = Vec::with_capacity(columns);
                for column in 0..columns {
                    values.push(
                        batch
                            .column(column)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(row)
                            .to_string(),
                    );
                }
                rows.push(values);
            }
        }
        Ok(rows)
    }

    fn mem_table_plan(batch: RecordBatch) -> PlanResult<Arc<LogicalPlan>> {
        let table = Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?);
        Ok(Arc::new(LogicalPlan::TableScan(TableScan::try_new(
            UNNAMED_TABLE,
            provider_as_source(table),
            None,
            vec![],
            None,
        )?)))
    }

    async fn create_empty_graph_views(ctx: &SessionContext) -> PlanResult<()> {
        let catalog = ctx.extension::<CatalogManager>()?;
        let string_field = |name| Field::new(name, DataType::Utf8, true);
        catalog
            .create_temporary_view(
                GRAPH_NODES_TABLE,
                CreateTemporaryViewOptions {
                    input: empty_plan(Schema::new(vec![
                        string_field("id"),
                        string_field("label"),
                        string_field("props"),
                    ]))?,
                    columns: vec![
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                    ],
                    if_not_exists: false,
                    replace: true,
                    comment: None,
                    properties: graph_table_properties(GRAPH_TABLE_KIND_NODE, "Person"),
                },
            )
            .await?;
        catalog
            .create_temporary_view(
                GRAPH_EDGES_TABLE,
                CreateTemporaryViewOptions {
                    input: empty_plan(Schema::new(vec![
                        string_field("edge_key"),
                        string_field("id"),
                        string_field("src_id"),
                        string_field("src_label"),
                        string_field("dst_id"),
                        string_field("dst_label"),
                        string_field("edge_type"),
                        string_field("props"),
                    ]))?,
                    columns: vec![
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                    ],
                    if_not_exists: false,
                    replace: true,
                    comment: None,
                    properties: vec![],
                },
            )
            .await?;
        Ok(())
    }

    async fn create_graph_views_with_data(ctx: &SessionContext) -> PlanResult<()> {
        let catalog = ctx.extension::<CatalogManager>()?;
        let string_field = |name| Field::new(name, DataType::Utf8, true);
        let nodes_schema = Arc::new(Schema::new(vec![
            string_field("id"),
            string_field("label"),
            string_field("props"),
        ]));
        let edges_schema = Arc::new(Schema::new(vec![
            string_field("edge_key"),
            string_field("id"),
            string_field("src_id"),
            string_field("src_label"),
            string_field("dst_id"),
            string_field("dst_label"),
            string_field("edge_type"),
            string_field("props"),
        ]));
        let nodes = record_batch(
            Arc::clone(&nodes_schema),
            vec![
                vec!["1", "2", "3"],
                vec!["Person", "Person", "Document"],
                vec![
                    r#"{"age":"42","name":"Alice"}"#,
                    r#"{"age":"31","name":"Bob"}"#,
                    r#"{"age":"42","name":"Paper"}"#,
                ],
            ],
        )?;
        let edges = record_batch(
            Arc::clone(&edges_schema),
            vec![
                vec!["edge-key-1", "edge-key-2", "edge-key-3"],
                vec!["edge-1", "edge-2", "edge-3"],
                vec!["1", "2", "3"],
                vec!["Person", "Person", "Document"],
                vec!["2", "1", "2"],
                vec!["Person", "Person", "Person"],
                vec!["KNOWS", "LIKES", "KNOWS"],
                vec![
                    r#"{"since":"2020"}"#,
                    r#"{"since":"2021"}"#,
                    r#"{"since":"2022"}"#,
                ],
            ],
        )?;

        catalog
            .create_temporary_view(
                GRAPH_NODES_TABLE,
                CreateTemporaryViewOptions {
                    input: mem_table_plan(nodes)?,
                    columns: vec![
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                    ],
                    if_not_exists: false,
                    replace: true,
                    comment: None,
                    properties: vec![],
                },
            )
            .await?;
        catalog
            .create_temporary_view(
                GRAPH_EDGES_TABLE,
                CreateTemporaryViewOptions {
                    input: mem_table_plan(edges)?,
                    columns: vec![
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                    ],
                    if_not_exists: false,
                    replace: true,
                    comment: None,
                    properties: vec![],
                },
            )
            .await?;
        Ok(())
    }

    async fn create_typed_graph_views_with_data(ctx: &SessionContext) -> PlanResult<()> {
        create_empty_graph_views(ctx).await?;
        let catalog = ctx.extension::<CatalogManager>()?;
        let string_field = |name| Field::new(name, DataType::Utf8, true);
        let person_schema = Arc::new(Schema::new(vec![
            string_field("id"),
            string_field("age"),
            string_field("name"),
        ]));
        let knows_schema = Arc::new(Schema::new(vec![
            string_field("edge_key"),
            string_field("id"),
            string_field("src_id"),
            string_field("dst_id"),
            string_field("since"),
        ]));
        let people = record_batch(
            Arc::clone(&person_schema),
            vec![vec!["1", "2"], vec!["42", "31"], vec!["Alice", "Bob"]],
        )?;
        let knows = record_batch(
            Arc::clone(&knows_schema),
            vec![
                vec!["edge-key-1"],
                vec!["edge-1"],
                vec!["1"],
                vec!["2"],
                vec!["2020"],
            ],
        )?;

        catalog
            .create_temporary_view(
                "grust_node_person",
                CreateTemporaryViewOptions {
                    input: mem_table_plan(people)?,
                    columns: vec![
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                    ],
                    if_not_exists: false,
                    replace: true,
                    comment: None,
                    properties: vec![],
                },
            )
            .await?;
        catalog
            .create_temporary_view(
                "grust_edge_knows",
                CreateTemporaryViewOptions {
                    input: mem_table_plan(knows)?,
                    columns: vec![
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                    ],
                    if_not_exists: false,
                    replace: true,
                    comment: None,
                    properties: graph_table_properties(GRAPH_TABLE_KIND_EDGE, "KNOWS"),
                },
            )
            .await?;
        Ok(())
    }

    async fn create_mismatched_typed_node_view(ctx: &SessionContext) -> PlanResult<()> {
        let catalog = ctx.extension::<CatalogManager>()?;
        let string_field = |name| Field::new(name, DataType::Utf8, true);
        let person_schema = Arc::new(Schema::new(vec![
            string_field("id"),
            string_field("age"),
            string_field("name"),
        ]));
        let people = record_batch(
            Arc::clone(&person_schema),
            vec![vec!["1"], vec!["42"], vec!["Typed Alice"]],
        )?;
        catalog
            .create_temporary_view(
                "grust_node_person",
                CreateTemporaryViewOptions {
                    input: mem_table_plan(people)?,
                    columns: vec![
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                    ],
                    if_not_exists: false,
                    replace: true,
                    comment: None,
                    properties: graph_table_properties(GRAPH_TABLE_KIND_NODE, "Animal"),
                },
            )
            .await?;
        Ok(())
    }

    async fn create_mismatched_typed_edge_view(ctx: &SessionContext) -> PlanResult<()> {
        let catalog = ctx.extension::<CatalogManager>()?;
        let string_field = |name| Field::new(name, DataType::Utf8, true);
        let knows_schema = Arc::new(Schema::new(vec![
            string_field("edge_key"),
            string_field("id"),
            string_field("src_id"),
            string_field("dst_id"),
            string_field("since"),
        ]));
        let knows = record_batch(
            Arc::clone(&knows_schema),
            vec![
                vec!["typed-edge-key"],
                vec!["typed-edge"],
                vec!["1"],
                vec!["2"],
                vec!["2020"],
            ],
        )?;
        catalog
            .create_temporary_view(
                "grust_edge_knows",
                CreateTemporaryViewOptions {
                    input: mem_table_plan(knows)?,
                    columns: vec![
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                    ],
                    if_not_exists: false,
                    replace: true,
                    comment: None,
                    properties: graph_table_properties(GRAPH_TABLE_KIND_EDGE, "LIKES"),
                },
            )
            .await?;
        Ok(())
    }

    async fn create_partial_typed_node_view(ctx: &SessionContext) -> PlanResult<()> {
        let catalog = ctx.extension::<CatalogManager>()?;
        let string_field = |name| Field::new(name, DataType::Utf8, true);
        let person_schema = Arc::new(Schema::new(vec![string_field("id")]));
        let people = record_batch(Arc::clone(&person_schema), vec![vec!["1", "2"]])?;
        catalog
            .create_temporary_view(
                "grust_node_person",
                CreateTemporaryViewOptions {
                    input: mem_table_plan(people)?,
                    columns: vec![CreateTemporaryViewColumnOptions { comment: None }],
                    if_not_exists: false,
                    replace: true,
                    comment: None,
                    properties: vec![],
                },
            )
            .await?;
        Ok(())
    }

    async fn create_structurally_incomplete_typed_edge_view(
        ctx: &SessionContext,
    ) -> PlanResult<()> {
        let catalog = ctx.extension::<CatalogManager>()?;
        let string_field = |name| Field::new(name, DataType::Utf8, true);
        let knows_schema = Arc::new(Schema::new(vec![
            string_field("edge_key"),
            string_field("id"),
            string_field("src_id"),
        ]));
        let knows = record_batch(
            Arc::clone(&knows_schema),
            vec![vec!["edge-key-1"], vec!["edge-1"], vec!["1"]],
        )?;
        catalog
            .create_temporary_view(
                "grust_edge_knows",
                CreateTemporaryViewOptions {
                    input: mem_table_plan(knows)?,
                    columns: vec![
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                        CreateTemporaryViewColumnOptions { comment: None },
                    ],
                    if_not_exists: false,
                    replace: true,
                    comment: None,
                    properties: vec![],
                },
            )
            .await?;
        Ok(())
    }

    #[test]
    fn test_rewrite_graph_attribute() -> PlanResult<()> {
        let mut bindings = GraphBindings::default();
        bindings.bind_alias("a".to_string(), GraphAliasKind::Node)?;
        bindings.bind_alias("e".to_string(), GraphAliasKind::Edge)?;

        assert_eq!(
            rewrite_graph_expr(attr("a", "id"), &bindings),
            attr("a", "id")
        );
        assert_eq!(
            rewrite_graph_expr(attr("e", "label"), &bindings),
            attr("e", "edge_type")
        );
        assert_eq!(
            rewrite_graph_expr(attr("a", "age"), &bindings),
            json_prop("a", "age")
        );
        assert_eq!(
            rewrite_graph_expr(attr("e", "since"), &bindings),
            json_prop("e", "since")
        );

        Ok(())
    }

    #[test]
    fn test_typed_graph_table_field_compatibility() {
        let node_columns = table_columns(&["id", "age"]);
        let edge_columns = table_columns(&["edge_key", "src_id", "dst_id", "since"]);

        assert_eq!(GRAPH_TABLE_KIND_PROPERTY, "grust.graph.kind");
        assert_eq!(GRAPH_TABLE_LABEL_PROPERTY, "grust.graph.label");
        assert_eq!(GRAPH_TABLE_KIND_NODE, "node");
        assert_eq!(GRAPH_TABLE_KIND_EDGE, "edge");
        assert_eq!(
            typed_node_table("Person Profile").unwrap(),
            "grust_node_person_profile"
        );
        assert_eq!(typed_edge_table("Knows").unwrap(), "grust_edge_knows");
        assert!(typed_node_field_compatible("age"));
        assert!(typed_node_field_compatible("label"));
        assert!(!typed_node_field_compatible("props"));
        assert!(typed_edge_field_compatible("since"));
        assert!(typed_edge_field_compatible("label"));
        assert!(!typed_edge_field_compatible("src_label"));
        assert!(!typed_edge_field_compatible("dst_label"));
        assert!(!typed_edge_field_compatible("props"));
        assert!(typed_node_table_has_fields(
            &node_columns,
            &fields(&["id", "label", "age"])
        ));
        assert!(!typed_node_table_has_fields(
            &node_columns,
            &fields(&["edge_type"])
        ));
        assert!(!typed_node_table_has_fields(
            &node_columns,
            &fields(&["props"])
        ));
        assert_eq!(
            typed_node_table_missing_fields(&node_columns, &fields(&["id", "name", "props"])),
            ["name", "props"]
        );

        assert!(typed_edge_table_has_fields(
            &edge_columns,
            &fields(&["src_id", "dst_id", "edge_type", "label", "since"])
        ));
        assert!(!typed_edge_table_has_fields(
            &edge_columns,
            &fields(&["src_label"])
        ));
        assert!(!typed_edge_table_has_fields(
            &edge_columns,
            &fields(&["props"])
        ));
        assert!(!typed_edge_table_has_fields(
            &table_columns(&["edge_key", "id", "src_id"]),
            &fields(&["src_id", "dst_id", "edge_type"])
        ));
        assert_eq!(
            typed_edge_table_missing_fields(
                &table_columns(&["edge_key", "id", "src_id", "src_label", "props"]),
                &fields(&["src_id", "dst_id", "src_label", "props"])
            ),
            ["dst_id", "props", "src_label"]
        );
        assert_eq!(
            graph_table_metadata_mismatches(
                &graph_table_properties(GRAPH_TABLE_KIND_NODE, "Animal"),
                GRAPH_TABLE_KIND_NODE,
                "Person"
            ),
            [r#"grust.graph.label="Animal", expected "Person""#]
        );
        assert!(graph_table_metadata_mismatches(
            &graph_table_properties(GRAPH_TABLE_KIND_EDGE, "KNOWS"),
            GRAPH_TABLE_KIND_EDGE,
            "KNOWS"
        )
        .is_empty());
    }

    #[tokio::test]
    async fn test_resolve_cypher_graph_query_over_grust_views() -> PlanResult<()> {
        let ctx = create_session()?;
        create_empty_graph_views(&ctx).await?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        let plan = from_ast_statement(parse_one_statement(
            "MATCH (a:Person)-[e:KNOWS]->(b:Person) \
             WHERE a.age = '42' \
             RETURN a.id, e.label, b.name \
             ORDER BY b.name LIMIT 10",
        )?)?;

        let named = resolver.resolve_named_plan(plan).await?;
        let debug = format!("{:?}", named.plan);

        assert!(debug.contains("SubqueryAlias"), "{debug}");
        assert!(debug.contains("Bare { table: \"a\" }"), "{debug}");
        assert!(debug.contains("Bare { table: \"e\" }"), "{debug}");
        assert!(debug.contains("Bare { table: \"b\" }"), "{debug}");
        assert!(debug.contains("src_id"), "{debug}");
        assert!(debug.contains("dst_id"), "{debug}");
        assert!(debug.contains("edge_type"), "{debug}");
        assert!(debug.contains("JsonAsText"), "{debug}");
        assert_eq!(
            named.fields,
            Some(vec![
                "a.id".to_string(),
                "e.label".to_string(),
                "b.name".to_string()
            ])
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_cypher_graph_query_over_grust_views() -> PlanResult<()> {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        create_graph_views_with_data(&ctx).await?;
        let plan = from_ast_statement(parse_one_statement(
            "MATCH (a:Person)-[e:KNOWS]->(b:Person) \
             WHERE a.age = '42' \
             RETURN a.id, e.label, b.name \
             ORDER BY b.name LIMIT 10",
        )?)?;

        let named = resolver.resolve_named_plan(plan).await?;
        assert_eq!(
            named.fields,
            Some(vec![
                "a.id".to_string(),
                "e.label".to_string(),
                "b.name".to_string()
            ])
        );
        let batches = ctx
            .execute_logical_plan(named.plan)
            .await?
            .collect()
            .await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "1"
        );
        assert_eq!(
            batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "KNOWS"
        );
        assert_eq!(
            batches[0]
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "Bob"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_cypher_graph_query_property_maps_over_grust_views() -> PlanResult<()> {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        create_graph_views_with_data(&ctx).await?;
        let plan = from_ast_statement(parse_one_statement(
            "MATCH (a:Person {age: '42'})-[e:KNOWS {since: '2020'}]->(b:Person {name: 'Bob'}) \
             RETURN a.id, e.id, b.name",
        )?)?;

        let named = resolver.resolve_named_plan(plan).await?;
        assert_eq!(
            named.fields,
            Some(vec![
                "a.id".to_string(),
                "e.id".to_string(),
                "b.name".to_string()
            ])
        );
        let batches = ctx
            .execute_logical_plan(named.plan)
            .await?
            .collect()
            .await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "1"
        );
        assert_eq!(
            batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "edge-1"
        );
        assert_eq!(
            batches[0]
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "Bob"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_cypher_graph_query_returns_generic_edge_identity() -> PlanResult<()> {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        create_graph_views_with_data(&ctx).await?;
        let plan = from_ast_statement(parse_one_statement(
            "MATCH (a:Person)-[e:KNOWS]->(b:Person) \
             RETURN e.id, e.edge_key \
             ORDER BY e.id LIMIT 1",
        )?)?;

        let named = resolver.resolve_named_plan(plan).await?;
        assert_eq!(
            named.fields,
            Some(vec!["e.id".to_string(), "e.edge_key".to_string()])
        );
        let batches = ctx
            .execute_logical_plan(named.plan)
            .await?
            .collect()
            .await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "edge-1"
        );
        assert_eq!(
            batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "edge-key-1"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_cypher_graph_query_shorthand_relationship() -> PlanResult<()> {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        create_graph_views_with_data(&ctx).await?;
        let plan = from_ast_statement(parse_one_statement(
            "MATCH (a:Person)-->(b:Person) \
             WHERE a.age = '42' \
             RETURN b.name \
             ORDER BY b.name LIMIT 10",
        )?)?;

        let named = resolver.resolve_named_plan(plan).await?;
        assert_eq!(named.fields, Some(vec!["b.name".to_string()]));
        let batches = ctx
            .execute_logical_plan(named.plan)
            .await?
            .collect()
            .await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "Bob"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_cypher_graph_query_limit_all() -> PlanResult<()> {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        create_graph_views_with_data(&ctx).await?;
        let plan = from_ast_statement(parse_one_statement(
            "MATCH (a:Person)-->(b:Person) \
             RETURN b.name \
             ORDER BY b.name \
             SKIP 1 \
             LIMIT ALL",
        )?)?;

        let named = resolver.resolve_named_plan(plan).await?;
        assert_eq!(named.fields, Some(vec!["b.name".to_string()]));
        let batches = ctx
            .execute_logical_plan(named.plan)
            .await?
            .collect()
            .await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "Bob"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_cypher_graph_query_over_typed_grust_views() -> PlanResult<()> {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        create_typed_graph_views_with_data(&ctx).await?;
        let plan = from_ast_statement(parse_one_statement(
            "MATCH (a:Person)-[e:KNOWS]->(b:Person) \
             WHERE a.age = '42' \
             RETURN a.id, e.label, b.name \
             ORDER BY b.name LIMIT 10",
        )?)?;

        let named = resolver.resolve_named_plan(plan).await?;
        let debug = format!("{:?}", named.plan);
        assert!(debug.contains("Literal(Utf8(\"KNOWS\")"), "{debug}");
        assert!(!debug.contains("JsonAsText"), "{debug}");
        assert_eq!(
            named.fields,
            Some(vec![
                "a.id".to_string(),
                "e.label".to_string(),
                "b.name".to_string()
            ])
        );
        let batches = ctx
            .execute_logical_plan(named.plan)
            .await?
            .collect()
            .await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "1"
        );
        assert_eq!(
            batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "KNOWS"
        );
        assert_eq!(
            batches[0]
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "Bob"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_cypher_graph_query_label_only_uses_typed_grust_views() -> PlanResult<()> {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        create_typed_graph_views_with_data(&ctx).await?;
        let plan = from_ast_statement(parse_one_statement(
            "MATCH (a:Person)-[e:KNOWS]->(b:Person) \
             RETURN e.label",
        )?)?;

        let named = resolver.resolve_named_plan(plan).await?;
        let batches = ctx
            .execute_logical_plan(named.plan)
            .await?
            .collect()
            .await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "KNOWS"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_cypher_graph_query_property_maps_over_typed_grust_views() -> PlanResult<()>
    {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        create_typed_graph_views_with_data(&ctx).await?;
        let plan = from_ast_statement(parse_one_statement(
            "MATCH (a:Person {age: '42'})-[e:KNOWS {since: '2020'}]->(b:Person {name: 'Bob'}) \
             RETURN a.id, e.id, b.name",
        )?)?;

        let named = resolver.resolve_named_plan(plan).await?;
        let debug = format!("{:?}", named.plan);
        assert!(!debug.contains("JsonAsText"), "{debug}");
        assert_eq!(
            named.fields,
            Some(vec![
                "a.id".to_string(),
                "e.id".to_string(),
                "b.name".to_string()
            ])
        );
        let batches = ctx
            .execute_logical_plan(named.plan)
            .await?
            .collect()
            .await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "1"
        );
        assert_eq!(
            batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "edge-1"
        );
        assert_eq!(
            batches[0]
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "Bob"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_cypher_graph_query_multiple_patterns_over_typed_grust_views(
    ) -> PlanResult<()> {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        create_typed_graph_views_with_data(&ctx).await?;
        let plan = from_ast_statement(parse_one_statement(
            "MATCH (a:Person)-[e:KNOWS]->(b:Person), (b)<-[f:KNOWS]-(a:Person) \
             WHERE a.age = '42' \
             RETURN a.id, b.name, e.id, f.id \
             ORDER BY b.name LIMIT 10",
        )?)?;

        let named = resolver.resolve_named_plan(plan).await?;
        let debug = format!("{:?}", named.plan);
        assert!(!debug.contains("JsonAsText"), "{debug}");
        assert_eq!(
            named.fields,
            Some(vec![
                "a.id".to_string(),
                "b.name".to_string(),
                "e.id".to_string(),
                "f.id".to_string()
            ])
        );
        let batches = ctx
            .execute_logical_plan(named.plan)
            .await?
            .collect()
            .await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "1"
        );
        assert_eq!(
            batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "Bob"
        );
        assert_eq!(
            batches[0]
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "edge-1"
        );
        assert_eq!(
            batches[0]
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "edge-1"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_cypher_graph_query_conflicting_typed_alias_labels() -> PlanResult<()> {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        create_typed_graph_views_with_data(&ctx).await?;

        let node_conflict = from_ast_statement(parse_one_statement(
            "MATCH (a:Person)-[e:KNOWS]->(b:Person), (a:Document)-->(b:Person) \
             RETURN a.id",
        )?)?;
        let named = resolver.resolve_named_plan(node_conflict).await?;
        let batches = ctx
            .execute_logical_plan(named.plan)
            .await?
            .collect()
            .await?;
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            0
        );

        let edge_conflict = from_ast_statement(parse_one_statement(
            "MATCH (a:Person)-[e:KNOWS]->(b:Person), (a)-[e:LIKES]->(b) \
             RETURN e.id",
        )?)?;
        let named = resolver.resolve_named_plan(edge_conflict).await?;
        let batches = ctx
            .execute_logical_plan(named.plan)
            .await?
            .collect()
            .await?;
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            0
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cypher_graph_query_falls_back_when_typed_table_lacks_fields() -> PlanResult<()> {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        create_graph_views_with_data(&ctx).await?;
        create_partial_typed_node_view(&ctx).await?;
        let plan = from_ast_statement(parse_one_statement(
            "MATCH (a:Person)-[e:KNOWS]->(b:Person) \
             WHERE a.age = '42' \
             RETURN a.id, e.label, b.name \
             ORDER BY b.name LIMIT 10",
        )?)?;

        let named = resolver.resolve_named_plan(plan).await?;
        let debug = format!("{:?}", named.plan);
        assert!(debug.contains("JsonAsText"), "{debug}");
        let batches = ctx
            .execute_logical_plan(named.plan)
            .await?
            .collect()
            .await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0]
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "Bob"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cypher_graph_query_falls_back_when_typed_edge_lacks_structural_fields(
    ) -> PlanResult<()> {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        create_graph_views_with_data(&ctx).await?;
        create_structurally_incomplete_typed_edge_view(&ctx).await?;
        let plan = from_ast_statement(parse_one_statement(
            "MATCH (a:Person)-[e:KNOWS]->(b:Person) \
             RETURN b.name \
             ORDER BY b.name LIMIT 10",
        )?)?;

        let named = resolver.resolve_named_plan(plan).await?;
        let batches = ctx
            .execute_logical_plan(named.plan)
            .await?
            .collect()
            .await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "Bob"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cypher_graph_query_falls_back_when_typed_node_metadata_mismatches(
    ) -> PlanResult<()> {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        create_graph_views_with_data(&ctx).await?;
        create_mismatched_typed_node_view(&ctx).await?;

        let rows = collect_graph_query_rows(
            &ctx,
            &resolver,
            "MATCH (a:Person) RETURN a.name ORDER BY a.name",
            1,
        )
        .await?;
        assert_eq!(rows, vec![vec!["Alice"], vec!["Bob"]]);

        Ok(())
    }

    #[tokio::test]
    async fn test_cypher_graph_query_falls_back_when_typed_edge_metadata_mismatches(
    ) -> PlanResult<()> {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        create_graph_views_with_data(&ctx).await?;
        create_mismatched_typed_edge_view(&ctx).await?;

        let rows = collect_graph_query_rows(
            &ctx,
            &resolver,
            "MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN e.id ORDER BY e.id",
            1,
        )
        .await?;
        assert_eq!(rows, vec![vec!["edge-1"]]);

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_cypher_graph_query_directions() -> PlanResult<()> {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        create_graph_views_with_data(&ctx).await?;

        for sql in [
            "MATCH (b:Person)<-[e:KNOWS]-(a:Person) \
             WHERE a.age = '42' \
             RETURN b.name \
             ORDER BY b.name LIMIT 10",
            "MATCH (a:Person)-[e:KNOWS]-(b:Person) \
             WHERE a.age = '42' \
             RETURN b.name \
             ORDER BY b.name LIMIT 10",
        ] {
            let plan = from_ast_statement(parse_one_statement(sql)?)?;
            let named = resolver.resolve_named_plan(plan).await?;
            assert_eq!(named.fields, Some(vec!["b.name".to_string()]));
            let batches = ctx
                .execute_logical_plan(named.plan)
                .await?
                .collect()
                .await?;
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 1);
            assert_eq!(
                batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "Bob"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_cypher_graph_query_direction_semantics() -> PlanResult<()> {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        create_graph_views_with_data(&ctx).await?;

        let outgoing = collect_graph_query_rows(
            &ctx,
            &resolver,
            "MATCH (a)-[e]->(b) \
             RETURN a.id, e.id, b.id \
             ORDER BY e.id",
            3,
        )
        .await?;
        assert_eq!(
            outgoing,
            vec![
                vec!["1".to_string(), "edge-1".to_string(), "2".to_string()],
                vec!["2".to_string(), "edge-2".to_string(), "1".to_string()],
                vec!["3".to_string(), "edge-3".to_string(), "2".to_string()],
            ]
        );

        let incoming = collect_graph_query_rows(
            &ctx,
            &resolver,
            "MATCH (a)<-[e]-(b) \
             RETURN a.id, e.id, b.id \
             ORDER BY e.id",
            3,
        )
        .await?;
        assert_eq!(
            incoming,
            vec![
                vec!["2".to_string(), "edge-1".to_string(), "1".to_string()],
                vec!["1".to_string(), "edge-2".to_string(), "2".to_string()],
                vec!["2".to_string(), "edge-3".to_string(), "3".to_string()],
            ]
        );

        let undirected = collect_graph_query_rows(
            &ctx,
            &resolver,
            "MATCH (a)-[e]-(b) \
             RETURN a.id, e.id, b.id \
             ORDER BY e.id, a.id",
            3,
        )
        .await?;
        assert_eq!(
            undirected,
            vec![
                vec!["1".to_string(), "edge-1".to_string(), "2".to_string()],
                vec!["2".to_string(), "edge-1".to_string(), "1".to_string()],
                vec!["1".to_string(), "edge-2".to_string(), "2".to_string()],
                vec!["2".to_string(), "edge-2".to_string(), "1".to_string()],
                vec!["2".to_string(), "edge-3".to_string(), "3".to_string()],
                vec!["3".to_string(), "edge-3".to_string(), "2".to_string()],
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_cypher_graph_query_multiple_patterns_share_aliases() -> PlanResult<()> {
        let ctx = create_session()?;
        let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::new()?));
        create_graph_views_with_data(&ctx).await?;
        let plan = from_ast_statement(parse_one_statement(
            "MATCH (a:Person)-[e:KNOWS]->(b:Person), (b)<-[f:KNOWS]-(d:Document) \
             WHERE a.age = '42' \
             RETURN a.id, b.name, d.name, e.id, f.id \
             ORDER BY d.name LIMIT 10",
        )?)?;

        let named = resolver.resolve_named_plan(plan).await?;
        assert_eq!(
            named.fields,
            Some(vec![
                "a.id".to_string(),
                "b.name".to_string(),
                "d.name".to_string(),
                "e.id".to_string(),
                "f.id".to_string()
            ])
        );
        let batches = ctx
            .execute_logical_plan(named.plan)
            .await?
            .collect()
            .await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "1"
        );
        assert_eq!(
            batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "Bob"
        );
        assert_eq!(
            batches[0]
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "Paper"
        );
        assert_eq!(
            batches[0]
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "edge-1"
        );
        assert_eq!(
            batches[0]
                .column(4)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "edge-3"
        );

        Ok(())
    }
}
