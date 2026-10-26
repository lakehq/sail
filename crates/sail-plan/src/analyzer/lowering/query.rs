//! Checked, one-way lowering. This module neither binds names nor inserts
//! implicit casts. model::Attribute names are assigned exactly once at this boundary.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion_common::{Column, DFSchema, DFSchemaRef};
use datafusion_expr::expr::{
    AggregateFunction, AggregateFunctionParams, Case, Exists, InList, InSubquery,
};
use datafusion_expr::logical_plan::InvariantLevel;
use datafusion_expr::{
    EmptyRelation, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Subquery, Union, Values, expr,
};

use crate::analyzer::{NamedPlan, model};
use crate::error::{PlanError, PlanResult};

pub fn lower_query(query: model::Query) -> PlanResult<NamedPlan> {
    let output = query.schema.resolved("query output schema")?;
    let names = output
        .attributes
        .iter()
        .filter(|a| !a.hidden)
        .map(|a| a.name.clone())
        .collect();
    let mut context = LoweringContext::default();
    let mut plan = context.plan(&query)?;
    if output.attributes.iter().any(|a| a.hidden) {
        let expressions = output
            .attributes
            .iter()
            .filter(|a| !a.hidden)
            .map(|a| datafusion_expr::Expr::Column(Column::from_name(backend_name(a.id))))
            .collect::<Vec<_>>();
        plan = LogicalPlanBuilder::new(plan)
            .project(expressions)?
            .build()?;
    }
    plan.check_invariants(InvariantLevel::Executable)?;
    Ok(NamedPlan {
        plan,
        fields: Some(names),
    })
}

#[derive(Default)]
struct LoweringContext {
    expression_ids: HashSet<model::ExprId>,
    outer: Vec<model::QuerySchema>,
    lambdas: Vec<model::QuerySchema>,
}

pub(crate) fn backend_name(id: model::AttributeId) -> String {
    format!("#{}", id.0)
}

fn backend_schema(schema: &model::QuerySchema) -> PlanResult<DFSchemaRef> {
    let mut ids = HashSet::new();
    let fields = schema
        .attributes
        .iter()
        .map(|attribute| {
            if !ids.insert(attribute.id) {
                return Err(PlanError::analysis(format!(
                    "duplicate output attribute: {:?}",
                    attribute.id
                )));
            }
            let field = attribute
                .result_type
                .resolved("output attribute type")?
                .to_field(backend_name(attribute.id));
            Ok((None, field))
        })
        .collect::<PlanResult<Vec<_>>>()?;
    Ok(Arc::new(DFSchema::new_with_metadata(
        fields,
        HashMap::new(),
    )?))
}

impl LoweringContext {
    fn plan(&mut self, plan: &model::Query) -> PlanResult<LogicalPlan> {
        let output = plan.schema.resolved("plan schema")?;
        let expected_schema = backend_schema(output)?;
        let lowered = match plan.kind.resolved("plan node")? {
            model::QueryKind::Scope { input } => self.plan(input)?,
            model::QueryKind::Empty { produce_one_row } => LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: *produce_one_row,
                schema: Arc::new(DFSchema::empty()),
            }),
            model::QueryKind::Range {
                start,
                end,
                step,
                partitions,
            } => {
                let [attribute] = output.attributes.as_slice() else {
                    return Err(PlanError::analysis("range requires one output attribute"));
                };
                LogicalPlan::Extension(datafusion_expr::Extension {
                    node: Arc::new(sail_logical_plan::range::RangeNode::try_new(
                        backend_name(attribute.id),
                        *start,
                        *end,
                        *step,
                        *partitions,
                    )?),
                })
            }
            model::QueryKind::RowIdentity { input, attribute, kind } => {
                let input = Arc::new(self.plan(input)?);
                let name = backend_name(attribute.id);
                let node: Arc<dyn datafusion_expr::UserDefinedLogicalNode> = match kind {
                    model::RowIdentityKind::Monotonic => Arc::new(sail_logical_plan::monotonic_id::MonotonicIdNode::try_new(input, name)?),
                    model::RowIdentityKind::Partition => Arc::new(sail_logical_plan::spark_partition_id::SparkPartitionIdNode::try_new(input, name)?),
                };
                LogicalPlan::Extension(datafusion_expr::Extension { node })
            }
            model::QueryKind::MapPartitions { input, function } => LogicalPlan::Extension(datafusion_expr::Extension {
                node: Arc::new(sail_logical_plan::map_partitions::MapPartitionsNode::try_new(
                    Arc::new(self.plan(input)?), output.attributes.iter().map(|a| backend_name(a.id)).collect(), vec![None; output.attributes.len()], Arc::clone(function),
                )?),
            }),
            model::QueryKind::RemoteCheckpoint { relation_id } => LogicalPlan::Extension(datafusion_expr::Extension {
                node: Arc::new(sail_logical_plan::remote_checkpoint::RemoteCheckpointRelationNode::try_new(
                    relation_id.clone(), Arc::new(expected_schema.as_arrow().clone()),
                )?),
            }),
            model::QueryKind::Unnest { input, column, outer } => {
                let input = self.plan(input)?;
                let plan = datafusion_expr::builder::unnest_with_options(
                    input,
                    vec![Column::from_name(backend_name(*column))],
                    datafusion_common::UnnestOptions::new().with_null_handling(if *outer {
                        datafusion_common::NullHandling::PreserveAndExpandEmpty
                    } else {
                        datafusion_common::NullHandling::Drop
                    }),
                )?;
                rename_outputs(plan, output)?
            }
            model::QueryKind::DistinctOn { input, keys } => {
                let lowered = self.plan(input)?;
                let keys = keys.iter().map(|e| self.expression(e, input.schema.resolved("distinct input")?)).collect::<PlanResult<Vec<_>>>()?;
                let select = lowered.schema().columns().into_iter().map(datafusion_expr::Expr::Column).collect();
                LogicalPlan::Distinct(datafusion_expr::Distinct::On(datafusion_expr::DistinctOn::try_new(keys, select, None, Arc::new(lowered))?))
            }
            model::QueryKind::Repartition {
                input,
                partitioning,
            } => {
                let lowered = self.plan(input)?;
                let schema = input.schema.resolved("repartition input")?;
                use sail_logical_plan::repartition::{
                    ExplicitRepartitionKind, ExplicitRepartitionNode,
                };
                let (kind, count, expressions) = match partitioning {
                    model::Partitioning::RoundRobin(count) => (ExplicitRepartitionKind::RoundRobin, Some(*count), vec![]),
                    model::Partitioning::Coalesce(count) => (ExplicitRepartitionKind::Coalesce, Some(*count), vec![]),
                    model::Partitioning::Hash(expressions, count) => (
                        ExplicitRepartitionKind::Hash,
                        Some(*count),
                        expressions
                            .iter()
                            .map(|expression| self.expression(expression, schema))
                            .collect::<PlanResult<_>>()?,
                    ),
                    model::Partitioning::Distribute(expressions) => (
                        ExplicitRepartitionKind::Hash,
                        None,
                        expressions
                            .iter()
                            .map(|expression| self.expression(expression, schema))
                            .collect::<PlanResult<_>>()?,
                    ),
                };
                LogicalPlan::Extension(datafusion_expr::Extension {
                    node: Arc::new(ExplicitRepartitionNode::new(
                        Arc::new(lowered),
                        count,
                        kind,
                        expressions,
                    )),
                })
            }

            model::QueryKind::Values { rows } => {
                let values = rows
                    .iter()
                    .map(|row| {
                        if row.len() != output.attributes.len() {
                            return Err(PlanError::analysis(
                                "VALUES row width does not match its schema",
                            ));
                        }
                        row.iter()
                            .zip(&output.attributes)
                            .map(|(value, attribute)| {
                                let value_type =
                                    value.result_type.resolved("VALUES expression type")?;
                                let attribute_type =
                                    attribute.result_type.resolved("VALUES attribute type")?;
                                if value_type.data_type != attribute_type.data_type
                                    || (value_type.nullable && !attribute_type.nullable)
                                    || value_type.metadata != attribute_type.metadata
                                {
                                    return Err(PlanError::analysis(
                                        "VALUES requires unresolved type coercion",
                                    ));
                                }
                                self.expression(value, &model::QuerySchema::default())
                            })
                            .collect::<PlanResult<Vec<_>>>()
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                LogicalPlan::Values(Values {
                    schema: Arc::clone(&expected_schema),
                    values,
                })
            }
            model::QueryKind::Scan {
                name,
                source,
                projection,
            } => {
                // Arrow permits duplicate source names; bind slots by ordinal before
                // constructing a DataFusion schema, which requires unique names.
                let mut names = HashSet::new();
                let source = if source.schema().fields().iter().any(|f| !names.insert(f.name().clone())) {
                    let provider = datafusion::datasource::source_as_provider(source)?;
                    let names = (0..source.schema().fields().len()).map(|i| format!("__sail_source_{i}")).collect();
                    datafusion::datasource::provider_as_source(Arc::new(
                        sail_common_datafusion::rename::table_provider::RenameTableProvider::try_new(provider, names)?,
                    ))
                } else { Arc::clone(source) };
                let scan = LogicalPlanBuilder::scan(name.clone(), source, projection.clone())?.build()?;
                rename_outputs(scan, output)?
            }
            model::QueryKind::Project { input, expressions } => {
                let lowered_input = self.plan(input)?;
                let schema = input.schema.resolved("projection input")?;
                let expressions = self.projections(expressions, schema)?;
                LogicalPlan::Projection(datafusion_expr::Projection::try_new_with_schema(
                    expressions,
                    Arc::new(lowered_input),
                    Arc::clone(&expected_schema),
                )?)
            }
            model::QueryKind::Filter { input, condition } => {
                let lowered_input = self.plan(input)?;
                let predicate =
                    self.expression(condition, input.schema.resolved("filter input")?)?;
                LogicalPlanBuilder::new(lowered_input)
                    .filter(predicate)?
                    .build()?
            }
            model::QueryKind::Aggregate { input, grouping, grouping_sets, grouping_id, aggregates } => {
                let lowered_input = self.plan(input)?;
                let schema = input.schema.resolved("aggregate input")?;
                let aggregates = self.projections(aggregates, schema)?;
                if let Some(sets) = grouping_sets {
                    if grouping_id.is_none() { return Err(PlanError::analysis("grouping sets require an ID attribute")); }
                    let grouping = grouping.iter().map(|p| Ok(self.expression(&p.expression, schema)?.alias(backend_name(p.attribute.id)))).collect::<PlanResult<Vec<_>>>()?;
                    let sets = sets.iter().map(|set| set.iter().map(|i| grouping.get(*i).cloned().ok_or_else(|| PlanError::analysis("invalid grouping set index"))).collect::<PlanResult<Vec<_>>>()).collect::<PlanResult<Vec<_>>>()?;
                    let aggregate = LogicalPlan::Aggregate(datafusion_expr::Aggregate::try_new(Arc::new(lowered_input), vec![datafusion_expr::Expr::GroupingSet(expr::GroupingSet::GroupingSets(sets))], aggregates)?);
                    rename_outputs(aggregate, output)?
                } else {
                    if grouping_id.is_some() { return Err(PlanError::analysis("grouping ID requires grouping sets")); }
                    let grouping = self.projections(grouping, schema)?;
                    LogicalPlan::Aggregate(datafusion_expr::Aggregate::try_new_with_schema(Arc::new(lowered_input), grouping, aggregates, Arc::clone(&expected_schema))?)
                }
            }
            model::QueryKind::Sort {
                input,
                order,
                global,
                required,
            } => {
                let lowered_input = self.plan(input)?;
                let schema = input.schema.resolved("sort input")?;
                let order = self.sorts(order, schema)?;
                if *required {
                    LogicalPlan::Extension(datafusion_expr::Extension {
                        node: Arc::new(sail_logical_plan::sort::RequiredSortNode::new(
                            Arc::new(lowered_input), order, None, !*global,
                        )),
                    })
                } else if *global {
                    LogicalPlan::Sort(datafusion_expr::Sort { input: Arc::new(lowered_input), expr: order, fetch: None })
                } else {
                    LogicalPlan::Extension(datafusion_expr::Extension {
                        node: Arc::new(sail_logical_plan::sort::SortWithinPartitionsNode::new(
                            Arc::new(lowered_input),
                            order,
                            None,
                        )),
                    })
                }
            }
            model::QueryKind::Window { input, expressions } => {
                let lowered_input = self.plan(input)?;
                let schema = input.schema.resolved("window input")?;
                let expressions = self.projections(expressions, schema)?;
                LogicalPlan::Window(datafusion_expr::Window::try_new_with_schema(
                    expressions,
                    Arc::new(lowered_input),
                    Arc::clone(&expected_schema),
                )?)
            }
            model::QueryKind::Limit { input, skip, fetch } => {
                let lowered_input = self.plan(input)?;
                let schema = input.schema.resolved("limit input")?;
                let skip = skip
                    .as_ref()
                    .map(|expression| self.expression(expression, schema))
                    .transpose()?;
                let fetch = fetch
                    .as_ref()
                    .map(|expression| self.expression(expression, schema))
                    .transpose()?;
                LogicalPlanBuilder::new(lowered_input)
                    .limit_by_expr(skip, fetch)?
                    .build()?
            }
            model::QueryKind::Join {
                lateral,
                left,
                right,
                kind,
                condition,
            } => {
                let lowered_left = self.plan(left)?;
                let lowered_right = if *lateral {
                    let subquery = self.subquery(right, left.schema.resolved("lateral left input")?)?;
                    if subquery.outer_ref_columns.is_empty() { Arc::unwrap_or_clone(subquery.subquery) } else { LogicalPlan::Subquery(subquery) }
                } else { self.plan(right)? };
                let schema = model::QuerySchema {
                    attributes: left
                        .schema
                        .resolved("join left input")?
                        .attributes
                        .iter()
                        .chain(&right.schema.resolved("join right input")?.attributes)
                        .cloned()
                        .collect(),
                };
                let condition = condition
                    .as_ref()
                    .map(|expression| self.expression(expression, &schema))
                    .transpose()?;
                let joined = LogicalPlanBuilder::new(lowered_left)
                    .join_on(lowered_right, *kind, condition)?
                    .build()?;
                if matches!(kind, datafusion_expr::JoinType::LeftMark | datafusion_expr::JoinType::RightMark) {
                    rename_outputs(joined, output)?
                } else { joined }
            }
            model::QueryKind::Union { inputs } => {
                if inputs.is_empty() {
                    return Err(PlanError::analysis("UNION requires at least one input"));
                }
                let inputs = inputs
                    .iter()
                    .map(|input| {
                        let input = self.plan(input)?;
                        if input.schema().fields().len() != output.attributes.len() {
                            return Err(PlanError::analysis(
                                "UNION input width does not match its schema",
                            ));
                        }
                        for (field, target) in
                            input.schema().fields().iter().zip(expected_schema.fields())
                        {
                            if field.data_type() != target.data_type()
                                || (field.is_nullable() && !target.is_nullable())
                                || field.metadata() != target.metadata()
                            {
                                return Err(PlanError::analysis(
                                    "UNION requires unresolved type coercion",
                                ));
                            }
                        }
                        let expressions = input.schema().columns().into_iter().zip(&output.attributes)
                            .map(|(column, attribute)| datafusion_expr::Expr::Column(column).alias(backend_name(attribute.id))).collect::<Vec<_>>();
                        let input = LogicalPlanBuilder::new(input).project(expressions)?.build()?;
                        Ok(Arc::new(input))
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                LogicalPlan::Union(Union {
                    inputs,
                    schema: Arc::clone(&expected_schema),
                })
            }
            model::QueryKind::Distinct { input } => LogicalPlanBuilder::new(self.plan(input)?)
                .distinct()?
                .build()?,
        };
        // Functional dependencies are backend optimization metadata, not
        // output attributes. The lowering builder may derive additional ones.
        if lowered.schema().inner() != expected_schema.inner()
            || !lowered
                .schema()
                .iter()
                .zip(expected_schema.iter())
                .all(|((a, _), (b, _))| a == b)
        {
            return Err(PlanError::analysis(format!(
                "lowered plan schema differs from the resolved schema: expected {expected_schema:?}, got {:?}",
                lowered.schema()
            )));
        }
        Ok(lowered)
    }

    fn projections(
        &mut self,
        projections: &[model::Projection],
        schema: &model::QuerySchema,
    ) -> PlanResult<Vec<datafusion_expr::Expr>> {
        projections
            .iter()
            .map(|projection| {
                let expression_type = projection
                    .expression
                    .result_type
                    .resolved("projection expression type")?;
                let attribute_type = projection
                    .attribute
                    .result_type
                    .resolved("projection attribute type")?;
                if expression_type != attribute_type {
                    return Err(PlanError::analysis(
                        "projection expression and output attribute disagree",
                    ));
                }
                Ok(self
                    .expression(&projection.expression, schema)?
                    .alias(backend_name(projection.attribute.id)))
            })
            .collect()
    }

    fn sorts(
        &mut self,
        order: &[model::SortKey],
        schema: &model::QuerySchema,
    ) -> PlanResult<Vec<expr::Sort>> {
        order
            .iter()
            .map(|sort| {
                Ok(expr::Sort::new(
                    self.expression(&sort.expression, schema)?,
                    sort.ascending,
                    sort.nulls_first,
                ))
            })
            .collect()
    }

    fn subquery(
        &mut self,
        query: &model::Query,
        schema: &model::QuerySchema,
    ) -> PlanResult<Subquery> {
        self.outer.push(schema.clone());
        let result = self.plan(query);
        self.outer.pop();
        let plan = result?;
        let outer_ref_columns = plan.all_out_ref_exprs();
        Ok(Subquery {
            subquery: Arc::new(plan),
            outer_ref_columns,
            spans: Default::default(),
        })
    }

    fn expression(
        &mut self,
        expression: &model::Expr,
        schema: &model::QuerySchema,
    ) -> PlanResult<datafusion_expr::Expr> {
        if !self.expression_ids.insert(expression.id) {
            return Err(PlanError::analysis(format!(
                "duplicate expression identity: {:?}",
                expression.id
            )));
        }
        let expected = expression.result_type.resolved("expression type")?;
        let lowered = match expression.kind.resolved("expression node")? {
            model::ExprKind::Literal(value) => datafusion_expr::Expr::Literal(
                value.clone(),
                (!expected.metadata.is_empty()).then(|| {
                    datafusion_common::metadata::FieldMetadata::from(expected.metadata.clone())
                }),
            ),
            model::ExprKind::WithMetadata(inner) => {
                let source = inner.result_type.resolved("metadata source")?;
                let value = self.expression(inner, schema)?;
                if source
                    .metadata
                    .keys()
                    .all(|key| expected.metadata.contains_key(key))
                {
                    value.alias_with_metadata(
                        format!("__sail_expression_{}", expression.id.0),
                        Some(datafusion_common::metadata::FieldMetadata::from(
                            expected.metadata.clone(),
                        )),
                    )
                } else {
                    // Aliases merge metadata. A field-bearing identity cast is
                    // required only when analysis explicitly removes source keys.
                    datafusion_expr::Expr::Cast(expr::Cast::new_from_field(
                        Box::new(value),
                        expected.to_field(format!("__sail_expression_{}", expression.id.0)),
                    ))
                }
            }
            model::ExprKind::TruthTest {
                expression,
                value,
                negated,
            } => {
                let expression = Box::new(self.expression(expression, schema)?);
                match (value, negated) {
                    (Some(true), false) => datafusion_expr::Expr::IsTrue(expression),
                    (Some(true), true) => datafusion_expr::Expr::IsNotTrue(expression),
                    (Some(false), false) => datafusion_expr::Expr::IsFalse(expression),
                    (Some(false), true) => datafusion_expr::Expr::IsNotFalse(expression),
                    (None, false) => datafusion_expr::Expr::IsUnknown(expression),
                    (None, true) => datafusion_expr::Expr::IsNotUnknown(expression),
                }
            }
            model::ExprKind::Attribute(id) => {
                schema.attribute(*id)?;
                datafusion_expr::Expr::Column(Column::from_name(backend_name(*id)))
            }
            model::ExprKind::OuterAttribute { depth, attribute } => {
                let index = self
                    .outer
                    .len()
                    .checked_sub(*depth)
                    .filter(|_| *depth > 0)
                    .ok_or_else(|| PlanError::analysis("invalid outer query depth"))?;
                let attribute = self.outer[index].attribute(*attribute)?;
                let field = attribute
                    .result_type
                    .resolved("outer attribute type")?
                    .to_field(backend_name(attribute.id));
                datafusion_expr::Expr::OuterReferenceColumn(
                    field,
                    Column::from_name(backend_name(attribute.id)),
                )
            }
            model::ExprKind::ScalarFunction {
                function,
                arguments,
            } => {
                let arguments = arguments
                    .iter()
                    .map(|argument| self.expression(argument, schema))
                    .collect::<PlanResult<Vec<_>>>()?;
                function.call(arguments)
            }
            model::ExprKind::AggregateFunction {
                function,
                arguments,
                distinct,
                filter,
                order_by,
                null_treatment,
            } => {
                let args = arguments
                    .iter()
                    .map(|argument| self.expression(argument, schema))
                    .collect::<PlanResult<Vec<_>>>()?;
                let filter = filter
                    .as_ref()
                    .map(|filter| self.expression(filter, schema).map(Box::new))
                    .transpose()?;
                let order_by = self.sorts(order_by, schema)?;
                datafusion_expr::Expr::AggregateFunction(AggregateFunction {
                    func: Arc::clone(function),
                    params: AggregateFunctionParams {
                        args,
                        distinct: *distinct,
                        filter,
                        order_by,
                        null_treatment: *null_treatment,
                    },
                })
            }
            model::ExprKind::Binary {
                left,
                operator,
                right,
            } => datafusion_expr::Expr::BinaryExpr(expr::BinaryExpr::new(
                Box::new(self.expression(left, schema)?),
                *operator,
                Box::new(self.expression(right, schema)?),
            )),
            model::ExprKind::WindowFunction {
                function,
                arguments,
                partition_by,
                order_by,
                frame,
                filter,
                distinct,
                null_treatment,
            } => {
                let args = arguments
                    .iter()
                    .map(|argument| self.expression(argument, schema))
                    .collect::<PlanResult<Vec<_>>>()?;
                let partition_by = partition_by
                    .iter()
                    .map(|expression| self.expression(expression, schema))
                    .collect::<PlanResult<Vec<_>>>()?;
                let order_by = self.sorts(order_by, schema)?;
                let filter = filter
                    .as_ref()
                    .map(|filter| self.expression(filter, schema).map(Box::new))
                    .transpose()?;
                datafusion_expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
                    fun: function.clone(),
                    params: expr::WindowFunctionParams {
                        args,
                        partition_by,
                        order_by,
                        window_frame: frame.clone(),
                        null_treatment: *null_treatment,
                        distinct: *distinct,
                        filter,
                    },
                }))
            }
            model::ExprKind::HigherOrderFunction {
                function,
                arguments,
            } => {
                let args = arguments
                    .iter()
                    .map(|argument| match argument {
                        model::FunctionArg::Value(value) => self.expression(value, schema),
                        model::FunctionArg::Lambda {
                            id,
                            parameters,
                            body,
                        } => {
                            if !self.expression_ids.insert(*id) {
                                return Err(PlanError::analysis(format!(
                                    "duplicate lambda identity: {id:?}"
                                )));
                            }
                            // Validate parameter IDs and types even if the body does
                            // not reference every parameter.
                            backend_schema(parameters)?;
                            let params = parameters
                                .attributes
                                .iter()
                                .map(|a| backend_name(a.id))
                                .collect();
                            self.lambdas.push(parameters.clone());
                            let result = self.expression(body, schema);
                            self.lambdas.pop();
                            Ok(datafusion_expr::Expr::Lambda(expr::Lambda::new(
                                params, result?,
                            )))
                        }
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                datafusion_expr::Expr::HigherOrderFunction(expr::HigherOrderFunction::new(
                    Arc::clone(function),
                    args,
                ))
            }
            model::ExprKind::LambdaVariable(id) => {
                let attribute = self
                    .lambdas
                    .iter()
                    .rev()
                    .find_map(|parameters| parameters.attributes.iter().find(|a| a.id == *id))
                    .ok_or_else(|| {
                        PlanError::analysis(format!("lambda variable {id:?} is not in scope"))
                    })?;
                let field = attribute
                    .result_type
                    .resolved("lambda parameter type")?
                    .to_field(backend_name(*id));
                datafusion_expr::Expr::LambdaVariable(expr::LambdaVariable::new(
                    backend_name(*id),
                    Some(field),
                ))
            }
            model::ExprKind::Cast {
                expression,
                target,
                safe,
            } => {
                let value = Box::new(self.expression(expression, schema)?);
                let field = target.to_field("");
                if *safe {
                    datafusion_expr::Expr::TryCast(expr::TryCast { expr: value, field })
                } else {
                    datafusion_expr::Expr::Cast(expr::Cast { expr: value, field })
                }
            }
            model::ExprKind::Not(value) => {
                datafusion_expr::Expr::Not(Box::new(self.expression(value, schema)?))
            }
            model::ExprKind::IsNull(value) => {
                datafusion_expr::Expr::IsNull(Box::new(self.expression(value, schema)?))
            }
            model::ExprKind::IsNotNull(value) => {
                datafusion_expr::Expr::IsNotNull(Box::new(self.expression(value, schema)?))
            }
            model::ExprKind::Negative(value) => {
                datafusion_expr::Expr::Negative(Box::new(self.expression(value, schema)?))
            }
            model::ExprKind::Case {
                operand,
                branches,
                otherwise,
            } => {
                let operand = operand
                    .as_ref()
                    .map(|operand| self.expression(operand, schema).map(Box::new))
                    .transpose()?;
                let branches = branches
                    .iter()
                    .map(|(when, then)| {
                        Ok((
                            Box::new(self.expression(when, schema)?),
                            Box::new(self.expression(then, schema)?),
                        ))
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                let otherwise = otherwise
                    .as_ref()
                    .map(|otherwise| self.expression(otherwise, schema).map(Box::new))
                    .transpose()?;
                datafusion_expr::Expr::Case(Case::new(operand, branches, otherwise))
            }
            model::ExprKind::Between {
                expression,
                low,
                high,
                negated,
            } => datafusion_expr::Expr::Between(expr::Between::new(
                Box::new(self.expression(expression, schema)?),
                *negated,
                Box::new(self.expression(low, schema)?),
                Box::new(self.expression(high, schema)?),
            )),
            model::ExprKind::Like {
                expression,
                pattern,
                negated,
                escape_char,
                case_insensitive,
            } => datafusion_expr::Expr::Like(expr::Like::new(
                *negated,
                Box::new(self.expression(expression, schema)?),
                Box::new(self.expression(pattern, schema)?),
                *escape_char,
                *case_insensitive,
            )),
            model::ExprKind::SimilarTo {
                expression,
                pattern,
                negated,
                escape_char,
                case_insensitive,
            } => datafusion_expr::Expr::SimilarTo(expr::Like::new(
                *negated,
                Box::new(self.expression(expression, schema)?),
                Box::new(self.expression(pattern, schema)?),
                *escape_char,
                *case_insensitive,
            )),
            model::ExprKind::InList {
                expression,
                values,
                negated,
            } => {
                let expression = Box::new(self.expression(expression, schema)?);
                let values = values
                    .iter()
                    .map(|value| self.expression(value, schema))
                    .collect::<PlanResult<Vec<_>>>()?;
                datafusion_expr::Expr::InList(InList::new(expression, values, *negated))
            }
            model::ExprKind::ScalarSubquery(query) => {
                datafusion_expr::Expr::ScalarSubquery(self.subquery(query, schema)?)
            }
            model::ExprKind::Exists { query, negated } => {
                datafusion_expr::Expr::Exists(Exists::new(self.subquery(query, schema)?, *negated))
            }
            model::ExprKind::InSubquery {
                expression,
                query,
                negated,
            } => {
                let expression = Box::new(self.expression(expression, schema)?);
                datafusion_expr::Expr::InSubquery(InSubquery::new(
                    expression,
                    self.subquery(query, schema)?,
                    *negated,
                ))
            }
        };
        let mut actual = model::ExprType::from_field(
            lowered
                .to_field(backend_schema(schema)?.as_ref())?
                .1
                .as_ref(),
        );
        // Use resolved child nullability rather than recursively re-deriving it
        // from backend expressions. DataFusion's scalar-subquery and null-safe
        // comparison rules differ from SQL's semantic result fields.
        match expression.kind.resolved("expression node")? {
            model::ExprKind::ScalarSubquery(_) => actual.nullable = true,
            model::ExprKind::WithMetadata(child)
            | model::ExprKind::Not(child)
            | model::ExprKind::Negative(child) => {
                actual.nullable = child.result_type.resolved("child type")?.nullable;
            }
            model::ExprKind::Binary {
                left,
                operator,
                right,
            } => {
                actual.nullable = !matches!(
                    operator,
                    datafusion_expr::Operator::IsDistinctFrom
                        | datafusion_expr::Operator::IsNotDistinctFrom
                ) && (left.result_type.resolved("left type")?.nullable
                    || right.result_type.resolved("right type")?.nullable);
            }
            model::ExprKind::ScalarFunction {
                function,
                arguments,
            } => {
                let fields = crate::analyzer::typing::argument_fields(arguments)?;
                let literals = arguments
                    .iter()
                    .map(|arg| match &arg.kind {
                        crate::analyzer::deferred::Deferred::Resolved(
                            model::ExprKind::Literal(value),
                        ) => Some(value),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                actual.nullable = function
                    .return_field_from_args(datafusion_expr::ReturnFieldArgs {
                        arg_fields: &fields,
                        scalar_arguments: &literals,
                    })?
                    .is_nullable();
            }
            _ => {}
        }
        if &actual != expected {
            return Err(PlanError::analysis(format!(
                "stale type for {:?}: expected {expected:?}, got {actual:?}",
                expression.id
            )));
        }
        Ok(lowered)
    }
}

fn rename_outputs(plan: LogicalPlan, schema: &model::QuerySchema) -> PlanResult<LogicalPlan> {
    let columns = plan.schema().columns();
    if columns.len() != schema.attributes.len() {
        return Err(PlanError::analysis(
            "source column count does not match resolved schema",
        ));
    }
    let expressions = columns
        .into_iter()
        .zip(&schema.attributes)
        .map(|(column, attribute)| {
            datafusion_expr::Expr::Column(column).alias(backend_name(attribute.id))
        })
        .collect::<Vec<_>>();
    Ok(LogicalPlanBuilder::new(plan)
        .project(expressions)?
        .build()?)
}

/// Evaluate an already analyzed constant at an analysis-time execution boundary.
/// No name resolution or coercion occurs here.
pub fn evaluate_constant(expression: &model::Expr) -> PlanResult<datafusion_common::ScalarValue> {
    let expression =
        LoweringContext::default().expression(expression, &model::QuerySchema::default())?;
    use datafusion_common::tree_node::{Transformed, TreeNode};
    let evaluator = sail_common_datafusion::literal::LiteralEvaluator::new();
    // Some constant kernels return one-element arrays. Materialize their
    // children as scalars before invoking literal-sensitive parent functions.
    let expression = expression
        .transform_up(|expression| {
            Ok(match evaluator.evaluate(&expression) {
                Ok(value) => Transformed::yes(datafusion_expr::Expr::Literal(value, None)),
                Err(_) => Transformed::no(expression),
            })
        })?
        .data;
    Ok(evaluator.evaluate(&expression)?)
}

/// Expose display names exactly once for a sink or another external consumer.
/// Internal bindings remain attribute IDs throughout the entire query tree.
pub(crate) fn external_plan(input: model::Query) -> PlanResult<LogicalPlan> {
    let named = crate::analyzer::lowering::lower_query(input)?;
    let names = named.fields.unwrap_or_default();
    let expressions = named
        .plan
        .schema()
        .columns()
        .into_iter()
        .zip(names)
        .map(|(column, name)| datafusion_expr::Expr::Column(column).alias(name))
        .collect::<Vec<_>>();
    Ok(LogicalPlanBuilder::new(named.plan)
        .project(expressions)?
        .build()?)
}

/// Bind a checked expression to the external schema used by a data source.
pub(crate) fn external_expression(
    expression: &model::Expr,
    schema: &model::QuerySchema,
) -> PlanResult<datafusion_expr::Expr> {
    use datafusion_common::tree_node::{Transformed, TreeNode};
    let expression = LoweringContext::default().expression(expression, schema)?;
    let names = schema
        .attributes
        .iter()
        .map(|a| (backend_name(a.id), a.name.clone()))
        .collect::<HashMap<_, _>>();
    Ok(expression
        .transform_up(|expression| {
            Ok(match expression {
                datafusion_expr::Expr::Column(column)
                    if column.relation.is_none() && names.contains_key(&column.name) =>
                {
                    let name = names.get(&column.name).ok_or_else(|| {
                        datafusion_common::internal_datafusion_err!("missing external attribute")
                    })?;
                    Transformed::yes(datafusion_expr::Expr::Column(Column::from_name(name)))
                }
                expression => Transformed::no(expression),
            })
        })?
        .data)
}

pub(crate) fn checked_expression(
    expression: &model::Expr,
    schema: &model::QuerySchema,
) -> PlanResult<datafusion_expr::Expr> {
    LoweringContext::default().expression(expression, schema)
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;
    use datafusion_common::ScalarValue;
    use sail_function::scalar::array::spark_array::SparkArray;

    use super::*;
    use crate::analyzer::deferred::Deferred;
    use crate::analyzer::typing::resolve_scalar_call;

    fn attribute(
        id: model::AttributeId,
        name: &str,
        result_type: model::ExprType,
    ) -> model::Attribute {
        model::Attribute {
            id,
            origin: id,
            name: name.into(),
            qualifier: vec![],
            qualified_position: 0,
            plan_ids: vec![],
            hidden: false,
            result_type: Deferred::Resolved(result_type),
        }
    }

    fn project(expression: model::Expr, output: model::Attribute) -> model::Query {
        model::Query {
            kind: Deferred::Resolved(model::QueryKind::Project {
                input: Box::new(model::Query {
                    kind: Deferred::Resolved(model::QueryKind::Empty {
                        produce_one_row: true,
                    }),
                    schema: Deferred::Resolved(model::QuerySchema::default()),
                }),
                expressions: vec![model::Projection {
                    expression,
                    attribute: output.clone(),
                }],
            }),
            schema: Deferred::Resolved(model::QuerySchema {
                attributes: vec![output],
            }),
        }
    }

    #[test]
    fn scalar_arguments_are_normalized_before_lowering() -> PlanResult<()> {
        let mut ids = model::IdGenerator::default();
        let arguments = vec![
            model::Expr::literal(ids.expression(), ScalarValue::Int32(Some(1))),
            model::Expr::literal(ids.expression(), ScalarValue::Int64(Some(2))),
        ];
        let expression = resolve_scalar_call(
            ids.expression(),
            Arc::new(SparkArray::new().into()),
            arguments,
            &mut ids,
        )?;
        let model::ExprKind::ScalarFunction { arguments, .. } =
            expression.kind.resolved("scalar call")?
        else {
            return Err(PlanError::internal("expected scalar function"));
        };
        assert!(matches!(
            arguments[0].kind.resolved("typed literal")?,
            model::ExprKind::Literal(ScalarValue::Int64(Some(1)))
        ));
        let output = attribute(
            ids.attribute(),
            "items",
            expression.result_type.resolved("result")?.clone(),
        );
        let lowered = lower_query(project(expression, output))?;
        assert_eq!(lowered.fields, Some(vec!["items".into()]));
        assert!(!lowered.plan.schema().field(0).is_nullable());
        assert!(
            matches!(lowered.plan.schema().field(0).data_type(), DataType::List(field) if field.data_type() == &DataType::Int64 && !field.is_nullable())
        );
        Ok(())
    }

    #[test]
    fn unresolved_and_stale_types_are_rejected() -> PlanResult<()> {
        let expression = model::Expr::literal(model::ExprId(1), ScalarValue::Int32(Some(1)));
        let output = attribute(
            model::AttributeId(1),
            "x",
            model::ExprType::new(DataType::Int64, false),
        );
        assert!(lower_query(project(expression.clone(), output)).is_err());
        let mut unresolved = expression;
        unresolved.result_type = Deferred::unresolved(model::Infer);
        let output = attribute(
            model::AttributeId(1),
            "x",
            model::ExprType::new(DataType::Int32, false),
        );
        assert!(lower_query(project(unresolved, output)).is_err());
        Ok(())
    }

    #[test]
    fn duplicate_display_names_do_not_collide() -> PlanResult<()> {
        let schema = model::QuerySchema {
            attributes: vec![
                attribute(
                    model::AttributeId(1),
                    "value",
                    model::ExprType::new(DataType::Int32, false),
                ),
                attribute(
                    model::AttributeId(2),
                    "value",
                    model::ExprType::new(DataType::Int32, false),
                ),
            ],
        };
        let query = model::Query {
            kind: Deferred::Resolved(model::QueryKind::Values {
                rows: vec![vec![
                    model::Expr::literal(model::ExprId(1), ScalarValue::Int32(Some(1))),
                    model::Expr::literal(model::ExprId(2), ScalarValue::Int32(Some(2))),
                ]],
            }),
            schema: Deferred::Resolved(schema),
        };
        let lowered = lower_query(query)?;
        assert_eq!(lowered.fields, Some(vec!["value".into(), "value".into()]));
        assert_ne!(
            lowered.plan.schema().field(0).name(),
            lowered.plan.schema().field(1).name()
        );
        Ok(())
    }

    #[test]
    fn unresolved_subquery_does_not_escape_validation() -> PlanResult<()> {
        let expression = model::Expr {
            id: model::ExprId(1),
            kind: Deferred::Resolved(model::ExprKind::Exists {
                query: Box::new(model::Query::unresolved(sail_common::spec::QueryPlan::new(
                    sail_common::spec::QueryNode::Empty {
                        produce_one_row: true,
                    },
                ))),
                negated: false,
            }),
            result_type: Deferred::Resolved(model::ExprType::new(DataType::Boolean, false)),
        };
        let output = attribute(
            model::AttributeId(1),
            "exists",
            model::ExprType::new(DataType::Boolean, false),
        );
        assert!(lower_query(project(expression, output)).is_err());
        Ok(())
    }
}
