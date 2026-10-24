//! Checked, one-way lowering. This module neither binds names nor inserts
//! implicit casts. Attribute names are assigned exactly once at this boundary.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion_common::{Column, DFSchema, DFSchemaRef};
use datafusion_expr::expr::{
    AggregateFunction, AggregateFunctionParams, Case, Exists, InList, InSubquery,
};
use datafusion_expr::logical_plan::InvariantLevel;
use datafusion_expr::{
    EmptyRelation, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Subquery, Union, Values,
    expr,
};

use super::model::*;
use crate::analysis::NamedPlan;
use crate::error::{PlanError, PlanResult};

pub fn lower_query(query: QueryNode) -> PlanResult<NamedPlan> {
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
            .map(|a| Expr::Column(Column::from_name(backend_name(a.id))))
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
    expression_ids: HashSet<ExprId>,
    outer: Vec<QuerySchema>,
    lambdas: Vec<QuerySchema>,
}

pub(super) fn backend_name(id: AttributeId) -> String {
    format!("#{}", id.0)
}

fn backend_schema(schema: &QuerySchema) -> PlanResult<DFSchemaRef> {
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
    fn plan(&mut self, plan: &QueryNode) -> PlanResult<LogicalPlan> {
        let output = plan.schema.resolved("plan schema")?;
        let expected_schema = backend_schema(output)?;
        let lowered = match plan.kind.resolved("plan node")? {
            QueryKind::Scope { input } => self.plan(input)?,
            QueryKind::Empty { produce_one_row } => LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: *produce_one_row,
                schema: Arc::new(DFSchema::empty()),
            }),
            QueryKind::Range {
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
            QueryKind::RowIdentity { input, attribute, kind } => {
                let input = Arc::new(self.plan(input)?);
                let name = backend_name(attribute.id);
                let node: Arc<dyn datafusion_expr::UserDefinedLogicalNode> = match kind {
                    RowIdentityKind::Monotonic => Arc::new(sail_logical_plan::monotonic_id::MonotonicIdNode::try_new(input, name)?),
                    RowIdentityKind::Partition => Arc::new(sail_logical_plan::spark_partition_id::SparkPartitionIdNode::try_new(input, name)?),
                };
                LogicalPlan::Extension(datafusion_expr::Extension { node })
            }
            QueryKind::MapPartitions { input, function } => LogicalPlan::Extension(datafusion_expr::Extension {
                node: Arc::new(sail_logical_plan::map_partitions::MapPartitionsNode::try_new(
                    Arc::new(self.plan(input)?), output.attributes.iter().map(|a| backend_name(a.id)).collect(), vec![None; output.attributes.len()], Arc::clone(function),
                )?),
            }),
            QueryKind::RemoteCheckpoint { relation_id } => LogicalPlan::Extension(datafusion_expr::Extension {
                node: Arc::new(sail_logical_plan::remote_checkpoint::RemoteCheckpointRelationNode::try_new(
                    relation_id.clone(), Arc::new(expected_schema.as_arrow().clone()),
                )?),
            }),
            QueryKind::Unnest { input, column, outer } => {
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
            QueryKind::DistinctOn { input, keys } => {
                let lowered = self.plan(input)?;
                let keys = keys.iter().map(|e| self.expression(e, input.schema.resolved("distinct input")?)).collect::<PlanResult<Vec<_>>>()?;
                let select = lowered.schema().columns().into_iter().map(Expr::Column).collect();
                LogicalPlan::Distinct(datafusion_expr::Distinct::On(datafusion_expr::DistinctOn::try_new(keys, select, None, Arc::new(lowered))?))
            }
            QueryKind::Repartition {
                input,
                partitioning,
            } => {
                let lowered = self.plan(input)?;
                let schema = input.schema.resolved("repartition input")?;
                use sail_logical_plan::repartition::{
                    ExplicitRepartitionKind as Kind, ExplicitRepartitionNode,
                };
                let (kind, count, expressions) = match partitioning {
                    Partitioning::RoundRobin(count) => (Kind::RoundRobin, Some(*count), vec![]),
                    Partitioning::Coalesce(count) => (Kind::Coalesce, Some(*count), vec![]),
                    Partitioning::Hash(expressions, count) => (
                        Kind::Hash,
                        Some(*count),
                        expressions
                            .iter()
                            .map(|expression| self.expression(expression, schema))
                            .collect::<PlanResult<_>>()?,
                    ),
                    Partitioning::Distribute(expressions) => (
                        Kind::Hash,
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

            QueryKind::Values { rows } => {
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
                                self.expression(value, &QuerySchema::default())
                            })
                            .collect::<PlanResult<Vec<_>>>()
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                LogicalPlan::Values(Values {
                    schema: Arc::clone(&expected_schema),
                    values,
                })
            }
            QueryKind::Scan {
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
            QueryKind::Project { input, expressions } => {
                let lowered_input = self.plan(input)?;
                let schema = input.schema.resolved("projection input")?;
                let expressions = self.projections(expressions, schema)?;
                LogicalPlan::Projection(datafusion_expr::Projection::try_new_with_schema(
                    expressions,
                    Arc::new(lowered_input),
                    Arc::clone(&expected_schema),
                )?)
            }
            QueryKind::Filter { input, condition } => {
                let lowered_input = self.plan(input)?;
                let predicate =
                    self.expression(condition, input.schema.resolved("filter input")?)?;
                LogicalPlanBuilder::new(lowered_input)
                    .filter(predicate)?
                    .build()?
            }
            QueryKind::Aggregate { input, grouping, grouping_sets, grouping_id, aggregates } => {
                let lowered_input = self.plan(input)?;
                let schema = input.schema.resolved("aggregate input")?;
                let aggregates = self.projections(aggregates, schema)?;
                if let Some(sets) = grouping_sets {
                    if grouping_id.is_none() { return Err(PlanError::analysis("grouping sets require an ID attribute")); }
                    let grouping = grouping.iter().map(|p| Ok(self.expression(&p.expression, schema)?.alias(backend_name(p.attribute.id)))).collect::<PlanResult<Vec<_>>>()?;
                    let sets = sets.iter().map(|set| set.iter().map(|i| grouping.get(*i).cloned().ok_or_else(|| PlanError::analysis("invalid grouping set index"))).collect::<PlanResult<Vec<_>>>()).collect::<PlanResult<Vec<_>>>()?;
                    let aggregate = LogicalPlan::Aggregate(datafusion_expr::Aggregate::try_new(Arc::new(lowered_input), vec![Expr::GroupingSet(expr::GroupingSet::GroupingSets(sets))], aggregates)?);
                    rename_outputs(aggregate, output)?
                } else {
                    if grouping_id.is_some() { return Err(PlanError::analysis("grouping ID requires grouping sets")); }
                    let grouping = self.projections(grouping, schema)?;
                    LogicalPlan::Aggregate(datafusion_expr::Aggregate::try_new_with_schema(Arc::new(lowered_input), grouping, aggregates, Arc::clone(&expected_schema))?)
                }
            }
            QueryKind::Sort {
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
            QueryKind::Window { input, expressions } => {
                let lowered_input = self.plan(input)?;
                let schema = input.schema.resolved("window input")?;
                let expressions = self.projections(expressions, schema)?;
                LogicalPlan::Window(datafusion_expr::Window::try_new_with_schema(
                    expressions,
                    Arc::new(lowered_input),
                    Arc::clone(&expected_schema),
                )?)
            }
            QueryKind::Limit { input, skip, fetch } => {
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
            QueryKind::Join {
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
                let schema = QuerySchema {
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
            QueryKind::Union { inputs } => {
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
                            .map(|(column, attribute)| Expr::Column(column).alias(backend_name(attribute.id))).collect::<Vec<_>>();
                        let input = LogicalPlanBuilder::new(input).project(expressions)?.build()?;
                        Ok(Arc::new(input))
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                LogicalPlan::Union(Union {
                    inputs,
                    schema: Arc::clone(&expected_schema),
                })
            }
            QueryKind::Distinct { input } => LogicalPlanBuilder::new(self.plan(input)?)
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
        projections: &[Projection],
        schema: &QuerySchema,
    ) -> PlanResult<Vec<Expr>> {
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

    fn sorts(&mut self, order: &[SortKey], schema: &QuerySchema) -> PlanResult<Vec<expr::Sort>> {
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

    fn subquery(&mut self, query: &QueryNode, schema: &QuerySchema) -> PlanResult<Subquery> {
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

    fn expression(&mut self, expression: &ExprNode, schema: &QuerySchema) -> PlanResult<Expr> {
        if !self.expression_ids.insert(expression.id) {
            return Err(PlanError::analysis(format!(
                "duplicate expression identity: {:?}",
                expression.id
            )));
        }
        let expected = expression.result_type.resolved("expression type")?;
        let lowered = match expression.kind.resolved("expression node")? {
            ExprKind::Literal(value) => Expr::Literal(
                value.clone(),
                (!expected.metadata.is_empty()).then(|| {
                    datafusion_common::metadata::FieldMetadata::from(expected.metadata.clone())
                }),
            ),
            ExprKind::WithMetadata(inner) => {
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
                    Expr::Cast(expr::Cast::new_from_field(
                        Box::new(value),
                        expected.to_field(format!("__sail_expression_{}", expression.id.0)),
                    ))
                }
            }
            ExprKind::TruthTest {
                expression,
                value,
                negated,
            } => {
                let expression = Box::new(self.expression(expression, schema)?);
                match (value, negated) {
                    (Some(true), false) => Expr::IsTrue(expression),
                    (Some(true), true) => Expr::IsNotTrue(expression),
                    (Some(false), false) => Expr::IsFalse(expression),
                    (Some(false), true) => Expr::IsNotFalse(expression),
                    (None, false) => Expr::IsUnknown(expression),
                    (None, true) => Expr::IsNotUnknown(expression),
                }
            }
            ExprKind::Attribute(id) => {
                schema.attribute(*id)?;
                Expr::Column(Column::from_name(backend_name(*id)))
            }
            ExprKind::OuterAttribute { depth, attribute } => {
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
                Expr::OuterReferenceColumn(field, Column::from_name(backend_name(attribute.id)))
            }
            ExprKind::ScalarFunction {
                function,
                arguments,
            } => {
                let arguments = arguments
                    .iter()
                    .map(|argument| self.expression(argument, schema))
                    .collect::<PlanResult<Vec<_>>>()?;
                function.call(arguments)
            }
            ExprKind::AggregateFunction {
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
                Expr::AggregateFunction(AggregateFunction {
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
            ExprKind::Binary {
                left,
                operator,
                right,
            } => Expr::BinaryExpr(expr::BinaryExpr::new(
                Box::new(self.expression(left, schema)?),
                *operator,
                Box::new(self.expression(right, schema)?),
            )),
            ExprKind::WindowFunction {
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
                Expr::WindowFunction(Box::new(expr::WindowFunction {
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
            ExprKind::HigherOrderFunction {
                function,
                arguments,
            } => {
                let args = arguments
                    .iter()
                    .map(|argument| match argument {
                        FunctionArg::Value(value) => self.expression(value, schema),
                        FunctionArg::Lambda {
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
                            Ok(Expr::Lambda(expr::Lambda::new(params, result?)))
                        }
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                Expr::HigherOrderFunction(expr::HigherOrderFunction::new(
                    Arc::clone(function),
                    args,
                ))
            }
            ExprKind::LambdaVariable(id) => {
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
                Expr::LambdaVariable(expr::LambdaVariable::new(backend_name(*id), Some(field)))
            }
            ExprKind::Cast {
                expression,
                target,
                safe,
            } => {
                let value = Box::new(self.expression(expression, schema)?);
                let field = target.to_field("");
                if *safe {
                    Expr::TryCast(expr::TryCast { expr: value, field })
                } else {
                    Expr::Cast(expr::Cast { expr: value, field })
                }
            }
            ExprKind::Not(value) => Expr::Not(Box::new(self.expression(value, schema)?)),
            ExprKind::IsNull(value) => Expr::IsNull(Box::new(self.expression(value, schema)?)),
            ExprKind::IsNotNull(value) => {
                Expr::IsNotNull(Box::new(self.expression(value, schema)?))
            }
            ExprKind::Negative(value) => Expr::Negative(Box::new(self.expression(value, schema)?)),
            ExprKind::Case {
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
                Expr::Case(Case::new(operand, branches, otherwise))
            }
            ExprKind::Between {
                expression,
                low,
                high,
                negated,
            } => Expr::Between(expr::Between::new(
                Box::new(self.expression(expression, schema)?),
                *negated,
                Box::new(self.expression(low, schema)?),
                Box::new(self.expression(high, schema)?),
            )),
            ExprKind::Like {
                expression,
                pattern,
                negated,
                escape_char,
                case_insensitive,
            } => Expr::Like(expr::Like::new(
                *negated,
                Box::new(self.expression(expression, schema)?),
                Box::new(self.expression(pattern, schema)?),
                *escape_char,
                *case_insensitive,
            )),
            ExprKind::SimilarTo {
                expression,
                pattern,
                negated,
                escape_char,
                case_insensitive,
            } => Expr::SimilarTo(expr::Like::new(
                *negated,
                Box::new(self.expression(expression, schema)?),
                Box::new(self.expression(pattern, schema)?),
                *escape_char,
                *case_insensitive,
            )),
            ExprKind::InList {
                expression,
                values,
                negated,
            } => {
                let expression = Box::new(self.expression(expression, schema)?);
                let values = values
                    .iter()
                    .map(|value| self.expression(value, schema))
                    .collect::<PlanResult<Vec<_>>>()?;
                Expr::InList(InList::new(expression, values, *negated))
            }
            ExprKind::ScalarSubquery(query) => Expr::ScalarSubquery(self.subquery(query, schema)?),
            ExprKind::Exists { query, negated } => {
                Expr::Exists(Exists::new(self.subquery(query, schema)?, *negated))
            }
            ExprKind::InSubquery {
                expression,
                query,
                negated,
            } => {
                let expression = Box::new(self.expression(expression, schema)?);
                Expr::InSubquery(InSubquery::new(
                    expression,
                    self.subquery(query, schema)?,
                    *negated,
                ))
            }
        };
        let mut actual = ExprType::from_field(
            lowered
                .to_field(backend_schema(schema)?.as_ref())?
                .1
                .as_ref(),
        );
        // Use resolved child nullability rather than recursively re-deriving it
        // from backend expressions. DataFusion's scalar-subquery and null-safe
        // comparison rules differ from SQL's semantic result fields.
        match expression.kind.resolved("expression node")? {
            ExprKind::ScalarSubquery(_) => actual.nullable = true,
            ExprKind::WithMetadata(child) | ExprKind::Not(child) | ExprKind::Negative(child) => {
                actual.nullable = child.result_type.resolved("child type")?.nullable;
            }
            ExprKind::Binary {
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
            ExprKind::ScalarFunction {
                function,
                arguments,
            } => {
                let fields = crate::analysis::typing::argument_fields(arguments)?;
                let literals = arguments
                    .iter()
                    .map(|arg| match &arg.kind {
                        super::deferred::Deferred::Resolved(ExprKind::Literal(value)) => {
                            Some(value)
                        }
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

fn rename_outputs(plan: LogicalPlan, schema: &QuerySchema) -> PlanResult<LogicalPlan> {
    let columns = plan.schema().columns();
    if columns.len() != schema.attributes.len() {
        return Err(PlanError::analysis(
            "source column count does not match resolved schema",
        ));
    }
    let expressions = columns
        .into_iter()
        .zip(&schema.attributes)
        .map(|(column, attribute)| Expr::Column(column).alias(backend_name(attribute.id)))
        .collect::<Vec<_>>();
    Ok(LogicalPlanBuilder::new(plan)
        .project(expressions)?
        .build()?)
}

/// Evaluate an already analyzed constant at an analysis-time execution boundary.
/// No name resolution or coercion occurs here.
pub fn evaluate_constant(expression: &ExprNode) -> PlanResult<datafusion_common::ScalarValue> {
    let expression = LoweringContext::default().expression(expression, &QuerySchema::default())?;
    use datafusion_common::tree_node::{Transformed, TreeNode};
    let evaluator = sail_common_datafusion::literal::LiteralEvaluator::new();
    // Some constant kernels return one-element arrays. Materialize their
    // children as scalars before invoking literal-sensitive parent functions.
    let expression = expression
        .transform_up(|expression| {
            Ok(match evaluator.evaluate(&expression) {
                Ok(value) => Transformed::yes(Expr::Literal(value, None)),
                Err(_) => Transformed::no(expression),
            })
        })?
        .data;
    Ok(evaluator.evaluate(&expression)?)
}

/// Expose display names exactly once for a sink or another external consumer.
/// Internal bindings remain attribute IDs throughout the entire query tree.
pub(super) fn external_plan(input: QueryNode) -> PlanResult<LogicalPlan> {
    let named = crate::analysis::lowering::lower_query(input)?;
    let names = named.fields.unwrap_or_default();
    let expressions = named
        .plan
        .schema()
        .columns()
        .into_iter()
        .zip(names)
        .map(|(column, name)| Expr::Column(column).alias(name))
        .collect::<Vec<_>>();
    Ok(LogicalPlanBuilder::new(named.plan)
        .project(expressions)?
        .build()?)
}

/// Bind a checked expression to the external schema used by a data source.
pub(super) fn external_expression(expression: &ExprNode, schema: &QuerySchema) -> PlanResult<Expr> {
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
                Expr::Column(column)
                    if column.relation.is_none() && names.contains_key(&column.name) =>
                {
                    let name = names.get(&column.name).ok_or_else(|| {
                        datafusion_common::internal_datafusion_err!("missing external attribute")
                    })?;
                    Transformed::yes(Expr::Column(Column::from_name(name)))
                }
                expression => Transformed::no(expression),
            })
        })?
        .data)
}

pub(super) fn checked_expression(expression: &ExprNode, schema: &QuerySchema) -> PlanResult<Expr> {
    LoweringContext::default().expression(expression, schema)
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;
    use datafusion_common::ScalarValue;
    use sail_function::scalar::array::spark_array::SparkArray;

    use super::*;
    use crate::analysis::deferred::Deferred;
    use crate::analysis::typing::resolve_scalar_call;

    fn attribute(id: AttributeId, name: &str, result_type: ExprType) -> Attribute {
        Attribute {
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

    fn project(expression: ExprNode, output: Attribute) -> QueryNode {
        QueryNode {
            kind: Deferred::Resolved(QueryKind::Project {
                input: Box::new(QueryNode {
                    kind: Deferred::Resolved(QueryKind::Empty {
                        produce_one_row: true,
                    }),
                    schema: Deferred::Resolved(QuerySchema::default()),
                }),
                expressions: vec![Projection {
                    expression,
                    attribute: output.clone(),
                }],
            }),
            schema: Deferred::Resolved(QuerySchema {
                attributes: vec![output],
            }),
        }
    }

    #[test]
    fn scalar_arguments_are_normalized_before_lowering() -> PlanResult<()> {
        let mut ids = IdGenerator::default();
        let arguments = vec![
            ExprNode::literal(ids.expression(), ScalarValue::Int32(Some(1))),
            ExprNode::literal(ids.expression(), ScalarValue::Int64(Some(2))),
        ];
        let expression = resolve_scalar_call(
            ids.expression(),
            Arc::new(SparkArray::new().into()),
            arguments,
            &mut ids,
        )?;
        let ExprKind::ScalarFunction { arguments, .. } = expression.kind.resolved("scalar call")?
        else {
            return Err(PlanError::internal("expected scalar function"));
        };
        assert!(matches!(
            arguments[0].kind.resolved("typed literal")?,
            ExprKind::Literal(ScalarValue::Int64(Some(1)))
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
        let expression = ExprNode::literal(ExprId(1), ScalarValue::Int32(Some(1)));
        let output = attribute(AttributeId(1), "x", ExprType::new(DataType::Int64, false));
        assert!(lower_query(project(expression.clone(), output)).is_err());
        let mut unresolved = expression;
        unresolved.result_type = Deferred::unresolved(Infer);
        let output = attribute(AttributeId(1), "x", ExprType::new(DataType::Int32, false));
        assert!(lower_query(project(unresolved, output)).is_err());
        Ok(())
    }

    #[test]
    fn duplicate_display_names_do_not_collide() -> PlanResult<()> {
        let schema = QuerySchema {
            attributes: vec![
                attribute(
                    AttributeId(1),
                    "value",
                    ExprType::new(DataType::Int32, false),
                ),
                attribute(
                    AttributeId(2),
                    "value",
                    ExprType::new(DataType::Int32, false),
                ),
            ],
        };
        let query = QueryNode {
            kind: Deferred::Resolved(QueryKind::Values {
                rows: vec![vec![
                    ExprNode::literal(ExprId(1), ScalarValue::Int32(Some(1))),
                    ExprNode::literal(ExprId(2), ScalarValue::Int32(Some(2))),
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
        let expression = ExprNode {
            id: ExprId(1),
            kind: Deferred::Resolved(ExprKind::Exists {
                query: Box::new(QueryNode::unresolved(sail_common::spec::QueryPlan::new(
                    sail_common::spec::QueryNode::Empty {
                        produce_one_row: true,
                    },
                ))),
                negated: false,
            }),
            result_type: Deferred::Resolved(ExprType::new(DataType::Boolean, false)),
        };
        let output = attribute(
            AttributeId(1),
            "exists",
            ExprType::new(DataType::Boolean, false),
        );
        assert!(lower_query(project(expression, output)).is_err());
        Ok(())
    }
}
