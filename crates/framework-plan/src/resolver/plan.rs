use std::collections::HashMap;
use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion::arrow::datatypes as adt;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::function::TableFunction;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::datasource::{provider_as_source, MemTable, TableProvider};
use datafusion::execution::context::DataFilePaths;
use datafusion::logical_expr::{
    logical_plan as plan, Aggregate, Expr, Extension, LogicalPlan, UNNAMED_TABLE,
};
use datafusion_common::tree_node::{TreeNode, TreeNodeRewriter};
use datafusion_common::{
    Column, DFSchema, DFSchemaRef, ParamValues, ScalarValue, SchemaReference, TableReference,
};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_rewriter::normalize_col;
use datafusion_expr::utils::{columnize_expr, expand_qualified_wildcard, expand_wildcard};
use datafusion_expr::{build_join_schema, LogicalPlanBuilder};
use framework_common::spec;
use framework_common::utils::{cast_record_batch, read_record_batches, rename_logical_plan};

use crate::error::{PlanError, PlanResult};
use crate::extension::function::multi_expr::MultiExpr;
use crate::extension::logical::{
    CatalogCommand, CatalogCommandNode, RangeNode, ShowStringFormat, ShowStringNode,
    ShowStringStyle, SortWithinPartitionsNode,
};
use crate::extension::source::rename::RenameTableProvider;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::explode::ExplodeRewriter;
use crate::resolver::tree::window::WindowRewriter;
use crate::resolver::tree::PlanRewriter;
use crate::resolver::PlanResolver;
use crate::utils::ItemTaker;

pub(crate) fn build_schema_reference(name: spec::ObjectName) -> PlanResult<SchemaReference> {
    let names: Vec<String> = name.into();
    match names.as_slice() {
        [a] => Ok(SchemaReference::Bare {
            schema: Arc::from(a.as_str()),
        }),
        [a, b] => Ok(SchemaReference::Full {
            catalog: Arc::from(a.as_str()),
            schema: Arc::from(b.as_str()),
        }),
        _ => Err(PlanError::invalid(format!("schema reference: {:?}", names))),
    }
}

fn build_table_reference(name: spec::ObjectName) -> PlanResult<TableReference> {
    let names: Vec<String> = name.into();
    match names.as_slice() {
        [a] => Ok(TableReference::Bare {
            table: Arc::from(a.as_str()),
        }),
        [a, b] => Ok(TableReference::Partial {
            schema: Arc::from(a.as_str()),
            table: Arc::from(b.as_str()),
        }),
        [a, b, c] => Ok(TableReference::Full {
            catalog: Arc::from(a.as_str()),
            schema: Arc::from(b.as_str()),
            table: Arc::from(c.as_str()),
        }),
        _ => Err(PlanError::invalid(format!("table reference: {:?}", names))),
    }
}

pub struct NamedPlan {
    pub plan: LogicalPlan,
    /// The user-facing fields for query plan,
    /// or `None` for a non-query plan (e.g. a DDL statement).
    pub fields: Option<Vec<String>>,
}

impl PlanResolver<'_> {
    #[async_recursion]
    pub(super) async fn resolve_plan(
        &self,
        plan: spec::Plan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::PlanNode;

        let plan_id = plan.plan_id;
        let plan = match plan.node {
            PlanNode::Read {
                read_type,
                is_streaming: _,
            } => {
                use spec::ReadType;

                match read_type {
                    ReadType::NamedTable {
                        identifier,
                        options,
                    } => {
                        if !options.is_empty() {
                            return Err(PlanError::todo("table options"));
                        }
                        let table_reference = build_table_reference(identifier)?;
                        let df: DataFrame = self.ctx.table(table_reference).await?;
                        let plan = df.into_optimized_plan()?;
                        let names = state.register_fields(plan.schema().inner());
                        rename_logical_plan(plan, &names)?
                    }
                    ReadType::Udtf {
                        identifier,
                        arguments,
                        options,
                    } => {
                        if !options.is_empty() {
                            return Err(PlanError::todo("ReadType::UDTF options"));
                        }
                        // TODO: Handle qualified table reference.
                        let function_name = build_table_reference(identifier)?;
                        let function_name = function_name.table();
                        let schema = DFSchema::empty();
                        let (_, arguments) =
                            self.resolve_alias_expressions_and_names(arguments, &schema, state)?;
                        let table_function = self.ctx.table_function(function_name)?;
                        let table_provider = table_function.create_table_provider(&arguments)?;
                        let names = state.register_fields(&table_provider.schema());
                        let table_provider = RenameTableProvider::try_new(table_provider, names)?;
                        LogicalPlan::TableScan(plan::TableScan::try_new(
                            function_name,
                            provider_as_source(Arc::new(table_provider)),
                            None,
                            vec![],
                            None,
                        )?)
                    }
                    ReadType::DataSource {
                        format,
                        schema: _,
                        options: _,
                        paths,
                        predicates: _,
                    } => {
                        let urls = paths.to_urls()?;
                        if urls.is_empty() {
                            return Err(PlanError::invalid("empty data source paths"));
                        }
                        let (format, extension): (Arc<dyn FileFormat>, _) = match format.as_deref()
                        {
                            Some("json") => (Arc::new(JsonFormat::default()), ".json"),
                            Some("csv") => (Arc::new(CsvFormat::default()), ".csv"),
                            Some("parquet") => (Arc::new(ParquetFormat::new()), ".parquet"),
                            other => {
                                return Err(PlanError::unsupported(format!(
                                    "unsupported data source format: {:?}",
                                    other
                                )))
                            }
                        };
                        let options = ListingOptions::new(format).with_file_extension(extension);
                        // TODO: use provided schema if available
                        let schema = options.infer_schema(&self.ctx.state(), &urls[0]).await?;
                        let config = ListingTableConfig::new_with_multi_paths(urls)
                            .with_listing_options(options)
                            .with_schema(schema);
                        let provider = Arc::new(ListingTable::try_new(config)?);
                        let names = state.register_fields(&provider.schema());
                        let provider = RenameTableProvider::try_new(provider, names)?;
                        LogicalPlan::TableScan(plan::TableScan::try_new(
                            UNNAMED_TABLE,
                            provider_as_source(Arc::new(provider)),
                            None,
                            vec![],
                            None,
                        )?)
                    }
                }
            }
            PlanNode::Project { input, expressions } => {
                let input = match input {
                    Some(x) => self.resolve_plan(*x, state).await?,
                    None => LogicalPlan::EmptyRelation(plan::EmptyRelation {
                        // allows literal projection with no input
                        produce_one_row: true,
                        schema: DFSchemaRef::new(DFSchema::empty()),
                    }),
                };
                let schema = input.schema();
                let expr = self.resolve_named_expressions(expressions, schema, state)?;
                let (input, expr) = self.rewrite_wildcard(input, expr, state)?;
                let (input, expr) =
                    self.rewrite_projection::<ExplodeRewriter>(input, expr, state)?;
                let (input, expr) =
                    self.rewrite_projection::<WindowRewriter>(input, expr, state)?;
                let expr = self.rewrite_multi_expr(expr)?;
                let expr = self.rewrite_named_expressions(expr, state)?;
                let has_aggregate = expr.iter().any(|e| {
                    e.exists(|e| match e {
                        Expr::AggregateFunction(_) => Ok(true),
                        _ => Ok(false),
                    })
                    .unwrap_or(false)
                });
                if has_aggregate {
                    LogicalPlan::Aggregate(Aggregate::try_new(Arc::new(input), vec![], expr)?)
                } else {
                    LogicalPlan::Projection(plan::Projection::try_new(expr, Arc::new(input))?)
                }
            }
            PlanNode::Filter { input, condition } => {
                let input = self.resolve_plan(*input, state).await?;
                let schema = input.schema();
                let predicate = self.resolve_expression(condition, schema, state)?;
                let filter = plan::Filter::try_new(predicate, Arc::new(input))?;
                LogicalPlan::Filter(filter)
            }
            PlanNode::Join {
                left,
                right,
                join_condition,
                join_type,
                using_columns,
                join_data_type,
            } => {
                use spec::JoinType;

                let left = self.resolve_plan(*left, state).await?;
                let right = self.resolve_plan(*right, state).await?;
                let (join_type, is_cross_join) = match join_type {
                    JoinType::Inner => (plan::JoinType::Inner, false),
                    JoinType::LeftOuter => (plan::JoinType::Left, false),
                    JoinType::RightOuter => (plan::JoinType::Right, false),
                    JoinType::FullOuter => (plan::JoinType::Full, false),
                    JoinType::LeftSemi => (plan::JoinType::LeftSemi, false),
                    JoinType::LeftAnti => (plan::JoinType::LeftAnti, false),
                    // use inner join type to build the schema for cross join
                    JoinType::Cross => (plan::JoinType::Inner, true),
                };
                let schema = build_join_schema(left.schema(), right.schema(), &join_type)?;
                if is_cross_join {
                    if join_condition.is_some() {
                        return Err(PlanError::invalid("cross join with join condition"));
                    }
                    if !using_columns.is_empty() {
                        return Err(PlanError::invalid("cross join with using columns"));
                    }
                    if join_data_type.is_some() {
                        return Err(PlanError::invalid("cross join with join data type"));
                    }
                    return Ok(LogicalPlan::CrossJoin(plan::CrossJoin {
                        left: Arc::new(left),
                        right: Arc::new(right),
                        schema: Arc::new(schema),
                    }));
                }
                // FIXME: resolve using columns
                // TODO: add more validation logic here and in the plan optimizer
                // See `LogicalPlanBuilder` for details about such logic.
                let (on, filter, join_constraint) =
                    if join_condition.is_some() && using_columns.is_empty() {
                        let condition = join_condition
                            .map(|c| self.resolve_expression(c, &schema, state))
                            .transpose()?;
                        (vec![], condition, plan::JoinConstraint::On)
                    } else if join_condition.is_none() && !using_columns.is_empty() {
                        let on = using_columns
                            .into_iter()
                            .map(|name| {
                                let column = Expr::Column(Column::new_unqualified(name));
                                (column.clone(), column)
                            })
                            .collect();
                        (on, None, plan::JoinConstraint::Using)
                    } else {
                        return Err(PlanError::invalid(
                            "expecting either join condition or using columns",
                        ));
                    };
                LogicalPlan::Join(plan::Join {
                    left: Arc::new(left),
                    right: Arc::new(right),
                    on,
                    filter,
                    join_type,
                    join_constraint,
                    schema: Arc::new(schema),
                    null_equals_null: false,
                })
            }
            PlanNode::SetOperation {
                left,
                right,
                set_op_type,
                is_all,
                by_name: _,
                allow_missing_columns: _,
            } => {
                use spec::SetOpType;

                // TODO: support set operation by name
                let left = self.resolve_plan(*left, state).await?;
                let right = self.resolve_plan(*right, state).await?;
                match set_op_type {
                    SetOpType::Intersect => LogicalPlanBuilder::intersect(left, right, is_all)?,
                    SetOpType::Union => {
                        if is_all {
                            LogicalPlanBuilder::from(left).union(right)?.build()?
                        } else {
                            LogicalPlanBuilder::from(left)
                                .union_distinct(right)?
                                .build()?
                        }
                    }
                    SetOpType::Except => LogicalPlanBuilder::except(left, right, is_all)?,
                }
            }
            PlanNode::Sort {
                input,
                order,
                is_global,
            } => {
                let input = self.resolve_plan(*input, state).await?;
                let schema = input.schema();
                let expr = self.resolve_sort_orders(order, schema, state)?;
                if is_global {
                    LogicalPlan::Sort(plan::Sort {
                        expr,
                        input: Arc::new(input),
                        fetch: None,
                    })
                } else {
                    LogicalPlan::Extension(Extension {
                        node: Arc::new(SortWithinPartitionsNode::new(Arc::new(input), expr, None)),
                    })
                }
            }
            PlanNode::Limit { input, limit } => {
                let input = self.resolve_plan(*input, state).await?;
                LogicalPlan::Limit(plan::Limit {
                    skip: 0,
                    fetch: Some(limit),
                    input: Arc::new(input),
                })
            }
            PlanNode::Aggregate {
                input,
                group_type,
                grouping_expressions,
                aggregate_expressions,
                pivot,
            } => {
                use spec::GroupType;

                if pivot.is_some() {
                    return Err(PlanError::todo("pivot"));
                }
                if group_type != GroupType::GroupBy {
                    return Err(PlanError::todo("unsupported aggregate group type"));
                }
                let input = self.resolve_plan(*input, state).await?;
                let schema = input.schema();
                let grouping_expressions =
                    self.resolve_named_expressions(grouping_expressions, schema, state)?;
                let grouping_expressions =
                    self.rewrite_named_expressions(grouping_expressions, state)?;
                let aggregate_expressions =
                    self.resolve_named_expressions(aggregate_expressions, schema, state)?;
                let aggregate_expressions =
                    self.rewrite_named_expressions(aggregate_expressions, state)?;
                LogicalPlan::Aggregate(Aggregate::try_new(
                    Arc::new(input),
                    grouping_expressions,
                    aggregate_expressions,
                )?)
            }
            PlanNode::WithParameters {
                input,
                positional_arguments,
                named_arguments,
            } => {
                let input = self.resolve_plan(*input, state).await?;
                let input = if !positional_arguments.is_empty() {
                    let params = positional_arguments
                        .into_iter()
                        .map(|arg| self.resolve_literal(arg))
                        .collect::<PlanResult<_>>()?;
                    input.with_param_values(ParamValues::List(params))?
                } else {
                    input
                };
                if !named_arguments.is_empty() {
                    let params = named_arguments
                        .into_iter()
                        .map(|(name, arg)| -> PlanResult<(String, ScalarValue)> {
                            Ok((name, self.resolve_literal(arg)?))
                        })
                        .collect::<PlanResult<_>>()?;
                    input.with_param_values(ParamValues::Map(params))?
                } else {
                    input
                }
            }
            PlanNode::LocalRelation { data, schema } => {
                let batches = if let Some(data) = data {
                    read_record_batches(data)?
                } else {
                    vec![]
                };
                let (schema, batches) = if let Some(schema) = schema {
                    let schema: adt::SchemaRef = Arc::new(self.resolve_schema(schema)?);
                    let batches = batches
                        .into_iter()
                        .map(|b| Ok(cast_record_batch(b, schema.clone())?))
                        .collect::<PlanResult<_>>()?;
                    (schema, batches)
                } else if let [batch, ..] = batches.as_slice() {
                    (batch.schema(), batches)
                } else {
                    return Err(PlanError::invalid("missing schema for local relation"));
                };
                let names = state.register_fields(&schema);
                let provider = RenameTableProvider::try_new(
                    Arc::new(MemTable::try_new(schema, vec![batches])?),
                    names,
                )?;
                LogicalPlan::TableScan(plan::TableScan::try_new(
                    UNNAMED_TABLE,
                    provider_as_source(Arc::new(provider)),
                    None,
                    vec![],
                    None,
                )?)
            }
            PlanNode::Sample { .. } => {
                return Err(PlanError::todo("sample"));
            }
            PlanNode::Offset { input, offset } => {
                let input = self.resolve_plan(*input, state).await?;
                LogicalPlan::Limit(plan::Limit {
                    skip: offset,
                    fetch: None,
                    input: Arc::new(input),
                })
            }
            PlanNode::Deduplicate {
                input,
                column_names,
                all_columns_as_keys,
                within_watermark,
            } => {
                let input = self.resolve_plan(*input, state).await?;
                let schema = input.schema();
                if within_watermark {
                    return Err(PlanError::todo("deduplicate within watermark"));
                }
                if !column_names.is_empty() && !all_columns_as_keys {
                    let column_names: Vec<String> =
                        column_names.into_iter().map(|x| x.into()).collect();
                    let on_expr: Vec<Expr> = schema
                        .columns()
                        .into_iter()
                        .filter(|column| {
                            state
                                .get_field_name(column.name())
                                .is_ok_and(|x| column_names.contains(x))
                        })
                        .map(Expr::Column)
                        .collect();
                    let select_expr: Vec<Expr> =
                        schema.columns().into_iter().map(Expr::Column).collect();
                    LogicalPlan::Distinct(plan::Distinct::On(plan::DistinctOn::try_new(
                        on_expr,
                        select_expr,
                        None,
                        Arc::new(input),
                    )?))
                } else if column_names.is_empty() && all_columns_as_keys {
                    LogicalPlan::Distinct(plan::Distinct::All(Arc::new(input)))
                } else {
                    return Err(PlanError::invalid(
                        "must either specify deduplicate column names or use all columns as keys",
                    ));
                }
            }
            PlanNode::Range {
                start,
                end,
                step,
                num_partitions,
            } => {
                let start = start.unwrap_or(0);
                // TODO: use parallelism in Spark configuration as the default
                let num_partitions = num_partitions.unwrap_or(1);
                if num_partitions < 1 {
                    return Err(PlanError::invalid(format!(
                        "invalid number of partitions: {}",
                        num_partitions
                    )));
                }
                let resolved = state.register_field("id");
                LogicalPlan::Extension(Extension {
                    node: Arc::new(RangeNode::try_new(
                        resolved,
                        start,
                        end,
                        step,
                        num_partitions,
                    )?),
                })
            }
            PlanNode::SubqueryAlias {
                input,
                alias,
                qualifier,
            } => LogicalPlan::SubqueryAlias(plan::SubqueryAlias::try_new(
                Arc::new(self.resolve_plan(*input, state).await?),
                build_table_reference(spec::ObjectName::new_qualified(alias, qualifier))?,
            )?),
            PlanNode::Repartition {
                input,
                num_partitions,
                shuffle: _,
            } => {
                let input = self.resolve_plan(*input, state).await?;
                // TODO: handle shuffle partition
                LogicalPlan::Repartition(plan::Repartition {
                    input: Arc::new(input),
                    partitioning_scheme: plan::Partitioning::RoundRobinBatch(num_partitions),
                })
            }
            PlanNode::ToDf {
                input,
                column_names,
            } => {
                let input = self.resolve_plan(*input, state).await?;
                let schema = input.schema();
                if column_names.len() != schema.fields().len() {
                    return Err(PlanError::invalid(format!(
                        "number of column names ({}) does not match number of columns ({})",
                        column_names.len(),
                        schema.fields().len()
                    )));
                }
                let expr = schema
                    .columns()
                    .into_iter()
                    .zip(column_names.into_iter())
                    .map(|(col, name)| NamedExpr::new(vec![name.into()], Expr::Column(col)))
                    .collect();
                let expr = self.rewrite_named_expressions(expr, state)?;
                LogicalPlan::Projection(plan::Projection::try_new(expr, Arc::new(input))?)
            }
            PlanNode::WithColumnsRenamed {
                input,
                rename_columns_map,
            } => {
                let input = self.resolve_plan(*input, state).await?;
                let rename_columns_map: HashMap<String, String> = rename_columns_map
                    .into_iter()
                    .map(|(k, v)| (k.into(), v.into()))
                    .collect();
                let schema = input.schema();
                let expr = schema
                    .columns()
                    .into_iter()
                    .map(|column| {
                        let name = state.get_field_name(column.name())?;
                        match rename_columns_map.get(name) {
                            Some(n) => Ok(NamedExpr::new(vec![n.clone()], Expr::Column(column))),
                            None => {
                                Ok(NamedExpr::new(vec![name.to_string()], Expr::Column(column)))
                            }
                        }
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                let expr = self.rewrite_named_expressions(expr, state)?;
                LogicalPlan::Projection(plan::Projection::try_new(expr, Arc::new(input))?)
            }
            PlanNode::ShowString {
                input,
                num_rows,
                truncate,
                vertical,
            } => {
                let input = self.resolve_plan(*input, state).await?;
                let style = match vertical {
                    true => ShowStringStyle::Vertical,
                    false => ShowStringStyle::Default,
                };
                let format =
                    ShowStringFormat::new(state.register_field("show_string"), style, truncate);
                let names = state.get_field_names(input.schema().inner())?;
                LogicalPlan::Extension(Extension {
                    node: Arc::new(ShowStringNode::try_new(
                        Arc::new(input),
                        names,
                        num_rows,
                        format,
                    )?),
                })
            }
            PlanNode::Drop {
                input,
                columns,
                column_names,
            } => {
                let input = self.resolve_plan(*input, state).await?;
                let schema = input.schema();
                let mut excluded_names = vec![];
                let mut excluded_fields = vec![];
                for col in column_names {
                    excluded_names.push(col.into());
                }
                for col in columns {
                    if let spec::Expr::UnresolvedAttribute { name, plan_id } = col {
                        let name: Vec<String> = name.into();
                        let name = name.one().map_err(|_| {
                            PlanError::invalid("expecting a single column name to drop")
                        })?;
                        if let Some(plan_id) = plan_id {
                            let field = state
                                .get_resolved_field_name_in_plan(plan_id, &name)?
                                .clone();
                            excluded_fields.push(field)
                        } else {
                            excluded_names.push(name);
                        }
                    } else {
                        return Err(PlanError::invalid("expecting column name to drop"));
                    }
                }
                let expr: Vec<Expr> = schema
                    .columns()
                    .into_iter()
                    .filter(|column| {
                        let name = column.name().to_string();
                        !excluded_fields.contains(&name)
                            && state
                                .get_field_name(&name)
                                .is_ok_and(|x| !excluded_names.contains(x))
                    })
                    .map(Expr::Column)
                    .collect();
                LogicalPlan::Projection(plan::Projection::try_new(expr, Arc::new(input))?)
            }
            PlanNode::Tail { .. } => {
                return Err(PlanError::todo("tail"));
            }
            PlanNode::WithColumns { input, aliases } => {
                let input = self.resolve_plan(*input, state).await?;
                let schema = input.schema();
                let mut aliases: HashMap<String, (Expr, bool)> = aliases
                    .into_iter()
                    .map(|expr| {
                        let (name, expr) = match expr {
                            // TODO: handle alias metadata
                            spec::Expr::Alias {
                                name,
                                expr,
                                metadata: _,
                            } => {
                                let name = name
                                    .one()
                                    .map_err(|_| PlanError::invalid("multi-alias for column"))?;
                                (name, *expr)
                            }
                            _ => {
                                return Err(PlanError::invalid(
                                    "alias expression expected for column",
                                ))
                            }
                        };
                        let expr = self.resolve_expression(expr, schema, state)?;
                        Ok((name.into(), (expr, false)))
                    })
                    .collect::<PlanResult<_>>()?;
                let mut expr = schema
                    .columns()
                    .into_iter()
                    .map(|column| {
                        let name = state.get_field_name(column.name())?;
                        match aliases.get_mut(name) {
                            Some((e, exists)) => {
                                *exists = true;
                                Ok(NamedExpr::new(vec![name.to_string()], e.clone()))
                            }
                            None => {
                                Ok(NamedExpr::new(vec![name.to_string()], Expr::Column(column)))
                            }
                        }
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                for (name, (e, exists)) in &aliases {
                    if !exists {
                        expr.push(NamedExpr::new(vec![name.clone()], e.clone()));
                    }
                }
                let (input, expr) =
                    self.rewrite_projection::<ExplodeRewriter>(input, expr, state)?;
                let (input, expr) =
                    self.rewrite_projection::<WindowRewriter>(input, expr, state)?;
                let expr = self.rewrite_multi_expr(expr)?;
                let expr = self.rewrite_named_expressions(expr, state)?;
                LogicalPlan::Projection(plan::Projection::try_new(expr, Arc::new(input))?)
            }
            PlanNode::Hint { .. } => {
                return Err(PlanError::todo("hint"));
            }
            PlanNode::Unpivot { .. } => {
                return Err(PlanError::todo("unpivot"));
            }
            PlanNode::ToSchema { .. } => {
                return Err(PlanError::todo("to schema"));
            }
            PlanNode::RepartitionByExpression {
                input,
                partition_expressions,
                num_partitions,
            } => {
                let input = self.resolve_plan(*input, state).await?;
                let schema = input.schema();
                let expr = self.resolve_expressions(partition_expressions, schema, state)?;
                let num_partitions = num_partitions
                    .ok_or_else(|| PlanError::todo("rebalance partitioning by expression"))?;
                LogicalPlan::Repartition(plan::Repartition {
                    input: Arc::new(input),
                    partitioning_scheme: plan::Partitioning::Hash(expr, num_partitions),
                })
            }
            PlanNode::MapPartitions { .. } => {
                return Err(PlanError::todo("map partitions"));
            }
            PlanNode::CollectMetrics { .. } => {
                return Err(PlanError::todo("collect metrics"));
            }
            PlanNode::Parse { .. } => {
                return Err(PlanError::todo("parse"));
            }
            PlanNode::GroupMap { .. } => {
                return Err(PlanError::todo("group map"));
            }
            PlanNode::CoGroupMap { .. } => {
                return Err(PlanError::todo("co-group map"));
            }
            PlanNode::WithWatermark { .. } => {
                return Err(PlanError::todo("with watermark"));
            }
            PlanNode::ApplyInPandasWithState { .. } => {
                return Err(PlanError::todo("apply in pandas with state"));
            }
            PlanNode::HtmlString {
                input,
                num_rows,
                truncate,
            } => {
                let input = self.resolve_plan(*input, state).await?;
                let format = ShowStringFormat::new(
                    state.register_field("html_string"),
                    ShowStringStyle::Html,
                    truncate,
                );
                let names = state.get_field_names(input.schema().inner())?;
                LogicalPlan::Extension(Extension {
                    node: Arc::new(ShowStringNode::try_new(
                        Arc::new(input),
                        names,
                        num_rows,
                        format,
                    )?),
                })
            }
            PlanNode::CachedLocalRelation { .. } => {
                return Err(PlanError::todo("cached local relation"));
            }
            PlanNode::CachedRemoteRelation { .. } => {
                return Err(PlanError::todo("cached remote relation"));
            }
            PlanNode::CommonInlineUserDefinedTableFunction(udtf) => {
                // TODO: Function arg for if pyspark_udtf or not
                use framework_python::udf::pyspark_udtf::PySparkUDTF;

                let spec::CommonInlineUserDefinedTableFunction {
                    function_name,
                    deterministic,
                    arguments,
                    function,
                } = udtf;

                let schema = DFSchema::empty();
                let arguments = self.resolve_expressions(arguments, &schema, state)?;

                let (return_type, _eval_type, _command, _python_version) = match &function {
                    spec::TableFunctionDefinition::PythonUdtf {
                        return_type,
                        eval_type,
                        command,
                        python_version,
                    } => (return_type, eval_type, command, python_version),
                };

                let return_type: adt::DataType = self.resolve_data_type(return_type.clone())?;
                let return_schema: adt::SchemaRef = match return_type {
                    adt::DataType::Struct(ref fields) => {
                        Arc::new(adt::Schema::new(fields.clone()))
                    },
                    _ => {
                        return Err(PlanError::invalid(format!(
                            "Invalid Python user-defined table function return type. Expect a struct type, but got {}",
                            return_type
                        )))
                    }
                };

                let python_udtf: PySparkUDTF = PySparkUDTF::new(
                    return_type,
                    return_schema,
                    function,
                    self.config.spark_udf_config.clone(),
                    deterministic,
                );

                let table_function =
                    TableFunction::new(function_name.clone(), Arc::new(python_udtf));
                let table_provider = table_function.create_table_provider(&arguments)?;
                let names = state.register_fields(&table_provider.schema());
                let table_provider = RenameTableProvider::try_new(table_provider, names)?;
                LogicalPlan::TableScan(plan::TableScan::try_new(
                    function_name,
                    provider_as_source(Arc::new(table_provider)),
                    None,
                    vec![],
                    None,
                )?)
            }
            PlanNode::FillNa { .. } => {
                return Err(PlanError::todo("fill na"));
            }
            PlanNode::DropNa { .. } => {
                return Err(PlanError::todo("drop na"));
            }
            PlanNode::ReplaceNa { .. } => {
                return Err(PlanError::todo("replace"));
            }
            PlanNode::StatSummary { .. } => {
                return Err(PlanError::todo("summary"));
            }
            PlanNode::StatCrosstab { .. } => {
                return Err(PlanError::todo("crosstab"));
            }
            PlanNode::StatDescribe { .. } => {
                return Err(PlanError::todo("describe"));
            }
            PlanNode::StatCov { .. } => {
                return Err(PlanError::todo("cov"));
            }
            PlanNode::StatCorr { .. } => {
                return Err(PlanError::todo("corr"));
            }
            PlanNode::StatApproxQuantile { .. } => {
                return Err(PlanError::todo("approx quantile"));
            }
            PlanNode::StatFreqItems { .. } => {
                return Err(PlanError::todo("freq items"));
            }
            PlanNode::StatSampleBy { .. } => {
                return Err(PlanError::todo("sample by"));
            }
            PlanNode::CurrentDatabase {} => LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::CurrentDatabase,
                    self.config.clone(),
                )?),
            }),
            PlanNode::SetCurrentDatabase { database_name } => LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::SetCurrentDatabase {
                        database_name: database_name.into(),
                    },
                    self.config.clone(),
                )?),
            }),
            PlanNode::ListDatabases {
                catalog,
                database_pattern,
            } => LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::ListDatabases {
                        catalog: catalog.map(|x| x.into()),
                        database_pattern,
                    },
                    self.config.clone(),
                )?),
            }),
            PlanNode::ListTables {
                database,
                table_pattern,
            } => LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::ListTables {
                        database: database.map(build_schema_reference).transpose()?,
                        table_pattern,
                    },
                    self.config.clone(),
                )?),
            }),
            PlanNode::ListFunctions {
                database,
                function_pattern,
            } => LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::ListFunctions {
                        database: database.map(build_schema_reference).transpose()?,
                        function_pattern,
                    },
                    self.config.clone(),
                )?),
            }),
            PlanNode::ListColumns { table } => LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::ListColumns {
                        table: build_table_reference(table)?,
                    },
                    self.config.clone(),
                )?),
            }),
            PlanNode::GetDatabase { database } => LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::GetDatabase {
                        database: build_schema_reference(database)?,
                    },
                    self.config.clone(),
                )?),
            }),
            PlanNode::GetTable { table } => LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::GetTable {
                        table: build_table_reference(table)?,
                    },
                    self.config.clone(),
                )?),
            }),
            PlanNode::GetFunction { function } => LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::GetFunction {
                        function: build_table_reference(function)?,
                    },
                    self.config.clone(),
                )?),
            }),
            PlanNode::DatabaseExists { database } => LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::DatabaseExists {
                        database: build_schema_reference(database)?,
                    },
                    self.config.clone(),
                )?),
            }),
            PlanNode::TableExists { table } => LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::TableExists {
                        table: build_table_reference(table)?,
                    },
                    self.config.clone(),
                )?),
            }),
            PlanNode::FunctionExists { function } => LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::FunctionExists {
                        function: build_table_reference(function)?,
                    },
                    self.config.clone(),
                )?),
            }),
            PlanNode::CreateTable {
                table,
                path,
                source,
                description: _,
                schema,
                options,
            } => {
                // TODO: use spark.sql.sources.default to get the default source
                let read = spec::Plan::new(PlanNode::Read {
                    read_type: spec::ReadType::DataSource {
                        // TODO: is `source` and `format` equivalent?
                        format: source,
                        schema,
                        options,
                        paths: path.map(|x| vec![x]).unwrap_or_default(),
                        predicates: vec![],
                    },
                    is_streaming: false,
                });
                LogicalPlan::Extension(Extension {
                    node: Arc::new(CatalogCommandNode::try_new(
                        CatalogCommand::CreateTable {
                            table: build_table_reference(table)?,
                            plan: Arc::new(self.resolve_plan(read, state).await?),
                        },
                        self.config.clone(),
                    )?),
                })
            }
            PlanNode::DropTemporaryView {
                view,
                is_global,
                if_exists,
            } => LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::DropTemporaryView {
                        view: build_table_reference(view)?,
                        is_global,
                        if_exists,
                    },
                    self.config.clone(),
                )?),
            }),
            PlanNode::DropDatabase {
                database,
                if_exists,
                cascade,
            } => LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::DropDatabase {
                        database: build_schema_reference(database)?,
                        if_exists,
                        cascade,
                    },
                    self.config.clone(),
                )?),
            }),
            PlanNode::DropFunction {
                function,
                if_exists,
                is_temporary,
            } => LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::DropFunction {
                        function: build_table_reference(function)?,
                        if_exists,
                        is_temporary,
                    },
                    self.config.clone(),
                )?),
            }),
            PlanNode::DropTable {
                table,
                if_exists,
                purge,
            } => LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::DropTable {
                        table: build_table_reference(table)?,
                        if_exists,
                        purge,
                    },
                    self.config.clone(),
                )?),
            }),
            PlanNode::DropView { view, if_exists } => LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::DropView {
                        view: build_table_reference(view)?,
                        if_exists,
                    },
                    self.config.clone(),
                )?),
            }),
            PlanNode::RecoverPartitions { .. } => {
                return Err(PlanError::todo("PlanNode::RecoverPartitions"))
            }
            PlanNode::IsCached { .. } => return Err(PlanError::todo("PlanNode::IsCached")),
            PlanNode::CacheTable { .. } => return Err(PlanError::todo("PlanNode::CacheTable")),
            PlanNode::UncacheTable { .. } => return Err(PlanError::todo("PlanNode::UncacheTable")),
            PlanNode::ClearCache {} => return Err(PlanError::todo("PlanNode::ClearCache")),
            PlanNode::RefreshTable { .. } => return Err(PlanError::todo("PlanNode::RefreshTable")),
            PlanNode::RefreshByPath { .. } => {
                return Err(PlanError::todo("PlanNode::RefreshByPath"))
            }
            PlanNode::CurrentCatalog => LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::CurrentCatalog,
                    self.config.clone(),
                )?),
            }),
            PlanNode::SetCurrentCatalog { catalog_name } => LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::SetCurrentCatalog {
                        catalog_name: catalog_name.into(),
                    },
                    self.config.clone(),
                )?),
            }),
            PlanNode::ListCatalogs { catalog_pattern } => LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::ListCatalogs { catalog_pattern },
                    self.config.clone(),
                )?),
            }),
            PlanNode::CreateDatabase {
                database,
                if_not_exists,
                comment,
                location,
                properties,
            } => {
                let properties = properties.into_iter().collect::<Vec<_>>();
                LogicalPlan::Extension(Extension {
                    node: Arc::new(CatalogCommandNode::try_new(
                        CatalogCommand::CreateDatabase {
                            database: build_schema_reference(database)?,
                            if_not_exists,
                            comment,
                            location,
                            properties,
                        },
                        self.config.clone(),
                    )?),
                })
            }
            PlanNode::RegisterFunction(_) => return Err(PlanError::todo("register function")),
            PlanNode::RegisterTableFunction(_) => {
                return Err(PlanError::todo("register table function"))
            }
            PlanNode::CreateTemporaryView { .. } => {
                return Err(PlanError::todo("create temporary view"))
            }
            PlanNode::Write { .. } => return Err(PlanError::todo("write")),
            PlanNode::Empty { produce_one_row } => {
                LogicalPlan::EmptyRelation(plan::EmptyRelation {
                    produce_one_row,
                    schema: DFSchemaRef::new(DFSchema::empty()),
                })
            }
            PlanNode::Values(values) => {
                let schema = DFSchema::empty();
                let values = values
                    .into_iter()
                    .map(|row| self.resolve_expressions(row, &schema, state))
                    .collect::<PlanResult<Vec<_>>>()?;
                let plan = LogicalPlanBuilder::values(values)?.build()?;
                let expr = plan
                    .schema()
                    .columns()
                    .into_iter()
                    .enumerate()
                    .map(|(i, col)| NamedExpr::new(vec![format!("col{i}")], Expr::Column(col)))
                    .collect::<Vec<_>>();
                let expr = self.rewrite_named_expressions(expr, state)?;
                LogicalPlan::Projection(plan::Projection::try_new(expr, Arc::new(plan))?)
            }
            PlanNode::TableAlias {
                input,
                name,
                columns,
            } => {
                let input = self.resolve_plan(*input, state).await?;
                let schema = input.schema();
                let input = if columns.is_empty() {
                    input
                } else {
                    if columns.len() != schema.fields().len() {
                        return Err(PlanError::invalid(format!(
                            "number of column names ({}) does not match number of columns ({})",
                            columns.len(),
                            schema.fields().len()
                        )));
                    }
                    let expr: Vec<Expr> = schema
                        .columns()
                        .into_iter()
                        .zip(columns.into_iter())
                        .map(|(col, name)| {
                            Expr::Column(col.clone()).alias(state.register_field(name))
                        })
                        .collect();
                    LogicalPlan::Projection(plan::Projection::try_new(expr, Arc::new(input))?)
                };
                LogicalPlan::SubqueryAlias(plan::SubqueryAlias::try_new(
                    Arc::new(input),
                    TableReference::Bare {
                        table: Arc::from(String::from(name)),
                    },
                )?)
            }
        };
        if self.is_query_plan(&plan) {
            self.verify_query_plan(&plan, state)?;
            self.register_schema_with_plan_id(&plan, plan_id, state)?;
        };
        Ok(plan)
    }

    pub async fn resolve_named_plan(&self, plan: spec::Plan) -> PlanResult<NamedPlan> {
        let mut state = PlanResolverState::new();
        let plan = self.resolve_plan(plan, &mut state).await?;
        let fields = if self.is_query_plan(&plan) {
            Some(state.get_field_names(plan.schema().inner())?)
        } else {
            None
        };
        Ok(NamedPlan { plan, fields })
    }

    fn is_query_plan(&self, plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Window(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::CrossJoin(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::RecursiveQuery(_)
            | LogicalPlan::Unnest(_) => true,
            LogicalPlan::Statement(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Prepare(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::DescribeTable(_) => false,
            LogicalPlan::Extension(Extension { node }) => !node.as_any().is::<CatalogCommandNode>(),
        }
    }

    fn verify_query_plan(&self, plan: &LogicalPlan, state: &PlanResolverState) -> PlanResult<()> {
        let invalid = plan
            .schema()
            .fields()
            .iter()
            .filter_map(|f| {
                if state.get_field_name(f.name()).is_ok() {
                    None
                } else {
                    Some(f.name().to_string())
                }
            })
            .collect::<Vec<_>>();
        if invalid.is_empty() {
            Ok(())
        } else {
            Err(PlanError::internal(format!(
                "a plan resolver bug has produced invalid fields: {:?}",
                invalid,
            )))
        }
    }

    fn register_schema_with_plan_id(
        &self,
        plan: &LogicalPlan,
        plan_id: Option<i64>,
        state: &mut PlanResolverState,
    ) -> PlanResult<()> {
        if let Some(plan_id) = plan_id {
            for field in plan.schema().fields() {
                let resolved = field.name().clone();
                let name = state.get_field_name(&resolved)?.clone();
                state.register_attribute(plan_id, name, resolved);
            }
        }
        Ok(())
    }

    fn rewrite_wildcard(
        &self,
        input: LogicalPlan,
        expr: Vec<NamedExpr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<NamedExpr>)> {
        let schema = input.schema();
        let mut projected = vec![];
        for e in expr {
            let NamedExpr {
                name,
                expr,
                metadata,
            } = e;
            match expr {
                Expr::Wildcard { qualifier: None } => {
                    for e in expand_wildcard(schema, &input, None)? {
                        projected.push(NamedExpr::try_from_column_expr(e, state)?)
                    }
                }
                Expr::Wildcard {
                    qualifier: Some(qualifier),
                } => {
                    for e in expand_qualified_wildcard(&qualifier, schema, None)? {
                        projected.push(NamedExpr::try_from_column_expr(e, state)?)
                    }
                }
                _ => projected.push(NamedExpr {
                    name,
                    expr: columnize_expr(normalize_col(expr, &input)?, &input)?,
                    metadata,
                }),
            }
        }
        Ok((input, projected))
    }

    fn rewrite_projection<'s, T>(
        &self,
        input: LogicalPlan,
        expr: Vec<NamedExpr>,
        state: &'s mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<NamedExpr>)>
    where
        T: PlanRewriter<'s> + TreeNodeRewriter<Node = Expr>,
    {
        let mut rewriter = T::new_from_plan(input, state);
        let expr = expr
            .into_iter()
            .map(|e| {
                let NamedExpr {
                    name,
                    expr,
                    metadata,
                } = e;
                Ok(NamedExpr {
                    name,
                    expr: expr.rewrite(&mut rewriter)?.data,
                    metadata,
                })
            })
            .collect::<PlanResult<Vec<_>>>()?;
        Ok((rewriter.into_plan(), expr))
    }

    fn rewrite_multi_expr(&self, expr: Vec<NamedExpr>) -> PlanResult<Vec<NamedExpr>> {
        let mut out = vec![];
        for e in expr {
            let NamedExpr {
                name,
                expr,
                metadata,
            } = e;
            match expr {
                Expr::ScalarFunction(ScalarFunction { func, args }) => {
                    if func.inner().as_any().is::<MultiExpr>() {
                        // The metadata from the original expression are ignored.
                        if name.len() == args.len() {
                            for (name, arg) in name.into_iter().zip(args) {
                                out.push(NamedExpr::new(vec![name], arg));
                            }
                        } else {
                            for arg in args {
                                out.push(NamedExpr::try_from_alias_expr(arg)?);
                            }
                        }
                    } else {
                        out.push(NamedExpr {
                            name,
                            expr: func.call(args),
                            metadata,
                        });
                    }
                }
                _ => {
                    out.push(NamedExpr {
                        name,
                        expr,
                        metadata,
                    });
                }
            };
        }
        Ok(out)
    }

    fn rewrite_named_expressions(
        &self,
        expr: Vec<NamedExpr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<Expr>> {
        expr.into_iter()
            .map(|e| {
                let NamedExpr {
                    name,
                    expr,
                    metadata: _, // TODO: set field metadata
                } = e;
                let name = name
                    .one()
                    .map_err(|_| PlanError::invalid("one name expected for expression"))?;
                if let Expr::Column(Column {
                    name: column_name, ..
                }) = &expr
                {
                    if state.get_field_name(column_name).ok() == Some(&name) {
                        return Ok(expr);
                    }
                }
                Ok(expr.alias(state.register_field(name)))
            })
            .collect()
    }
}
