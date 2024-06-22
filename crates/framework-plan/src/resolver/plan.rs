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
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::execution::context::DataFilePaths;
use datafusion::logical_expr::{
    logical_plan as plan, Aggregate, Expr, Extension, LogicalPlan, UNNAMED_TABLE,
};
use datafusion::sql::unparser::expr_to_sql;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::{
    Column, DFSchema, DFSchemaRef, ParamValues, ScalarValue, SchemaReference, TableReference,
};
use datafusion_expr::expr::Alias;
use datafusion_expr::{build_join_schema, LogicalPlanBuilder};
use framework_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::extension::analyzer::alias::rewrite_multi_alias;
use crate::extension::analyzer::explode::rewrite_explode;
use crate::extension::analyzer::wildcard::rewrite_wildcard;
use crate::extension::analyzer::window::rewrite_window;
use crate::extension::logical::{
    CatalogCommand, CatalogCommandNode, RangeNode, ShowStringFormat, ShowStringNode,
    ShowStringStyle, SortWithinPartitionsNode,
};
use crate::resolver::utils::{cast_record_batch, read_record_batches};
use crate::resolver::{PlanResolver, PlanResolverState};

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

impl PlanResolver<'_> {
    #[async_recursion]
    pub async fn resolve_plan(
        &self,
        plan: spec::Plan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::PlanNode;

        match plan.node {
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
                        Ok(df.into_optimized_plan()?)
                    }
                    ReadType::UDTF {
                        identifier,
                        arguments,
                        options,
                    } => {
                        if !options.is_empty() {
                            return Err(PlanError::todo("ReadType::UDTF options"));
                        }

                        let table = build_table_reference(identifier)?.table().to_string();

                        let schema = DFSchema::empty();
                        let arguments: Vec<Expr> = arguments
                            .into_iter()
                            .map(|x| self.resolve_expression(x, &schema, state))
                            .collect::<PlanResult<Vec<Expr>>>()?;
                        println!("CHECK HERE arguments: {:?}", arguments);
                        let argument_strings: Vec<String> = arguments
                            .iter()
                            .map(|arg| {
                                let sql = expr_to_sql(arg).map_err(|e| {
                                    PlanError::invalid(format!(
                                        "Failed to convert expr to SQL: {}",
                                        e
                                    ))
                                })?;
                                Ok(format!("{}", sql))
                            })
                            .collect::<PlanResult<Vec<String>>>()?;

                        let sql_query =
                            format!("SELECT * FROM {}({});", table, argument_strings.join(", "));
                        println!("CHECK HERE sql_query: {}", sql_query);
                        let df: DataFrame = self.ctx.sql(&sql_query).await?;
                        Ok(df.into_optimized_plan()?)
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
                        Ok(LogicalPlan::TableScan(plan::TableScan::try_new(
                            UNNAMED_TABLE,
                            provider_as_source(provider),
                            None,
                            vec![],
                            None,
                        )?))
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
                let expr: Vec<Expr> = expressions
                    .into_iter()
                    .map(|e| self.resolve_expression(e, schema, state))
                    .collect::<PlanResult<_>>()?;
                let expr = rewrite_multi_alias(expr)?;
                let (input, expr) = rewrite_wildcard(input, expr)?;
                let (input, expr) = rewrite_explode(input, expr)?;
                let (input, expr) = rewrite_window(input, expr)?;
                let has_aggregate = expr.iter().any(|e| {
                    e.exists(|e| match e {
                        Expr::AggregateFunction(_) => Ok(true),
                        _ => Ok(false),
                    })
                    .unwrap_or(false)
                });
                if has_aggregate {
                    Ok(LogicalPlan::Aggregate(Aggregate::try_new(
                        Arc::new(input),
                        vec![],
                        expr,
                    )?))
                } else {
                    Ok(LogicalPlan::Projection(plan::Projection::try_new(
                        expr,
                        Arc::new(input),
                    )?))
                }
            }
            PlanNode::Filter { input, condition } => {
                let input = self.resolve_plan(*input, state).await?;
                let schema = input.schema();
                let predicate = self.resolve_expression(condition, schema, state)?.unalias();
                let filter = plan::Filter::try_new(predicate, Arc::new(input))?;
                Ok(LogicalPlan::Filter(filter))
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
                Ok(LogicalPlan::Join(plan::Join {
                    left: Arc::new(left),
                    right: Arc::new(right),
                    on,
                    filter,
                    join_type,
                    join_constraint,
                    schema: Arc::new(schema),
                    null_equals_null: false,
                }))
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
                    SetOpType::Intersect => Ok(LogicalPlanBuilder::intersect(left, right, is_all)?),
                    SetOpType::Union => {
                        if is_all {
                            Ok(LogicalPlanBuilder::from(left).union(right)?.build()?)
                        } else {
                            Ok(LogicalPlanBuilder::from(left)
                                .union_distinct(right)?
                                .build()?)
                        }
                    }
                    SetOpType::Except => Ok(LogicalPlanBuilder::except(left, right, is_all)?),
                }
            }
            PlanNode::Sort {
                input,
                order,
                is_global,
            } => {
                let input = self.resolve_plan(*input, state).await?;
                let schema = input.schema();
                let expr = order
                    .into_iter()
                    .map(|o| self.resolve_sort_order(o, schema, state))
                    .collect::<PlanResult<_>>()?;
                if is_global {
                    Ok(LogicalPlan::Sort(plan::Sort {
                        expr,
                        input: Arc::new(input),
                        fetch: None,
                    }))
                } else {
                    Ok(LogicalPlan::Extension(Extension {
                        node: Arc::new(SortWithinPartitionsNode::new(Arc::new(input), expr, None)),
                    }))
                }
            }
            PlanNode::Limit { input, limit } => {
                let input = self.resolve_plan(*input, state).await?;
                Ok(LogicalPlan::Limit(plan::Limit {
                    skip: 0,
                    fetch: Some(limit),
                    input: Arc::new(input),
                }))
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
                let group_expr = grouping_expressions
                    .into_iter()
                    .map(|e| self.resolve_expression(e, schema, state))
                    .collect::<PlanResult<_>>()?;
                let aggr_expr = aggregate_expressions
                    .into_iter()
                    .map(|e| self.resolve_expression(e, schema, state))
                    .collect::<PlanResult<_>>()?;
                Ok(LogicalPlan::Aggregate(Aggregate::try_new(
                    Arc::new(input),
                    group_expr,
                    aggr_expr,
                )?))
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
                let input = if !named_arguments.is_empty() {
                    let params = named_arguments
                        .into_iter()
                        .map(|(name, arg)| -> PlanResult<(String, ScalarValue)> {
                            Ok((name, self.resolve_literal(arg)?))
                        })
                        .collect::<PlanResult<_>>()?;
                    input.with_param_values(ParamValues::Map(params))?
                } else {
                    input
                };
                Ok(input)
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
                let provider = MemTable::try_new(schema, vec![batches])?;
                Ok(LogicalPlan::TableScan(plan::TableScan::try_new(
                    UNNAMED_TABLE,
                    provider_as_source(Arc::new(provider)),
                    None,
                    vec![],
                    None,
                )?))
            }
            PlanNode::Sample { .. } => {
                return Err(PlanError::todo("sample"));
            }
            PlanNode::Offset { input, offset } => {
                let input = self.resolve_plan(*input, state).await?;
                Ok(LogicalPlan::Limit(plan::Limit {
                    skip: offset,
                    fetch: None,
                    input: Arc::new(input),
                }))
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
                let distinct = if !column_names.is_empty() && !all_columns_as_keys {
                    let on_expr: Vec<Expr> = column_names
                        .iter()
                        .flat_map(|name| {
                            schema
                                .columns_with_unqualified_name(name.into())
                                .into_iter()
                        })
                        .map(|x| Expr::Column(x.clone()))
                        .collect();
                    let select_expr: Vec<Expr> = schema
                        .columns()
                        .iter()
                        .map(|x| Expr::Column(x.clone()))
                        .collect();
                    plan::Distinct::On(plan::DistinctOn::try_new(
                        on_expr,
                        select_expr,
                        None,
                        Arc::new(input),
                    )?)
                } else if column_names.is_empty() && all_columns_as_keys {
                    plan::Distinct::All(Arc::new(input))
                } else {
                    return Err(PlanError::invalid(
                        "must either specify deduplicate column names or use all columns as keys",
                    ));
                };
                Ok(LogicalPlan::Distinct(distinct))
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
                Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(RangeNode::try_new(start, end, step, num_partitions)?),
                }))
            }
            PlanNode::SubqueryAlias {
                input,
                alias,
                qualifier,
            } => Ok(LogicalPlan::SubqueryAlias(plan::SubqueryAlias::try_new(
                Arc::new(self.resolve_plan(*input, state).await?),
                build_table_reference(spec::ObjectName::new_qualified(alias, qualifier))?,
            )?)),
            PlanNode::Repartition {
                input,
                num_partitions,
                shuffle: _,
            } => {
                let input = self.resolve_plan(*input, state).await?;
                // TODO: handle shuffle partition
                Ok(LogicalPlan::Repartition(plan::Repartition {
                    input: Arc::new(input),
                    partitioning_scheme: plan::Partitioning::RoundRobinBatch(num_partitions),
                }))
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
                let expr: Vec<Expr> = schema
                    .columns()
                    .iter()
                    .zip(column_names.iter())
                    .map(|(col, name)| Expr::Column(col.clone()).alias(name.clone()))
                    .collect();
                Ok(LogicalPlan::Projection(plan::Projection::try_new(
                    expr,
                    Arc::new(input),
                )?))
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
                let expr: Vec<Expr> = schema
                    .columns()
                    .iter()
                    .map(|column| {
                        let name = &column.name;
                        let expr = Expr::Column(column.clone());
                        match rename_columns_map.get(name) {
                            Some(n) => expr.alias(n),
                            None => expr,
                        }
                    })
                    .collect();
                Ok(LogicalPlan::Projection(plan::Projection::try_new(
                    expr,
                    Arc::new(input),
                )?))
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
                let format = ShowStringFormat::new(style, truncate);
                Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(ShowStringNode::try_new(Arc::new(input), num_rows, format)?),
                }))
            }
            PlanNode::Drop {
                input,
                columns,
                column_names,
            } => {
                let input = self.resolve_plan(*input, state).await?;
                let schema = input.schema();
                if !columns.is_empty() {
                    return Err(PlanError::todo("drop column expressions"));
                }
                let column_names: Vec<String> =
                    column_names.into_iter().map(|x| x.into()).collect();
                let expr: Vec<Expr> = schema
                    .columns()
                    .iter()
                    .filter(|column| !column_names.contains(&column.name))
                    .map(|column| Expr::Column(column.clone()))
                    .collect();
                Ok(LogicalPlan::Projection(plan::Projection::try_new(
                    expr,
                    Arc::new(input),
                )?))
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
                                mut name,
                                expr,
                                metadata: _,
                            } => {
                                if name.len() == 1 {
                                    (name.pop().unwrap(), *expr)
                                } else {
                                    return Err(PlanError::invalid("multi-alias for column"));
                                }
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
                let mut expr: Vec<Expr> = schema
                    .columns()
                    .iter()
                    .map(|column| {
                        let name = &column.name;
                        match aliases.get_mut(name) {
                            Some((e, exists)) => {
                                *exists = true;
                                e.clone().alias(name)
                            }
                            None => Expr::Column(column.clone()),
                        }
                    })
                    .collect();
                for (name, (e, exists)) in &aliases {
                    if !exists {
                        expr.push(e.clone().alias(name));
                    }
                }
                let (input, expr) = rewrite_explode(input, expr)?;
                let (input, expr) = rewrite_window(input, expr)?;
                Ok(LogicalPlan::Projection(plan::Projection::try_new(
                    expr,
                    Arc::new(input),
                )?))
            }
            PlanNode::Hint { .. } => {
                return Err(PlanError::todo("hint"));
            }
            PlanNode::Unpivot { .. } => {
                return Err(PlanError::todo("unpivot"));
            }
            PlanNode::ToSchema { .. } => {
                return Err(PlanError::todo("to schema"));
                // TODO: Close but doesn't quite work.
                // let input = to_schema.input.as_ref().required("input relation")?;
                // let input =  self.resolve_plan(input, state).await?;
                // let schema = to_schema.schema.as_ref().required("schema")?;
                // let schema: DataType = from_spark_built_in_data_type(schema)?;
                // let fields = match &schema {
                //     DataType::Struct(fields) => fields,
                //     _ => return Err(PlannerError::invalid("expected struct type")),
                // };
                // let expr: Vec<Expr> = fields
                //     .iter()
                //     .map(|field| Expr::Column(Column::from_name(field.name())))
                //     .collect();
                // Ok(LogicalPlan::Projection(plan::Projection::try_new(
                //     expr,
                //     Arc::new(input),
                // )?))
            }
            PlanNode::RepartitionByExpression {
                input,
                partition_expressions,
                num_partitions,
            } => {
                let input = self.resolve_plan(*input, state).await?;
                let schema = input.schema();
                let expr: Vec<Expr> = partition_expressions
                    .into_iter()
                    .map(|e| self.resolve_expression(e, schema, state))
                    .collect::<PlanResult<_>>()?;
                let num_partitions = num_partitions
                    .ok_or_else(|| PlanError::todo("rebalance partitioning by expression"))?;
                Ok(LogicalPlan::Repartition(plan::Repartition {
                    input: Arc::new(input),
                    partitioning_scheme: plan::Partitioning::Hash(expr, num_partitions),
                }))
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
                let format = ShowStringFormat::new(ShowStringStyle::Html, truncate);
                Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(ShowStringNode::try_new(Arc::new(input), num_rows, format)?),
                }))
            }
            PlanNode::CachedLocalRelation { .. } => {
                return Err(PlanError::todo("cached local relation"));
            }
            PlanNode::CachedRemoteRelation { .. } => {
                return Err(PlanError::todo("cached remote relation"));
            }
            PlanNode::CommonInlineUserDefinedTableFunction(udtf) => {
                // TODO: Function arg for if pyspark_udtf or not
                use framework_python::cereal::pyspark_udtf::{
                    deserialize_pyspark_udtf, PySparkUDTF as CerealPySparkUDTF,
                };
                use framework_python::udf::pyspark_udtf::PySparkUDTF;

                let spec::CommonInlineUserDefinedTableFunction {
                    function_name,
                    deterministic,
                    arguments,
                    function,
                } = udtf;

                let schema = DFSchema::empty();
                let arguments: Vec<Expr> = arguments
                    .into_iter()
                    .map(|x| self.resolve_expression(x, &schema, state))
                    .collect::<PlanResult<Vec<Expr>>>()?;

                let (return_type, eval_type, command, python_version) = match function {
                    spec::TableFunctionDefinition::PythonUdtf {
                        return_type,
                        eval_type,
                        command,
                        python_version,
                    } => (return_type, eval_type, command, python_version),
                };

                let return_type: adt::DataType = self.resolve_data_type(return_type)?;
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

                let python_function: CerealPySparkUDTF = deserialize_pyspark_udtf(
                    &python_version,
                    &command,
                    &eval_type,
                    &(arguments.len() as i32),
                    &return_type,
                    &self.config.spark_udf_config,
                )
                .map_err(|e| {
                    PlanError::invalid(format!("Python UDF deserialization error: {}", e))
                })?;

                let python_udtf: PySparkUDTF =
                    PySparkUDTF::new(return_schema, python_function, deterministic, eval_type);

                let table_function =
                    TableFunction::new(function_name.clone(), Arc::new(python_udtf));
                let table_provider = table_function.create_table_provider(&arguments)?;
                Ok(LogicalPlan::TableScan(plan::TableScan::try_new(
                    function_name,
                    provider_as_source(table_provider),
                    None,
                    vec![],
                    None,
                )?))
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
            PlanNode::CurrentDatabase {} => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::CurrentDatabase,
                    self.config.clone(),
                )?),
            })),
            PlanNode::SetCurrentDatabase { database_name } => {
                Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(CatalogCommandNode::try_new(
                        CatalogCommand::SetCurrentDatabase {
                            database_name: database_name.into(),
                        },
                        self.config.clone(),
                    )?),
                }))
            }
            PlanNode::ListDatabases {
                catalog,
                database_pattern,
            } => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::ListDatabases {
                        catalog: catalog.map(|x| x.into()),
                        database_pattern,
                    },
                    self.config.clone(),
                )?),
            })),
            PlanNode::ListTables {
                database,
                table_pattern,
            } => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::ListTables {
                        database: database.map(build_schema_reference).transpose()?,
                        table_pattern,
                    },
                    self.config.clone(),
                )?),
            })),
            PlanNode::ListFunctions {
                database,
                function_pattern,
            } => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::ListFunctions {
                        database: database.map(build_schema_reference).transpose()?,
                        function_pattern,
                    },
                    self.config.clone(),
                )?),
            })),
            PlanNode::ListColumns { table } => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::ListColumns {
                        table: build_table_reference(table)?,
                    },
                    self.config.clone(),
                )?),
            })),
            PlanNode::GetDatabase { database } => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::GetDatabase {
                        database: build_schema_reference(database)?,
                    },
                    self.config.clone(),
                )?),
            })),
            PlanNode::GetTable { table } => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::GetTable {
                        table: build_table_reference(table)?,
                    },
                    self.config.clone(),
                )?),
            })),
            PlanNode::GetFunction { function } => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::GetFunction {
                        function: build_table_reference(function)?,
                    },
                    self.config.clone(),
                )?),
            })),
            PlanNode::DatabaseExists { database } => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::DatabaseExists {
                        database: build_schema_reference(database)?,
                    },
                    self.config.clone(),
                )?),
            })),
            PlanNode::TableExists { table } => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::TableExists {
                        table: build_table_reference(table)?,
                    },
                    self.config.clone(),
                )?),
            })),
            PlanNode::FunctionExists { function } => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::FunctionExists {
                        function: build_table_reference(function)?,
                    },
                    self.config.clone(),
                )?),
            })),
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
                Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(CatalogCommandNode::try_new(
                        CatalogCommand::CreateTable {
                            table: build_table_reference(table)?,
                            plan: Arc::new(self.resolve_plan(read, state).await?),
                        },
                        self.config.clone(),
                    )?),
                }))
            }
            PlanNode::DropTemporaryView {
                view,
                is_global,
                if_exists,
            } => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::DropTemporaryView {
                        view: build_table_reference(view)?,
                        is_global,
                        if_exists,
                    },
                    self.config.clone(),
                )?),
            })),
            PlanNode::DropDatabase {
                database,
                if_exists,
                cascade,
            } => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::DropDatabase {
                        database: build_schema_reference(database)?,
                        if_exists,
                        cascade,
                    },
                    self.config.clone(),
                )?),
            })),
            PlanNode::DropFunction {
                function,
                if_exists,
                is_temporary,
            } => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::DropFunction {
                        function: build_table_reference(function)?,
                        if_exists,
                        is_temporary,
                    },
                    self.config.clone(),
                )?),
            })),
            PlanNode::DropTable {
                table,
                if_exists,
                purge,
            } => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::DropTable {
                        table: build_table_reference(table)?,
                        if_exists,
                        purge,
                    },
                    self.config.clone(),
                )?),
            })),
            PlanNode::DropView { view, if_exists } => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::DropView {
                        view: build_table_reference(view)?,
                        if_exists,
                    },
                    self.config.clone(),
                )?),
            })),
            PlanNode::RecoverPartitions { .. } => {
                Err(PlanError::todo("PlanNode::RecoverPartitions"))
            }
            PlanNode::IsCached { .. } => Err(PlanError::todo("PlanNode::IsCached")),
            PlanNode::CacheTable { .. } => Err(PlanError::todo("PlanNode::CacheTable")),
            PlanNode::UncacheTable { .. } => Err(PlanError::todo("PlanNode::UncacheTable")),
            PlanNode::ClearCache {} => Err(PlanError::todo("PlanNode::ClearCache")),
            PlanNode::RefreshTable { .. } => Err(PlanError::todo("PlanNode::RefreshTable")),
            PlanNode::RefreshByPath { .. } => Err(PlanError::todo("PlanNode::RefreshByPath")),
            PlanNode::CurrentCatalog => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::CurrentCatalog,
                    self.config.clone(),
                )?),
            })),
            PlanNode::SetCurrentCatalog { catalog_name } => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::SetCurrentCatalog {
                        catalog_name: catalog_name.into(),
                    },
                    self.config.clone(),
                )?),
            })),
            PlanNode::ListCatalogs { catalog_pattern } => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::ListCatalogs { catalog_pattern },
                    self.config.clone(),
                )?),
            })),
            PlanNode::CreateDatabase {
                database,
                if_not_exists,
                comment,
                location,
                properties,
            } => {
                let properties = properties.into_iter().collect::<Vec<_>>();
                Ok(LogicalPlan::Extension(Extension {
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
                }))
            }
            PlanNode::RegisterFunction(_) => Err(PlanError::todo("register function")),
            PlanNode::RegisterTableFunction(_) => Err(PlanError::todo("register table function")),
            PlanNode::CreateTemporaryView { .. } => Err(PlanError::todo("create temporary view")),
            PlanNode::Write { .. } => Err(PlanError::todo("write")),
            PlanNode::Empty { produce_one_row } => {
                Ok(LogicalPlan::EmptyRelation(plan::EmptyRelation {
                    produce_one_row,
                    schema: DFSchemaRef::new(DFSchema::empty()),
                }))
            }
            PlanNode::Values(values) => {
                let schema = DFSchema::empty();
                let values = values
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|value| -> PlanResult<_> {
                                Ok(self.resolve_expression(value, &schema, state)?.unalias())
                            })
                            .collect::<PlanResult<Vec<_>>>()
                    })
                    .collect::<PlanResult<Vec<_>>>()?;
                Ok(LogicalPlanBuilder::values(values)?.build()?)
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
                        .iter()
                        .zip(columns.iter())
                        .map(|(col, name)| Expr::Column(col.clone()).alias(name.clone()))
                        .collect();
                    LogicalPlan::Projection(plan::Projection::try_new(expr, Arc::new(input))?)
                };
                Ok(LogicalPlan::SubqueryAlias(plan::SubqueryAlias::try_new(
                    Arc::new(input),
                    build_table_reference(spec::ObjectName::new_unqualified(name))?,
                )?))
            }
        }
    }
}
