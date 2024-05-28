use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes as adt;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::execution::context::{DataFilePaths, SessionContext};
use datafusion::logical_expr::{
    logical_plan as plan, Aggregate, Expr, Extension, LogicalPlan, UNNAMED_TABLE,
};
use datafusion_common::{
    Column, DFSchema, DFSchemaRef, ParamValues, ScalarValue, SchemaReference, TableReference,
};
use datafusion_expr::{build_join_schema, LogicalPlanBuilder};
use framework_common::spec;

use crate::error::{SparkError, SparkResult};
use crate::expression::from_spark_expression;
use crate::extension::analyzer::alias::rewrite_multi_alias;
use crate::extension::analyzer::explode::rewrite_explode;
use crate::extension::analyzer::wildcard::rewrite_wildcard;
use crate::extension::analyzer::window::rewrite_window;
use crate::extension::logical::{CatalogCommand, CatalogCommandNode, RangeNode};
use crate::schema::cast_record_batch;
use crate::spark::connect::execute_plan_response::ArrowBatch;

pub(crate) fn read_arrow_batches(data: Vec<u8>) -> Result<Vec<RecordBatch>, SparkError> {
    let cursor = Cursor::new(data);
    let mut reader = StreamReader::try_new(cursor, None)?;
    let mut batches = Vec::new();
    while let Some(batch) = reader.next() {
        batches.push(batch?);
    }
    Ok(batches)
}

fn build_schema_reference(name: spec::ObjectName) -> SparkResult<SchemaReference> {
    let names: Vec<String> = name.into();
    match names.as_slice() {
        [a] => Ok(SchemaReference::Bare {
            schema: Arc::from(a.as_str()),
        }),
        [a, b] => Ok(SchemaReference::Full {
            catalog: Arc::from(a.as_str()),
            schema: Arc::from(b.as_str()),
        }),
        _ => Err(SparkError::invalid(format!(
            "schema reference: {:?}",
            names
        ))),
    }
}

fn build_table_reference(name: spec::ObjectName) -> SparkResult<TableReference> {
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
        _ => Err(SparkError::invalid(format!("table reference: {:?}", names))),
    }
}

pub(crate) async fn to_arrow_batch(batch: &RecordBatch) -> SparkResult<ArrowBatch> {
    let mut output = ArrowBatch::default();
    {
        let cursor = Cursor::new(&mut output.data);
        let mut writer = StreamWriter::try_new(cursor, batch.schema().as_ref())?;
        writer.write(batch)?;
        output.row_count += batch.num_rows() as i64;
        writer.finish()?;
    }
    Ok(output)
}

#[async_recursion]
pub(crate) async fn from_spark_relation(
    ctx: &SessionContext,
    plan: spec::Plan,
) -> SparkResult<LogicalPlan> {
    use spec::PlanNode;

    let state = ctx.state();
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
                        return Err(SparkError::todo("table options"));
                    }
                    let table_reference = build_table_reference(identifier)?;
                    let df: DataFrame = ctx.table(table_reference).await?;
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
                        return Err(SparkError::invalid("empty data source paths"));
                    }
                    let (format, extension): (Arc<dyn FileFormat>, _) = match format.as_deref() {
                        Some("json") => (Arc::new(JsonFormat::default()), ".json"),
                        Some("csv") => (Arc::new(CsvFormat::default()), ".csv"),
                        Some("parquet") => (Arc::new(ParquetFormat::new()), ".parquet"),
                        _ => return Err(SparkError::unsupported("unsupported data source format")),
                    };
                    let options = ListingOptions::new(format).with_file_extension(extension);
                    // TODO: use provided schema if available
                    let schema = options.infer_schema(&state, &urls[0]).await?;
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
                Some(x) => from_spark_relation(ctx, *x).await?,
                None => LogicalPlan::EmptyRelation(plan::EmptyRelation {
                    // allows literal projection with no input
                    produce_one_row: true,
                    schema: DFSchemaRef::new(DFSchema::empty()),
                }),
            };
            let schema = input.schema();
            let expr: Vec<Expr> = expressions
                .into_iter()
                .map(|e| from_spark_expression(e, schema))
                .collect::<SparkResult<_>>()?;
            let expr = rewrite_multi_alias(expr)?;
            let (input, expr) = rewrite_wildcard(input, expr)?;
            let (input, expr) = rewrite_explode(input, expr)?;
            let (input, expr) = rewrite_window(input, expr)?;
            Ok(LogicalPlan::Projection(plan::Projection::try_new(
                expr,
                Arc::new(input),
            )?))
        }
        PlanNode::Filter { input, condition } => {
            let input = from_spark_relation(ctx, *input).await?;
            let schema = input.schema();
            let predicate = from_spark_expression(condition, schema)?;
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

            let left = from_spark_relation(ctx, *left).await?;
            let right = from_spark_relation(ctx, *right).await?;
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
            let schema = build_join_schema(&left.schema(), &right.schema(), &join_type)?;
            if is_cross_join {
                if join_condition.is_some() {
                    return Err(SparkError::invalid("cross join with join condition"));
                }
                if using_columns.len() > 0 {
                    return Err(SparkError::invalid("cross join with using columns"));
                }
                if join_data_type.is_some() {
                    return Err(SparkError::invalid("cross join with join data type"));
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
                        .map(|c| from_spark_expression(c, &schema))
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
                    return Err(SparkError::invalid(
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
        PlanNode::SetOperation { .. } => {
            return Err(SparkError::todo("set operation"));
        }
        PlanNode::Sort {
            input,
            order,
            is_global: _,
        } => {
            // TODO: handle sort.is_global
            let input = from_spark_relation(ctx, *input).await?;
            let schema = input.schema();
            let expr = order
                .into_iter()
                .map(|o| {
                    let expr = spec::Expr::SortOrder(o);
                    from_spark_expression(expr, schema)
                })
                .collect::<SparkResult<_>>()?;
            Ok(LogicalPlan::Sort(plan::Sort {
                expr,
                input: Arc::new(input),
                fetch: None,
            }))
        }
        PlanNode::Limit { input, limit } => {
            let input = from_spark_relation(ctx, *input).await?;
            Ok(LogicalPlan::Limit(plan::Limit {
                skip: 0,
                fetch: Some(limit as usize),
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
                return Err(SparkError::todo("pivot"));
            }
            if group_type != GroupType::GroupBy {
                return Err(SparkError::todo("unsupported aggregate group type"));
            }
            let input = from_spark_relation(ctx, *input).await?;
            let schema = input.schema();
            let group_expr = grouping_expressions
                .into_iter()
                .map(|e| from_spark_expression(e, schema))
                .collect::<SparkResult<_>>()?;
            let aggr_expr = aggregate_expressions
                .into_iter()
                .map(|e| from_spark_expression(e, schema))
                .collect::<SparkResult<_>>()?;
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
            let input = from_spark_relation(ctx, *input).await?;
            let input = if positional_arguments.len() > 0 {
                let params = positional_arguments
                    .into_iter()
                    .map(|arg| -> SparkResult<ScalarValue> { Ok(arg.try_into()?) })
                    .collect::<SparkResult<_>>()?;
                input.with_param_values(ParamValues::List(params))?
            } else {
                input
            };
            let input = if named_arguments.len() > 0 {
                let params = named_arguments
                    .into_iter()
                    .map(|(name, arg)| -> SparkResult<(String, ScalarValue)> {
                        Ok((name, arg.try_into()?))
                    })
                    .collect::<SparkResult<_>>()?;
                input.with_param_values(ParamValues::Map(params))?
            } else {
                input
            };
            Ok(input)
        }
        PlanNode::LocalRelation { data, schema } => {
            let batches = if let Some(data) = data {
                read_arrow_batches(data)?
            } else {
                vec![]
            };
            let (schema, batches) = if let Some(schema) = schema {
                let schema: adt::SchemaRef = Arc::new(schema.try_into()?);
                let batches = batches
                    .into_iter()
                    .map(|b| cast_record_batch(b, schema.clone()))
                    .collect::<SparkResult<_>>()?;
                (schema, batches)
            } else if let [batch, ..] = batches.as_slice() {
                (batch.schema(), batches)
            } else {
                return Err(SparkError::invalid("missing schema for local relation"));
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
            return Err(SparkError::todo("sample"));
        }
        PlanNode::Offset { input, offset } => {
            let input = from_spark_relation(ctx, *input).await?;
            Ok(LogicalPlan::Limit(plan::Limit {
                skip: offset as usize,
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
            let input = from_spark_relation(ctx, *input).await?;
            let schema = input.schema();
            if within_watermark {
                return Err(SparkError::todo("deduplicate within watermark"));
            }
            let distinct = if column_names.len() > 0 && !all_columns_as_keys {
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
            } else if column_names.len() == 0 && all_columns_as_keys {
                plan::Distinct::All(Arc::new(input))
            } else {
                return Err(SparkError::invalid(
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
                return Err(SparkError::invalid(format!(
                    "invalid number of partitions: {}",
                    num_partitions
                )));
            }
            Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(RangeNode::try_new(start, end, step, num_partitions as u32)?),
            }))
        }
        PlanNode::SubqueryAlias {
            input,
            alias,
            qualifier,
        } => Ok(LogicalPlan::SubqueryAlias(plan::SubqueryAlias::try_new(
            Arc::new(from_spark_relation(ctx, *input).await?),
            build_table_reference(spec::ObjectName::new_qualified(alias, qualifier))?,
        )?)),
        PlanNode::Repartition {
            input,
            num_partitions,
            shuffle: _,
        } => {
            let input = from_spark_relation(ctx, *input).await?;
            // TODO: handle shuffle partition
            Ok(LogicalPlan::Repartition(plan::Repartition {
                input: Arc::new(input),
                partitioning_scheme: plan::Partitioning::RoundRobinBatch(num_partitions as usize),
            }))
        }
        PlanNode::ToDf {
            input,
            column_names,
        } => {
            let input = from_spark_relation(ctx, *input).await?;
            let schema = input.schema();
            if column_names.len() != schema.fields().len() {
                return Err(SparkError::invalid(format!(
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
            let input = from_spark_relation(ctx, *input).await?;
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
        PlanNode::ShowString { .. } => {
            return Err(SparkError::todo("show string"));
        }
        PlanNode::Drop {
            input,
            columns,
            column_names,
        } => {
            let input = from_spark_relation(ctx, *input).await?;
            let schema = input.schema();
            if !columns.is_empty() {
                return Err(SparkError::todo("drop column expressions"));
            }
            let column_names: Vec<String> = column_names.into_iter().map(|x| x.into()).collect();
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
            return Err(SparkError::todo("tail"));
        }
        PlanNode::WithColumns { input, aliases } => {
            let input = from_spark_relation(ctx, *input).await?;
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
                                return Err(SparkError::invalid("multi-alias for column"));
                            }
                        }
                        _ => {
                            return Err(SparkError::invalid("alias expression expected for column"))
                        }
                    };
                    let expr = from_spark_expression(expr, schema)?;
                    Ok((name.into(), (expr, false)))
                })
                .collect::<SparkResult<_>>()?;
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
            return Err(SparkError::todo("hint"));
        }
        PlanNode::Unpivot { .. } => {
            return Err(SparkError::todo("unpivot"));
        }
        PlanNode::ToSchema { .. } => {
            return Err(SparkError::todo("to schema"));
            // TODO: Close but doesn't quite work.
            // let input = to_schema.input.as_ref().required("input relation")?;
            // let input = from_spark_relation(ctx, input).await?;
            // let schema = to_schema.schema.as_ref().required("schema")?;
            // let schema: DataType = from_spark_built_in_data_type(schema)?;
            // let fields = match &schema {
            //     DataType::Struct(fields) => fields,
            //     _ => return Err(SparkError::invalid("expected struct type")),
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
            let input = from_spark_relation(ctx, *input).await?;
            let schema = input.schema();
            let expr: Vec<Expr> = partition_expressions
                .into_iter()
                .map(|e| from_spark_expression(e, schema))
                .collect::<SparkResult<_>>()?;
            let num_partitions = num_partitions
                .ok_or_else(|| SparkError::todo("rebalance partitioning by expression"))?;
            Ok(LogicalPlan::Repartition(plan::Repartition {
                input: Arc::new(input),
                partitioning_scheme: plan::Partitioning::Hash(expr, num_partitions as usize),
            }))
        }
        PlanNode::MapPartitions { .. } => {
            return Err(SparkError::todo("map partitions"));
        }
        PlanNode::CollectMetrics { .. } => {
            return Err(SparkError::todo("collect metrics"));
        }
        PlanNode::Parse { .. } => {
            return Err(SparkError::todo("parse"));
        }
        PlanNode::GroupMap { .. } => {
            return Err(SparkError::todo("group map"));
        }
        PlanNode::CoGroupMap { .. } => {
            return Err(SparkError::todo("co-group map"));
        }
        PlanNode::WithWatermark { .. } => {
            return Err(SparkError::todo("with watermark"));
        }
        PlanNode::ApplyInPandasWithState { .. } => {
            return Err(SparkError::todo("apply in pandas with state"));
        }
        PlanNode::HtmlString { .. } => {
            return Err(SparkError::todo("html string"));
        }
        PlanNode::CachedLocalRelation { .. } => {
            return Err(SparkError::todo("cached local relation"));
        }
        PlanNode::CachedRemoteRelation { .. } => {
            return Err(SparkError::todo("cached remote relation"));
        }
        PlanNode::CommonInlineUserDefinedTableFunction { .. } => {
            return Err(SparkError::todo(
                "common inline user defined table function",
            ));
        }
        PlanNode::FillNa { .. } => {
            return Err(SparkError::todo("fill na"));
        }
        PlanNode::DropNa { .. } => {
            return Err(SparkError::todo("drop na"));
        }
        PlanNode::ReplaceNa { .. } => {
            return Err(SparkError::todo("replace"));
        }
        PlanNode::StatSummary { .. } => {
            return Err(SparkError::todo("summary"));
        }
        PlanNode::StatCrosstab { .. } => {
            return Err(SparkError::todo("crosstab"));
        }
        PlanNode::StatDescribe { .. } => {
            return Err(SparkError::todo("describe"));
        }
        PlanNode::StatCov { .. } => {
            return Err(SparkError::todo("cov"));
        }
        PlanNode::StatCorr { .. } => {
            return Err(SparkError::todo("corr"));
        }
        PlanNode::StatApproxQuantile { .. } => {
            return Err(SparkError::todo("approx quantile"));
        }
        PlanNode::StatFreqItems { .. } => {
            return Err(SparkError::todo("freq items"));
        }
        PlanNode::StatSampleBy { .. } => {
            return Err(SparkError::todo("sample by"));
        }
        PlanNode::CurrentDatabase {} => Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(
                CatalogCommand::CurrentDatabase,
            )?),
        })),
        PlanNode::SetCurrentDatabase { database_name } => Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(
                CatalogCommand::SetCurrentDatabase {
                    database_name: database_name.into(),
                },
            )?),
        })),
        PlanNode::ListDatabases {
            catalog,
            database_pattern,
        } => Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(
                CatalogCommand::ListDatabases {
                    catalog: catalog.map(|x| x.into()),
                    database_pattern,
                },
            )?),
        })),
        PlanNode::ListTables {
            database,
            table_pattern,
        } => Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(CatalogCommand::ListTables {
                database: database.map(|x| build_schema_reference(x)).transpose()?,
                table_pattern,
            })?),
        })),
        PlanNode::ListFunctions {
            database,
            function_pattern,
        } => Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(
                CatalogCommand::ListFunctions {
                    database: database.map(|x| build_schema_reference(x)).transpose()?,
                    function_pattern,
                },
            )?),
        })),
        PlanNode::ListColumns { table } => Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(CatalogCommand::ListColumns {
                table: build_table_reference(table)?,
            })?),
        })),
        PlanNode::GetDatabase { database } => Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(CatalogCommand::GetDatabase {
                database: build_schema_reference(database)?,
            })?),
        })),
        PlanNode::GetTable { table } => Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(CatalogCommand::GetTable {
                table: build_table_reference(table)?,
            })?),
        })),
        PlanNode::GetFunction { function } => Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(CatalogCommand::GetFunction {
                function: build_table_reference(function)?,
            })?),
        })),
        PlanNode::DatabaseExists { database } => Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(
                CatalogCommand::DatabaseExists {
                    database: build_schema_reference(database)?,
                },
            )?),
        })),
        PlanNode::TableExists { table } => Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(CatalogCommand::TableExists {
                table: build_table_reference(table)?,
            })?),
        })),
        PlanNode::FunctionExists { function } => Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(
                CatalogCommand::FunctionExists {
                    function: build_table_reference(function)?,
                },
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
            let read = spec::Plan::new(spec::PlanNode::Read {
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
                node: Arc::new(CatalogCommandNode::try_new(CatalogCommand::CreateTable {
                    table: build_table_reference(table)?,
                    plan: Arc::new(from_spark_relation(ctx, read).await?),
                })?),
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
            )?),
        })),
        PlanNode::DropDatabase {
            database,
            if_exists,
            cascade,
        } => Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(CatalogCommand::DropDatabase {
                database: build_schema_reference(database)?,
                if_exists,
                cascade,
            })?),
        })),
        PlanNode::DropFunction {
            function,
            if_exists,
            is_temporary,
        } => Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(CatalogCommand::DropFunction {
                function: build_table_reference(function)?,
                if_exists,
                is_temporary,
            })?),
        })),
        PlanNode::DropTable {
            table,
            if_exists,
            purge,
        } => Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(CatalogCommand::DropTable {
                table: build_table_reference(table)?,
                if_exists,
                purge,
            })?),
        })),
        PlanNode::DropView { view, if_exists } => Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(CatalogCommand::DropView {
                view: build_table_reference(view)?,
                if_exists,
            })?),
        })),
        PlanNode::RecoverPartitions { .. } => Err(SparkError::todo("PlanNode::RecoverPartitions")),
        PlanNode::IsCached { .. } => Err(SparkError::todo("PlanNode::IsCached")),
        PlanNode::CacheTable { .. } => Err(SparkError::todo("PlanNode::CacheTable")),
        PlanNode::UncacheTable { .. } => Err(SparkError::todo("PlanNode::UncacheTable")),
        PlanNode::ClearCache {} => Err(SparkError::todo("PlanNode::ClearCache")),
        PlanNode::RefreshTable { .. } => Err(SparkError::todo("PlanNode::RefreshTable")),
        PlanNode::RefreshByPath { .. } => Err(SparkError::todo("PlanNode::RefreshByPath")),
        PlanNode::CurrentCatalog => Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(CatalogCommand::CurrentCatalog)?),
        })),
        PlanNode::SetCurrentCatalog { catalog_name } => Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(
                CatalogCommand::SetCurrentCatalog {
                    catalog_name: catalog_name.into(),
                },
            )?),
        })),
        PlanNode::ListCatalogs { catalog_pattern } => Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(CatalogCommand::ListCatalogs {
                catalog_pattern,
            })?),
        })),
        PlanNode::CreateDatabase {
            database,
            if_not_exists,
            comment,
            location,
            properties,
        } => {
            let properties = properties
                .into_iter()
                .map(|(k, v)| (k, v))
                .collect::<Vec<_>>();
            Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(CatalogCommandNode::try_new(
                    CatalogCommand::CreateDatabase {
                        database: build_schema_reference(database)?,
                        if_not_exists,
                        comment,
                        location,
                        properties,
                    },
                )?),
            }))
        }
        PlanNode::RegisterFunction(_) => Err(SparkError::todo("register function")),
        PlanNode::RegisterTableFunction(_) => Err(SparkError::todo("register table function")),
        PlanNode::CreateTemporaryView { .. } => Err(SparkError::todo("create temporary view")),
        PlanNode::Write { .. } => Err(SparkError::todo("write")),
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
                        .map(|value| from_spark_expression(value, &schema))
                        .collect::<SparkResult<Vec<_>>>()
                })
                .collect::<SparkResult<Vec<_>>>()?;
            Ok(LogicalPlanBuilder::values(values)?.build()?)
        }
        PlanNode::TableAlias {
            input,
            name,
            columns,
        } => {
            let input = from_spark_relation(ctx, *input).await?;
            let schema = input.schema();
            let input = if columns.is_empty() {
                input
            } else {
                if columns.len() != schema.fields().len() {
                    return Err(SparkError::invalid(format!(
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
