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
    Column, Constraints, DFSchema, DFSchemaRef, ParamValues, ScalarValue, TableReference,
};
use datafusion_expr::{build_join_schema, DdlStatement, TableType};
use framework_common::spec;

use crate::error::{SparkError, SparkResult};
use crate::expression::from_spark_expression;
use crate::extension::analyzer::alias::rewrite_multi_alias;
use crate::extension::analyzer::explode::rewrite_explode;
use crate::extension::analyzer::wildcard::rewrite_wildcard;
use crate::extension::analyzer::window::rewrite_window;
use crate::schema::cast_record_batch;
use crate::spark::connect::execute_plan_response::ArrowBatch;
use crate::sql::session_catalog::catalog::list_catalogs_metadata;
use crate::sql::session_catalog::column::{list_catalog_table_columns, CatalogTableColumn};
use crate::sql::session_catalog::database::{
    get_catalog_database, list_catalog_databases, parse_optional_db_name_with_defaults,
};
use crate::sql::session_catalog::table::{
    catalog_table_type_to_table_type, get_catalog_table, list_catalog_tables, CatalogTableType,
};
use crate::sql::session_catalog::{
    catalog::{create_catalog_metadata_memtable, CatalogMetadata},
    column::create_catalog_column_memtable,
    database::{create_catalog_database_memtable, CatalogDatabase},
    table::{create_catalog_table_memtable, CatalogTable},
};
use crate::utils::CaseInsensitiveHashMap;

pub(crate) fn read_arrow_batches(data: Vec<u8>) -> Result<Vec<RecordBatch>, SparkError> {
    let cursor = Cursor::new(data);
    let mut reader = StreamReader::try_new(cursor, None)?;
    let mut batches = Vec::new();
    while let Some(batch) = reader.next() {
        batches.push(batch?);
    }
    Ok(batches)
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
                    unparsed_identifier,
                    options,
                } => {
                    if !options.is_empty() {
                        return Err(SparkError::todo("table options"));
                    }
                    let table_reference = TableReference::from(unparsed_identifier);
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
            // TODO: handle rewrites in SQL parsing
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
                            .columns_with_unqualified_name(name.as_str())
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
                node: Arc::new(crate::extension::logical::RangeNode::try_new(
                    start,
                    end,
                    step,
                    num_partitions as u32,
                )?),
            }))
        }
        PlanNode::SubqueryAlias {
            input,
            mut alias,
            qualifier,
        } => {
            // TODO: handle quoted identifiers
            for q in qualifier.iter().rev() {
                alias = format!("{}.{}", q, alias);
            }
            Ok(LogicalPlan::SubqueryAlias(plan::SubqueryAlias::try_new(
                Arc::new(from_spark_relation(ctx, *input).await?),
                alias,
            )?))
        }
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
                .map(|(col, name)| Expr::Column(col.clone()).alias(name))
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
                    Ok((name, (expr, false)))
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
        PlanNode::CurrentDatabase {} => {
            let results: DataFrame = ctx
                .sql("SELECT CAST($1 AS STRING)")
                .await?
                .with_param_values(vec![ScalarValue::Utf8(Some(
                    state.config().options().catalog.default_schema.to_string(),
                ))])?;
            Ok(results.into_optimized_plan()?)
        }
        PlanNode::SetCurrentDatabase { database_name } => {
            ctx.state_weak_ref()
                .upgrade()
                .ok_or_else(|| SparkError::internal("invalid session context"))?
                .write()
                .config_mut()
                .options_mut()
                .catalog
                .default_schema = database_name;
            Ok(LogicalPlan::EmptyRelation(plan::EmptyRelation {
                produce_one_row: false,
                schema: DFSchemaRef::new(DFSchema::empty()),
            }))
        }
        PlanNode::ListDatabases { pattern } => {
            let databases: Vec<CatalogDatabase> =
                list_catalog_databases(None, pattern.as_deref(), &ctx)?;
            let provider: MemTable = create_catalog_database_memtable(databases)?;
            Ok(LogicalPlan::TableScan(plan::TableScan::try_new(
                UNNAMED_TABLE,
                provider_as_source(Arc::new(provider)),
                None,
                vec![],
                None,
            )?))
        }
        PlanNode::ListTables {
            database_name,
            pattern,
        } => {
            let (catalog_name, database_name): (String, String) =
                parse_optional_db_name_with_defaults(
                    database_name.as_deref(),
                    &state.config().options().catalog.default_catalog,
                    &state.config().options().catalog.default_schema,
                )?;
            let catalog_tables: Vec<CatalogTable> = list_catalog_tables(
                Some(&catalog_name),
                Some(&database_name),
                pattern.as_deref(),
                &ctx,
            )
            .await?;
            let provider: MemTable = create_catalog_table_memtable(catalog_tables)?;
            Ok(LogicalPlan::TableScan(plan::TableScan::try_new(
                UNNAMED_TABLE,
                provider_as_source(Arc::new(provider)),
                None,
                vec![],
                None,
            )?))
        }
        PlanNode::ListFunctions { .. } => Err(SparkError::todo("PlanNode::ListFunctions")),
        PlanNode::ListColumns {
            table_name,
            database_name,
        } => {
            let (catalog_name, database_name): (String, String) =
                parse_optional_db_name_with_defaults(
                    database_name.as_deref(),
                    &state.config().options().catalog.default_catalog,
                    &state.config().options().catalog.default_schema,
                )?;
            let columns: Vec<CatalogTableColumn> =
                list_catalog_table_columns(&catalog_name, &database_name, &table_name, &ctx)
                    .await?;
            let provider: MemTable = create_catalog_column_memtable(columns)?;
            Ok(LogicalPlan::TableScan(plan::TableScan::try_new(
                UNNAMED_TABLE,
                provider_as_source(Arc::new(provider)),
                None,
                vec![],
                None,
            )?))
        }
        PlanNode::GetDatabase { database_name } => {
            let databases: Vec<CatalogDatabase> = get_catalog_database(&database_name, &ctx)?;
            let provider: MemTable = create_catalog_database_memtable(databases)?;
            Ok(LogicalPlan::TableScan(plan::TableScan::try_new(
                UNNAMED_TABLE,
                provider_as_source(Arc::new(provider)),
                None,
                vec![],
                None,
            )?))
        }
        PlanNode::GetTable {
            table_name,
            database_name,
        } => {
            let (catalog_name, database_name): (String, String) =
                parse_optional_db_name_with_defaults(
                    database_name.as_deref(),
                    &state.config().options().catalog.default_catalog,
                    &state.config().options().catalog.default_schema,
                )?;
            let tables: Vec<CatalogTable> =
                get_catalog_table(&table_name, &catalog_name, &database_name, &ctx).await?;
            let provider: MemTable = create_catalog_table_memtable(tables)?;
            Ok(LogicalPlan::TableScan(plan::TableScan::try_new(
                UNNAMED_TABLE,
                provider_as_source(Arc::new(provider)),
                None,
                vec![],
                None,
            )?))
        }
        PlanNode::GetFunction { .. } => Err(SparkError::todo("PlanNode::GetFunction")),
        PlanNode::DatabaseExists { database_name } => {
            let databases: Vec<CatalogDatabase> = get_catalog_database(&database_name, &ctx)?;
            let db_exists = !databases.is_empty();
            let results: DataFrame = ctx
                .sql("SELECT CAST($1 AS BOOLEAN)")
                .await?
                .with_param_values(vec![ScalarValue::Boolean(Some(db_exists))])?;
            Ok(results.into_optimized_plan()?)
        }
        PlanNode::TableExists {
            table_name,
            database_name,
        } => {
            let (catalog_name, database_name): (String, String) =
                parse_optional_db_name_with_defaults(
                    database_name.as_deref(),
                    &state.config().options().catalog.default_catalog,
                    &state.config().options().catalog.default_schema,
                )?;
            let tables: Vec<CatalogTable> =
                get_catalog_table(&table_name, &catalog_name, &database_name, &ctx).await?;
            let table_exists = !tables.is_empty();
            let results: DataFrame = ctx
                .sql("SELECT CAST($1 AS BOOLEAN)")
                .await?
                .with_param_values(vec![ScalarValue::Boolean(Some(table_exists))])?;
            Ok(results.into_optimized_plan()?)
        }
        PlanNode::FunctionExists { .. } => Err(SparkError::todo("PlanNode::FunctionExists")),
        PlanNode::CreateTable {
            table_name,
            path,
            source,
            description,
            schema,
            options,
        } => {
            let table_ref = TableReference::from(table_name.to_string());
            let (_table_type, _location): (TableType, Option<&str>) =
                if let Some(path) = path.as_deref() {
                    // TODO: Should covert to a "Path" then uri
                    return Err(SparkError::todo(format!(
                        "CreateTable (External) with path: {:?}",
                        path
                    )));
                } else {
                    (
                        catalog_table_type_to_table_type(CatalogTableType::MANAGED),
                        None,
                    )
                };
            // TODO: use spark.sql.sources.default to get the default source
            let _source: &str = source.as_deref().unwrap_or("parquet");
            let description: &str = description.as_deref().unwrap_or("");
            let _options: CaseInsensitiveHashMap<String> = CaseInsensitiveHashMap::from(options);

            let mut metadata: HashMap<String, String> = HashMap::new();
            metadata.insert("description".to_string(), description.to_string());
            let schema: adt::Schema = schema.unwrap_or_default().try_into()?;
            let schema = adt::SchemaRef::new(schema.with_metadata(metadata));

            let batch = RecordBatch::new_empty(schema);
            let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;

            Ok(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(
                plan::CreateMemoryTable {
                    name: table_ref,
                    constraints: Constraints::empty(), // TODO: Check if exists in options
                    input: Arc::new(LogicalPlan::TableScan(plan::TableScan::try_new(
                        UNNAMED_TABLE,
                        provider_as_source(Arc::new(table)),
                        None,
                        vec![],
                        None,
                    )?)),
                    if_not_exists: false,    // TODO: Check if exists in options
                    or_replace: false,       // TODO: Check if exists in options
                    column_defaults: vec![], // TODO: Check if exists in options
                },
            )))
        }
        PlanNode::DropTemporaryView { view_name } => {
            // TODO: DataFusion returns an empty DataFrame on DropView
            //  But Spark expects a Boolean value.
            //  We can do this for now instead of having to create a LogicalPlan Extension
            let drop_view_plan = LogicalPlan::Ddl(DdlStatement::DropView(plan::DropView {
                name: TableReference::from(view_name),
                if_exists: false,
                schema: DFSchemaRef::new(DFSchema::empty()),
            }));
            let result = match ctx.execute_logical_plan(drop_view_plan).await {
                Ok(_) => ScalarValue::Boolean(Some(true)),
                Err(_) => ScalarValue::Boolean(Some(false)),
            };
            let df: DataFrame = ctx
                .sql("SELECT CAST($1 AS BOOLEAN)")
                .await?
                .with_param_values(vec![result])?;
            Ok(df.into_optimized_plan()?)
        }
        PlanNode::DropGlobalTemporaryView { view_name } => {
            // TODO: DataFusion returns an empty DataFrame on DropView
            //  But Spark expects a Boolean value.
            //  We can do this for now instead of having to create a LogicalPlan Extension
            let drop_view_plan = LogicalPlan::Ddl(DdlStatement::DropView(plan::DropView {
                name: TableReference::from(view_name),
                if_exists: false,
                schema: DFSchemaRef::new(DFSchema::empty()),
            }));
            let result = match ctx.execute_logical_plan(drop_view_plan).await {
                Ok(_) => ScalarValue::Boolean(Some(true)),
                Err(_) => ScalarValue::Boolean(Some(false)),
            };
            let df: DataFrame = ctx
                .sql("SELECT CAST($1 AS BOOLEAN)")
                .await?
                .with_param_values(vec![result])?;
            Ok(df.into_optimized_plan()?)
        }
        PlanNode::RecoverPartitions { .. } => Err(SparkError::todo("PlanNode::RecoverPartitions")),
        PlanNode::IsCached { .. } => Err(SparkError::todo("PlanNode::IsCached")),
        PlanNode::CacheTable { .. } => Err(SparkError::todo("PlanNode::CacheTable")),
        PlanNode::UncacheTable { .. } => Err(SparkError::todo("PlanNode::UncacheTable")),
        PlanNode::ClearCache {} => Err(SparkError::todo("PlanNode::ClearCache")),
        PlanNode::RefreshTable { .. } => Err(SparkError::todo("PlanNode::RefreshTable")),
        PlanNode::RefreshByPath { .. } => Err(SparkError::todo("PlanNode::RefreshByPath")),
        PlanNode::CurrentCatalog {} => {
            let results: DataFrame = ctx
                .sql("SELECT CAST($1 AS STRING)")
                .await?
                .with_param_values(vec![ScalarValue::Utf8(Some(
                    state.config().options().catalog.default_catalog.to_string(),
                ))])?;
            Ok(results.into_optimized_plan()?)
        }
        PlanNode::SetCurrentCatalog { catalog_name } => {
            ctx.state_weak_ref()
                .upgrade()
                .ok_or_else(|| SparkError::internal("invalid session context"))?
                .write()
                .config_mut()
                .options_mut()
                .catalog
                .default_catalog = catalog_name;
            Ok(LogicalPlan::EmptyRelation(plan::EmptyRelation {
                produce_one_row: false,
                schema: DFSchemaRef::new(DFSchema::empty()),
            }))
        }
        PlanNode::ListCatalogs { pattern } => {
            let metadata: Vec<CatalogMetadata> = list_catalogs_metadata(pattern.as_deref(), &ctx)?;
            let provider: MemTable = create_catalog_metadata_memtable(metadata)?;
            Ok(LogicalPlan::TableScan(plan::TableScan::try_new(
                UNNAMED_TABLE,
                provider_as_source(Arc::new(provider)),
                None,
                vec![],
                None,
            )?))
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
    }
}
