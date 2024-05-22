use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{
    DataType, Fields, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
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
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion_common::{
    Column, Constraints, DFSchema, DFSchemaRef, ParamValues, ScalarValue, TableReference,
};
use datafusion_expr::{build_join_schema, DdlStatement, TableType};

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::expression::{from_spark_expression, from_spark_literal_to_scalar};
use crate::extension::analyzer::alias::rewrite_multi_alias;
use crate::extension::analyzer::explode::rewrite_explode;
use crate::extension::analyzer::wildcard::rewrite_wildcard;
use crate::extension::analyzer::window::rewrite_window;
use crate::schema::{cast_record_batch, from_spark_built_in_data_type};
use crate::spark::connect as sc;
use crate::spark::connect::execute_plan_response::ArrowBatch;
use crate::spark::connect::Relation;
use crate::sql::data_type::parse_spark_schema;
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
    relation: &Relation,
) -> SparkResult<LogicalPlan> {
    use crate::spark::connect::relation::RelType;

    let Relation { common, rel_type } = relation;
    let _common = common;
    let state = ctx.state();
    let rel_type = rel_type.as_ref().required("relation type")?;
    match rel_type {
        RelType::Read(read) => {
            use sc::read::ReadType;

            let _is_streaming = read.is_streaming;
            match read.read_type.as_ref().required("read type")? {
                ReadType::NamedTable(named_table) => {
                    let unparsed_identifier: &String = &named_table.unparsed_identifier;
                    let options: &HashMap<String, String> = &named_table.options;
                    if !options.is_empty() {
                        // TODO: Handle options
                        return Err(SparkError::unsupported("table options"));
                    }
                    let table_reference = TableReference::from(unparsed_identifier);
                    let df: DataFrame = ctx.table(table_reference.clone()).await?;
                    Ok(df.into_optimized_plan()?)
                }
                ReadType::DataSource(source) => {
                    let urls = source.paths.clone().to_urls()?;
                    if urls.is_empty() {
                        return Err(SparkError::invalid("empty data source paths"));
                    }
                    let (format, extension): (Arc<dyn FileFormat>, _) = match source
                        .format
                        .as_ref()
                        .map(|f| f.as_str())
                    {
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
        RelType::Project(project) => {
            let input = project.input.as_ref().required("projection input")?;
            let input = from_spark_relation(ctx, input).await?;
            let schema = input.schema();
            let expr: Vec<Expr> = project
                .expressions
                .iter()
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
        RelType::Filter(filter) => {
            let input = filter.input.as_ref().required("filter input")?;
            let condition = filter.condition.as_ref().required("filter condition")?;
            let input = from_spark_relation(ctx, input).await?;
            let schema = input.schema();
            let predicate = from_spark_expression(condition, schema)?;
            let filter = plan::Filter::try_new(predicate, Arc::new(input))?;
            Ok(LogicalPlan::Filter(filter))
        }
        RelType::Join(join) => {
            use sc::join::JoinType;

            let left = join.left.as_ref().required("join left")?;
            let right = join.right.as_ref().required("join right")?;
            let left = from_spark_relation(ctx, left).await?;
            let right = from_spark_relation(ctx, right).await?;
            let join_type = match JoinType::try_from(join.join_type)? {
                JoinType::Inner => plan::JoinType::Inner,
                JoinType::LeftOuter => plan::JoinType::Left,
                JoinType::RightOuter => plan::JoinType::Right,
                JoinType::FullOuter => plan::JoinType::Full,
                JoinType::LeftSemi => plan::JoinType::LeftSemi,
                JoinType::LeftAnti => plan::JoinType::LeftAnti,
                // use inner join type to build the schema for cross join
                JoinType::Cross => plan::JoinType::Inner,
                JoinType::Unspecified => return Err(SparkError::invalid("join type")),
            };
            let schema = build_join_schema(&left.schema(), &right.schema(), &join_type)?;
            if join.join_type == JoinType::Cross as i32 {
                if join.join_condition.is_some() {
                    return Err(SparkError::invalid("cross join with join condition"));
                }
                if join.using_columns.len() > 0 {
                    return Err(SparkError::invalid("cross join with using columns"));
                }
                if join.join_data_type.is_some() {
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
                if join.join_condition.is_some() && join.using_columns.is_empty() {
                    let condition = join
                        .join_condition
                        .as_ref()
                        .map(|c| from_spark_expression(c, &schema))
                        .transpose()?;
                    (vec![], condition, plan::JoinConstraint::On)
                } else if join.join_condition.is_none() && !join.using_columns.is_empty() {
                    let on = join
                        .using_columns
                        .iter()
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
        RelType::SetOp(_) => {
            return Err(SparkError::todo("set operation"));
        }
        RelType::Sort(sort) => {
            // TODO: handle sort.is_global
            let input = sort.input.as_ref().required("sort input")?;
            let input = from_spark_relation(ctx, input).await?;
            let schema = input.schema();
            let expr = sort
                .order
                .iter()
                .map(|o| {
                    let o = Box::from(o.clone());
                    let expr = sc::Expression {
                        expr_type: Some(sc::expression::ExprType::SortOrder(o)),
                    };
                    from_spark_expression(&expr, schema)
                })
                .collect::<SparkResult<_>>()?;
            Ok(LogicalPlan::Sort(plan::Sort {
                expr,
                input: Arc::new(input),
                fetch: None,
            }))
        }
        RelType::Limit(limit) => {
            let input = limit.input.as_ref().required("limit input")?;
            let input = from_spark_relation(ctx, input).await?;
            Ok(LogicalPlan::Limit(plan::Limit {
                skip: 0,
                fetch: Some(limit.limit as usize),
                input: Arc::new(input),
            }))
        }
        RelType::Aggregate(aggregate) => {
            use sc::aggregate::GroupType;

            if aggregate.pivot.is_some() {
                return Err(SparkError::todo("pivot"));
            }
            let group_type = GroupType::try_from(aggregate.group_type).required("group type")?;
            if group_type != GroupType::Unspecified && group_type != GroupType::Groupby {
                return Err(SparkError::todo("unsupported aggregate group type"));
            }
            let input = aggregate.input.as_ref().required("aggregate input")?;
            let input = from_spark_relation(ctx, input).await?;
            let schema = input.schema();
            let group_expr = aggregate
                .grouping_expressions
                .iter()
                .map(|e| from_spark_expression(e, schema))
                .collect::<SparkResult<_>>()?;
            let aggr_expr = aggregate
                .aggregate_expressions
                .iter()
                .map(|e| from_spark_expression(e, schema))
                .collect::<SparkResult<_>>()?;
            Ok(LogicalPlan::Aggregate(Aggregate::try_new(
                Arc::new(input),
                group_expr,
                aggr_expr,
            )?))
        }
        RelType::Sql(sc::Sql {
            query,
            args,
            pos_args,
        }) => {
            let query = &query.replace(" database ", " SCHEMA ");
            let query = &query.replace(" DATABASE ", " SCHEMA ");
            let statement = Parser::new(&GenericDialect {})
                .try_with_sql(query)?
                .parse_statement()?;
            let plan = state
                .statement_to_plan(Statement::Statement(Box::new(statement)))
                .await?;
            if pos_args.len() == 0 && args.len() == 0 {
                Ok(plan)
            } else if pos_args.len() > 0 && args.len() == 0 {
                let params = pos_args
                    .iter()
                    .map(|arg| from_spark_literal_to_scalar(arg))
                    .collect::<SparkResult<_>>()?;
                Ok(plan.with_param_values(ParamValues::List(params))?)
            } else if pos_args.len() == 0 && args.len() > 0 {
                let params = args
                    .iter()
                    .map(|(i, arg)| from_spark_literal_to_scalar(arg).map(|v| (i.clone(), v)))
                    .collect::<SparkResult<_>>()?;
                Ok(plan.with_param_values(ParamValues::Map(params))?)
            } else {
                Err(SparkError::invalid(
                    "both positional and named arguments are specified",
                ))
            }
        }
        RelType::LocalRelation(local) => {
            let batches = if let Some(data) = &local.data {
                read_arrow_batches(data.clone())?
            } else {
                vec![]
            };
            let (schema, batches) = if let Some(schema) = local.schema.as_ref() {
                let schema = parse_spark_schema(schema)?;
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
        RelType::Sample(_) => {
            return Err(SparkError::todo("sample"));
        }
        RelType::Offset(offset) => {
            let input = offset.input.as_ref().required("offset input")?;
            let input = from_spark_relation(ctx, input).await?;
            Ok(LogicalPlan::Limit(plan::Limit {
                skip: offset.offset as usize,
                fetch: None,
                input: Arc::new(input),
            }))
        }
        RelType::Deduplicate(dedup) => {
            let input = dedup.input.as_ref().required("deduplicate input")?;
            let input = from_spark_relation(ctx, input).await?;
            let schema = input.schema();
            let within_watermark = dedup.within_watermark.unwrap_or(false);
            if within_watermark {
                return Err(SparkError::todo("deduplicate within watermark"));
            }
            let column_names = &dedup.column_names;
            let all_columns_as_keys = dedup.all_columns_as_keys.unwrap_or(false);
            let distinct = if column_names.len() > 0 && !all_columns_as_keys {
                let on_expr: Vec<Expr> = column_names
                    .iter()
                    .map(|name| {
                        // TODO: handle qualified column names
                        let field = schema.field_with_unqualified_name(name)?;
                        Ok(Expr::Column(field.qualified_column()))
                    })
                    .collect::<SparkResult<_>>()?;
                let select_expr: Vec<Expr> = schema
                    .fields()
                    .iter()
                    .map(|field| Expr::Column(field.qualified_column()))
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
        RelType::Range(range) => {
            let start = range.start.unwrap_or(0);
            let end = range.end;
            let step = range.step;
            // TODO: use parallelism in Spark configuration as the default
            let num_partitions = range.num_partitions.unwrap_or(1);
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
        RelType::SubqueryAlias(sub) => {
            let input = sub.input.as_ref().required("subquery alias input")?;
            // TODO: handle quoted identifiers
            let mut alias = sub.alias.clone();
            for q in sub.qualifier.iter().rev() {
                alias = format!("{}.{}", q, alias);
            }
            Ok(LogicalPlan::SubqueryAlias(plan::SubqueryAlias::try_new(
                Arc::new(from_spark_relation(ctx, input).await?),
                alias,
            )?))
        }
        RelType::Repartition(repartition) => {
            let input = repartition.input.as_ref().required("repartition input")?;
            let input = from_spark_relation(ctx, input).await?;
            // TODO: handle shuffle partition
            let _ = repartition.shuffle;
            Ok(LogicalPlan::Repartition(plan::Repartition {
                input: Arc::new(input),
                partitioning_scheme: plan::Partitioning::RoundRobinBatch(
                    repartition.num_partitions as usize,
                ),
            }))
        }
        RelType::ToDf(to_df) => {
            let input = to_df.input.as_ref().required("input relation")?;
            let names = &to_df.column_names;
            let input = from_spark_relation(ctx, input).await?;
            let schema = input.schema();
            if names.len() != schema.fields().len() {
                return Err(SparkError::invalid(format!(
                    "number of column names ({}) does not match number of columns ({})",
                    names.len(),
                    schema.fields().len()
                )));
            }
            let expr: Vec<Expr> = schema
                .fields()
                .iter()
                .zip(names.iter())
                .map(|(field, name)| Expr::Column(field.qualified_column()).alias(name))
                .collect();
            Ok(LogicalPlan::Projection(plan::Projection::try_new(
                expr,
                Arc::new(input),
            )?))
        }
        RelType::WithColumnsRenamed(rename) => {
            let input = rename.input.as_ref().required("input relation")?;
            let input = from_spark_relation(ctx, input).await?;
            let rename_map = &rename.rename_columns_map;
            let schema = input.schema();
            let expr: Vec<Expr> = schema
                .fields()
                .iter()
                .map(|field| {
                    let name = field.name();
                    let column = Expr::Column(field.qualified_column());
                    match rename_map.get(name) {
                        Some(n) => column.alias(n),
                        None => column,
                    }
                })
                .collect();
            Ok(LogicalPlan::Projection(plan::Projection::try_new(
                expr,
                Arc::new(input),
            )?))
        }
        RelType::ShowString(_) => {
            return Err(SparkError::todo("show string"));
        }
        RelType::Drop(drop) => {
            let input = drop.input.as_ref().required("input relation")?;
            let input = from_spark_relation(ctx, input).await?;
            let schema = input.schema();
            let columns_to_drop = &drop.columns;
            if !columns_to_drop.is_empty() {
                return Err(SparkError::todo("drop column expressions"));
            }
            let names_to_drop = &drop.column_names;
            let expr: Vec<Expr> = schema
                .fields()
                .iter()
                .filter(|field| !names_to_drop.contains(&field.name()))
                .map(|field| Expr::Column(field.qualified_column()))
                .collect();
            Ok(LogicalPlan::Projection(plan::Projection::try_new(
                expr,
                Arc::new(input),
            )?))
        }
        RelType::Tail(_tail) => {
            return Err(SparkError::todo("tail"));
        }
        RelType::WithColumns(columns) => {
            let input = columns.input.as_ref().required("input relation")?;
            let input = from_spark_relation(ctx, input).await?;
            let schema = input.schema();
            let mut aliases: HashMap<String, (Expr, bool)> = columns
                .aliases
                .iter()
                .map(|x| {
                    // TODO: handle quoted identifiers
                    let name = x.name.join(".");
                    let expr = from_spark_expression(
                        x.expr.as_ref().required("column alias expression")?,
                        schema,
                    )?;
                    // TODO: handle metadata
                    let _ = x.metadata;
                    Ok((name.clone(), (expr, false)))
                })
                .collect::<SparkResult<_>>()?;
            let mut expr: Vec<Expr> = schema
                .fields()
                .iter()
                .map(|field| {
                    let name = field.name();
                    match aliases.get_mut(name) {
                        Some((e, exists)) => {
                            *exists = true;
                            e.clone().alias(name)
                        }
                        None => Expr::Column(field.qualified_column()),
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
        RelType::Hint(_) => {
            return Err(SparkError::todo("hint"));
        }
        RelType::Unpivot(_) => {
            return Err(SparkError::todo("unpivot"));
        }
        RelType::ToSchema(to_schema) => {
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
        RelType::RepartitionByExpression(repartition) => {
            let input = repartition.input.as_ref().required("repartition input")?;
            let input = from_spark_relation(ctx, input).await?;
            let schema = input.schema();
            let expr: Vec<Expr> = repartition
                .partition_exprs
                .iter()
                .map(|e| from_spark_expression(e, schema))
                .collect::<SparkResult<_>>()?;
            let num_partitions = repartition
                .num_partitions
                .ok_or_else(|| SparkError::todo("rebalance partitioning by expression"))?;
            Ok(LogicalPlan::Repartition(plan::Repartition {
                input: Arc::new(input),
                partitioning_scheme: plan::Partitioning::Hash(expr, num_partitions as usize),
            }))
        }
        RelType::MapPartitions(_) => {
            return Err(SparkError::todo("map partitions"));
        }
        RelType::CollectMetrics(_) => {
            return Err(SparkError::todo("collect metrics"));
        }
        RelType::Parse(_) => {
            return Err(SparkError::todo("parse"));
        }
        RelType::GroupMap(_) => {
            return Err(SparkError::todo("group map"));
        }
        RelType::CoGroupMap(_) => {
            return Err(SparkError::todo("co-group map"));
        }
        RelType::WithWatermark(_) => {
            return Err(SparkError::todo("with watermark"));
        }
        RelType::ApplyInPandasWithState(_) => {
            return Err(SparkError::todo("apply in pandas with state"));
        }
        RelType::HtmlString(_) => {
            return Err(SparkError::todo("html string"));
        }
        RelType::CachedLocalRelation(_) => {
            return Err(SparkError::todo("cached local relation"));
        }
        RelType::CachedRemoteRelation(_) => {
            return Err(SparkError::todo("cached remote relation"));
        }
        RelType::CommonInlineUserDefinedTableFunction(_) => {
            return Err(SparkError::todo(
                "common inline user defined table function",
            ));
        }
        RelType::FillNa(_) => {
            return Err(SparkError::todo("fill na"));
        }
        RelType::DropNa(_) => {
            return Err(SparkError::todo("drop na"));
        }
        RelType::Replace(_) => {
            return Err(SparkError::todo("replace"));
        }
        RelType::Summary(_) => {
            return Err(SparkError::todo("summary"));
        }
        RelType::Crosstab(_) => {
            return Err(SparkError::todo("crosstab"));
        }
        RelType::Describe(_) => {
            return Err(SparkError::todo("describe"));
        }
        RelType::Cov(_) => {
            return Err(SparkError::todo("cov"));
        }
        RelType::Corr(_) => {
            return Err(SparkError::todo("corr"));
        }
        RelType::ApproxQuantile(_) => {
            return Err(SparkError::todo("approx quantile"));
        }
        RelType::FreqItems(_) => {
            return Err(SparkError::todo("freq items"));
        }
        RelType::SampleBy(_) => {
            return Err(SparkError::todo("sample by"));
        }
        RelType::Catalog(catalog) => {
            use sc::catalog::CatType;
            match catalog.cat_type.as_ref().required("catalog type")? {
                CatType::CurrentDatabase(_current_database) => {
                    let results: DataFrame = ctx
                        .sql("SELECT CAST($1 AS STRING)")
                        .await?
                        .with_param_values(vec![(
                            "1",
                            ScalarValue::Utf8(Some(
                                state.config().options().catalog.default_schema.to_string(),
                            )),
                        )])?;
                    Ok(results.into_optimized_plan()?)
                }
                CatType::SetCurrentDatabase(set_current_database) => {
                    let _db_name = set_current_database.db_name.as_str();
                    // TODO: Uncomment when we upgrade to DataFusion 38.0.0
                    // ctx.state().config().options_mut().catalog.default_schema = db_name;
                    Ok(LogicalPlan::EmptyRelation(plan::EmptyRelation {
                        produce_one_row: false,
                        schema: DFSchemaRef::new(DFSchema::empty()),
                    }))
                }
                CatType::ListDatabases(list_databases) => {
                    let database_pattern: Option<&str> = list_databases.pattern.as_deref();
                    let catalog_databases: Vec<CatalogDatabase> =
                        list_catalog_databases(None, database_pattern, &ctx)?;
                    let provider: MemTable = create_catalog_database_memtable(catalog_databases)?;
                    Ok(LogicalPlan::TableScan(plan::TableScan::try_new(
                        UNNAMED_TABLE,
                        provider_as_source(Arc::new(provider)),
                        None,
                        vec![],
                        None,
                    )?))
                }
                CatType::ListTables(list_tables) => {
                    let db_name: Option<&str> = list_tables.db_name.as_deref();
                    let (catalog_name, database_name): (String, String) =
                        parse_optional_db_name_with_defaults(
                            db_name,
                            &state.config().options().catalog.default_catalog,
                            &state.config().options().catalog.default_schema,
                        )?;
                    let table_pattern: Option<&str> = list_tables.pattern.as_deref();
                    let catalog_tables: Vec<CatalogTable> = list_catalog_tables(
                        Some(&catalog_name),
                        Some(&database_name),
                        table_pattern,
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
                CatType::ListFunctions(_) => Err(SparkError::todo("CatType::ListFunctions")),
                CatType::ListColumns(list_columns) => {
                    let table_name = list_columns.table_name.as_str();
                    let (catalog_name, database_name): (String, String) =
                        parse_optional_db_name_with_defaults(
                            list_columns.db_name.as_deref(),
                            &state.config().options().catalog.default_catalog,
                            &state.config().options().catalog.default_schema,
                        )?;
                    let catalog_table_columns: Vec<CatalogTableColumn> =
                        list_catalog_table_columns(
                            &catalog_name,
                            &database_name,
                            &table_name,
                            &ctx,
                        )
                        .await?;
                    let provider: MemTable = create_catalog_column_memtable(catalog_table_columns)?;
                    Ok(LogicalPlan::TableScan(plan::TableScan::try_new(
                        UNNAMED_TABLE,
                        provider_as_source(Arc::new(provider)),
                        None,
                        vec![],
                        None,
                    )?))
                }
                CatType::GetDatabase(get_database) => {
                    let db_name = get_database.db_name.as_str();
                    let catalog_databases: Vec<CatalogDatabase> =
                        get_catalog_database(&db_name, &ctx)?;
                    let provider: MemTable = create_catalog_database_memtable(catalog_databases)?;
                    Ok(LogicalPlan::TableScan(plan::TableScan::try_new(
                        UNNAMED_TABLE,
                        provider_as_source(Arc::new(provider)),
                        None,
                        vec![],
                        None,
                    )?))
                }
                CatType::GetTable(get_table) => {
                    let table_name = get_table.table_name.as_str();
                    let (catalog_name, database_name): (String, String) =
                        parse_optional_db_name_with_defaults(
                            get_table.db_name.as_deref(),
                            &state.config().options().catalog.default_catalog,
                            &state.config().options().catalog.default_schema,
                        )?;
                    let catalog_tables: Vec<CatalogTable> =
                        get_catalog_table(&table_name, &catalog_name, &database_name, &ctx).await?;
                    let provider: MemTable = create_catalog_table_memtable(catalog_tables)?;
                    Ok(LogicalPlan::TableScan(plan::TableScan::try_new(
                        UNNAMED_TABLE,
                        provider_as_source(Arc::new(provider)),
                        None,
                        vec![],
                        None,
                    )?))
                }
                CatType::GetFunction(_) => Err(SparkError::todo("CatType::GetFunction")),
                CatType::DatabaseExists(database_exists) => {
                    let db_name = database_exists.db_name.as_str();
                    let catalog_databases: Vec<CatalogDatabase> =
                        get_catalog_database(&db_name, &ctx)?;
                    let db_exists = !catalog_databases.is_empty();
                    let results: DataFrame = ctx
                        .sql("SELECT CAST($1 AS BOOLEAN)")
                        .await?
                        .with_param_values(vec![("1", ScalarValue::Boolean(Some(db_exists)))])?;
                    Ok(results.into_optimized_plan()?)
                }
                CatType::TableExists(table_exists) => {
                    let table_name = table_exists.table_name.as_str();
                    let (catalog_name, database_name): (String, String) =
                        parse_optional_db_name_with_defaults(
                            table_exists.db_name.as_deref(),
                            &state.config().options().catalog.default_catalog,
                            &state.config().options().catalog.default_schema,
                        )?;
                    let catalog_tables: Vec<CatalogTable> =
                        get_catalog_table(&table_name, &catalog_name, &database_name, &ctx).await?;
                    let table_exists = !catalog_tables.is_empty();
                    let results: DataFrame = ctx
                        .sql("SELECT CAST($1 AS BOOLEAN)")
                        .await?
                        .with_param_values(vec![("1", ScalarValue::Boolean(Some(table_exists)))])?;
                    Ok(results.into_optimized_plan()?)
                }
                CatType::FunctionExists(_) => Err(SparkError::todo("CatType::FunctionExists")),
                CatType::CreateExternalTable(_create_external_table) => {
                    // Same as create table essentially.
                    Err(SparkError::todo("CatType::CreateExternalTable"))
                }
                CatType::CreateTable(create_table) => {
                    let table_name = create_table.table_name.as_str();
                    let table_ref: TableReference = TableReference::from(table_name.to_string());
                    let (table_type, location): (TableType, Option<&str>) =
                        if let Some(path) = create_table.path.as_deref() {
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
                    let source: &str = create_table.source.as_deref().unwrap_or("parquet");
                    let description: &str = create_table.description.as_deref().unwrap_or("");
                    let fields: Fields = match create_table.schema.as_ref() {
                        Some(schema) => {
                            let data_type = from_spark_built_in_data_type(schema)?;
                            match data_type {
                                DataType::Struct(fields) => fields,
                                _ => return Err(SparkError::invalid("external table schema")),
                            }
                        }
                        None => Fields::empty(),
                    };
                    let options: CaseInsensitiveHashMap<String> =
                        CaseInsensitiveHashMap::from(create_table.options.clone());

                    let mut metadata: HashMap<String, String> = HashMap::new();
                    metadata.insert("description".to_string(), description.to_string());
                    let schema = ArrowSchema::new_with_metadata(fields, metadata);
                    let batch = RecordBatch::new_empty(ArrowSchemaRef::new(schema));
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
                            if_not_exists: false, // TODO: Check if exists in options
                            or_replace: false,    // TODO: Check if exists in options
                            column_defaults: vec![], // TODO: Check if exists in options
                        },
                    )))
                }
                CatType::DropTempView(drop_temp_view) => {
                    // TODO: DataFusion returns an empty DataFrame on DropView
                    //  But Spark expects a Boolean value.
                    //  We can do this for now instead of having to create a LogicalPlan Extension
                    let drop_view_plan = LogicalPlan::Ddl(DdlStatement::DropView(plan::DropView {
                        name: TableReference::from(drop_temp_view.view_name.to_string()),
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
                        .with_param_values(vec![("1", result)])?;
                    Ok(df.into_optimized_plan()?)
                }
                CatType::DropGlobalTempView(drop_global_temp_view) => {
                    // TODO: DataFusion returns an empty DataFrame on DropView
                    //  But Spark expects a Boolean value.
                    //  We can do this for now instead of having to create a LogicalPlan Extension
                    let drop_view_plan = LogicalPlan::Ddl(DdlStatement::DropView(plan::DropView {
                        name: TableReference::from(drop_global_temp_view.view_name.to_string()),
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
                        .with_param_values(vec![("1", result)])?;
                    Ok(df.into_optimized_plan()?)
                }
                CatType::RecoverPartitions(_) => {
                    Err(SparkError::todo("CatType::RecoverPartitions"))
                }
                CatType::IsCached(_) => Err(SparkError::todo("CatType::IsCached")),
                CatType::CacheTable(_) => Err(SparkError::todo("CatType::CacheTable")),
                CatType::UncacheTable(_) => Err(SparkError::todo("CatType::UncacheTable")),
                CatType::ClearCache(_) => Err(SparkError::todo("CatType::ClearCache")),
                CatType::RefreshTable(_) => Err(SparkError::todo("CatType::RefreshTable")),
                CatType::RefreshByPath(_) => Err(SparkError::todo("CatType::RefreshByPath")),
                CatType::CurrentCatalog(_) => {
                    let results: DataFrame = ctx
                        .sql("SELECT CAST($1 AS STRING)")
                        .await?
                        .with_param_values(vec![(
                            "1",
                            ScalarValue::Utf8(Some(
                                state.config().options().catalog.default_catalog.to_string(),
                            )),
                        )])?;
                    Ok(results.into_optimized_plan()?)
                }
                CatType::SetCurrentCatalog(set_current_catalog) => {
                    let _catalog_name = set_current_catalog.catalog_name.as_str();
                    // TODO: Uncomment when we upgrade to DataFusion 38.0.0
                    // ctx.state().config().options_mut().catalog.default_catalog = catalog_name;
                    Ok(LogicalPlan::EmptyRelation(plan::EmptyRelation {
                        produce_one_row: false,
                        schema: DFSchemaRef::new(DFSchema::empty()),
                    }))
                }
                CatType::ListCatalogs(list_catalogs) => {
                    let pattern: Option<&str> = list_catalogs.pattern.as_deref();
                    let catalogs_metadata: Vec<CatalogMetadata> =
                        list_catalogs_metadata(pattern, &ctx)?;
                    let provider: MemTable = create_catalog_metadata_memtable(catalogs_metadata)?;
                    Ok(LogicalPlan::TableScan(plan::TableScan::try_new(
                        UNNAMED_TABLE,
                        provider_as_source(Arc::new(provider)),
                        None,
                        vec![],
                        None,
                    )?))
                }
            }
        }
        RelType::Extension(_) => {
            return Err(SparkError::unsupported("Spark relation extension"));
        }
        RelType::Unknown(_) => {
            return Err(SparkError::unsupported("unknown Spark relation"));
        }
    }
}
