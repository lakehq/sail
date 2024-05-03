use arrow::datatypes::{Schema, SchemaRef};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::common::{ParamValues, TableReference};
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::datasource::provider::DefaultTableFactory;
use datafusion::datasource::{provider_as_source, DefaultTableSource};
use datafusion::execution::context::{DataFilePaths, SessionContext};
use datafusion::logical_expr::{
    logical_plan as plan, Aggregate, Expr, Extension, LogicalPlan, UNNAMED_TABLE,
};
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser::ast::Ident;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion_expr::LogicalPlanBuilder;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::expression::{from_spark_expression, from_spark_literal_to_scalar};
use crate::extension::analyzer::alias::rewrite_multi_alias;
use crate::extension::analyzer::explode::rewrite_explode;
use crate::extension::analyzer::wildcard::rewrite_wildcard;
use crate::schema::parse_spark_schema_string;
use crate::spark::connect as sc;
use crate::spark::connect::execute_plan_response::ArrowBatch;
use crate::spark::connect::Relation;
use crate::sql::new_sql_parser;

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

#[derive(Default, Clone, Debug)]
struct CaseInsensitiveStringMap(HashMap<String, String>);
impl CaseInsensitiveStringMap {
    fn new(map: &HashMap<String, String>) -> Self {
        let mut case_insensitive_map = HashMap::new();
        for (key, value) in map {
            case_insensitive_map.insert(key.to_lowercase(), value.clone());
        }
        CaseInsensitiveStringMap(case_insensitive_map)
    }

    fn insert(&mut self, key: String, value: String) {
        self.0.insert(key.to_lowercase(), value);
    }

    fn get(&self, key: &str) -> Option<&String> {
        self.0.get(&key.to_lowercase())
    }
}

#[derive(Clone, Debug)]
struct UnresolvedRelation {
    multipart_identifier: Vec<String>,
    options: CaseInsensitiveStringMap,
    is_streaming: bool,
}
impl UnresolvedRelation {
    fn new(
        multipart_identifier: Vec<String>,
        options: CaseInsensitiveStringMap,
        is_streaming: bool,
    ) -> Self {
        UnresolvedRelation {
            multipart_identifier,
            options,
            is_streaming,
        }
    }
}

#[async_recursion]
pub(crate) async fn from_spark_relation(
    ctx: &SessionContext,
    relation: &Relation,
) -> SparkResult<LogicalPlan> {
    use crate::spark::connect::relation::RelType;

    let Relation { common, rel_type } = relation;
    let state = ctx.state();
    let rel_type = rel_type.as_ref().required("relation type")?;
    match rel_type {
        RelType::Read(read) => {
            use sc::read::ReadType;

            let is_streaming = read.is_streaming;
            match &read.read_type.as_ref().required("read type")? {
                ReadType::NamedTable(named_table) => {
                    let unparsed_identifier: &String = &named_table.unparsed_identifier;
                    let options: &HashMap<String, String> = &named_table.options;

                    let case_insensitive_options: CaseInsensitiveStringMap =
                        CaseInsensitiveStringMap::new(options);

                    let multipart_identifier: Vec<String> = Parser::new(&GenericDialect {})
                        .try_with_sql(unparsed_identifier)?
                        .parse_multipart_identifier()?
                        .into_iter()
                        .map(|ident| ident.value)
                        .collect();

                    let table_reference: TableReference = match multipart_identifier.len() {
                        0 => {
                            return Err(SparkError::invalid("No table name found in NamedTable"));
                        }
                        1 => TableReference::Bare {
                            table: multipart_identifier[0].clone().into(),
                        },
                        2 => TableReference::Partial {
                            schema: multipart_identifier[0].clone().into(),
                            table: multipart_identifier[1].clone().into(),
                        },
                        _ => TableReference::Full {
                            catalog: multipart_identifier[0].clone().into(),
                            schema: multipart_identifier[1].clone().into(),
                            table: multipart_identifier[2].clone().into(),
                        },
                    };

                    let unresolved_relation: UnresolvedRelation = UnresolvedRelation::new(
                        multipart_identifier.clone(),
                        case_insensitive_options,
                        is_streaming,
                    );

                    let schema: SchemaRef = Arc::new(Schema::empty());
                    Ok(LogicalPlan::TableScan(plan::TableScan::try_new(
                        multipart_identifier.last().unwrap().clone(),
                        Arc::new(DefaultTableSource::new(Arc::new(EmptyTable::new(schema)))),
                        None,
                        vec![],
                        None,
                    )?))

                    // return Err(SparkError::invalid("empty data source paths"));
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
        RelType::Join(_) => {
            return Err(SparkError::todo("join"));
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
            println!(
                "RelType::Limit CHECK HERE INPUT: {:?}, LIMIT: {:?}",
                input, limit.limit
            );
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
            let statement = new_sql_parser(query)?.parse_one_statement()?;
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
            let schema = if let [batch, ..] = batches.as_slice() {
                batch.schema()
            } else {
                let schema = local.schema.as_ref().required("local relation schema")?;
                parse_spark_schema_string(schema)?
            };
            let provider = datafusion::datasource::MemTable::try_new(schema, vec![batches])?;
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
        RelType::Tail(tail) => {
            let input: &Box<Relation> = tail.input.as_ref().required("limit input")?;
            let input: LogicalPlan = from_spark_relation(ctx, input).await?;
            let limit: i32 = tail.limit;
            println!("CHECK HERE INPUT: {:?}, LIMIT: {:?}", input, limit);
            Ok(LogicalPlan::Limit(plan::Limit {
                skip: 0, // TODO: THIS SHOULDNT BE 0!! JUST TESTING
                fetch: Some(limit as usize),
                input: Arc::new(input),
            }))
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
        RelType::ToSchema(_) => {
            return Err(SparkError::todo("to schema"));
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
        RelType::Catalog(_) => {
            return Err(SparkError::todo("catalog"));
        }
        RelType::Extension(_) => {
            return Err(SparkError::unsupported("Spark relation extension"));
        }
        RelType::Unknown(_) => {
            return Err(SparkError::unsupported("unknown Spark relation"));
        }
    }
}
