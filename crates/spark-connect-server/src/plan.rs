use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::common::ParamValues;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::datasource::provider_as_source;
use datafusion::execution::context::{DataFilePaths, SessionContext};
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::{logical_plan as plan, LogicalPlanBuilder, UNNAMED_TABLE};
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::expression::{
    from_spark_expression, from_spark_literal_to_scalar, from_spark_sort_order,
};
use crate::spark::connect as sc;
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

pub(crate) async fn to_arrow_batch(
    batch: &RecordBatch,
    schema: SchemaRef,
) -> SparkResult<ArrowBatch> {
    let mut output = ArrowBatch::default();
    {
        let cursor = Cursor::new(&mut output.data);
        let mut writer = StreamWriter::try_new(cursor, schema.as_ref())?;
        writer.write(batch)?;
        output.row_count += batch.num_rows() as i64;
        writer.finish()?;
    }
    Ok(output)
}

#[async_recursion]
pub(crate) async fn from_spark_relation(
    ctx: &SessionContext,
    relation: &sc::Relation,
) -> SparkResult<LogicalPlan> {
    use crate::spark::connect::relation::RelType;

    let sc::Relation { common, rel_type } = relation;
    let _ = common;
    let state = ctx.state();
    let rel_type = rel_type.as_ref().required("relation type")?;
    match rel_type {
        RelType::Read(read) => {
            use sc::read::ReadType;

            let _ = read.is_streaming;
            match read.read_type.as_ref().required("read type")? {
                ReadType::NamedTable(_) => {
                    return Err(SparkError::todo("named table read type"));
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
                    Ok(
                        LogicalPlanBuilder::scan(
                            UNNAMED_TABLE,
                            provider_as_source(provider),
                            None,
                        )?
                        .build()?,
                    )
                }
            }
        }
        RelType::Project(project) => {
            let input = project.input.as_ref().required("projection input")?;
            let input = from_spark_relation(ctx, input).await?;
            let expressions = project
                .expressions
                .iter()
                .map(|e| from_spark_expression(e, input.schema()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(plan::builder::project(input, expressions)?)
        }
        RelType::Filter(filter) => {
            let input = filter.input.as_ref().required("filter input")?;
            let condition = filter.condition.as_ref().required("filter condition")?;
            let input = from_spark_relation(ctx, input).await?;
            let predicate = from_spark_expression(condition, input.schema())?;
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
            let expr = sort
                .order
                .iter()
                .map(|o| from_spark_sort_order(o, input.schema()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(LogicalPlan::Sort(plan::Sort {
                expr,
                input: Arc::new(input),
                fetch: None,
            }))
        }
        RelType::Limit(limit) => {
            let input = limit.input.as_ref().required("limit input")?;
            Ok(LogicalPlan::Limit(plan::Limit {
                skip: 0,
                fetch: Some(limit.limit as usize),
                input: Arc::new(from_spark_relation(ctx, input).await?),
            }))
        }
        RelType::Aggregate(_) => {
            return Err(SparkError::todo("aggregate"));
        }
        RelType::Sql(sc::Sql {
            query,
            args,
            pos_args,
        }) => {
            let dialect = GenericDialect {};
            let mut statements = Parser::parse_sql(&dialect, query)?;
            if statements.len() == 1 {
                let plan = ctx
                    .state()
                    .statement_to_plan(Statement::Statement(Box::new(statements.pop().unwrap())))
                    .await?;
                if pos_args.len() == 0 && args.len() == 0 {
                    Ok(plan)
                } else if pos_args.len() > 0 && args.len() == 0 {
                    let params = pos_args
                        .iter()
                        .map(|arg| from_spark_literal_to_scalar(arg))
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(plan.with_param_values(ParamValues::List(params))?)
                } else if pos_args.len() == 0 && args.len() > 0 {
                    let params = args
                        .iter()
                        .map(|(i, arg)| from_spark_literal_to_scalar(arg).map(|v| (i.clone(), v)))
                        .collect::<Result<HashMap<_, _>, _>>()?;
                    Ok(plan.with_param_values(ParamValues::Map(params))?)
                } else {
                    Err(SparkError::invalid(
                        "both positional and named arguments are specified",
                    ))
                }
            } else {
                Err(SparkError::todo("multiple statements in SQL query"))
            }
        }
        RelType::LocalRelation(sc::LocalRelation { data, schema }) => {
            let batches = if let Some(data) = data {
                read_arrow_batches(data.clone())?
            } else {
                vec![]
            };
            let schema = if let [batch, ..] = batches.as_slice() {
                batch.schema()
            } else {
                let _ = schema.as_ref().required("local relation schema")?;
                return Err(SparkError::todo("parse schema from spark schema"));
            };
            let provider = datafusion::datasource::MemTable::try_new(schema, vec![batches])?;
            Ok(LogicalPlanBuilder::scan(
                UNNAMED_TABLE,
                provider_as_source(Arc::new(provider)),
                None,
            )?
            .build()?)
        }
        RelType::Sample(_) => {
            return Err(SparkError::todo("sample"));
        }
        RelType::Offset(offset) => {
            let input = offset.input.as_ref().required("offset input")?;
            Ok(LogicalPlan::Limit(plan::Limit {
                skip: offset.offset as usize,
                fetch: None,
                input: Arc::new(from_spark_relation(ctx, input).await?),
            }))
        }
        RelType::Deduplicate(_) => {
            return Err(SparkError::todo("deduplicate"));
        }
        RelType::Range(_) => {
            return Err(SparkError::todo("range"));
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
        RelType::Repartition(_) => {
            return Err(SparkError::todo("repartition"));
        }
        RelType::ToDf(_) => {
            return Err(SparkError::todo("to dataframe"));
        }
        RelType::WithColumnsRenamed(_) => {
            return Err(SparkError::todo("with columns renamed"));
        }
        RelType::ShowString(_) => {
            return Err(SparkError::todo("show string"));
        }
        RelType::Drop(_) => {
            return Err(SparkError::todo("drop"));
        }
        RelType::Tail(_) => {
            return Err(SparkError::todo("tail"));
        }
        RelType::WithColumns(_) => {
            return Err(SparkError::todo("with columns"));
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
        RelType::RepartitionByExpression(_) => {
            return Err(SparkError::todo("repartition by expression"));
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
