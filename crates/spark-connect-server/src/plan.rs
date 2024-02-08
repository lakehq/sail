use crate::error::{ProtoFieldExt, SparkError};
use crate::expression::{
    from_spark_expression, from_spark_literal_to_scalar, from_spark_sort_order,
};
use crate::spark::connect as sc;
use crate::spark::connect::execute_plan_response::ArrowBatch;
use crate::spark::connect::relation as scr;
use async_recursion::async_recursion;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::common::ParamValues;
use datafusion::execution::context::SessionContext;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::logical_plan as plan;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use tonic::codegen::tokio_stream::StreamExt;

pub(crate) fn read_arrow_batches(data: Vec<u8>) -> Result<Vec<RecordBatch>, SparkError> {
    let cursor = Cursor::new(data);
    let mut reader = StreamReader::try_new(cursor, None)?;
    let mut batches = Vec::new();
    while let Some(batch) = reader.next() {
        batches.push(batch?);
    }
    Ok(batches)
}

pub(crate) async fn write_arrow_batches(
    mut batches: SendableRecordBatchStream,
) -> Result<ArrowBatch, SparkError> {
    let mut output = ArrowBatch::default();
    {
        let cursor = Cursor::new(&mut output.data);
        let mut writer = StreamWriter::try_new(cursor, &batches.schema())?;
        while let Some(batch) = batches.next().await {
            let batch = batch?;
            writer.write(&batch)?;
            output.row_count += batch.num_rows() as i64;
        }
        writer.finish()?;
    }
    Ok(output)
}

#[async_recursion]
pub(crate) async fn from_spark_relation(
    ctx: &SessionContext,
    relation: &sc::Relation,
) -> Result<LogicalPlan, SparkError> {
    let sc::Relation { common, rel_type } = relation;
    let rel_type = rel_type.as_ref().required("relation type")?;
    match rel_type {
        scr::RelType::Read(_) => {
            todo!()
        }
        scr::RelType::Project(project) => {
            let input = project.input.as_ref().required("projection input")?;
            let input = from_spark_relation(ctx, input).await?;
            let expressions = project
                .expressions
                .iter()
                .map(|e| from_spark_expression(e, input.schema()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(plan::builder::project(input, expressions)?)
        }
        scr::RelType::Filter(filter) => {
            let input = filter.input.as_ref().required("filter input")?;
            let condition = filter.condition.as_ref().required("filter condition")?;
            let input = from_spark_relation(ctx, input).await?;
            let predicate = from_spark_expression(condition, input.schema())?;
            let filter = plan::Filter::try_new(predicate, Arc::new(input))?;
            Ok(LogicalPlan::Filter(filter))
        }
        scr::RelType::Join(_) => {
            todo!()
        }
        scr::RelType::SetOp(_) => {
            todo!()
        }
        scr::RelType::Sort(sort) => {
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
        scr::RelType::Limit(limit) => {
            let input = limit.input.as_ref().required("limit input")?;
            Ok(LogicalPlan::Limit(plan::Limit {
                skip: 0,
                fetch: Some(limit.limit as usize),
                input: Arc::new(from_spark_relation(ctx, input).await?),
            }))
        }
        scr::RelType::Aggregate(_) => {
            todo!()
        }
        scr::RelType::Sql(sc::Sql {
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
                todo!("multiple statements in SQL query")
            }
        }
        scr::RelType::LocalRelation(sc::LocalRelation { data, schema }) => {
            let batches = if let Some(data) = data {
                read_arrow_batches(data.clone())?
            } else {
                vec![]
            };
            let schema = if let [batch, ..] = batches.as_slice() {
                batch.schema()
            } else {
                todo!("parse schema from spark schema")
            };
            let provider = datafusion::datasource::MemTable::try_new(schema, vec![batches])?;
            Ok(plan::LogicalPlanBuilder::scan(
                plan::builder::UNNAMED_TABLE,
                datafusion::datasource::provider_as_source(Arc::new(provider)),
                None,
            )?
            .build()?)
        }
        scr::RelType::Sample(_) => {
            todo!()
        }
        scr::RelType::Offset(offset) => {
            let input = offset.input.as_ref().required("offset input")?;
            Ok(LogicalPlan::Limit(plan::Limit {
                skip: offset.offset as usize,
                fetch: None,
                input: Arc::new(from_spark_relation(ctx, input).await?),
            }))
        }
        scr::RelType::Deduplicate(_) => {
            todo!()
        }
        scr::RelType::Range(_) => {
            todo!()
        }
        scr::RelType::SubqueryAlias(sub) => {
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
        scr::RelType::Repartition(_) => {
            todo!()
        }
        scr::RelType::ToDf(_) => {
            todo!()
        }
        scr::RelType::WithColumnsRenamed(_) => {
            todo!()
        }
        scr::RelType::ShowString(_) => {
            todo!()
        }
        scr::RelType::Drop(_) => {
            todo!()
        }
        scr::RelType::Tail(_) => {
            todo!()
        }
        scr::RelType::WithColumns(_) => {
            todo!()
        }
        scr::RelType::Hint(_) => {
            todo!()
        }
        scr::RelType::Unpivot(_) => {
            todo!()
        }
        scr::RelType::ToSchema(_) => {
            todo!()
        }
        scr::RelType::RepartitionByExpression(_) => {
            todo!()
        }
        scr::RelType::MapPartitions(_) => {
            todo!()
        }
        scr::RelType::CollectMetrics(_) => {
            todo!()
        }
        scr::RelType::Parse(_) => {
            todo!()
        }
        scr::RelType::GroupMap(_) => {
            todo!()
        }
        scr::RelType::CoGroupMap(_) => {
            todo!()
        }
        scr::RelType::WithWatermark(_) => {
            todo!()
        }
        scr::RelType::ApplyInPandasWithState(_) => {
            todo!()
        }
        scr::RelType::HtmlString(_) => {
            todo!()
        }
        scr::RelType::CachedLocalRelation(_) => {
            todo!()
        }
        scr::RelType::CachedRemoteRelation(_) => {
            todo!()
        }
        scr::RelType::CommonInlineUserDefinedTableFunction(_) => {
            todo!()
        }
        scr::RelType::FillNa(_) => {
            todo!()
        }
        scr::RelType::DropNa(_) => {
            todo!()
        }
        scr::RelType::Replace(_) => {
            todo!()
        }
        scr::RelType::Summary(_) => {
            todo!()
        }
        scr::RelType::Crosstab(_) => {
            todo!()
        }
        scr::RelType::Describe(_) => {
            todo!()
        }
        scr::RelType::Cov(_) => {
            todo!()
        }
        scr::RelType::Corr(_) => {
            todo!()
        }
        scr::RelType::ApproxQuantile(_) => {
            todo!()
        }
        scr::RelType::FreqItems(_) => {
            todo!()
        }
        scr::RelType::SampleBy(_) => {
            todo!()
        }
        scr::RelType::Catalog(_) => {
            todo!()
        }
        scr::RelType::Extension(_) => {
            todo!()
        }
        scr::RelType::Unknown(_) => {
            todo!()
        }
    }
}

pub(crate) async fn execute_plan(
    ctx: &SessionContext,
    plan: &LogicalPlan,
) -> Result<SendableRecordBatchStream, SparkError> {
    let plan = ctx.state().create_physical_plan(&plan).await?;
    Ok(plan.execute(0, Arc::new(TaskContext::default()))?)
}
