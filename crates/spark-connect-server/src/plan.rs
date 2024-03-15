use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::common::ParamValues;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::logical_plan as plan;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::expression::{
    from_spark_expression, from_spark_literal_to_scalar, from_spark_sort_order,
};
use crate::spark::connect as sc;
use crate::spark::connect::execute_plan_response::ArrowBatch;
use crate::spark::connect::relation;

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
    let sc::Relation { common, rel_type } = relation;
    let rel_type = rel_type.as_ref().required("relation type")?;
    match rel_type {
        relation::RelType::Read(_) => {
            todo!()
        }
        relation::RelType::Project(project) => {
            let input = project.input.as_ref().required("projection input")?;
            let input = from_spark_relation(ctx, input).await?;
            let expressions = project
                .expressions
                .iter()
                .map(|e| from_spark_expression(e, input.schema()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(plan::builder::project(input, expressions)?)
        }
        relation::RelType::Filter(filter) => {
            let input = filter.input.as_ref().required("filter input")?;
            let condition = filter.condition.as_ref().required("filter condition")?;
            let input = from_spark_relation(ctx, input).await?;
            let predicate = from_spark_expression(condition, input.schema())?;
            let filter = plan::Filter::try_new(predicate, Arc::new(input))?;
            Ok(LogicalPlan::Filter(filter))
        }
        relation::RelType::Join(_) => {
            todo!()
        }
        relation::RelType::SetOp(_) => {
            todo!()
        }
        relation::RelType::Sort(sort) => {
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
        relation::RelType::Limit(limit) => {
            let input = limit.input.as_ref().required("limit input")?;
            Ok(LogicalPlan::Limit(plan::Limit {
                skip: 0,
                fetch: Some(limit.limit as usize),
                input: Arc::new(from_spark_relation(ctx, input).await?),
            }))
        }
        relation::RelType::Aggregate(_) => {
            todo!()
        }
        relation::RelType::Sql(sc::Sql {
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
        relation::RelType::LocalRelation(sc::LocalRelation { data, schema }) => {
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
        relation::RelType::Sample(_) => {
            todo!()
        }
        relation::RelType::Offset(offset) => {
            let input = offset.input.as_ref().required("offset input")?;
            Ok(LogicalPlan::Limit(plan::Limit {
                skip: offset.offset as usize,
                fetch: None,
                input: Arc::new(from_spark_relation(ctx, input).await?),
            }))
        }
        relation::RelType::Deduplicate(_) => {
            todo!()
        }
        relation::RelType::Range(_) => {
            todo!()
        }
        relation::RelType::SubqueryAlias(sub) => {
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
        relation::RelType::Repartition(_) => {
            todo!()
        }
        relation::RelType::ToDf(_) => {
            todo!()
        }
        relation::RelType::WithColumnsRenamed(_) => {
            todo!()
        }
        relation::RelType::ShowString(_) => {
            todo!()
        }
        relation::RelType::Drop(_) => {
            todo!()
        }
        relation::RelType::Tail(_) => {
            todo!()
        }
        relation::RelType::WithColumns(_) => {
            todo!()
        }
        relation::RelType::Hint(_) => {
            todo!()
        }
        relation::RelType::Unpivot(_) => {
            todo!()
        }
        relation::RelType::ToSchema(_) => {
            todo!()
        }
        relation::RelType::RepartitionByExpression(_) => {
            todo!()
        }
        relation::RelType::MapPartitions(_) => {
            todo!()
        }
        relation::RelType::CollectMetrics(_) => {
            todo!()
        }
        relation::RelType::Parse(_) => {
            todo!()
        }
        relation::RelType::GroupMap(_) => {
            todo!()
        }
        relation::RelType::CoGroupMap(_) => {
            todo!()
        }
        relation::RelType::WithWatermark(_) => {
            todo!()
        }
        relation::RelType::ApplyInPandasWithState(_) => {
            todo!()
        }
        relation::RelType::HtmlString(_) => {
            todo!()
        }
        relation::RelType::CachedLocalRelation(_) => {
            todo!()
        }
        relation::RelType::CachedRemoteRelation(_) => {
            todo!()
        }
        relation::RelType::CommonInlineUserDefinedTableFunction(_) => {
            todo!()
        }
        relation::RelType::FillNa(_) => {
            todo!()
        }
        relation::RelType::DropNa(_) => {
            todo!()
        }
        relation::RelType::Replace(_) => {
            todo!()
        }
        relation::RelType::Summary(_) => {
            todo!()
        }
        relation::RelType::Crosstab(_) => {
            todo!()
        }
        relation::RelType::Describe(_) => {
            todo!()
        }
        relation::RelType::Cov(_) => {
            todo!()
        }
        relation::RelType::Corr(_) => {
            todo!()
        }
        relation::RelType::ApproxQuantile(_) => {
            todo!()
        }
        relation::RelType::FreqItems(_) => {
            todo!()
        }
        relation::RelType::SampleBy(_) => {
            todo!()
        }
        relation::RelType::Catalog(_) => {
            todo!()
        }
        relation::RelType::Extension(_) => {
            todo!()
        }
        relation::RelType::Unknown(_) => {
            todo!()
        }
    }
}
