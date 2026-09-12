// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! End to end tests that drive Lance through Sail's `TableFormat` interface.
//!
//! Every test goes through the entry points Sail uses: `create_writer` for
//! `df.write.format("lance")` and `create_source` for
//! `spark.read.format("lance")`. Plans are built with the DataFrame API rather
//! than SQL, which is how Sail plans a Spark query.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Float32Type, Schema};
use async_trait::async_trait;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{TableProvider, source_as_provider};
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, displayable};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::{Constraints, Result};
use datafusion_expr::{LogicalPlan, col, lit};
use sail_common_datafusion::datasource::{
    OptionLayer, SinkInfo, SinkMode, SourceInfo, TableFormat, TableFormatRegistry,
};
use sail_lance::{LancePhysicalPlanner, LanceTableFormat};
use tempfile::TempDir;

/// A session that plans the Lance write node, which is what registering
/// [`LancePhysicalPlanner`] with Sail's extension planners gives a Sail session.
#[derive(Debug)]
struct LanceQueryPlanner;

#[async_trait]
impl QueryPlanner for LanceQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(LancePhysicalPlanner)])
            .create_physical_plan(logical_plan, session)
            .await
    }
}

fn session_with(config: SessionConfig) -> SessionContext {
    SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_query_planner(Arc::new(LanceQueryPlanner))
            .build(),
    )
}

fn session() -> SessionContext {
    session_with(SessionConfig::new())
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
            true,
        ),
    ]))
}

fn batch(ids: &[i64], names: &[&str], vectors: &[[f32; 2]]) -> Result<RecordBatch> {
    let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        vectors
            .iter()
            .map(|vector| Some(vector.iter().map(|value| Some(*value)).collect::<Vec<_>>())),
        2,
    );
    Ok(RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(names.to_vec())),
            Arc::new(vectors),
        ],
    )?)
}

fn options(items: &[(&str, &str)]) -> Vec<OptionLayer> {
    vec![OptionLayer::OptionList {
        items: items
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
    }]
}

async fn write(
    ctx: &SessionContext,
    uri: &str,
    batch: RecordBatch,
    mode: SinkMode,
    write_options: &[(&str, &str)],
) -> Result<()> {
    let input = ctx.read_batch(batch)?.logical_plan().clone();
    let mut items = vec![("path", uri)];
    items.extend_from_slice(write_options);
    let info = SinkInfo {
        input,
        mode,
        partition_by: vec![],
        bucket_by: None,
        sort_order: vec![],
        options: options(&items),
        lakehouse_table: None,
    };
    let plan = LanceTableFormat.create_writer(&ctx.state(), info).await?;
    DataFrame::new(ctx.state(), plan).collect().await?;
    Ok(())
}

async fn open(
    ctx: &SessionContext,
    uri: &str,
    read_options: &[(&str, &str)],
) -> Result<Arc<dyn TableProvider>> {
    let info = SourceInfo {
        paths: vec![uri.to_string()],
        lakehouse_table: None,
        schema: None,
        constraints: Constraints::default(),
        partition_by: vec![],
        bucket_by: None,
        sort_order: vec![],
        options: options(read_options),
        read_case_sensitive: false,
    };
    let source = LanceTableFormat.create_source(&ctx.state(), info).await?;
    // Sail requires the source to be a `DefaultTableSource`, which is what this
    // unwraps back into a provider.
    source_as_provider(&source)
}

async fn read(ctx: &SessionContext, uri: &str, read_options: &[(&str, &str)]) -> Result<DataFrame> {
    let provider = open(ctx, uri, read_options).await?;
    ctx.read_table(provider)
}

/// Returns the values of an `Int64` column, sorted, so that a comparison does
/// not depend on the order partitions happen to finish in.
fn int_column(batches: &[RecordBatch], name: &str) -> Result<Vec<i64>> {
    let mut values = Vec::new();
    for batch in batches {
        let column = batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| {
                datafusion_common::exec_datafusion_err!("column {name} is not an Int64 column")
            })?;
        values.extend(column.iter().flatten());
    }
    values.sort_unstable();
    Ok(values)
}

fn float_column(batches: &[RecordBatch], name: &str) -> Result<Vec<f32>> {
    let mut values = Vec::new();
    for batch in batches {
        let column = batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<Float32Array>())
            .ok_or_else(|| {
                datafusion_common::exec_datafusion_err!("column {name} is not a Float32 column")
            })?;
        values.extend(column.iter().flatten());
    }
    Ok(values)
}

fn string_column(batches: &[RecordBatch], name: &str) -> Result<Vec<String>> {
    let mut values = Vec::new();
    for batch in batches {
        let column = batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                datafusion_common::exec_datafusion_err!("column {name} is not a string column")
            })?;
        values.extend(column.iter().flatten().map(str::to_string));
    }
    values.sort();
    Ok(values)
}

/// Returns the message of a call that must fail.
fn error_message<T>(result: Result<T>) -> String {
    match result {
        Ok(_) => "the call unexpectedly succeeded".to_string(),
        Err(error) => error.to_string(),
    }
}

fn dataset_uri(directory: &TempDir) -> String {
    directory.path().join("people.lance").display().to_string()
}

fn people() -> Result<RecordBatch> {
    batch(
        &[1, 2, 3],
        &["ada", "grace", "alan"],
        &[[1.0, 1.0], [2.0, 2.0], [3.0, 3.0]],
    )
}

#[tokio::test]
async fn a_dataset_written_through_sail_reads_back_through_sail() -> Result<()> {
    let directory = TempDir::new()?;
    let uri = dataset_uri(&directory);
    let ctx = session();

    write(&ctx, &uri, people()?, SinkMode::ErrorIfExists, &[]).await?;
    let batches = read(&ctx, &uri, &[]).await?.collect().await?;

    assert_eq!(int_column(&batches, "id")?, vec![1, 2, 3]);
    assert_eq!(
        string_column(&batches, "name")?,
        vec!["ada".to_string(), "alan".to_string(), "grace".to_string()]
    );
    let provider = open(&ctx, &uri, &[]).await?;
    assert_eq!(
        provider.schema().fields().len(),
        3,
        "the dataset schema is read from Lance"
    );
    Ok(())
}

#[tokio::test]
async fn filters_projections_and_limits_reach_the_lance_scan() -> Result<()> {
    let directory = TempDir::new()?;
    let uri = dataset_uri(&directory);
    let ctx = session();
    write(&ctx, &uri, people()?, SinkMode::ErrorIfExists, &[]).await?;

    let query = read(&ctx, &uri, &[])
        .await?
        .filter(
            col("id")
                .gt(lit(1_i64))
                .and(col("name").not_eq(lit("alan"))),
        )?
        .select(vec![col("name")])?;

    let explained = displayable(query.clone().create_physical_plan().await?.as_ref())
        .indent(true)
        .to_string();
    assert!(explained.contains("LanceScanExec"), "{explained}");
    // DataFusion splits the conjunction into two filters, and both are pushed.
    assert!(
        explained.contains("filter=id > Int64(1) AND name != Utf8(\"alan\")"),
        "{explained}"
    );
    assert!(
        !explained.contains("FilterExec"),
        "an exact pushdown leaves no filter above the scan: {explained}"
    );
    assert!(
        !explained.contains("vector"),
        "the unused vector column must not be read: {explained}"
    );

    let batches = query.collect().await?;
    assert_eq!(string_column(&batches, "name")?, vec!["grace".to_string()]);

    // A limit only reaches the scan when nothing between them can change the
    // row count.
    let limited = read(&ctx, &uri, &[]).await?.limit(0, Some(2))?;
    let explained = displayable(limited.clone().create_physical_plan().await?.as_ref())
        .indent(true)
        .to_string();
    assert!(explained.contains("limit=2"), "{explained}");
    assert_eq!(
        limited
            .collect()
            .await?
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );
    Ok(())
}

#[tokio::test]
async fn a_scalar_function_filter_is_evaluated_by_the_lance_scan() -> Result<()> {
    let directory = TempDir::new()?;
    let uri = dataset_uri(&directory);
    let ctx = session();
    write(&ctx, &uri, people()?, SinkMode::ErrorIfExists, &[]).await?;

    // The filter is a DataFusion expression, not a rendered string, so a
    // scalar function goes to Lance with its implementation attached.
    let query = read(&ctx, &uri, &[])
        .await?
        .filter(
            datafusion::functions::unicode::expr_fn::character_length(col("name")).eq(lit(3_i64)),
        )?
        .select(vec![col("id")])?;
    let explained = displayable(query.clone().create_physical_plan().await?.as_ref())
        .indent(true)
        .to_string();
    assert!(
        explained.contains("filter=character_length(name)"),
        "{explained}"
    );
    assert!(
        explained.contains("columns=[id]"),
        "a column only the filter needs is not part of the projection: {explained}"
    );

    let batches = query.collect().await?;
    assert_eq!(int_column(&batches, "id")?, vec![1]);
    Ok(())
}

#[tokio::test]
async fn an_empty_projection_still_reads_one_column() -> Result<()> {
    let directory = TempDir::new()?;
    let uri = dataset_uri(&directory);
    let ctx = session();
    write(&ctx, &uri, people()?, SinkMode::ErrorIfExists, &[]).await?;

    // This is the projection DataFusion pushes down for `count(*)`.
    let provider = open(&ctx, &uri, &[]).await?;
    let plan = provider
        .scan(&ctx.state(), Some(&vec![]), &[], None)
        .await?;
    let explained = displayable(plan.as_ref()).indent(true).to_string();
    assert!(explained.contains("columns=[id]"), "{explained}");

    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 3);
    assert!(batches.iter().all(|batch| batch.num_columns() == 0));
    Ok(())
}

#[tokio::test]
async fn write_modes_follow_spark_semantics() -> Result<()> {
    let directory = TempDir::new()?;
    let uri = dataset_uri(&directory);
    let ctx = session();
    let first = || batch(&[1], &["ada"], &[[1.0, 1.0]]);
    let second = || batch(&[2], &["grace"], &[[2.0, 2.0]]);

    write(&ctx, &uri, first()?, SinkMode::ErrorIfExists, &[]).await?;
    let error = error_message(write(&ctx, &uri, second()?, SinkMode::ErrorIfExists, &[]).await);
    assert!(error.contains("already exists"), "{error}");

    write(&ctx, &uri, second()?, SinkMode::IgnoreIfExists, &[]).await?;
    let batches = read(&ctx, &uri, &[]).await?.collect().await?;
    assert_eq!(
        int_column(&batches, "id")?,
        vec![1],
        "an ignored write must not change the dataset"
    );

    write(&ctx, &uri, second()?, SinkMode::Append, &[]).await?;
    let batches = read(&ctx, &uri, &[]).await?.collect().await?;
    assert_eq!(int_column(&batches, "id")?, vec![1, 2]);

    write(&ctx, &uri, second()?, SinkMode::Overwrite, &[]).await?;
    let batches = read(&ctx, &uri, &[]).await?.collect().await?;
    assert_eq!(int_column(&batches, "id")?, vec![2]);
    Ok(())
}

#[tokio::test]
async fn an_append_creates_a_dataset_that_does_not_exist_yet() -> Result<()> {
    let directory = TempDir::new()?;
    let uri = dataset_uri(&directory);
    let ctx = session();

    write(&ctx, &uri, people()?, SinkMode::Append, &[]).await?;
    let batches = read(&ctx, &uri, &[]).await?.collect().await?;
    assert_eq!(int_column(&batches, "id")?, vec![1, 2, 3]);
    Ok(())
}

#[tokio::test]
async fn an_earlier_dataset_version_can_be_read() -> Result<()> {
    let directory = TempDir::new()?;
    let uri = dataset_uri(&directory);
    let ctx = session();
    write(
        &ctx,
        &uri,
        batch(&[1], &["ada"], &[[1.0, 1.0]])?,
        SinkMode::ErrorIfExists,
        &[],
    )
    .await?;
    write(
        &ctx,
        &uri,
        batch(&[2], &["grace"], &[[2.0, 2.0]])?,
        SinkMode::Append,
        &[],
    )
    .await?;

    let latest = read(&ctx, &uri, &[]).await?.collect().await?;
    assert_eq!(int_column(&latest, "id")?, vec![1, 2]);

    let original = read(&ctx, &uri, &[("version", "1")])
        .await?
        .collect()
        .await?;
    assert_eq!(int_column(&original, "id")?, vec![1]);

    let as_of = LanceTableFormat
        .create_source(
            &ctx.state(),
            SourceInfo {
                paths: vec![uri.clone()],
                lakehouse_table: None,
                schema: None,
                constraints: Constraints::default(),
                partition_by: vec![],
                bucket_by: None,
                sort_order: vec![],
                options: vec![
                    OptionLayer::TableLocation { value: uri.clone() },
                    OptionLayer::AsOfIntegerVersion { value: 1 },
                ],
                read_case_sensitive: false,
            },
        )
        .await?;
    let batches = ctx
        .read_table(source_as_provider(&as_of)?)?
        .collect()
        .await?;
    assert_eq!(
        int_column(&batches, "id")?,
        vec![1],
        "Sail's time travel layer must select the dataset version"
    );
    Ok(())
}

#[tokio::test]
async fn a_vector_search_is_pushed_into_the_scan() -> Result<()> {
    let directory = TempDir::new()?;
    let uri = dataset_uri(&directory);
    let ctx = session();
    write(
        &ctx,
        &uri,
        batch(
            &[1, 2, 3],
            &["ada", "grace", "alan"],
            &[[0.0, 0.0], [10.0, 10.0], [20.0, 20.0]],
        )?,
        SinkMode::ErrorIfExists,
        &[],
    )
    .await?;

    let query = read(
        &ctx,
        &uri,
        &[
            ("nearest.column", "vector"),
            ("nearest.query", "[0.0, 0.5]"),
            ("nearest.k", "2"),
        ],
    )
    .await?
    .select(vec![col("id"), col("_distance")])?;

    let explained = displayable(query.clone().create_physical_plan().await?.as_ref())
        .indent(true)
        .to_string();
    assert!(
        explained.contains("nearest={column: vector, k: 2}"),
        "{explained}"
    );

    let batches = query.collect().await?;
    assert_eq!(int_column(&batches, "id")?, vec![1, 2]);
    let distances = float_column(&batches, "_distance")?;
    assert_eq!(distances.len(), 2);
    assert!(
        distances[0] < distances[1],
        "the scan returns rows nearest first: {distances:?}"
    );
    Ok(())
}

#[tokio::test]
async fn fragments_are_scanned_in_parallel() -> Result<()> {
    let directory = TempDir::new()?;
    let uri = dataset_uri(&directory);
    let ctx = session_with(SessionConfig::new().with_target_partitions(3));
    write(
        &ctx,
        &uri,
        people()?,
        SinkMode::ErrorIfExists,
        &[("max_rows_per_file", "1")],
    )
    .await?;

    let provider = open(&ctx, &uri, &[]).await?;
    let plan = provider.scan(&ctx.state(), None, &[], None).await?;
    assert_eq!(
        plan.output_partitioning().partition_count(),
        3,
        "each fragment is scanned by its own partition"
    );

    let batches = ctx.read_table(provider)?.collect().await?;
    assert_eq!(int_column(&batches, "id")?, vec![1, 2, 3]);
    Ok(())
}

#[tokio::test]
async fn unsupported_requests_are_rejected_with_a_clear_error() -> Result<()> {
    let directory = TempDir::new()?;
    let uri = dataset_uri(&directory);
    let ctx = session();
    write(&ctx, &uri, people()?, SinkMode::ErrorIfExists, &[]).await?;

    let error = error_message(read(&ctx, &uri, &[("batchSize", "10")]).await);
    assert!(error.contains("unknown option"), "{error}");

    let info = SourceInfo {
        paths: vec![uri.clone()],
        lakehouse_table: None,
        schema: None,
        constraints: Constraints::default(),
        partition_by: vec!["name".to_string()],
        bucket_by: None,
        sort_order: vec![],
        options: options(&[]),
        read_case_sensitive: false,
    };
    let error = error_message(LanceTableFormat.create_source(&ctx.state(), info).await);
    assert!(error.contains("partition columns"), "{error}");

    let missing = error_message(
        read(
            &ctx,
            &directory.path().join("absent.lance").display().to_string(),
            &[],
        )
        .await,
    );
    assert!(
        missing.contains("Not found") || missing.contains("not found"),
        "{missing}"
    );
    Ok(())
}

#[test]
fn the_table_format_registers_under_the_lance_name() -> Result<()> {
    let registry = TableFormatRegistry::new();
    LanceTableFormat::register(&registry)?;

    assert_eq!(registry.get("LANCE")?.name(), "lance");
    Ok(())
}

#[tokio::test]
async fn a_write_plan_explains_itself() -> Result<()> {
    let directory = TempDir::new()?;
    let uri = dataset_uri(&directory);
    let ctx = session();
    let input = ctx.read_batch(people()?)?.logical_plan().clone();
    let plan = LanceTableFormat
        .create_writer(
            &ctx.state(),
            SinkInfo {
                input,
                mode: SinkMode::Overwrite,
                partition_by: vec![],
                bucket_by: None,
                sort_order: vec![],
                options: options(&[("path", uri.as_str())]),
                lakehouse_table: None,
            },
        )
        .await?;

    let logical = format!("{}", plan.display_indent());
    assert!(logical.contains("LanceWrite: uri="), "{logical}");
    assert!(logical.contains("mode=Overwrite"), "{logical}");

    let physical = DataFrame::new(ctx.state(), plan)
        .create_physical_plan()
        .await?;
    let explained = displayable(physical.as_ref()).indent(true).to_string();
    assert!(explained.contains("LanceDataSink"), "{explained}");
    Ok(())
}

#[tokio::test]
async fn a_failing_input_does_not_leave_a_dataset_behind() -> Result<()> {
    let directory = TempDir::new()?;
    let uri = dataset_uri(&directory);
    let ctx = session();

    // Dividing by the zero in the second row fails part way through the write.
    let ids = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1, 0])) as ArrayRef],
    )?;
    let input = ctx
        .read_batch(ids)?
        .select(vec![(lit(10_i64) / col("id")).alias("id")])?
        .logical_plan()
        .clone();
    let plan = LanceTableFormat
        .create_writer(
            &ctx.state(),
            SinkInfo {
                input,
                mode: SinkMode::ErrorIfExists,
                partition_by: vec![],
                bucket_by: None,
                sort_order: vec![],
                options: options(&[("path", uri.as_str())]),
                lakehouse_table: None,
            },
        )
        .await?;

    let error = error_message(DataFrame::new(ctx.state(), plan).collect().await);
    assert!(error.contains("Divide by zero"), "{error}");
    assert!(
        error_message(read(&ctx, &uri, &[]).await).contains("not found"),
        "a failed write must not commit a dataset"
    );
    Ok(())
}
