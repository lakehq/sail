use std::sync::atomic::AtomicUsize;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{ColumnarValue, Volatility, col, create_udf};
use futures::{StreamExt, TryStreamExt};
use sail_common_datafusion::replay::ReplayId;

use super::*;

async fn counted_plan(
    context: &SessionContext,
    calls: Arc<AtomicUsize>,
    fail_first: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let udf = create_udf(
        "replay_next",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Volatile,
        Arc::new(move |_args| {
            let value = calls.fetch_add(1, Ordering::SeqCst) + 1;
            if fail_first && value == 1 {
                return internal_err!("injected replay source failure");
            }
            Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(
                value as i64,
            ))))
        }),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )?;
    context
        .read_batch(batch)?
        .select(vec![udf.call(vec![col("v")]).alias("v")])?
        .create_physical_plan()
        .await
}

fn named(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(ReplayExec::try_new(
        ReplayId {
            scope: Default::default(),
            ordinal: 0,
        },
        plan,
    )?))
}

async fn values(stream: SendableRecordBatchStream) -> Result<Vec<i64>> {
    let mut output = vec![];
    for batch in stream.try_collect::<Vec<_>>().await? {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| internal_datafusion_err!("expected i64 output"))?;
        output.extend_from_slice(column.values());
    }
    Ok(output)
}

fn assert_clean(context: &SessionContext) {
    assert_eq!(context.runtime_env().disk_manager.used_disk_space(), 0);
    assert_eq!(context.runtime_env().memory_pool.reserved(), 0);
}

#[tokio::test]
async fn replay_runner_same_prepared_plan_has_isolated_live_epochs() -> Result<()> {
    let context = SessionContext::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let plan = named(counted_plan(&context, calls.clone(), false).await?)?;
    let runner = LocalJobRunner::new("replay-runner-test".into());
    let first = runner.execute(&context, plan.clone()).await?;
    let second = runner.execute(&context, plan).await?;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(runner.next_job_id.load(Ordering::SeqCst), 3);
    assert_eq!(values(first).await?, vec![1]);
    // Retiring the first root must not cancel or release the second epoch.
    assert_eq!(values(second).await?, vec![2]);
    assert_clean(&context);
    Ok(())
}

#[tokio::test]
async fn replay_runner_failed_epoch_does_not_poison_next_execute() -> Result<()> {
    let context = SessionContext::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let plan = named(counted_plan(&context, calls.clone(), true).await?)?;
    let runner = LocalJobRunner::default();
    assert!(runner.execute(&context, plan.clone()).await.is_err());
    assert_clean(&context);
    assert_eq!(
        values(runner.execute(&context, plan).await?).await?,
        vec![2]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_clean(&context);
    Ok(())
}

#[tokio::test]
async fn replay_runner_stop_cancels_sealed_root_without_recompute() -> Result<()> {
    let context = SessionContext::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let plan = named(counted_plan(&context, calls.clone(), false).await?)?;
    let runner = LocalJobRunner::default();
    let mut stream = runner.execute(&context, plan.clone()).await?;
    assert!(context.runtime_env().disk_manager.used_disk_space() > 0);
    runner.stop().await;
    assert!(matches!(stream.next().await, Some(Err(_))));
    drop(stream);
    assert!(runner.execute(&context, plan).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_clean(&context);
    Ok(())
}

#[tokio::test]
async fn replay_runner_plain_query_keeps_original_stream_lifecycle() -> Result<()> {
    let context = SessionContext::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let plan = counted_plan(&context, calls.clone(), false).await?;
    let runner = LocalJobRunner::default();
    let stream = runner.execute(&context, plan).await?;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(context.runtime_env().disk_manager.used_disk_space(), 0);
    runner.stop().await;
    // Before replay support, stop closed admission but did not cancel an
    // ordinary stream already returned to its caller. Preserve that fast path.
    assert_eq!(values(stream).await?, vec![1]);
    assert_clean(&context);
    Ok(())
}
