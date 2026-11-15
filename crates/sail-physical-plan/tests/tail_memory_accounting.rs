use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::{AsArray, Int64Array, RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion_common::{Result, internal_datafusion_err};
use futures::{StreamExt, TryStreamExt, stream};
use sail_physical_plan::tail::TailExec;

const SMALL_BATCHES: usize = 1024;
const POOL_LIMIT: usize = 1024 * 1024;

#[derive(Debug)]
struct Input {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    leave_pending: bool,
    emitted: Arc<AtomicUsize>,
}

impl PartitionStream for Input {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let emitted = Arc::clone(&self.emitted);
        let batches = stream::iter(self.batches.clone().into_iter().map(Ok)).inspect(move |_| {
            emitted.fetch_add(1, Ordering::SeqCst);
        });
        let rest = if self.leave_pending {
            stream::pending().boxed()
        } else {
            stream::empty().boxed()
        };
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            batches.chain(rest),
        ))
    }
}

fn zero_column_batch(rows: usize) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )?)
}

fn tail(
    batches: Vec<RecordBatch>,
    leave_pending: bool,
    fetch: usize,
) -> Result<(
    SendableRecordBatchStream,
    Arc<dyn MemoryPool>,
    Arc<AtomicUsize>,
)> {
    let pool = Arc::new(GreedyMemoryPool::new(POOL_LIMIT)) as Arc<dyn MemoryPool>;
    let context = Arc::new(
        TaskContext::default().with_runtime(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build_arc()?,
        ),
    );
    let schema = batches
        .first()
        .map(RecordBatch::schema)
        .ok_or_else(|| internal_datafusion_err!("test input needs a batch"))?;
    let emitted = Arc::new(AtomicUsize::new(0));
    let input = Arc::new(StreamingTableExec::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Input {
            schema,
            batches,
            leave_pending,
            emitted: Arc::clone(&emitted),
        })],
        None,
        [],
        false,
        None,
    )?);
    Ok((
        TailExec::new(input, fetch).execute(0, context)?,
        pool,
        emitted,
    ))
}

#[tokio::test]
async fn evicting_batches_does_not_make_live_queue_storage_available_to_other_consumers()
-> Result<()> {
    let mut batches = vec![zero_column_batch(1)?; SMALL_BATCHES];
    // This one batch replaces every buffered singleton but need not shrink the queue allocation.
    batches.push(zero_column_batch(2 * SMALL_BATCHES)?);
    let (mut output, pool, emitted) = tail(batches, true, SMALL_BATCHES)?;
    for _ in 0..SMALL_BATCHES {
        assert!(futures::poll!(output.next()).is_pending());
        if emitted.load(Ordering::SeqCst) == SMALL_BATCHES + 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(emitted.load(Ordering::SeqCst), SMALL_BATCHES + 1);
    // No array payload exists in this fixture. Even ignoring allocator and wrapper
    // overhead, the queue needed room for this many RecordBatch values.
    let queue_lower_bound = SMALL_BATCHES * std::mem::size_of::<RecordBatch>();
    assert!(pool.reserved() >= queue_lower_bound);
    let competitor = MemoryConsumer::new("competing consumer").register(&pool);
    assert!(
        competitor
            .try_grow(POOL_LIMIT - queue_lower_bound + 1)
            .is_err()
    );
    drop(output);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn partially_consuming_tail_does_not_release_live_queue_storage() -> Result<()> {
    let (mut output, pool, _) = tail(
        vec![zero_column_batch(1)?; SMALL_BATCHES],
        false,
        SMALL_BATCHES,
    )?;
    for _ in 0..SMALL_BATCHES - 1 {
        let batch = output.try_next().await?.ok_or_else(|| {
            internal_datafusion_err!("tail lost one of the retained singleton batches")
        })?;
        assert_eq!(batch.num_rows(), 1);
    }
    // Only one output batch remains, while the queue allocation is still alive.
    assert!(pool.reserved() >= SMALL_BATCHES * std::mem::size_of::<RecordBatch>());
    let last = output
        .try_next()
        .await?
        .ok_or_else(|| internal_datafusion_err!("tail lost its last retained singleton batch"))?;
    assert_eq!(last.num_rows(), 1);
    assert!(output.try_next().await?.is_none());
    drop(output);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn wrapping_and_growing_the_queue_preserves_the_suffix() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let lengths = std::iter::repeat_n(1, 63)
        .chain([100])
        .chain(std::iter::repeat_n(1, 128));
    let mut ordinal = 0_i64;
    let batches = lengths
        .map(|length| {
            let values = Int64Array::from_iter_values(ordinal..ordinal + length);
            ordinal += length;
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)])
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    // The large batch evicts many queue entries. New singleton batches then wrap
    // around and grow the queue before the large batch becomes wholly obsolete.
    let (output, pool, _) = tail(batches, false, 128)?;
    let batches = output.try_collect::<Vec<_>>().await?;
    let actual = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, (ordinal - 128..ordinal).collect::<Vec<_>>());
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn zero_column_extreme_row_counts_preserve_the_suffix() -> Result<()> {
    // Arrow permits zero-column row counts without allocating an array per row.
    // The total input exceeds usize even though the requested result fits in it.
    for (fetch, expected) in [(1, vec![1]), (usize::MAX, vec![usize::MAX - 1, 1])] {
        let (output, pool, _) = tail(
            vec![zero_column_batch(usize::MAX)?, zero_column_batch(1)?],
            false,
            fetch,
        )?;
        let batches = output.try_collect::<Vec<_>>().await?;
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(pool.reserved(), 0);
    }
    Ok(())
}
