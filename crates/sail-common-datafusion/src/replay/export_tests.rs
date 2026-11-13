use arrow::array::StringArray;
use arrow::buffer::Buffer;
use arrow::datatypes::{Field, Schema};
use arrow::ipc::reader::StreamDecoder;

use super::tests::{assert_clean, runtime};
use super::*;

fn partition(runtime: Arc<RuntimeEnv>) -> Result<ReplayPartition> {
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
    let mut builder = ReplayPartitionBuilder::try_new(schema.clone(), runtime, 0)?;
    // One row exceeds both the transfer chunk and normal 256KiB batch target.
    for _ in 0..3 {
        builder.append(RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["x".repeat(1024 * 1024)]))],
        )?)?;
    }
    builder.seal()
}

#[tokio::test]
async fn replay_export_stream_roundtrip_and_independent_leases() -> Result<()> {
    let runtime = runtime(16 * 1024 * 1024)?;
    let data = partition(runtime.clone())?;
    let mut first = data.export(&runtime)?;
    let mut second = data.export(&runtime)?;
    assert_eq!(first.rows, 3);
    assert!(first.length > 3 * 1024 * 1024);
    assert!(first.chunks.next().await.transpose()?.is_some());
    drop(first);
    drop(data);
    let mut decoder = StreamDecoder::new();
    let mut bytes = 0;
    let mut rows = 0;
    let mut chunks = 0;
    while let Some(chunk) = second.chunks.next().await.transpose()? {
        assert!(chunk.len() <= IO_BYTES);
        bytes += chunk.len() as u64;
        chunks += 1;
        let mut buffer = Buffer::from_vec(chunk);
        while !buffer.is_empty() {
            if let Some(batch) = decoder.decode(&mut buffer)? {
                rows += batch.num_rows();
                let array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| internal_datafusion_err!("expected string"))?;
                assert_eq!(array.value(0).len(), 1024 * 1024);
            }
        }
    }
    decoder.finish()?;
    assert_eq!(bytes, second.length);
    assert_eq!(rows, second.rows);
    assert!(chunks > 20);
    drop(decoder);
    drop(second);
    assert_clean(&runtime)
}

#[tokio::test]
async fn replay_export_drop_cancels_before_and_during_read() -> Result<()> {
    for poll in [false, true] {
        let runtime = runtime(16 * 1024 * 1024)?;
        let data = partition(runtime.clone())?;
        let mut export = data.export(&runtime)?;
        drop(data);
        if poll {
            assert!(export.chunks.next().await.transpose()?.is_some());
        }
        drop(export);
        assert_clean(&runtime)?;
    }
    Ok(())
}

#[tokio::test]
async fn replay_export_rejects_truncation_missing_file_and_low_memory() -> Result<()> {
    for missing in [false, true] {
        let runtime = runtime(16 * 1024 * 1024)?;
        let data = partition(runtime.clone())?;
        let too_small = super::tests::runtime(IO_BYTES)?;
        assert!(data.export(&too_small).is_err());
        assert_clean(&too_small)?;
        let path = data
            .file
            .as_ref()
            .and_then(|file| file.path())
            .ok_or_else(|| internal_datafusion_err!("expected local spill"))?
            .to_owned();
        let mut export = data.export(&runtime)?;
        if missing {
            std::fs::remove_file(path)?;
        } else {
            std::fs::OpenOptions::new()
                .write(true)
                .open(path)?
                .set_len(7)?;
        }
        let mut failed = false;
        while let Some(item) = export.chunks.next().await {
            if item.is_err() {
                failed = true;
                break;
            }
        }
        assert!(failed);
        drop(export);
        drop(data);
        assert_clean(&runtime)?;
    }
    Ok(())
}

#[tokio::test]
async fn replay_export_empty_partition_has_no_ipc_payload() -> Result<()> {
    let runtime = runtime(4 * 1024 * 1024)?;
    let data =
        ReplayPartitionBuilder::try_new(Arc::new(Schema::empty()), runtime.clone(), 0)?.seal()?;
    let mut export = data.export(&runtime)?;
    assert_eq!(export.length, 0);
    assert_eq!(export.rows, 0);
    assert!(export.chunks.next().await.is_none());
    drop(export);
    drop(data);
    assert_clean(&runtime)
}
