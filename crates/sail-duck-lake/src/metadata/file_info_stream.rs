use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_pyarrow::FromPyArrow;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use futures::Stream;
use pyo3::prelude::PyAnyMethods;
use pyo3::{PyResult, Python};
use sail_common_datafusion::array::record_batch::record_batch_with_schema;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::python::Modules;

const OUTPUT_CHANNEL_BUFFER: usize = 16;

pub struct PyFileInfoStream {
    schema: SchemaRef,
    inner: SendableRecordBatchStream,
    stop: Arc<AtomicBool>,
}

impl PyFileInfoStream {
    pub fn scan_data_files_arrow(
        schema: SchemaRef,
        url: String,
        table_id: u64,
        snapshot_id: Option<u64>,
        partition_filters: Option<Vec<(u64, Vec<String>)>>,
        required_column_ids: Option<Vec<u64>>,
        batch_size: usize,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let (output_tx, output_rx) = mpsc::channel(OUTPUT_CHANNEL_BUFFER);
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop);
        let schema_clone = schema.clone();

        std::thread::spawn(move || {
            let res: DataFusionResult<()> = Python::attach(|py| {
                let call: PyResult<()> = (|| {
                    let m = Modules::DUCKLAKE_METADATA.load(py)?;
                    let iter = m.getattr("scan_data_files_arrow")?.call1((
                        url.as_str(),
                        table_id,
                        snapshot_id,
                        partition_filters,
                        required_column_ids,
                        batch_size,
                    ))?;
                    for item in iter.try_iter()? {
                        if stop_clone.load(Ordering::Relaxed) {
                            break;
                        }
                        let item = item?;
                        let batch = RecordBatch::from_pyarrow_bound(&item);
                        if py
                            .detach(|| {
                                let batch = batch
                                    .map_err(|e| DataFusionError::External(e.into()))
                                    .and_then(|b| record_batch_with_schema(b, &schema_clone));
                                output_tx.blocking_send(batch).map_err(|_| {
                                    DataFusionError::Execution(
                                        "DuckLake metadata output channel closed".to_string(),
                                    )
                                })
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(())
                })();
                call.map_err(|e| DataFusionError::External(Box::new(e)))
            });

            if let Err(e) = res {
                let _ = output_tx.blocking_send(Err(e));
            }
        });

        let inner = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            ReceiverStream::new(output_rx),
        ));

        Ok(Box::pin(Self {
            schema,
            inner,
            stop,
        }))
    }
}

impl Drop for PyFileInfoStream {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

impl Stream for PyFileInfoStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl RecordBatchStream for PyFileInfoStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
