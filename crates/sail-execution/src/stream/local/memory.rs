use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::disk_manager::DiskManager;
use datafusion::execution::spill_file::{SpillFile, SpillWriter};
use futures::stream;
use tokio::sync::watch;

use crate::error::{ExecutionError, ExecutionResult};
use crate::stream::error::TaskStreamError;
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamChannelSink, TaskStreamWriteState};

/// A replayable stream with a bounded in-memory prefix and an append-only spill file.
/// Readers have independent cursors, so neither writes nor commit wait for readers.
/// Keeping a receiver in the manager allows tasks to subscribe after the writer finishes.
pub(crate) struct MemoryStream {
    receiver: watch::Receiver<StreamState>,
    sender: Option<watch::Sender<StreamState>>,
    buffer: usize,
}

#[derive(Default)]
struct StreamState {
    batches: Vec<RecordBatch>,
    spill: Option<Arc<dyn SpillFile>>,
    spilled_bytes: u64,
    terminal: Option<Result<(), TaskStreamError>>,
}

impl MemoryStream {
    pub fn new(buffer: usize) -> Self {
        let (sender, receiver) = watch::channel(StreamState::default());
        Self {
            receiver,
            sender: Some(sender),
            buffer,
        }
    }

    pub fn publish(&mut self, disk: Arc<DiskManager>) -> ExecutionResult<MemoryStreamWriter> {
        let sender = self.sender.take().ok_or_else(|| {
            ExecutionError::InternalError("memory stream can only be written once".into())
        })?;
        Ok(MemoryStreamWriter {
            sender,
            disk,
            writer: None,
            buffer: self.buffer,
        })
    }

    pub fn fail(&mut self, error: TaskStreamError) {
        if let Some(sender) = self.sender.take() {
            sender.send_modify(|state| state.terminal = Some(Err(error)));
        }
    }

    pub fn is_failed(&self) -> bool {
        self.receiver
            .borrow()
            .terminal
            .as_ref()
            .is_some_and(Result::is_err)
    }

    pub fn subscribe(&self) -> TaskStreamSource {
        let receiver = self.receiver.clone();
        Box::pin(stream::unfold(
            (receiver, 0, 0, false),
            |(mut receiver, mut index, mut offset, done)| async move {
                if done {
                    return None;
                }
                loop {
                    let (batch, spill, terminal) = {
                        let state = receiver.borrow_and_update();
                        (
                            state.batches.get(index).cloned(),
                            (offset < state.spilled_bytes)
                                .then(|| state.spill.clone())
                                .flatten(),
                            state.terminal.clone(),
                        )
                    };
                    if let Some(batch) = batch {
                        index += 1;
                        return Some((Ok(batch), (receiver, index, offset, false)));
                    }
                    if let Some(spill) = spill {
                        let result = tokio::task::spawn_blocking(move || -> Result<_> {
                            let path = spill.path().ok_or_else(|| {
                                DataFusionError::Execution(
                                    "shuffle replay requires a local spill file".into(),
                                )
                            })?;
                            let mut file = std::fs::File::open(path)?;
                            file.seek(SeekFrom::Start(offset))?;
                            let mut size = [0; 8];
                            file.read_exact(&mut size)?;
                            let size = u64::from_le_bytes(size);
                            let mut reader = StreamReader::try_new(file.take(size), None)?;
                            let batch = reader.next().transpose()?.ok_or_else(|| {
                                DataFusionError::Execution("empty shuffle spill frame".into())
                            })?;
                            Ok((batch, offset + 8 + size))
                        })
                        .await
                        .map_err(|e| TaskStreamError::External(Arc::new(e)))
                        .and_then(|x| x.map_err(|e| TaskStreamError::External(Arc::new(e))));
                        return match result {
                            Ok((batch, next)) => {
                                offset = next;
                                Some((Ok(batch), (receiver, index, offset, false)))
                            }
                            Err(error) => Some((Err(error), (receiver, index, offset, true))),
                        };
                    }
                    if let Some(terminal) = terminal {
                        return terminal
                            .err()
                            .map(|error| (Err(error), (receiver, index, offset, true)));
                    }
                    if receiver.changed().await.is_err() {
                        return Some((
                            Err(TaskStreamError::Unknown(
                                "memory stream writer dropped before commit".into(),
                            )),
                            (receiver, index, offset, true),
                        ));
                    }
                }
            },
        ))
    }
}

pub(crate) struct MemoryStreamWriter {
    sender: watch::Sender<StreamState>,
    disk: Arc<DiskManager>,
    writer: Option<Box<dyn SpillWriter>>,
    buffer: usize,
}

impl MemoryStreamWriter {
    pub fn fail(&mut self, error: TaskStreamError) {
        self.sender
            .send_modify(|state| state.terminal = Some(Err(error)));
    }
}

impl Drop for MemoryStreamWriter {
    fn drop(&mut self) {
        self.sender.send_modify(|state| {
            if state.terminal.is_none() {
                // Task failures are reported by the task monitor. Abort also handles
                // successful early stops, so do not mask the task's cause with a
                // generic stream error when its writer is dropped.
                state.terminal = Some(Ok(()));
            }
        });
    }
}

#[tonic::async_trait]
impl TaskStreamChannelSink for MemoryStreamWriter {
    async fn write(&mut self, batch: RecordBatch) -> Result<TaskStreamWriteState> {
        if self.sender.is_closed() {
            return Ok(TaskStreamWriteState::Closed);
        }
        if self.sender.borrow().batches.len() < self.buffer {
            self.sender.send_modify(|state| state.batches.push(batch));
            return Ok(TaskStreamWriteState::Active);
        }
        let disk = self.disk.clone();
        let writer = self.writer.take();
        let (writer, spill, size) = tokio::task::spawn_blocking(move || -> Result<_> {
            let (mut writer, spill) = match writer {
                Some(writer) => (writer, None),
                None => {
                    let spill = disk.create_tmp_file("spilling a task stream")?;
                    if spill.path().is_none() {
                        return Err(DataFusionError::Execution(
                            "shuffle replay requires a local spill file".into(),
                        ));
                    }
                    (spill.open_writer()?, Some(spill))
                }
            };
            let mut encoded = Cursor::new(Vec::new());
            let mut ipc = StreamWriter::try_new(&mut encoded, &batch.schema())?;
            ipc.write(&batch)?;
            ipc.finish()?;
            let bytes = encoded.into_inner();
            let size = bytes.len() as u64;
            writer.write_all(&size.to_le_bytes())?;
            writer.write_all(&bytes)?;
            writer.flush()?;
            Ok((writer, spill, size + 8))
        })
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))??;
        self.writer = Some(writer);
        self.sender.send_modify(|state| {
            if let Some(spill) = spill {
                state.spill = Some(spill);
            }
            state.spilled_bytes += size;
        });
        Ok(TaskStreamWriteState::Active)
    }

    async fn commit(mut self: Box<Self>) -> Result<()> {
        let result = match self.writer.take() {
            Some(mut writer) => tokio::task::spawn_blocking(move || writer.finish())
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))
                .and_then(|result| result),
            None => Ok(()),
        };
        self.sender.send_modify(|state| {
            state.terminal = Some(
                result
                    .as_ref()
                    .map(|_| ())
                    .map_err(|error| TaskStreamError::Unknown(error.to_string())),
            );
        });
        result
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}
