// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! The physical write into a Lance dataset.

use std::fmt::Formatter;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::sink::DataSink;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datafusion_common::{Result, plan_datafusion_err};
use futures::StreamExt;
use lance::Dataset;
use lance::dataset::{InsertBuilder, WriteMode, WriteParams};
use lance_file::version::LanceFileVersion;

use crate::exec::lance_error;
use crate::options::{LanceWriteMode, LanceWriteOptions};

/// Writes a DataFusion stream into a Lance dataset in one transaction.
#[derive(Debug)]
pub struct LanceDataSink {
    uri: String,
    mode: LanceWriteMode,
    options: LanceWriteOptions,
    schema: SchemaRef,
}

impl LanceDataSink {
    pub fn new(
        uri: String,
        mode: LanceWriteMode,
        options: LanceWriteOptions,
        schema: SchemaRef,
    ) -> Self {
        Self {
            uri,
            mode,
            options,
            schema,
        }
    }

    fn write_params(&self, mode: WriteMode) -> Result<WriteParams> {
        let defaults = WriteParams::default();
        let data_storage_version = self
            .options
            .file_format_version
            .as_deref()
            .map(|version| {
                LanceFileVersion::from_str(version).map_err(|e| {
                    plan_datafusion_err!("unsupported Lance file format version '{version}': {e}")
                })
            })
            .transpose()?;
        Ok(WriteParams {
            mode,
            max_rows_per_file: self
                .options
                .max_rows_per_file
                .unwrap_or(defaults.max_rows_per_file),
            max_rows_per_group: self
                .options
                .max_rows_per_group
                .unwrap_or(defaults.max_rows_per_group),
            max_bytes_per_file: self
                .options
                .max_bytes_per_file
                .unwrap_or(defaults.max_bytes_per_file),
            data_storage_version,
            enable_stable_row_ids: self
                .options
                .enable_stable_row_ids
                .unwrap_or(defaults.enable_stable_row_ids),
            ..defaults
        })
    }
}

impl DisplayAs for LanceDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "LanceDataSink: uri={}, mode={:?}", self.uri, self.mode)
    }
}

#[async_trait]
impl DataSink for LanceDataSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let Some(mode) = resolve_write_mode(&self.uri, self.mode).await? else {
            // `IgnoreIfExists` against an existing dataset: write nothing and
            // leave the dataset as it is.
            return Ok(0);
        };
        let params = self.write_params(mode)?;

        // Lance and Sail share a DataFusion version, so the stream is handed
        // over as it is. Counting the rows on the way through is the only thing
        // this wrapper does; an error in the stream reaches the Lance writer and
        // aborts the write, leaving no new dataset version behind.
        let rows = Arc::new(AtomicU64::new(0));
        let counted = {
            let rows = Arc::clone(&rows);
            let schema = data.schema();
            let stream = data.inspect(move |batch| {
                if let Ok(batch) = batch {
                    rows.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                }
            });
            Box::pin(RecordBatchStreamAdapter::new(schema, stream)) as SendableRecordBatchStream
        };

        InsertBuilder::new(self.uri.as_str())
            .with_params(&params)
            .execute_stream(counted)
            .await
            .map_err(lance_error)?;
        Ok(rows.load(Ordering::Relaxed))
    }
}

/// Resolves the Lance write mode against the dataset that is already there.
///
/// Returns `None` when the write must be skipped entirely.
async fn resolve_write_mode(uri: &str, mode: LanceWriteMode) -> Result<Option<WriteMode>> {
    match mode {
        LanceWriteMode::Overwrite => Ok(Some(WriteMode::Overwrite)),
        LanceWriteMode::Append => {
            if dataset_exists(uri).await? {
                Ok(Some(WriteMode::Append))
            } else {
                Ok(Some(WriteMode::Create))
            }
        }
        LanceWriteMode::ErrorIfExists => {
            if dataset_exists(uri).await? {
                Err(plan_datafusion_err!("Lance dataset already exists: {uri}"))
            } else {
                Ok(Some(WriteMode::Create))
            }
        }
        LanceWriteMode::IgnoreIfExists => {
            if dataset_exists(uri).await? {
                Ok(None)
            } else {
                Ok(Some(WriteMode::Create))
            }
        }
    }
}

async fn dataset_exists(uri: &str) -> Result<bool> {
    match Dataset::open(uri).await {
        Ok(_) => Ok(true),
        Err(lance::Error::DatasetNotFound { .. }) => Ok(false),
        Err(e) => Err(lance_error(e)),
    }
}
