//! Module for reading the change datafeed of delta tables
//!
//! # Example
//! ```rust ignore
//! let table = open_table("../path/to/table")?;
//! let builder = CdfLoadBuilder::new(table.log_store(), table.snapshot())
//!     .with_starting_version(3);
//!
//! let ctx = SessionContext::new();
//! let provider = DeltaCdfTableProvider::try_new(builder)?;
//! let df = ctx.read_table(provider).await?;

use std::sync::Arc;
use std::time::SystemTime;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Field, Schema};
use chrono::{DateTime, Utc};
use datafusion::common::config::TableParquetOptions;
use datafusion::common::ScalarValue;
use datafusion::datasource::memory::DataSourceExec;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, FileSource, ParquetSource,
};
use datafusion::execution::SessionState;
use datafusion::physical_expr::{expressions, PhysicalExpr};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use crate::delta_datafusion::{register_store, DataFusionMixins};
use deltalake::errors::DeltaResult;
use deltalake::kernel::{Action, Add, AddCDCFile, CommitInfo, Remove};
use deltalake::logstore::{get_actions, LogStoreRef};
use deltalake::table::state::DeltaTableState;
use deltalake::DeltaTableError;
use crate::delta_datafusion::cdf::*;

/// Builder for create a read of change data feeds for delta tables
#[derive(Clone, Debug)]
pub struct CdfLoadBuilder {
    /// A snapshot of the to-be-loaded table's state
    pub snapshot: DeltaTableState,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Version to read from
    starting_version: Option<i64>,
    /// Version to stop reading at
    ending_version: Option<i64>,
    /// Starting timestamp of commits to accept
    starting_timestamp: Option<DateTime<Utc>>,
    /// Ending timestamp of commits to accept
    ending_timestamp: Option<DateTime<Utc>>,
    /// Enable ending version or timestamp exceeding the last commit
    allow_out_of_range: bool,
}

impl CdfLoadBuilder {
    /// Create a new [`LoadBuilder`]
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            snapshot,
            log_store,
            starting_version: None,
            ending_version: None,
            starting_timestamp: None,
            ending_timestamp: None,
            allow_out_of_range: false,
        }
    }

    /// Version to start at (version 0 if not provided)
    pub fn with_starting_version(mut self, starting_version: i64) -> Self {
        self.starting_version = Some(starting_version);
        self
    }

    /// Version (inclusive) to end at
    pub fn with_ending_version(mut self, ending_version: i64) -> Self {
        self.ending_version = Some(ending_version);
        self
    }

    /// Timestamp (inclusive) to end at
    pub fn with_ending_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.ending_timestamp = Some(timestamp);
        self
    }

    /// Timestamp to start from
    pub fn with_starting_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.starting_timestamp = Some(timestamp);
        self
    }

    /// Enable ending version or timestamp exceeding the last commit
    pub fn with_allow_out_of_range(mut self) -> Self {
        self.allow_out_of_range = true;
        self
    }

    async fn calculate_earliest_version(&self) -> DeltaResult<i64> {
        unimplemented!("calculate_earliest_version method needs to be updated for datafusion 0.48 compatibility")
    }

    /// This is a rust version of https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/commands/cdc/CDCReader.scala#L418
    /// Which iterates through versions of the delta table collects the relevant actions / commit info and returns those
    /// groupings for later use. The scala implementation has a lot more edge case handling and read schema checking (and just error checking in general)
    /// than I have right now. I plan to extend the checks once we have a stable state of the initial implementation.
    async fn determine_files_to_read(
        &self,
    ) -> DeltaResult<(
        Vec<CdcDataSpec<AddCDCFile>>,
        Vec<CdcDataSpec<Add>>,
        Vec<CdcDataSpec<Remove>>,
    )> {
        unimplemented!("determine_files_to_read method needs to be updated for datafusion 0.48 compatibility")
    }

    #[inline]
    fn get_add_action_type() -> Option<ScalarValue> {
        Some(ScalarValue::Utf8(Some(String::from("insert"))))
    }

    #[inline]
    fn get_remove_action_type() -> Option<ScalarValue> {
        Some(ScalarValue::Utf8(Some(String::from("delete"))))
    }

    /// Executes the scan
    pub(crate) async fn build(
        &self,
        session_sate: &SessionState,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> DeltaResult<Arc<dyn ExecutionPlan>> {
        unimplemented!("CdfLoadBuilder::build method needs to be updated for datafusion 0.48 compatibility")
    }
}

// #[allow(unused)]
// /// Helper function to collect batches associated with reading CDF data
// pub(crate) async fn collect_batches(
//     num_partitions: usize,
//     stream: Arc<dyn ExecutionPlan>,
//     ctx: SessionContext,
// ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
//     let mut batches = vec![];
//     for p in 0..num_partitions {
//         let data: Vec<RecordBatch> =
//             crate::operations::collect_sendable_stream(stream.execute(p, ctx.task_ctx())?).await?;
//         batches.extend_from_slice(&data);
//     }
//     Ok(batches)
// }
