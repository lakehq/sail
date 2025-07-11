use std::sync::Arc;
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::config::TableParquetOptions;
use datafusion::common::ScalarValue;
use datafusion::datasource::memory::DataSourceExec;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, FileSource, ParquetSource,
};
use datafusion::execution::SessionState;
use datafusion::physical_expr::{expressions, PhysicalExpr};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use deltalake::errors::DeltaResult;
use deltalake::kernel::{Action, Add, AddCDCFile, CommitInfo, Remove};
use deltalake::logstore::{get_actions, LogStoreRef};
use deltalake::table::state::DeltaTableState;
use deltalake::DeltaTableError;

use crate::delta_datafusion::cdf::*;
use crate::delta_datafusion::{datafusion_to_delta_error, DataFusionMixins};

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
        let ts = self.starting_timestamp.unwrap_or(DateTime::UNIX_EPOCH);
        for v in 0..self.snapshot.version() {
            if let Ok(Some(bytes)) = self.log_store.read_commit_entry(v).await {
                if let Ok(actions) = get_actions(v, bytes).await {
                    if actions.iter().any(|action| {
                        matches!(action, Action::CommitInfo(CommitInfo {
                            timestamp: Some(t), ..
                        }) if ts.timestamp_millis() < *t)
                    }) {
                        return Ok(v);
                    }
                }
            }
        }
        Ok(0)
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
        if self.starting_version.is_none() && self.starting_timestamp.is_none() {
            return Err(DeltaTableError::NoStartingVersionOrTimestamp);
        }
        let start = if let Some(s) = self.starting_version {
            s
        } else {
            self.calculate_earliest_version().await?
        };

        let mut change_files: Vec<CdcDataSpec<AddCDCFile>> = vec![];
        let mut add_files: Vec<CdcDataSpec<Add>> = vec![];
        let mut remove_files: Vec<CdcDataSpec<Remove>> = vec![];

        // Start from 0 since if start > latest commit, the returned commit is not a valid commit
        let latest_version = match self.log_store.get_latest_version(start).await {
            Ok(latest_version) => latest_version,
            Err(DeltaTableError::InvalidVersion(_)) if self.allow_out_of_range => {
                return Ok((change_files, add_files, remove_files));
            }
            Err(e) => return Err(e),
        };

        let mut end = self.ending_version.unwrap_or(latest_version);

        if end > latest_version {
            end = latest_version;
        }

        if end < start {
            return if self.allow_out_of_range {
                Ok((change_files, add_files, remove_files))
            } else {
                Err(DeltaTableError::ChangeDataInvalidVersionRange { start, end })
            };
        }
        if start > latest_version {
            return if self.allow_out_of_range {
                Ok((change_files, add_files, remove_files))
            } else {
                Err(DeltaTableError::InvalidVersion(start))
            };
        }

        let starting_timestamp = self.starting_timestamp.unwrap_or(DateTime::UNIX_EPOCH);
        let ending_timestamp = self
            .ending_timestamp
            .unwrap_or(DateTime::from(SystemTime::now()));

        // Check that starting_timestamp is within boundaries of the latest version
        let latest_snapshot_bytes = self
            .log_store
            .read_commit_entry(latest_version)
            .await?
            .ok_or(DeltaTableError::InvalidVersion(latest_version))?;

        let latest_version_actions: Vec<Action> =
            get_actions(latest_version, latest_snapshot_bytes).await?;
        let latest_version_commit = latest_version_actions
            .iter()
            .find(|a| matches!(a, Action::CommitInfo(_)));

        if let Some(Action::CommitInfo(CommitInfo {
            timestamp: Some(latest_timestamp),
            ..
        })) = latest_version_commit
        {
            if starting_timestamp.timestamp_millis() > *latest_timestamp {
                return if self.allow_out_of_range {
                    Ok((change_files, add_files, remove_files))
                } else {
                    Err(DeltaTableError::ChangeDataTimestampGreaterThanCommit { ending_timestamp })
                };
            }
        }

        for version in start..=end {
            let snapshot_bytes = self
                .log_store
                .read_commit_entry(version)
                .await?
                .ok_or(DeltaTableError::InvalidVersion(version));

            let version_actions: Vec<Action> = get_actions(version, snapshot_bytes?).await?;

            let mut ts = 0;
            let mut cdc_actions = vec![];

            if self.starting_timestamp.is_some() || self.ending_timestamp.is_some() {
                // TODO: fallback on other actions for timestamps because CommitInfo action is optional
                // theoretically.
                let version_commit = version_actions
                    .iter()
                    .find(|a| matches!(a, Action::CommitInfo(_)));
                if let Some(Action::CommitInfo(CommitInfo {
                    timestamp: Some(t), ..
                })) = version_commit
                {
                    if starting_timestamp.timestamp_millis() > *t
                        || *t > ending_timestamp.timestamp_millis()
                    {
                        continue;
                    }
                }
            }

            for action in &version_actions {
                match action {
                    Action::Cdc(f) => cdc_actions.push(f.clone()),
                    Action::Metadata(md) => {
                        if let Some(Some(key)) = &md.configuration.get("delta.enableChangeDataFeed")
                        {
                            let key = key.to_lowercase();
                            // Check here to ensure the CDC function is enabled for the first version of the read
                            // and check in subsequent versions only that it was not disabled.
                            if (version == start && key != "true") || key == "false" {
                                return Err(DeltaTableError::ChangeDataNotRecorded {
                                    version,
                                    start,
                                    end,
                                });
                            }
                        } else if version == start {
                            return Err(DeltaTableError::ChangeDataNotEnabled { version });
                        };
                    }
                    Action::CommitInfo(ci) => {
                        ts = ci.timestamp.unwrap_or(0);
                    }
                    _ => {}
                }
            }

            if !cdc_actions.is_empty() {
                change_files.push(CdcDataSpec::new(version, ts, cdc_actions))
            } else {
                let add_actions = version_actions
                    .iter()
                    .filter_map(|a| match a {
                        Action::Add(a) if a.data_change => Some(a.clone()),
                        _ => None,
                    })
                    .collect::<Vec<Add>>();

                let remove_actions = version_actions
                    .iter()
                    .filter_map(|r| match r {
                        Action::Remove(r) if r.data_change => Some(r.clone()),
                        _ => None,
                    })
                    .collect::<Vec<Remove>>();

                if !add_actions.is_empty() {
                    add_files.push(CdcDataSpec::new(version, ts, add_actions));
                }

                if !remove_actions.is_empty() {
                    remove_files.push(CdcDataSpec::new(version, ts, remove_actions));
                }
            }
        }

        Ok((change_files, add_files, remove_files))
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
        _session_sate: &SessionState,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> DeltaResult<Arc<dyn ExecutionPlan>> {
        let (cdc, add, remove) = self.determine_files_to_read().await?;

        let partition_values = self.snapshot.metadata().partition_columns.clone();
        let schema = self.snapshot.input_schema()?;
        let schema_fields: Vec<Arc<Field>> = self
            .snapshot
            .input_schema()?
            .fields()
            .into_iter()
            .filter(|f| !partition_values.contains(f.name()))
            .cloned()
            .collect();

        let this_partition_values = partition_values
            .iter()
            .map(|name| schema.field_with_name(name).map(|f| f.to_owned()))
            .collect::<Result<Vec<_>, datafusion::arrow::error::ArrowError>>()?;

        // Setup for the Read Schemas of each kind of file, CDC files include commit action type so they need a slightly
        // different schema than standard add file reads
        let cdc_file_schema = create_cdc_schema(schema_fields.clone(), true);
        let add_remove_file_schema = create_cdc_schema(schema_fields, false);

        // Set up the mapping of partition columns to be projected into the final output batch
        // cdc for example has timestamp, version, and any table partitions mapped here.
        // add on the other hand has action type, timestamp, version and any additional table partitions because adds do
        // not include their actions
        let mut cdc_partition_cols = CDC_PARTITION_SCHEMA.clone();
        let mut add_remove_partition_cols = ADD_PARTITION_SCHEMA.clone();
        cdc_partition_cols.extend_from_slice(&this_partition_values);
        add_remove_partition_cols.extend_from_slice(&this_partition_values);

        // Set up the partition to physical file mapping, this is a mostly unmodified version of what is done in load
        let cdc_file_groups =
            create_partition_values(schema.clone(), cdc, &partition_values, None)?;
        let add_file_groups = create_partition_values(
            schema.clone(),
            add,
            &partition_values,
            Self::get_add_action_type(),
        )?;
        let remove_file_groups = create_partition_values(
            schema.clone(),
            remove,
            &partition_values,
            Self::get_remove_action_type(),
        )?;

        // Create the parquet scans for each associated type of file.
        let mut parquet_source = ParquetSource::new(TableParquetOptions::new());
        if let Some(filters) = filters {
            parquet_source = parquet_source.with_predicate(Arc::clone(filters));
        }
        let parquet_source: Arc<dyn FileSource> = Arc::new(parquet_source);

        // Create object store URL from log store
        let root_uri = url::Url::parse(&self.log_store.root_uri())
            .map_err(|e| DeltaTableError::Generic(format!("Invalid root URI: {e}")))?;
        let object_store_url = ObjectStoreUrl::parse(root_uri.as_str()).expect("Valid URL");

        let cdc_scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(
            FileScanConfigBuilder::new(
                object_store_url.clone(),
                Arc::clone(&cdc_file_schema),
                Arc::clone(&parquet_source),
            )
            .with_file_groups(cdc_file_groups.into_values().map(FileGroup::from).collect())
            .with_table_partition_cols(cdc_partition_cols)
            .build(),
        );

        let add_scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(
            FileScanConfigBuilder::new(
                object_store_url.clone(),
                Arc::clone(&add_remove_file_schema),
                Arc::clone(&parquet_source),
            )
            .with_file_groups(add_file_groups.into_values().map(FileGroup::from).collect())
            .with_table_partition_cols(add_remove_partition_cols.clone())
            .build(),
        );

        let remove_scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(
            FileScanConfigBuilder::new(
                object_store_url,
                Arc::clone(&add_remove_file_schema),
                parquet_source,
            )
            .with_file_groups(
                remove_file_groups
                    .into_values()
                    .map(FileGroup::from)
                    .collect(),
            )
            .with_table_partition_cols(add_remove_partition_cols)
            .build(),
        );

        // The output batches are then unioned to create a single output. Coalesce partitions is only here for the time
        // being for development. I plan to parallelize the reads once the base idea is correct.
        let union_scan: Arc<dyn ExecutionPlan> =
            Arc::new(UnionExec::new(vec![cdc_scan, add_scan, remove_scan]));

        // We project the union in the order of the input_schema + cdc cols at the end
        // This is to ensure the DeltaCdfTableProvider uses the correct schema construction.
        let mut fields = schema.fields().to_vec();
        for f in ADD_PARTITION_SCHEMA.clone() {
            fields.push(f.into());
        }
        let project_schema = Schema::new(fields);

        let union_schema = union_scan.schema();

        let expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = project_schema
            .fields()
            .into_iter()
            .map(|f| -> (Arc<dyn PhysicalExpr>, String) {
                let field_name = f.name();
                let expr = Arc::new(expressions::Column::new(
                    field_name,
                    union_schema
                        .index_of(field_name)
                        .expect("Field not found in union schema"),
                ));
                (expr, field_name.to_owned())
            })
            .collect();

        let scan = Arc::new(
            ProjectionExec::try_new(expressions, union_scan).map_err(datafusion_to_delta_error)?,
        );

        Ok(scan)
    }
}

// Helper function to collect batches associated with reading CDF data
#[allow(dead_code)]
pub(crate) async fn collect_batches(
    num_partitions: usize,
    stream: Arc<dyn ExecutionPlan>,
    ctx: SessionContext,
) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    let mut batches = vec![];
    for p in 0..num_partitions {
        let data: Vec<RecordBatch> =
            crate::operations::collect_sendable_stream(stream.execute(p, ctx.task_ctx())?).await?;
        batches.extend_from_slice(&data);
    }
    Ok(batches)
}
