// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright (2025) LakeSail, Inc.
// Modified in 2025 by LakeSail, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/delta_datafusion/mod.rs>

use std::collections::HashSet;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::stats::Statistics;
use datafusion::datasource::object_store::ObjectStoreUrl;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::kernel::snapshot::LogDataHandler;
use crate::kernel::{DeltaResult, DeltaTableError};
use crate::table::DeltaTableState;
pub const PATH_COLUMN: &str = "__sail_file_path";
pub const COMMIT_VERSION_COLUMN: &str = "_commit_version";
pub const COMMIT_TIMESTAMP_COLUMN: &str = "_commit_timestamp";

pub mod actions;
pub mod expressions;
pub mod provider;
pub mod pruning;
pub mod scan;
pub mod schema;

// Re-exports
pub use actions::{adds_to_remove_actions, partitioned_file_from_action};
pub use expressions::{
    collect_physical_columns, get_pushdown_filters, simplify_expr, PredicateProperties,
};
pub use provider::DeltaTableProvider;
pub use pruning::{prune_files, PruningResult};
pub use scan::build_file_scan_config;
pub use schema::{df_logical_schema, DataFusionMixins};

pub(crate) fn create_object_store_url(location: &Url) -> DeltaResult<ObjectStoreUrl> {
    Ok(ObjectStoreUrl::parse(
        &location[..url::Position::BeforePath],
    )?)
}

// Extension trait to add datafusion_table_statistics method to DeltaTableState
pub(crate) trait DeltaTableStateExt {
    fn datafusion_table_statistics(&self, mask: Option<&[bool]>) -> Option<Statistics>;
}

impl DeltaTableStateExt for DeltaTableState {
    fn datafusion_table_statistics(&self, mask: Option<&[bool]>) -> Option<Statistics> {
        if let Some(mask) = mask {
            let es = self.snapshot();
            let boolean_array = BooleanArray::from(mask.to_vec());
            let pruned_files = filter_record_batch(&es.files, &boolean_array).ok()?;
            LogDataHandler::new(&pruned_files, es.table_configuration()).statistics()
        } else {
            self.snapshot().log_data().statistics()
        }
    }
}

#[derive(Debug, Clone)]
/// Used to specify if additional metadata columns are exposed to the user
pub struct DeltaScanConfigBuilder {
    /// Include the source path for each record. The name of this column is determined by `file_column_name`
    include_file_column: bool,
    /// Column name that contains the source path.
    ///
    /// If include_file_column is true and the name is None then it will be auto-generated
    /// Otherwise the user provided name will be used
    file_column_name: Option<String>,
    /// Whether to wrap partition values in a dictionary encoding to potentially save space
    wrap_partition_values: bool,
    /// Whether to push down filter in end result or just prune the files
    enable_parquet_pushdown: bool,
    /// Schema to scan table with
    schema: Option<SchemaRef>,
    /// Include commit version/timestamp virtual columns.
    include_commit_metadata: bool,
    /// Column name that contains the commit version.
    commit_version_column_name: Option<String>,
    /// Column name that contains the commit timestamp.
    commit_timestamp_column_name: Option<String>,
}

impl Default for DeltaScanConfigBuilder {
    fn default() -> Self {
        DeltaScanConfigBuilder {
            include_file_column: false,
            file_column_name: None,
            wrap_partition_values: true,
            enable_parquet_pushdown: true,
            schema: None,
            include_commit_metadata: false,
            commit_version_column_name: None,
            commit_timestamp_column_name: None,
        }
    }
}

impl DeltaScanConfigBuilder {
    /// Construct a new instance of `DeltaScanConfigBuilder`
    pub fn new() -> Self {
        Self::default()
    }

    /// Indicate that a column containing a records file path is included.
    /// Column name is generated and can be determined once this Config is built
    pub fn with_file_column(mut self, include: bool) -> Self {
        self.include_file_column = include;
        self.file_column_name = None;
        self
    }

    /// Use the provided [SchemaRef] for the [DeltaScan]
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Indicate that commit metadata virtual columns are included.
    pub fn with_commit_metadata_columns(mut self, include: bool) -> Self {
        self.include_commit_metadata = include;
        self
    }

    /// Build a DeltaScanConfig and ensure no column name conflicts occur during downstream processing
    pub fn build(&self, snapshot: &DeltaTableState) -> DeltaResult<DeltaScanConfig> {
        let file_column_name = if self.include_file_column {
            let input_schema = snapshot.input_schema()?;
            let mut column_names: HashSet<&String> = HashSet::new();
            for field in input_schema.fields.iter() {
                column_names.insert(field.name());
            }

            match &self.file_column_name {
                Some(name) => {
                    if column_names.contains(name) {
                        return Err(DeltaTableError::generic(format!(
                            "Unable to add file path column since column with name {name} exits"
                        )));
                    }

                    Some(name.to_owned())
                }
                None => {
                    let prefix = PATH_COLUMN;
                    let mut idx = 0;
                    let mut name = prefix.to_owned();

                    while column_names.contains(&name) {
                        idx += 1;
                        name = format!("{prefix}_{idx}");
                    }

                    Some(name)
                }
            }
        } else {
            None
        };

        let (commit_version_column_name, commit_timestamp_column_name) =
            if self.include_commit_metadata {
                let input_schema = snapshot.input_schema()?;
                let mut column_names: HashSet<String> = input_schema
                    .fields
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                if let Some(file_column_name) = &file_column_name {
                    column_names.insert(file_column_name.clone());
                }

                let mut unique_name = |base: &str| -> String {
                    if !column_names.contains(base) {
                        column_names.insert(base.to_string());
                        return base.to_string();
                    }
                    let mut idx = 0;
                    loop {
                        idx += 1;
                        let candidate = format!("{base}_{idx}");
                        if !column_names.contains(&candidate) {
                            column_names.insert(candidate.clone());
                            return candidate;
                        }
                    }
                };

                (
                    Some(
                        self.commit_version_column_name
                            .clone()
                            .unwrap_or_else(|| unique_name(COMMIT_VERSION_COLUMN)),
                    ),
                    Some(
                        self.commit_timestamp_column_name
                            .clone()
                            .unwrap_or_else(|| unique_name(COMMIT_TIMESTAMP_COLUMN)),
                    ),
                )
            } else {
                (None, None)
            };

        Ok(DeltaScanConfig {
            file_column_name,
            wrap_partition_values: self.wrap_partition_values,
            enable_parquet_pushdown: self.enable_parquet_pushdown,
            schema: self.schema.clone(),
            commit_version_column_name,
            commit_timestamp_column_name,
        })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Include additional metadata columns during a [`DeltaScan`]
pub struct DeltaScanConfig {
    /// Include the source path for each record
    pub file_column_name: Option<String>,
    /// Wrap partition values in a dictionary encoding
    pub wrap_partition_values: bool,
    /// Allow pushdown of the scan filter
    pub enable_parquet_pushdown: bool,
    /// Schema to read as
    pub schema: Option<SchemaRef>,
    /// Commit version virtual column name.
    pub commit_version_column_name: Option<String>,
    /// Commit timestamp virtual column name.
    pub commit_timestamp_column_name: Option<String>,
}
