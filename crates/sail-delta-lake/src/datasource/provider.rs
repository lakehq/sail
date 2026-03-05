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

use std::any::Any;
use std::borrow::Cow;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::catalog::Session;
use datafusion::common::stats::Statistics;
use datafusion::common::Result;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;

use crate::datasource::{
    df_logical_schema, get_pushdown_filters, DeltaScanConfig, DeltaTableStateExt,
};
use crate::kernel::models::Add;
use crate::kernel::DeltaResult;
use crate::physical::scan_planner::plan_delta_scan;
use crate::storage::LogStoreRef;
use crate::table::DeltaTableState;

/// A Delta table provider that enables additional metadata columns to be included during the scan
pub struct DeltaTableProvider {
    snapshot: DeltaTableState,
    log_store: LogStoreRef,
    config: DeltaScanConfig,
    schema: Arc<ArrowSchema>,
    files: Option<Arc<Vec<Add>>>,
}

impl std::fmt::Debug for DeltaTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaTableProvider")
            .field("snapshot_version", &self.snapshot.version())
            .field("config", &self.config)
            .field("schema", &self.schema)
            .field(
                "files_len",
                &self.files.as_ref().map(|files| files.len()).unwrap_or(0),
            )
            .finish()
    }
}

impl DeltaTableProvider {
    pub fn try_new(
        snapshot: DeltaTableState,
        log_store: LogStoreRef,
        config: DeltaScanConfig,
    ) -> DeltaResult<Self> {
        Ok(DeltaTableProvider {
            schema: df_logical_schema(
                &snapshot,
                &config.file_column_name,
                &config.commit_version_column_name,
                &config.commit_timestamp_column_name,
                config.schema.clone(),
            )?,
            snapshot,
            log_store,
            config,
            files: None,
        })
    }

    pub fn with_files(mut self, files: Vec<Add>) -> DeltaTableProvider {
        self.files = Some(Arc::new(files));
        self
    }

    pub fn snapshot(&self) -> &DeltaTableState {
        &self.snapshot
    }

    pub fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }

    pub fn config(&self) -> &DeltaScanConfig {
        &self.config
    }
}

#[async_trait]
impl TableProvider for DeltaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan_delta_scan(
            session,
            &self.snapshot,
            &self.log_store,
            &self.config,
            self.files.clone(),
            projection,
            filters,
            limit,
        )
        .await
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let partition_cols = self.snapshot.metadata().partition_columns().as_slice();
        Ok(get_pushdown_filters(filter, partition_cols))
    }

    fn statistics(&self) -> Option<Statistics> {
        self.snapshot.datafusion_table_statistics(None)
    }
}
