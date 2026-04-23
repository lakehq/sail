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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result};
use object_store::ObjectStore;
use sail_data_source::options::gen::DeltaWriteOptions;
use url::Url;

use super::log_segment::LogSegmentFiles;
use crate::kernel::DeltaSnapshotConfig;
use crate::storage::{default_logstore, LogStoreRef, StorageConfig};
use crate::table::{open_table_with_object_store_and_table_config, DeltaTable};

/// Configuration shared by all Delta planners.
#[derive(Clone)]
pub struct DeltaPlannerConfig {
    pub table_url: Url,
    pub options: DeltaWriteOptions,
    pub metadata_configuration: HashMap<String, String>,
    pub partition_columns: Vec<String>,
    pub table_schema_for_cond: Option<SchemaRef>,
    pub table_exists: bool,
    /// Column-level generation expressions keyed by column name. Populated from
    /// `delta.generationExpression` metadata attached to the write input's logical
    /// schema. Used by the writer to carry generation metadata into the initial
    /// Delta commit (new tables) even when the physical planner strips the arrow
    /// field metadata set at logical-plan construction time.
    pub generation_expressions: HashMap<String, String>,
}

impl DeltaPlannerConfig {
    pub fn new(
        table_url: Url,
        options: DeltaWriteOptions,
        metadata_configuration: HashMap<String, String>,
        partition_columns: Vec<String>,
        table_schema_for_cond: Option<SchemaRef>,
        table_exists: bool,
    ) -> Self {
        Self {
            table_url,
            options,
            metadata_configuration,
            partition_columns,
            table_schema_for_cond,
            table_exists,
            generation_expressions: HashMap::new(),
        }
    }

    pub fn with_generation_expressions(
        mut self,
        generation_expressions: HashMap<String, String>,
    ) -> Self {
        self.generation_expressions = generation_expressions;
        self
    }
}

/// Shared planner context containing table/session state.
pub struct PlannerContext<'a> {
    session: &'a dyn Session,
    config: DeltaPlannerConfig,
    // Planner-local memoization cache used to avoid repeated `_delta_log` listings when
    // one planning request builds multiple log-replay branches (e.g. overwrite-if old/new).
    log_segment_files_cache: Arc<Mutex<HashMap<i64, LogSegmentFiles>>>,
}

impl<'a> PlannerContext<'a> {
    pub fn new(session: &'a dyn Session, config: DeltaPlannerConfig) -> Self {
        Self {
            session,
            config,
            log_segment_files_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn session(&self) -> &'a dyn Session {
        self.session
    }

    pub fn config(&self) -> &DeltaPlannerConfig {
        &self.config
    }

    pub fn table_url(&self) -> &Url {
        &self.config.table_url
    }

    pub fn options(&self) -> &DeltaWriteOptions {
        &self.config.options
    }

    pub fn partition_columns(&self) -> &[String] {
        &self.config.partition_columns
    }

    pub fn metadata_configuration(&self) -> &HashMap<String, String> {
        &self.config.metadata_configuration
    }

    pub fn table_schema_for_cond(&self) -> Option<SchemaRef> {
        self.config.table_schema_for_cond.clone()
    }

    pub fn table_exists(&self) -> bool {
        self.config.table_exists
    }

    pub fn generation_expressions(&self) -> &HashMap<String, String> {
        &self.config.generation_expressions
    }

    pub fn into_config(self) -> DeltaPlannerConfig {
        self.config
    }

    pub(crate) fn get_cached_log_segment_files(&self, version: i64) -> Option<LogSegmentFiles> {
        self.log_segment_files_cache
            .lock()
            .ok()
            .and_then(|cache| cache.get(&version).cloned())
    }

    pub(crate) fn set_cached_log_segment_files(&self, version: i64, files: LogSegmentFiles) {
        if let Ok(mut cache) = self.log_segment_files_cache.lock() {
            cache.insert(version, files);
        }
    }

    pub fn object_store(&self) -> Result<Arc<dyn ObjectStore>> {
        self.session
            .runtime_env()
            .object_store_registry
            .get_store(&self.config.table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    pub fn log_store(&self) -> Result<LogStoreRef> {
        let storage_config = StorageConfig;
        let object_store = self.object_store()?;
        let prefixed_store = storage_config
            .decorate_store(Arc::clone(&object_store), &self.config.table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(default_logstore(
            prefixed_store,
            object_store,
            &self.config.table_url,
            &storage_config,
        ))
    }

    pub async fn open_table(&self) -> Result<DeltaTable> {
        let object_store = self.object_store()?;
        // Planning-time code only needs the log segment / metadata; avoid eagerly materializing
        // the full active file list on the driver.
        let table_config = DeltaSnapshotConfig {
            require_files: false,
            ..Default::default()
        };

        open_table_with_object_store_and_table_config(
            self.config.table_url.clone(),
            object_store,
            StorageConfig,
            table_config,
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}
