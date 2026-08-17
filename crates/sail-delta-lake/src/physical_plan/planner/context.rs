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
use sail_common_datafusion::catalog::{CatalogTableColumnIdentity, LakehouseExecutionContext};
use sail_common_datafusion::datasource::PhysicalSinkMode;
use url::Url;

use super::log_segment::LogSegmentFiles;
use crate::delta_log::{LogStoreRef, StorageConfig, default_logstore};
use crate::options::r#gen::DeltaWriteOptions;
use crate::physical_plan::{
    DeltaCommitContext, DeltaWriteContext, DeltaWriterExecOptions, prepare_delta_write_context,
};
use crate::snapshot::DeltaSnapshotConfig;
use crate::table::{
    DeltaSnapshot, DeltaTable, catalog_managed_commit_context,
    create_delta_table_with_object_store, load_catalog_managed_commits_for_snapshot,
};

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
    /// Column-level default expressions keyed by column name. Populated from
    /// `CURRENT_DEFAULT` metadata attached to the write input's logical schema.
    pub default_expressions: HashMap<String, String>,
    /// Target catalog field nullability keyed by column name.
    pub target_nullability: HashMap<String, bool>,
    /// Logical schema override used for Delta metadata planning. It can carry
    /// nullability/metadata that the physical plan schema cannot represent after
    /// projection rewrites.
    pub metadata_schema: Option<SchemaRef>,
    pub identity_columns: HashMap<String, CatalogTableColumnIdentity>,
    pub table_snapshot: Option<Arc<DeltaSnapshot>>,
    pub lakehouse_table: Option<LakehouseExecutionContext>,
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
            default_expressions: HashMap::new(),
            target_nullability: HashMap::new(),
            metadata_schema: None,
            identity_columns: HashMap::new(),
            table_snapshot: None,
            lakehouse_table: None,
        }
    }

    pub fn with_generation_expressions(
        mut self,
        generation_expressions: HashMap<String, String>,
    ) -> Self {
        self.generation_expressions = generation_expressions;
        self
    }

    pub fn with_default_expressions(
        mut self,
        default_expressions: HashMap<String, String>,
    ) -> Self {
        self.default_expressions = default_expressions;
        self
    }

    pub fn with_target_nullability(mut self, target_nullability: HashMap<String, bool>) -> Self {
        self.target_nullability = target_nullability;
        self
    }

    pub fn with_metadata_schema(mut self, metadata_schema: Option<SchemaRef>) -> Self {
        self.metadata_schema = metadata_schema;
        self
    }

    pub fn with_identity_columns(
        mut self,
        identity_columns: HashMap<String, CatalogTableColumnIdentity>,
    ) -> Self {
        self.identity_columns = identity_columns;
        self
    }

    pub fn with_table_snapshot(mut self, table_snapshot: Option<Arc<DeltaSnapshot>>) -> Self {
        self.table_snapshot = table_snapshot;
        self
    }

    pub fn with_lakehouse_table(
        mut self,
        lakehouse_table: Option<LakehouseExecutionContext>,
    ) -> Self {
        self.lakehouse_table = lakehouse_table;
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

    pub fn default_expressions(&self) -> &HashMap<String, String> {
        &self.config.default_expressions
    }

    pub fn target_nullability(&self) -> &HashMap<String, bool> {
        &self.config.target_nullability
    }

    pub fn metadata_schema(&self) -> Option<&SchemaRef> {
        self.config.metadata_schema.as_ref()
    }

    pub fn identity_columns(&self) -> &HashMap<String, CatalogTableColumnIdentity> {
        &self.config.identity_columns
    }

    pub fn table_snapshot(&self) -> Option<&Arc<DeltaSnapshot>> {
        self.config.table_snapshot.as_ref()
    }

    pub fn catalog_table(&self) -> Option<&[String]> {
        self.config
            .lakehouse_table
            .as_ref()
            .map(LakehouseExecutionContext::catalog_table)
    }

    pub fn catalog_table_vec(&self) -> Option<Vec<String>> {
        self.catalog_table().map(<[String]>::to_vec)
    }

    pub fn lakehouse_table(&self) -> Option<&LakehouseExecutionContext> {
        self.config.lakehouse_table.as_ref()
    }

    pub fn commit_context(&self) -> DeltaCommitContext {
        self.table_snapshot()
            .map(|snapshot| DeltaCommitContext::from_snapshot(snapshot.as_ref()))
            .unwrap_or_default()
    }

    pub fn prepare_write_context(
        &self,
        input_schema: &SchemaRef,
        sink_mode: &PhysicalSinkMode,
        operation_override: Option<crate::spec::DeltaOperation>,
    ) -> Result<DeltaWriteContext> {
        let options = DeltaWriterExecOptions::from(self.options().clone())
            .with_generation_expressions(self.generation_expressions().clone())
            .with_default_expressions(self.default_expressions().clone())
            .with_target_nullability(self.target_nullability().clone())
            .with_identity_columns(self.identity_columns().clone());
        let input_schema = self.metadata_schema().unwrap_or(input_schema);
        prepare_delta_write_context(
            self.table_url(),
            self.table_snapshot().map(|snapshot| snapshot.as_ref()),
            &options,
            self.metadata_configuration(),
            self.partition_columns(),
            sink_mode,
            self.table_exists(),
            input_schema,
            operation_override,
        )
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

        if let Some(snapshot) = &self.config.table_snapshot {
            let mut table = create_delta_table_with_object_store(
                self.config.table_url.clone(),
                object_store,
                StorageConfig,
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
            table.config = table_config;
            table.state = Some(Arc::clone(snapshot));
            Ok(table)
        } else {
            let log_store = self.log_store()?;
            let mut table_config = table_config;
            if let Some(lakehouse_table) =
                catalog_managed_commit_context(self.config.lakehouse_table.as_ref())
            {
                table_config.catalog_managed_commits = load_catalog_managed_commits_for_snapshot(
                    &self.session,
                    lakehouse_table,
                    &self.config.table_url,
                    log_store.clone(),
                    None,
                )
                .await?;
            }
            let mut table = DeltaTable::new(log_store, table_config);
            table
                .load()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok(table)
        }
    }
}
