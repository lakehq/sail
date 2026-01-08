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

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result};
use object_store::ObjectStore;
use url::Url;

use crate::options::TableDeltaOptions;
use crate::storage::{default_logstore, LogStoreRef, StorageConfig};
use crate::table::{open_table_with_object_store, DeltaTable};

/// Configuration shared by all Delta planners.
#[derive(Clone)]
pub struct DeltaTableConfig {
    pub table_url: Url,
    pub options: TableDeltaOptions,
    pub partition_columns: Vec<String>,
    pub table_schema_for_cond: Option<SchemaRef>,
    pub table_exists: bool,
}

impl DeltaTableConfig {
    pub fn new(
        table_url: Url,
        options: TableDeltaOptions,
        partition_columns: Vec<String>,
        table_schema_for_cond: Option<SchemaRef>,
        table_exists: bool,
    ) -> Self {
        Self {
            table_url,
            options,
            partition_columns,
            table_schema_for_cond,
            table_exists,
        }
    }
}

/// Shared planner context containing table/session state.
pub struct PlannerContext<'a> {
    session: &'a dyn Session,
    config: DeltaTableConfig,
}

impl<'a> PlannerContext<'a> {
    pub fn new(session: &'a dyn Session, config: DeltaTableConfig) -> Self {
        Self { session, config }
    }

    pub fn session(&self) -> &'a dyn Session {
        self.session
    }

    pub fn config(&self) -> &DeltaTableConfig {
        &self.config
    }

    pub fn table_url(&self) -> &Url {
        &self.config.table_url
    }

    pub fn options(&self) -> &TableDeltaOptions {
        &self.config.options
    }

    pub fn partition_columns(&self) -> &[String] {
        &self.config.partition_columns
    }

    pub fn table_schema_for_cond(&self) -> Option<SchemaRef> {
        self.config.table_schema_for_cond.clone()
    }

    pub fn table_exists(&self) -> bool {
        self.config.table_exists
    }

    pub fn into_config(self) -> DeltaTableConfig {
        self.config
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
        Ok(default_logstore(
            Arc::clone(&object_store),
            object_store,
            &self.config.table_url,
            &storage_config,
        ))
    }

    pub async fn open_table(&self) -> Result<DeltaTable> {
        let object_store = self.object_store()?;
        open_table_with_object_store(
            self.config.table_url.clone(),
            object_store,
            Default::default(),
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}
