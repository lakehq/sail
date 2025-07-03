//! Set table properties on a Delta table
//!
//! This module provides the SetTablePropertiesBuilder for setting table properties on Delta tables.
//! Since we've disabled the datafusion feature in delta-rs, we need to implement
//! the middleware layer ourselves.

use std::collections::HashMap;

use deltalake::kernel::transaction::CommitProperties;
use deltalake::logstore::LogStoreRef;
use deltalake::operations::set_tbl_properties::SetTablePropertiesBuilder as DeltaSetTablePropertiesBuilder;
use deltalake::table::state::DeltaTableState;
use deltalake::{DeltaResult, DeltaTable};
use futures::future::BoxFuture;

/// Set table properties on a Delta table
pub struct SetTablePropertiesBuilder {
    /// A snapshot of the table's state
    snapshot: DeltaTableState,
    /// Properties to set
    properties: HashMap<String, String>,
    /// Raise if property doesn't exist
    raise_if_not_exists: bool,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
}

impl SetTablePropertiesBuilder {
    /// Create a new builder
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            snapshot,
            log_store,
            properties: HashMap::new(),
            raise_if_not_exists: true,
            commit_properties: CommitProperties::default(),
        }
    }

    /// Specify the properties to be set
    pub fn with_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.properties = properties;
        self
    }

    /// Specify if you want to raise if the property does not exist
    pub fn with_raise_if_not_exists(mut self, raise: bool) -> Self {
        self.raise_if_not_exists = raise;
        self
    }

    /// Additional metadata to be added to commit info
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    /// Execute the set table properties operation
    pub async fn execute(self) -> DeltaResult<DeltaTable> {
        // Use delta-rs's SetTablePropertiesBuilder internally
        let delta_builder =
            DeltaSetTablePropertiesBuilder::new(self.log_store.clone(), self.snapshot)
                .with_properties(self.properties)
                .with_raise_if_not_exists(self.raise_if_not_exists)
                .with_commit_properties(self.commit_properties);

        // Execute the operation using delta-rs
        delta_builder.await
    }
}

impl std::future::IntoFuture for SetTablePropertiesBuilder {
    type Output = DeltaResult<DeltaTable>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.execute().await })
    }
}