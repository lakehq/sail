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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use deltalake::kernel::StructType;
//     use deltalake::DeltaOps;
//     use std::env;
//     use std::path::PathBuf;

//     #[tokio::test]
//     async fn test_set_table_properties() -> DeltaResult<()> {
//         let mut temp_path = env::temp_dir();
//         temp_path.push(format!("test_set_table_properties_{}", uuid::Uuid::new_v4()));
//         let table_path = temp_path.to_str().unwrap();

//         // Create a test table
//         let schema = StructType::new(vec![]);
//         let table = DeltaOps::try_from_uri(table_path)
//             .await?
//             .create()
//             .with_columns(schema.fields().cloned())
//             .await?;

//         // Set table properties
//         let mut properties = HashMap::new();
//         properties.insert("delta.appendOnly".to_string(), "true".to_string());
//         properties.insert("test.property".to_string(), "test_value".to_string());

//         let builder = SetTablePropertiesBuilder::new(
//             table.log_store(),
//             table.snapshot()?.clone(),
//         )
//         .with_properties(properties.clone());

//         let updated_table = builder.execute().await?;

//         // Verify properties were set
//         let metadata = updated_table.metadata()?;
//         assert_eq!(
//             metadata.configuration.get("delta.appendOnly"),
//             Some(&Some("true".to_string()))
//         );
//         assert_eq!(
//             metadata.configuration.get("test.property"),
//             Some(&Some("test_value".to_string()))
//         );

//         Ok(())
//     }

//     #[tokio::test]
//     async fn test_set_table_properties_with_raise_false() -> DeltaResult<()> {
//         let mut temp_path = env::temp_dir();
//         temp_path.push(format!("test_set_table_properties_raise_false_{}", uuid::Uuid::new_v4()));
//         let table_path = temp_path.to_str().unwrap();

//         // Create a test table
//         let schema = StructType::new(vec![]);
//         let table = DeltaOps::try_from_uri(table_path)
//             .await?
//             .create()
//             .with_columns(schema.fields().cloned())
//             .await?;

//         // Set table properties with raise_if_not_exists = false
//         let mut properties = HashMap::new();
//         properties.insert("custom.property".to_string(), "custom_value".to_string());

//         let builder = SetTablePropertiesBuilder::new(
//             table.log_store(),
//             table.snapshot()?.clone(),
//         )
//         .with_properties(properties.clone())
//         .with_raise_if_not_exists(false);

//         let updated_table = builder.execute().await?;

//         // Verify properties were set
//         let metadata = updated_table.metadata()?;
//         assert_eq!(
//             metadata.configuration.get("custom.property"),
//             Some(&Some("custom_value".to_string()))
//         );

//         Ok(())
//     }
// }
