use deltalake::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake::kernel::{StructField, StructType, StructTypeExt};
use deltalake::logstore::LogStoreRef;
use deltalake::protocol::DeltaOperation;
use deltalake::table::state::DeltaTableState;
use deltalake::{DeltaResult, DeltaTable, DeltaTableError};
use futures::future::BoxFuture;
use itertools::Itertools;

use crate::operations::cast::merge_schema::merge_delta_struct;

/// Add new columns and/or nested fields to a table
pub struct AddColumnBuilder {
    /// A snapshot of the table's state
    snapshot: DeltaTableState,
    /// Fields to add/merge into schema
    fields: Option<Vec<StructField>>,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
}

impl AddColumnBuilder {
    /// Create a new builder
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            snapshot,
            log_store,
            fields: None,
            commit_properties: CommitProperties::default(),
        }
    }

    /// Specify the fields to be added
    pub fn with_fields(mut self, fields: impl IntoIterator<Item = StructField> + Clone) -> Self {
        self.fields = Some(fields.into_iter().collect());
        self
    }

    /// Additional metadata to be added to commit info
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    /// Execute the add column operation
    pub async fn execute(self) -> DeltaResult<DeltaTable> {
        let mut metadata = self.snapshot.metadata().clone();
        let fields = match self.fields.clone() {
            Some(v) => v,
            None => return Err(DeltaTableError::Generic("No fields provided".to_string())),
        };

        let fields_right = &StructType::new(fields.clone());

        // Check for generated columns - not allowed in new columns
        if !fields_right
            .get_generated_columns()
            .unwrap_or_default()
            .is_empty()
        {
            return Err(DeltaTableError::Generic(
                "New columns cannot be a generated column".to_string(),
            ));
        }

        let table_schema = self.snapshot.schema();
        let new_table_schema = merge_delta_struct(table_schema, fields_right)?;

        let current_protocol = self.snapshot.protocol();

        let new_protocol = current_protocol
            .clone()
            .apply_column_metadata_to_protocol(&new_table_schema)?
            .move_table_properties_into_features(&metadata.configuration);

        let operation = DeltaOperation::AddColumn {
            fields: fields.into_iter().collect_vec(),
        };

        metadata.schema_string = serde_json::to_string(&new_table_schema)?;

        let mut actions = vec![metadata.into()];

        if current_protocol != &new_protocol {
            actions.push(new_protocol.into())
        }

        let commit = CommitBuilder::from(self.commit_properties.clone())
            .with_actions(actions)
            .build(Some(&self.snapshot), self.log_store.clone(), operation)
            .await?;

        // Create a new table and load the updated version
        let mut table = DeltaTable::new(
            self.log_store,
            deltalake::table::builder::DeltaTableConfig::default(),
        );
        table.load_version(commit.snapshot().version()).await?;
        Ok(table)
    }
}

impl std::future::IntoFuture for AddColumnBuilder {
    type Output = DeltaResult<DeltaTable>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.execute().await })
    }
}
