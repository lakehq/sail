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

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result, internal_err};
use futures::StreamExt;
use futures::stream::once;
use object_store::ObjectStoreExt;
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use url::Url;

use crate::catalog_support::commit::{
    CatalogCommitOutcome, CatalogTableInfo, IcebergCatalogCommitCoordinator,
    IcebergCatalogCommitMode, catalog_requirements, table_metadata_location,
};
use crate::io::StoreContext;
use crate::operations::bootstrap::{
    NewTableMetadataStyle, PersistStrategy, bootstrap_first_snapshot,
    bootstrap_new_table_with_style, bootstrap_snapshot_action_commit,
};
use crate::operations::helpers::format_version_for_schema;
use crate::operations::{SnapshotProduceOperation, Transaction, TransactionAction};
use crate::physical_plan::action_schema::{CommitMeta, decode_actions_and_meta_from_batch};
use crate::physical_plan::commit::IcebergCommitInfo;
use crate::spec::catalog::TableUpdate;
use crate::spec::metadata::table_metadata::SnapshotLog;
use crate::spec::partition::{UnboundPartitionField, UnboundPartitionSpec};
use crate::spec::snapshots::MAIN_BRANCH;
use crate::spec::{PartitionSpec, Schema as IcebergSchema, TableMetadata, TableRequirement};
use crate::table::metadata_loader::{
    encode_metadata_file, load_metadata_file_bytes, metadata_file_extension_from_properties,
    metadata_file_version_from_path, metadata_location_to_object_path_string,
};
use crate::table_format::metadata_location_from_properties;
use crate::utils::get_object_store_from_context;
use crate::utils::metadata::metadata_files_for_version;
const MAX_COMMIT_RETRIES: usize = 5;

#[derive(Debug)]
pub struct IcebergCommitExec {
    input: Arc<dyn ExecutionPlan>,
    table_url: Url,
    lakehouse_table: Option<LakehouseExecutionContext>,
    cache: Arc<PlanProperties>,
}

impl IcebergCommitExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_url: Url,
        lakehouse_table: Option<LakehouseExecutionContext>,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            true,
        )]));
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            input,
            table_url,
            lakehouse_table,
            cache,
        }
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn lakehouse_table(&self) -> Option<&LakehouseExecutionContext> {
        self.lakehouse_table.as_ref()
    }

    fn apply_schema_update(table_meta: &mut TableMetadata, new_schema: IcebergSchema) {
        let schema_id = new_schema.schema_id();
        let highest_field_id = new_schema.highest_field_id();

        let mut replaced = false;
        for schema in table_meta.schemas.iter_mut() {
            if schema.schema_id() == schema_id {
                *schema = new_schema.clone();
                replaced = true;
                break;
            }
        }
        if !replaced {
            table_meta.schemas.push(new_schema.clone());
        }

        table_meta.current_schema_id = schema_id;
        table_meta.last_column_id = table_meta.last_column_id.max(highest_field_id);
        table_meta.format_version = table_meta
            .format_version
            .max(format_version_for_schema(&new_schema));
    }

    fn apply_partition_spec_update(table_meta: &mut TableMetadata, new_spec: PartitionSpec) {
        let spec_id = new_spec.spec_id();
        let mut replaced = false;
        for spec in table_meta.partition_specs.iter_mut() {
            if spec.spec_id() == spec_id {
                *spec = new_spec.clone();
                replaced = true;
                break;
            }
        }
        if !replaced {
            table_meta.partition_specs.push(new_spec.clone());
        }
        table_meta.default_spec_id = spec_id;
        if let Some(highest) = new_spec.highest_field_id() {
            table_meta.last_partition_id = table_meta.last_partition_id.max(highest);
        }
    }

    fn validate_requirements(
        table_meta: Option<&TableMetadata>,
        requirements: &[TableRequirement],
    ) -> Result<()> {
        for requirement in requirements {
            match requirement {
                TableRequirement::NotExist => {
                    if table_meta.is_some() {
                        return Err(DataFusionError::Plan(
                            "Iceberg table already exists but commit asserted non-existence."
                                .to_string(),
                        ));
                    }
                }
                TableRequirement::LastAssignedFieldIdMatch {
                    last_assigned_field_id,
                } => {
                    let meta = table_meta.ok_or_else(|| {
                        DataFusionError::Plan(
                            "Iceberg table metadata missing while validating field id requirement"
                                .to_string(),
                        )
                    })?;
                    if &meta.last_column_id != last_assigned_field_id {
                        return Err(DataFusionError::Plan(format!(
                            "Iceberg commit failed: expected last assigned field id {} but found {}. Reload table metadata and retry.",
                            last_assigned_field_id, meta.last_column_id
                        )));
                    }
                }
                TableRequirement::CurrentSchemaIdMatch { current_schema_id } => {
                    let meta = table_meta.ok_or_else(|| {
                        DataFusionError::Plan(
                            "Iceberg table metadata missing while validating schema requirement"
                                .to_string(),
                        )
                    })?;
                    if &meta.current_schema_id != current_schema_id {
                        return Err(DataFusionError::Plan(format!(
                            "Iceberg commit failed: expected current schema id {} but found {}. Reload table metadata and retry.",
                            current_schema_id, meta.current_schema_id
                        )));
                    }
                }
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: reference,
                    snapshot_id,
                } => {
                    let meta = table_meta.ok_or_else(|| {
                        DataFusionError::Plan(
                            "Iceberg table metadata missing while validating snapshot requirement"
                                .to_string(),
                        )
                    })?;
                    let actual = if reference == MAIN_BRANCH {
                        meta.current_snapshot_id
                    } else {
                        meta.refs
                            .get(reference)
                            .map(|ref_entry| ref_entry.snapshot_id)
                    };
                    let actual = actual.filter(|snapshot_id| *snapshot_id >= 0);
                    if &actual != snapshot_id {
                        return Err(DataFusionError::Plan(format!(
                            "Iceberg commit failed: reference '{}' expected snapshot {:?} but found {:?}",
                            reference, snapshot_id, actual
                        )));
                    }
                }
                TableRequirement::UuidMatch { uuid } => {
                    let meta = table_meta.ok_or_else(|| {
                        DataFusionError::Plan(
                            "Iceberg table metadata missing while validating UUID requirement"
                                .to_string(),
                        )
                    })?;
                    if meta.table_uuid.as_ref() != Some(uuid) {
                        return Err(DataFusionError::Plan(format!(
                            "Iceberg commit failed: expected table UUID {} but found {:?}. Reload table metadata and retry.",
                            uuid, meta.table_uuid
                        )));
                    }
                }
                TableRequirement::LastAssignedPartitionIdMatch {
                    last_assigned_partition_id,
                } => {
                    let meta = table_meta.ok_or_else(|| {
                        DataFusionError::Plan(
                            "Iceberg table metadata missing while validating partition id requirement"
                                .to_string(),
                        )
                    })?;
                    if &meta.last_partition_id != last_assigned_partition_id {
                        return Err(DataFusionError::Plan(format!(
                            "Iceberg commit failed: expected last assigned partition id {} but found {}. Reload table metadata and retry.",
                            last_assigned_partition_id, meta.last_partition_id
                        )));
                    }
                }
                TableRequirement::DefaultSpecIdMatch { default_spec_id } => {
                    let meta = table_meta.ok_or_else(|| {
                        DataFusionError::Plan(
                            "Iceberg table metadata missing while validating partition spec requirement"
                                .to_string(),
                        )
                    })?;
                    if &meta.default_spec_id != default_spec_id {
                        return Err(DataFusionError::Plan(format!(
                            "Iceberg commit failed: expected default partition spec id {} but found {}. Reload table metadata and retry.",
                            default_spec_id, meta.default_spec_id
                        )));
                    }
                }
                TableRequirement::DefaultSortOrderIdMatch {
                    default_sort_order_id,
                } => {
                    let meta = table_meta.ok_or_else(|| {
                        DataFusionError::Plan(
                            "Iceberg table metadata missing while validating sort order requirement"
                                .to_string(),
                        )
                    })?;
                    let actual = meta.default_sort_order_id.map(i64::from).unwrap_or(0);
                    if &actual != default_sort_order_id {
                        return Err(DataFusionError::Plan(format!(
                            "Iceberg commit failed: expected default sort order id {} but found {}. Reload table metadata and retry.",
                            default_sort_order_id, actual
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    fn unbound_partition_spec(spec: &PartitionSpec) -> UnboundPartitionSpec {
        let fields = spec
            .fields()
            .iter()
            .map(|field| UnboundPartitionField {
                source_id: field.source_id,
                name: field.name.clone(),
                transform: field.transform,
            })
            .collect();
        UnboundPartitionSpec { fields }
    }

    async fn load_catalog_table_info(
        context: &Arc<TaskContext>,
        catalog_table: &[String],
    ) -> Result<CatalogTableInfo> {
        IcebergCatalogCommitCoordinator::load_table_info(context.as_ref(), catalog_table).await
    }

    async fn load_catalog_metadata_location(
        context: &Arc<TaskContext>,
        catalog_table: &[String],
    ) -> Result<Option<String>> {
        IcebergCatalogCommitCoordinator::load_metadata_location(context.as_ref(), catalog_table)
            .await
    }

    async fn try_commit_to_catalog(
        context: &Arc<TaskContext>,
        catalog_table: &[String],
        lakehouse_table: &LakehouseExecutionContext,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
    ) -> Result<CatalogCommitOutcome> {
        IcebergCatalogCommitCoordinator::new(context.as_ref(), catalog_table)
            .commit(lakehouse_table, requirements, updates)
            .await
    }

    async fn update_catalog_metadata_location(
        context: &Arc<TaskContext>,
        catalog_table: &[String],
        existing_properties: &[(String, String)],
        previous_metadata_location: Option<&str>,
        new_metadata_location: &str,
    ) -> Result<()> {
        IcebergCatalogCommitCoordinator::new(context.as_ref(), catalog_table)
            .update_metadata_location(
                existing_properties,
                previous_metadata_location,
                new_metadata_location,
            )
            .await
    }

    fn table_metadata_location(table_url: &Url, metadata_file: &str) -> Result<String> {
        table_metadata_location(table_url, metadata_file)
    }

    fn merge_writer_commit_meta(
        accumulated: &mut Option<CommitMeta>,
        mut incoming: CommitMeta,
    ) -> Result<()> {
        let Some(existing) = accumulated.as_mut() else {
            *accumulated = Some(incoming);
            return Ok(());
        };

        let incoming_row_count = incoming.row_count;
        incoming.row_count = existing.row_count;
        if existing != &incoming {
            return Err(DataFusionError::Internal(
                "inconsistent commit_meta actions from Iceberg writer partitions".to_string(),
            ));
        }
        existing.row_count = existing
            .row_count
            .checked_add(incoming_row_count)
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "Iceberg writer row count overflow across partitions".to_string(),
                )
            })?;
        Ok(())
    }
}

#[async_trait]
impl ExecutionPlan for IcebergCommitExec {
    fn name(&self) -> &'static str {
        "IcebergCommitExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("IcebergCommitExec requires exactly one child");
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.table_url.clone(),
            self.lakehouse_table.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("IcebergCommitExec can only be executed in a single partition");
        }

        let input_partitions = self.input.output_partitioning().partition_count();
        if input_partitions != 1 {
            return internal_err!(
                "IcebergCommitExec requires exactly one input partition, got {input_partitions}"
            );
        }

        let input_stream = self.input.execute(0, Arc::clone(&context))?;

        let table_url = self.table_url.clone();
        let lakehouse_table = self.lakehouse_table.clone();
        let schema = self.schema();
        let future = async move {
            let object_store = get_object_store_from_context(&context, &table_url)?;
            let store_ctx = StoreContext::new(object_store.clone(), &table_url)?;

            // Read writer result as Arrow-native action batches (may be empty for IgnoreIfExists).
            let mut data = input_stream;
            let mut added_data_files = Vec::new();
            let mut commit_meta = None;
            while let Some(batch_result) = data.next().await {
                let batch = batch_result?;
                if batch.num_rows() == 0 {
                    continue;
                }
                let (adds, _deletes, meta) = decode_actions_and_meta_from_batch(&batch)?;
                added_data_files.extend(adds);
                if let Some(meta) = meta {
                    Self::merge_writer_commit_meta(&mut commit_meta, meta)?;
                }
            }

            // No-op path (e.g. IgnoreIfExists on existing table): no rows, no meta.
            if commit_meta.is_none() && added_data_files.is_empty() {
                let array = Arc::new(UInt64Array::from(vec![0u64]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            }

            let commit_meta = commit_meta.ok_or_else(|| {
                DataFusionError::Internal(
                    "missing commit_meta action from writer output".to_string(),
                )
            })?;

            let commit_info = IcebergCommitInfo {
                table_uri: commit_meta.table_uri,
                row_count: commit_meta.row_count,
                data_files: added_data_files,
                manifest_path: String::new(),
                manifest_list_path: String::new(),
                updates: vec![],
                requirements: commit_meta.requirements,
                table_properties: commit_meta.table_properties,
                lakehouse_table: commit_meta.lakehouse_table.or(lakehouse_table),
                operation: commit_meta.operation,
                schema: commit_meta.schema,
                partition_spec: commit_meta.partition_spec,
            };

            let catalog_table = commit_info
                .lakehouse_table
                .as_ref()
                .map(|context| context.catalog_table().to_vec());
            let CatalogTableInfo {
                metadata_location: catalog_status_metadata_location,
                is_catalog_managed_iceberg_table: is_catalog_status_managed_iceberg_table,
            } = match catalog_table.as_ref() {
                Some(table) => Self::load_catalog_table_info(&context, table).await?,
                None => CatalogTableInfo::default(),
            };
            let catalog_table_info = CatalogTableInfo {
                metadata_location: catalog_status_metadata_location,
                is_catalog_managed_iceberg_table: is_catalog_status_managed_iceberg_table,
            };
            let catalog_commit_mode = IcebergCatalogCommitMode::resolve(
                commit_info.lakehouse_table.as_ref(),
                &catalog_table_info,
                &commit_info.table_properties,
            );
            let table_property_metadata_location =
                metadata_location_from_properties(&commit_info.table_properties);
            let catalog_recorded_metadata_location = table_property_metadata_location
                .clone()
                .or(catalog_table_info.metadata_location.clone());
            let catalog_metadata_location = catalog_commit_mode
                .uses_catalog_metadata()
                .then(|| catalog_recorded_metadata_location.clone())
                .flatten();

            // Managed external catalogs use the authoritative metadata-location.
            // Path tables may record metadata-location in the session catalog for display, but
            // their current state is discovered from the metadata directory and version hint.
            let latest_meta_res = match catalog_metadata_location.as_deref() {
                Some(location) => Ok(metadata_location_to_object_path_string(location)?),
                None => crate::table::find_latest_metadata_file(&object_store, &table_url).await,
            };
            let catalog_metadata_table = catalog_table
                .as_ref()
                .filter(|_| catalog_commit_mode.uses_catalog_metadata());
            let catalog_commit_table = catalog_table
                .as_ref()
                .filter(|_| catalog_commit_mode.uses_catalog_commit());
            let catalog_metadata_update_table = catalog_table
                .as_ref()
                .filter(|_| catalog_commit_mode.uses_metadata_location_update());
            let catalog_registered_metadata_table = catalog_table
                .as_ref()
                .filter(|_| matches!(catalog_commit_mode, IcebergCatalogCommitMode::Filesystem));
            log::debug!(
                "Iceberg catalog commit context: table={:?}, metadata_location={:?}, mode={:?}",
                catalog_table,
                catalog_metadata_location,
                catalog_commit_mode
            );

            if latest_meta_res.is_err()
                && (matches!(commit_info.operation, crate::spec::Operation::Overwrite)
                    || matches!(commit_info.operation, crate::spec::Operation::Append))
            {
                Self::validate_requirements(None, &commit_info.requirements)?;
                if let Some(catalog_table) = catalog_metadata_update_table {
                    let bootstrap_result = bootstrap_new_table_with_style(
                        &table_url,
                        &store_ctx,
                        &commit_info,
                        NewTableMetadataStyle::Uuid,
                    )
                    .await?;
                    let new_metadata_location =
                        Self::table_metadata_location(&table_url, &bootstrap_result.metadata_file)?;
                    Self::update_catalog_metadata_location(
                        &context,
                        catalog_table,
                        &commit_info.table_properties,
                        None,
                        &new_metadata_location,
                    )
                    .await?;
                } else if catalog_commit_mode.uses_catalog_commit() {
                    return Err(DataFusionError::Plan(
                        "missing Iceberg metadata for catalog-authoritative table".to_string(),
                    ));
                } else {
                    // Bootstrap a new table using the Hadoop/path-table convention.
                    let bootstrap_result = bootstrap_new_table_with_style(
                        &table_url,
                        &store_ctx,
                        &commit_info,
                        NewTableMetadataStyle::Hadoop,
                    )
                    .await?;
                    if let Some(catalog_table) = catalog_registered_metadata_table {
                        let new_metadata_location = Self::table_metadata_location(
                            &table_url,
                            &bootstrap_result.metadata_file,
                        )?;
                        Self::update_catalog_metadata_location(
                            &context,
                            catalog_table,
                            &commit_info.table_properties,
                            catalog_recorded_metadata_location.as_deref(),
                            &new_metadata_location,
                        )
                        .await?;
                    }
                }

                let array = Arc::new(UInt64Array::from(vec![commit_info.row_count]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            }

            let initial_latest_meta = latest_meta_res?;

            let mut attempt = 0;
            loop {
                attempt += 1;
                let catalog_metadata_location = if attempt == 1 {
                    catalog_metadata_location.clone()
                } else if let Some(catalog_table) = catalog_metadata_table {
                    Self::load_catalog_metadata_location(&context, catalog_table).await?
                } else {
                    catalog_metadata_location.clone()
                };
                let latest_meta = if attempt == 1 {
                    initial_latest_meta.clone()
                } else if let Some(location) = catalog_metadata_location.as_deref() {
                    metadata_location_to_object_path_string(location)?
                } else {
                    crate::table::find_latest_metadata_file(&object_store, &table_url).await?
                };

                let bytes = load_metadata_file_bytes(&object_store, &latest_meta).await?;
                let mut table_meta = TableMetadata::from_json(&bytes)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Self::validate_requirements(Some(&table_meta), &commit_info.requirements)?;
                let original_format_version = table_meta.format_version;
                let mut metadata_updates = Vec::new();
                if let Some(new_schema) = commit_info.schema.clone() {
                    let schema_id = new_schema.schema_id();
                    let should_add_schema = !table_meta
                        .schemas
                        .iter()
                        .any(|schema| schema.schema_id() == schema_id);
                    let should_set_current_schema = table_meta.current_schema_id != schema_id;
                    Self::apply_schema_update(&mut table_meta, new_schema.clone());
                    if should_add_schema {
                        metadata_updates.push(TableUpdate::AddSchema {
                            schema: Box::new(new_schema),
                        });
                    }
                    if should_set_current_schema {
                        metadata_updates.push(TableUpdate::SetCurrentSchema { schema_id });
                    }
                }
                let mut partition_spec_for_commit = table_meta
                    .default_partition_spec()
                    .cloned()
                    .unwrap_or_else(PartitionSpec::unpartitioned_spec);
                if let Some(new_spec) = commit_info.partition_spec.clone() {
                    let spec = if new_spec.spec_id() == 0 && table_meta.default_spec_id != 0 {
                        new_spec.with_spec_id(table_meta.default_spec_id)
                    } else {
                        new_spec
                    };
                    let spec_id = spec.spec_id();
                    let should_add_spec = !table_meta
                        .partition_specs
                        .iter()
                        .any(|partition_spec| partition_spec.spec_id() == spec_id);
                    let should_set_default_spec = table_meta.default_spec_id != spec_id;
                    Self::apply_partition_spec_update(&mut table_meta, spec.clone());
                    partition_spec_for_commit = spec;
                    if should_add_spec {
                        metadata_updates.push(TableUpdate::AddSpec {
                            spec: Self::unbound_partition_spec(&partition_spec_for_commit),
                        });
                    }
                    if should_set_default_spec {
                        metadata_updates.push(TableUpdate::SetDefaultSpec { spec_id });
                    }
                }
                let maybe_snapshot = table_meta.current_snapshot().cloned();
                let schema_iceberg = table_meta.current_schema().cloned().ok_or_else(|| {
                    DataFusionError::Plan("No current schema in table metadata".to_string())
                })?;
                table_meta.format_version = table_meta
                    .format_version
                    .max(format_version_for_schema(&schema_iceberg));
                if table_meta.format_version > original_format_version {
                    metadata_updates.insert(
                        0,
                        TableUpdate::UpgradeFormatVersion {
                            format_version: table_meta.format_version,
                        },
                    );
                }
                let row_lineage_start_row_id = table_meta.row_lineage_start_row_id();

                // If metadata exists but there is no current snapshot (e.g. from a CREATE TABLE),
                // bootstrap the first snapshot as a normal metadata version.
                if maybe_snapshot.is_none()
                    && (matches!(commit_info.operation, crate::spec::Operation::Overwrite)
                        || matches!(commit_info.operation, crate::spec::Operation::Append))
                {
                    let mut catalog_fallback_table = catalog_metadata_update_table;
                    if let Some(catalog_table) = catalog_commit_table {
                        let action_commit = bootstrap_snapshot_action_commit(
                            &table_url,
                            &store_ctx,
                            &commit_info,
                            &table_meta,
                        )
                        .await?;
                        let action_requirements = action_commit.requirements().to_vec();
                        Self::validate_requirements(Some(&table_meta), &action_requirements)?;
                        let requirements = catalog_requirements(
                            &table_meta,
                            &commit_info.requirements,
                            &action_requirements,
                        );
                        let mut updates = metadata_updates.clone();
                        updates.extend(action_commit.into_updates());
                        match Self::try_commit_to_catalog(
                            &context,
                            catalog_table,
                            commit_info.lakehouse_table.as_ref().ok_or_else(|| {
                                DataFusionError::Internal(
                                    "missing lakehouse context for Iceberg catalog commit"
                                        .to_string(),
                                )
                            })?,
                            requirements,
                            updates,
                        )
                        .await?
                        {
                            CatalogCommitOutcome::Committed(committed) => {
                                if let Some(metadata_location) = committed.metadata_location() {
                                    log::debug!(
                                        "Iceberg catalog commit returned metadata-location={metadata_location}"
                                    );
                                }
                                if committed.payload().is_some() {
                                    log::trace!("Iceberg catalog commit returned a payload");
                                }
                                let array =
                                    Arc::new(UInt64Array::from(vec![commit_info.row_count]));
                                let batch = RecordBatch::try_new(schema, vec![array])?;
                                return Ok(batch);
                            }
                            CatalogCommitOutcome::NotSupported => {
                                if matches!(
                                    catalog_commit_mode,
                                    IcebergCatalogCommitMode::CompatibilityCatalogCommit
                                ) {
                                    catalog_fallback_table = Some(catalog_table);
                                } else {
                                    return Err(DataFusionError::Plan(
                                        "Iceberg catalog commit is not supported by the resolved catalog authority"
                                            .to_string(),
                                    ));
                                }
                            }
                            CatalogCommitOutcome::Conflict => {
                                if attempt >= MAX_COMMIT_RETRIES {
                                    return Err(commit_conflict_error());
                                }
                                continue;
                            }
                        }
                    }

                    let persist_strategy = if catalog_fallback_table.is_some() {
                        PersistStrategy::NewUuidVersion
                    } else {
                        PersistStrategy::NewVersion
                    };
                    let previous_metadata_file = catalog_fallback_table
                        .is_some()
                        .then_some(catalog_metadata_location.as_deref())
                        .flatten();
                    let bootstrap_result = bootstrap_first_snapshot(
                        &table_url,
                        &store_ctx,
                        &commit_info,
                        table_meta,
                        &latest_meta,
                        previous_metadata_file,
                        persist_strategy,
                    )
                    .await?;
                    if let (Some(catalog_table), Some(previous_metadata_location)) =
                        (catalog_fallback_table, catalog_metadata_location.as_deref())
                    {
                        let new_metadata_location = Self::table_metadata_location(
                            &table_url,
                            &bootstrap_result.metadata_file,
                        )?;
                        Self::update_catalog_metadata_location(
                            &context,
                            catalog_table,
                            &commit_info.table_properties,
                            Some(previous_metadata_location),
                            &new_metadata_location,
                        )
                        .await?;
                    } else if let Some(catalog_table) = catalog_registered_metadata_table {
                        let new_metadata_location = Self::table_metadata_location(
                            &table_url,
                            &bootstrap_result.metadata_file,
                        )?;
                        Self::update_catalog_metadata_location(
                            &context,
                            catalog_table,
                            &commit_info.table_properties,
                            catalog_recorded_metadata_location.as_deref(),
                            &new_metadata_location,
                        )
                        .await?;
                    }

                    let array = Arc::new(UInt64Array::from(vec![commit_info.row_count]));
                    let batch = RecordBatch::try_new(schema, vec![array])?;
                    return Ok(batch);
                }

                let snapshot = maybe_snapshot.ok_or_else(|| {
                    DataFusionError::Plan("No current snapshot in table metadata".to_string())
                })?;

                let current_version = metadata_file_version_from_path(&latest_meta).unwrap_or(0);
                let next_version = current_version + 1;

                let existing_for_next =
                    metadata_files_for_version(&store_ctx, next_version).await?;
                if !existing_for_next.is_empty() {
                    log::warn!(
                        "Detected existing metadata files for version {}: {:?}. Retrying attempt {}",
                        next_version,
                        existing_for_next,
                        attempt
                    );
                    if attempt >= MAX_COMMIT_RETRIES {
                        return Err(commit_conflict_error());
                    }
                    continue;
                }

                // Build transaction and action based on operation
                let tx = Transaction::new(table_url.to_string(), snapshot);
                let manifest_meta = tx.default_manifest_metadata(
                    &schema_iceberg,
                    &partition_spec_for_commit,
                    table_meta.format_version,
                );
                let action_commit = match commit_info.operation {
                    crate::spec::Operation::Append => {
                        let mut action = tx
                            .fast_append()
                            .with_store_context(store_ctx.clone())
                            .with_manifest_metadata(manifest_meta)
                            .with_row_lineage_start_row_id(row_lineage_start_row_id);
                        for df in commit_info.data_files.clone().into_iter() {
                            action.add_file(df);
                        }
                        Arc::new(action)
                            .commit(&tx)
                            .await
                            .map_err(DataFusionError::Execution)?
                    }
                    crate::spec::Operation::Overwrite => {
                        let producer = crate::operations::SnapshotProducer::new(
                            &tx,
                            commit_info.data_files.clone(),
                            Some(store_ctx.clone()),
                            Some(manifest_meta),
                        )
                        .with_row_lineage_start_row_id(row_lineage_start_row_id);
                        struct LocalOverwriteOperation;
                        impl SnapshotProduceOperation for LocalOverwriteOperation {
                            fn operation(&self) -> &'static str {
                                "overwrite"
                            }
                        }
                        producer
                            .commit(LocalOverwriteOperation)
                            .await
                            .map_err(DataFusionError::Execution)?
                    }
                    _ => {
                        return Err(DataFusionError::NotImplemented(
                            "Unsupported Iceberg operation in commit".to_string(),
                        ));
                    }
                };

                // Apply updates (only handle the ones we emit: AddSnapshot, SetSnapshotRef)
                let action_requirements = action_commit.requirements().to_vec();
                Self::validate_requirements(Some(&table_meta), &action_requirements)?;
                let action_updates = action_commit.into_updates();
                if let Some(catalog_table) = catalog_commit_table {
                    let requirements = catalog_requirements(
                        &table_meta,
                        &commit_info.requirements,
                        &action_requirements,
                    );
                    let mut updates = metadata_updates.clone();
                    updates.extend(action_updates.clone());
                    match Self::try_commit_to_catalog(
                        &context,
                        catalog_table,
                        commit_info.lakehouse_table.as_ref().ok_or_else(|| {
                            DataFusionError::Internal(
                                "missing lakehouse context for Iceberg catalog commit".to_string(),
                            )
                        })?,
                        requirements,
                        updates,
                    )
                    .await?
                    {
                        CatalogCommitOutcome::Committed(committed) => {
                            if let Some(metadata_location) = committed.metadata_location() {
                                log::debug!(
                                    "Iceberg catalog commit returned metadata-location={metadata_location}"
                                );
                            }
                            if committed.payload().is_some() {
                                log::trace!("Iceberg catalog commit returned a payload");
                            }
                            let array = Arc::new(UInt64Array::from(vec![commit_info.row_count]));
                            let batch = RecordBatch::try_new(schema, vec![array])?;
                            return Ok(batch);
                        }
                        CatalogCommitOutcome::NotSupported
                            if matches!(
                                catalog_commit_mode,
                                IcebergCatalogCommitMode::CompatibilityCatalogCommit
                            ) => {}
                        CatalogCommitOutcome::NotSupported => {
                            return Err(DataFusionError::Plan(
                                "Iceberg catalog commit is not supported by the resolved catalog authority"
                                    .to_string(),
                            ));
                        }
                        CatalogCommitOutcome::Conflict => {
                            if attempt >= MAX_COMMIT_RETRIES {
                                return Err(commit_conflict_error());
                            }
                            continue;
                        }
                    }
                }

                log::trace!("commit_exec: applying updates: {:?}", action_updates);
                let mut newest_snapshot_seq: Option<i64> = None;
                let mut newest_snapshot_added_rows: Option<i64> = None;
                let timestamp_ms = crate::utils::timestamp::monotonic_timestamp_ms();
                for upd in action_updates {
                    match upd {
                        TableUpdate::AddSnapshot { snapshot } => {
                            newest_snapshot_seq = Some(snapshot.sequence_number());
                            newest_snapshot_added_rows = snapshot.added_rows;
                            table_meta.snapshots.push(snapshot.clone());
                            table_meta.current_snapshot_id = Some(snapshot.snapshot_id());
                            table_meta.snapshot_log.push(SnapshotLog {
                                timestamp_ms,
                                snapshot_id: snapshot.snapshot_id(),
                            });
                        }
                        TableUpdate::SetSnapshotRef {
                            ref_name,
                            reference,
                        } => {
                            table_meta.refs.insert(ref_name, reference);
                        }
                        _ => {}
                    }
                }
                if let Some(seq) = newest_snapshot_seq
                    && seq > table_meta.last_sequence_number
                {
                    table_meta.last_sequence_number = seq;
                }
                table_meta.last_updated_ms = timestamp_ms;
                if let Some(added_rows) = newest_snapshot_added_rows {
                    table_meta.advance_next_row_id(added_rows);
                }

                // Add metadata_log entry referencing previous metadata file
                table_meta
                    .metadata_log
                    .push(crate::spec::metadata::table_metadata::MetadataLog {
                        timestamp_ms,
                        metadata_file: catalog_metadata_location
                            .clone()
                            .unwrap_or_else(|| latest_meta.clone()),
                    });

                let new_meta_json = table_meta
                    .to_json()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let file_extension =
                    metadata_file_extension_from_properties(&table_meta.properties)?;
                let use_uuid_metadata_file = catalog_metadata_update_table.is_some();
                let new_meta_rel = if use_uuid_metadata_file {
                    format!(
                        "metadata/{next_version:05}-{}{file_extension}",
                        uuid::Uuid::new_v4()
                    )
                } else {
                    format!("metadata/v{next_version}{file_extension}")
                };
                let new_metadata_location =
                    Self::table_metadata_location(&table_url, &new_meta_rel)?;
                let new_meta_bytes = encode_metadata_file(&new_meta_rel, &new_meta_json)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                log::trace!(
                    "Writing metadata: {} snapshot_id={:?} table_url={}",
                    new_meta_rel,
                    table_meta.current_snapshot_id,
                    table_url
                );

                let new_meta_path = object_store::path::Path::from(new_meta_rel.as_str());
                let put_opts = object_store::PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                };
                let payload = object_store::PutPayload::from(Bytes::from(new_meta_bytes));
                match store_ctx
                    .prefixed
                    .put_opts(&new_meta_path, payload, put_opts)
                    .await
                {
                    Ok(_) => {}
                    Err(object_store::Error::AlreadyExists { .. }) => {
                        log::warn!(
                            "Metadata file {} already exists for version {}. Retrying attempt {}",
                            new_meta_rel,
                            next_version,
                            attempt
                        );
                        if attempt >= MAX_COMMIT_RETRIES {
                            return Err(commit_conflict_error());
                        }
                        continue;
                    }
                    Err(e) => return Err(DataFusionError::External(Box::new(e))),
                }
                let version_files = metadata_files_for_version(&store_ctx, next_version).await?;
                let conflict_after_write = version_files.iter().any(|path| path != &new_meta_rel);
                if conflict_after_write {
                    log::warn!(
                        "Concurrent metadata writes detected for version {}: {:?}. Retrying attempt {}",
                        next_version,
                        version_files,
                        attempt
                    );
                    if let Err(err) = store_ctx.prefixed.delete(&new_meta_path).await {
                        log::warn!(
                            "Failed to delete conflicted metadata file {}: {:?}",
                            new_meta_rel,
                            err
                        );
                    }
                    if attempt >= MAX_COMMIT_RETRIES {
                        return Err(commit_conflict_error());
                    }
                    continue;
                }
                log::trace!("Metadata written successfully");

                let hint = if use_uuid_metadata_file {
                    new_meta_rel
                        .rsplit('/')
                        .next()
                        .unwrap_or(new_meta_rel.as_str())
                        .to_string()
                } else {
                    next_version.to_string()
                };
                let hint_bytes = Bytes::from(hint.into_bytes());
                let hint_path = object_store::path::Path::from("metadata/version-hint.text");
                store_ctx
                    .prefixed
                    .put(&hint_path, object_store::PutPayload::from(hint_bytes))
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                if let Some(catalog_table) = catalog_metadata_update_table {
                    Self::update_catalog_metadata_location(
                        &context,
                        catalog_table,
                        &commit_info.table_properties,
                        catalog_metadata_location.as_deref(),
                        &new_metadata_location,
                    )
                    .await?;
                } else if let Some(catalog_table) = catalog_registered_metadata_table {
                    Self::update_catalog_metadata_location(
                        &context,
                        catalog_table,
                        &commit_info.table_properties,
                        catalog_recorded_metadata_location.as_deref(),
                        &new_metadata_location,
                    )
                    .await?;
                }

                let array = Arc::new(UInt64Array::from(vec![commit_info.row_count]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                return Ok(batch);
            }
        };

        let stream = once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl DisplayAs for IcebergCommitExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "IcebergCommitExec(table_path={})", self.table_url)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: iceberg")?;
                write!(f, "table_path={}", self.table_url)
            }
        }
    }
}

fn commit_conflict_error() -> DataFusionError {
    DataFusionError::Execution(format!(
        "Iceberg commit failed after {MAX_COMMIT_RETRIES} retries due to concurrent metadata updates"
    ))
}
