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
use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::scalar::ScalarValue;
use datafusion::common::stats::{ColumnStatistics, Precision, Statistics};
use datafusion::common::{Result, ToDFSchema, plan_err};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_datasource::file_scan_config::output_partitioning_from_partition_fields;
use object_store::ObjectMeta;
use sail_common_datafusion::schema_evolution::{
    SchemaEvolutionCastColumnExpr, SchemaEvolutionPhysicalExprAdapterFactoryWithMatching,
    StructFieldMatching,
};
use url::Url;

use crate::datasource::expressions::simplify_expr;
use crate::datasource::partition_defaults::{IdentityPartitionDefaults, create_data_scan};
use crate::datasource::predicate::Predicate;
use crate::datasource::type_converter::{iceberg_field_id, iceberg_schema_to_arrow};
use crate::io::StoreContext;
use crate::physical_plan::delete_apply_exec::IcebergDeleteApplyExec;
use crate::physical_plan::discovery_exec::IcebergDiscoveryExec;
use crate::physical_plan::manifest_scan_exec::{IcebergManifestScanExec, ManifestPruning};
use crate::physical_plan::merge_metadata_exec::IcebergMergeMetadataExec;
use crate::physical_plan::metadata_scan_exec::IcebergMetadataScanExec;
use crate::row_level_metadata::{
    MERGE_FILE_METADATA_COLUMN, MERGE_PARTITION_SPEC_ID_COLUMN, RowLevelFileMetadata,
    RowLevelMetadataColumns, parquet_row_position_field,
};
use crate::spec::delete_index::DeleteFileRef;
use crate::spec::transform::Transform;
use crate::spec::types::values::{Datum, Literal};
use crate::spec::{DataFile, ManifestContentType, PartitionSpec, Schema, Snapshot};
use crate::utils::conversions::{primitive_to_scalar_default, to_scalar};
use crate::utils::get_object_store_from_session;

mod planning;
pub(crate) use planning::IcebergScanPlan;

fn iceberg_schema_evolution_adapter() -> Arc<dyn PhysicalExprAdapterFactory> {
    Arc::new(SchemaEvolutionPhysicalExprAdapterFactoryWithMatching::new(
        StructFieldMatching::FieldId,
    ))
}

/// A fixed Iceberg read snapshot and its scan refinements.
#[derive(Debug, Clone)]
pub struct IcebergScan {
    pub(crate) row_level_options: crate::logical::row_level::IcebergRowLevelOptions,
    /// The table location (URI)
    table_uri: String,
    /// The current schema of the table
    schema: Schema,
    schema_history: Vec<Schema>,
    name_mapping: Option<crate::spec::name_mapping::NameMapping>,
    format_version: crate::spec::FormatVersion,
    /// The current snapshot of the table
    snapshot: Option<Snapshot>,
    /// All partition specs referenced by the table
    partition_specs: Vec<PartitionSpec>,
    /// Default partition spec id (for schema ordering / partition metadata)
    default_spec_id: i32,
    /// Arrow schema for DataFusion
    arrow_schema: Arc<ArrowSchema>,
    /// Output schema exposed to DataFusion; may include MERGE metadata columns.
    output_schema: Arc<ArrowSchema>,
    /// Optional file-path metadata column for row-level write planning.
    file_column_name: Option<String>,
    /// Optional file-local row-index metadata column for row-level write planning.
    row_index_column_name: Option<String>,
    /// Candidate-file selection only; never a row filter on the rewrite input.
    copy_on_write_predicate: Option<Expr>,
    /// Whether to use the metadata-as-data read path (lazy manifest scanning)
    metadata_as_data_read: bool,
}

impl IcebergScan {
    pub(crate) fn metadata_aggregate_enabled(&self) -> bool {
        !self.metadata_as_data_read
            && self.file_column_name.is_none()
            && self.row_index_column_name.is_none()
            && self.copy_on_write_predicate.is_none()
    }

    pub(crate) fn exact_statistics(&self, planned: &IcebergScanPlan) -> Option<Statistics> {
        if planned.limit.is_some()
            || planned
                .tasks
                .iter()
                .any(|task| !task.deletes.is_empty() || !task.residual.is_empty())
        {
            return None;
        }
        Some(self.aggregate_statistics(planned.tasks.iter().map(|task| &task.data_file)))
    }

    fn statistic_scalar(
        &self,
        data_file: &DataFile,
        field_id: i32,
        datum: &Datum,
    ) -> Option<ScalarValue> {
        let field = self.schema.field_by_id(field_id)?;
        // Iceberg bounds exclude NaNs. They cannot prove a floating column is
        // constant unless the file explicitly records that there are no NaNs.
        if matches!(
            field.field_type.as_ref(),
            crate::spec::types::Type::Primitive(
                crate::spec::types::PrimitiveType::Float
                    | crate::spec::types::PrimitiveType::Double
            )
        ) && data_file.nan_value_counts().get(&field_id) != Some(&0)
        {
            return None;
        }
        to_scalar(
            &Literal::Primitive(datum.literal.clone()),
            field.field_type.as_ref(),
        )
        .inspect_err(|error| {
            log::debug!(
                "Ignoring Iceberg statistic for field ID {field_id} because it cannot be converted: {error}"
            );
        })
        .ok()
    }

    /// Create a new Iceberg table scan
    pub fn new(
        table_uri: impl ToString,
        schema: Schema,
        snapshot: Snapshot,
        partition_specs: Vec<PartitionSpec>,
        default_spec_id: i32,
    ) -> Result<Self> {
        let table_uri_str = table_uri.to_string();
        log::trace!("Creating table scan for: {}", table_uri_str);

        let arrow_schema = iceberg_schema_to_arrow(&schema).map_err(|e| {
            log::trace!("Failed to convert schema to Arrow: {:?}", e);
            e
        })?;
        let arrow_schema = Arc::new(Self::reorder_arrow_schema_for_identity_partitions(
            &schema,
            &partition_specs,
            default_spec_id,
            &arrow_schema,
        ));

        log::trace!(
            "Converted schema to Arrow with {} fields",
            arrow_schema.fields().len()
        );

        Ok(Self {
            row_level_options: Default::default(),
            table_uri: table_uri_str,
            schema_history: vec![schema.clone()],
            name_mapping: None,
            format_version: crate::spec::FormatVersion::V2,
            schema,
            snapshot: Some(snapshot),
            partition_specs,
            default_spec_id,
            output_schema: arrow_schema.clone(),
            arrow_schema,
            file_column_name: None,
            row_index_column_name: None,
            copy_on_write_predicate: None,
            metadata_as_data_read: false,
        })
    }

    /// Create a scan for an Iceberg table that has metadata but no current
    /// snapshot yet, such as a table created by plain `CREATE TABLE`.
    pub fn new_empty(
        table_uri: impl ToString,
        schema: Schema,
        partition_specs: Vec<PartitionSpec>,
        default_spec_id: i32,
    ) -> Result<Self> {
        let table_uri_str = table_uri.to_string();
        log::trace!("Creating empty table scan for: {}", table_uri_str);

        let arrow_schema = iceberg_schema_to_arrow(&schema).map_err(|e| {
            log::trace!("Failed to convert schema to Arrow: {:?}", e);
            e
        })?;
        let arrow_schema = Arc::new(Self::reorder_arrow_schema_for_identity_partitions(
            &schema,
            &partition_specs,
            default_spec_id,
            &arrow_schema,
        ));

        Ok(Self {
            row_level_options: Default::default(),
            table_uri: table_uri_str,
            schema_history: vec![schema.clone()],
            name_mapping: None,
            format_version: crate::spec::FormatVersion::V2,
            schema,
            snapshot: None,
            partition_specs,
            default_spec_id,
            output_schema: arrow_schema.clone(),
            arrow_schema,
            file_column_name: None,
            row_index_column_name: None,
            copy_on_write_predicate: None,
            metadata_as_data_read: false,
        })
    }

    /// Set whether to use the metadata-as-data read path.
    pub fn with_metadata_as_data_read(mut self, enabled: bool) -> Self {
        self.metadata_as_data_read = enabled;
        self
    }

    pub(crate) fn set_table_metadata(
        &mut self,
        metadata: &crate::spec::TableMetadata,
    ) -> Result<()> {
        self.schema_history = metadata.schemas.clone();
        self.format_version = metadata.format_version;
        self.row_level_options = metadata.into();
        self.name_mapping = metadata
            .properties
            .get(crate::spec::name_mapping::DEFAULT_SCHEMA_NAME_MAPPING)
            .map(|value| serde_json::from_str(value))
            .transpose()
            .map_err(|error| {
                datafusion::common::DataFusionError::Plan(format!(
                    "Invalid Iceberg name mapping: {error}"
                ))
            })?;
        if let Some(mapping) = &self.name_mapping {
            self.arrow_schema = Arc::new(crate::datasource::type_converter::apply_name_mapping(
                &self.arrow_schema,
                mapping,
            )?);
            self.output_schema = self.arrow_schema.clone();
        }
        Ok(())
    }

    pub fn file_column_name(&self) -> Option<&str> {
        self.file_column_name.as_deref()
    }

    pub fn row_index_column_name(&self) -> Option<&str> {
        self.row_index_column_name.as_deref()
    }

    pub fn with_file_column(mut self, name: &str) -> Result<Self> {
        self.file_column_name = Some(name.to_string());
        self.rebuild_output_schema()?;
        Ok(self)
    }

    pub fn with_row_index_column(mut self, name: &str) -> Result<Self> {
        self.row_index_column_name = Some(name.to_string());
        self.rebuild_output_schema()?;
        Ok(self)
    }

    pub(crate) fn has_row_lineage(&self) -> bool {
        self.format_version == crate::spec::FormatVersion::V3
    }

    fn rebuild_output_schema(&mut self) -> Result<()> {
        let metadata_columns = RowLevelMetadataColumns::new(
            self.file_column_name.as_deref(),
            self.row_index_column_name.as_deref(),
        );
        let metadata_columns = if self.file_column_name.is_some() {
            metadata_columns.with_delete_file_metadata()
        } else {
            metadata_columns
        };
        let data_schema = if self.has_row_lineage() && self.file_column_name.is_some() {
            crate::row_lineage::append_lineage_fields(self.arrow_schema.as_ref())?
        } else {
            self.arrow_schema.as_ref().clone()
        };
        self.output_schema = Arc::new(metadata_columns.append_to_schema(&data_schema)?);
        Ok(())
    }

    fn reorder_arrow_schema_for_identity_partitions(
        schema: &Schema,
        partition_specs: &[PartitionSpec],
        default_spec_id: i32,
        arrow_schema: &ArrowSchema,
    ) -> ArrowSchema {
        // BDD scenarios expect "data columns" first and identity-partition columns last (in spec order),
        // but only for identity-only partition specs. For mixed transform specs (e.g. `years(x), y`)
        // we keep the original schema order.
        let Some(spec) = partition_specs
            .iter()
            .find(|s| s.spec_id() == default_spec_id)
        else {
            return arrow_schema.clone();
        };
        if spec
            .fields()
            .iter()
            .any(|pf| !matches!(pf.transform, Transform::Identity))
        {
            return arrow_schema.clone();
        }

        let mut identity_cols: Vec<String> = Vec::new();
        for pf in spec.fields().iter() {
            if matches!(pf.transform, Transform::Identity)
                && let Some(field) = schema.field_by_id(pf.source_id)
            {
                identity_cols.push(field.name.clone());
            }
        }
        if identity_cols.is_empty() {
            return arrow_schema.clone();
        }

        let identity_set: std::collections::HashSet<&str> =
            identity_cols.iter().map(|s| s.as_str()).collect();
        let mut out_fields: Vec<datafusion::arrow::datatypes::FieldRef> = Vec::new();

        // Keep non-partition columns in original order.
        for f in arrow_schema.fields().iter() {
            if !identity_set.contains(f.name().as_str()) {
                out_fields.push(Arc::new((**f).clone()));
            }
        }
        // Append identity partition columns in spec order.
        for name in identity_cols {
            if let Ok(idx) = arrow_schema.index_of(&name) {
                out_fields.push(Arc::new(arrow_schema.field(idx).clone()));
            }
        }

        ArrowSchema::new(out_fields)
    }

    /// Get the table URI
    pub fn table_uri(&self) -> &str {
        &self.table_uri
    }

    /// Get the Iceberg schema
    pub fn iceberg_schema(&self) -> &Schema {
        &self.schema
    }

    /// Get the current snapshot
    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    pub(crate) fn select_copy_on_write_candidates(mut self, predicate: Expr) -> Self {
        self.copy_on_write_predicate = Some(predicate);
        self
    }

    pub(crate) async fn metadata_delete_paths(
        &self,
        session: &dyn Session,
        predicate: &Expr,
    ) -> Result<Option<Vec<String>>> {
        if self.snapshot.is_none() {
            return Ok(Some(Vec::new()));
        }
        let table_url = Url::parse(&self.table_uri)
            .map_err(|error| datafusion::common::DataFusionError::External(Box::new(error)))?;
        let object_store = get_object_store_from_session(session, &table_url)?;
        let store_ctx = StoreContext::new(object_store, &table_url)?;
        let manifests = self.load_manifest_list(&store_ctx).await?;
        let files = self
            .load_data_files_with_seq(&[], &store_ctx, &manifests)
            .await?;
        let selected = crate::datasource::copy_on_write::select_copy_on_write_files(
            predicate,
            &self.schema,
            &self.partition_specs,
            files,
        );
        Ok(selected.all_rows_match.then(|| {
            selected
                .candidates
                .into_iter()
                .map(|(file, _)| file.file_path)
                .collect()
        }))
    }

    pub(crate) async fn predicate_overwrite_paths(
        &self,
        session: &dyn Session,
        condition: &Expr,
    ) -> Result<Vec<String>> {
        if self.snapshot.is_none() {
            return Ok(Vec::new());
        }
        let default_spec = self
            .partition_specs
            .iter()
            .find(|spec| spec.spec_id() == self.default_spec_id)
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Plan(
                    "Iceberg table metadata has no default partition spec".to_string(),
                )
            })?;
        let identity_fields = default_spec
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| {
                matches!(field.transform, Transform::Identity).then_some((field.source_id, index))
            })
            .collect::<HashMap<_, _>>();
        for column in condition.column_refs() {
            let field = self.schema.field_by_name(&column.name).ok_or_else(|| {
                datafusion::common::DataFusionError::Plan(format!(
                    "predicate overwrite column '{}' is not present in the Iceberg schema",
                    column.name
                ))
            })?;
            if !identity_fields.contains_key(&field.id) {
                return Err(datafusion::common::DataFusionError::NotImplemented(
                    format!(
                        "Iceberg predicate overwrite supports only identity-partition columns; use DELETE ... WHERE followed by INSERT for '{}'",
                        column.name
                    ),
                ));
            }
        }

        let table_url = Url::parse(&self.table_uri)
            .map_err(|error| datafusion::common::DataFusionError::External(Box::new(error)))?;
        let object_store = get_object_store_from_session(session, &table_url)?;
        let store_ctx = StoreContext::new(object_store, &table_url)?;
        let manifest_list = self.load_manifest_list(&store_ctx).await?;
        if !self
            .build_delete_file_index(&store_ctx, &manifest_list)
            .await?
            .is_empty()
        {
            return plan_err!(
                "copy-on-write predicate overwrite is not supported for Iceberg tables with active delete files"
            );
        }
        let files = self
            .load_data_files_with_seq(&[], &store_ctx, &manifest_list)
            .await?
            .into_iter()
            .map(|(file, _)| file)
            .collect::<Vec<_>>();
        let df_schema = self.arrow_schema.clone().to_dfschema()?;
        let predicate = session.create_physical_expr(condition.clone(), &df_schema)?;
        let mut paths = Vec::new();
        for file in files {
            if file.partition_spec_id != default_spec.spec_id()
                || file.partition.len() != default_spec.fields().len()
            {
                return Err(datafusion::common::DataFusionError::NotImplemented(
                    "predicate overwrite is not supported for Iceberg tables with incomparable live partition specs"
                        .to_string(),
                ));
            }
            let columns = self
                .arrow_schema
                .fields()
                .iter()
                .map(|arrow_field| {
                    let scalar = self
                        .schema
                        .field_by_name(arrow_field.name())
                        .and_then(|field| {
                            identity_fields
                                .get(&field.id)
                                .copied()
                                .and_then(|index| file.partition.get(index))
                                .and_then(Option::as_ref)
                                .map(|literal| to_scalar(literal, &field.field_type))
                        })
                        .unwrap_or_else(|| ScalarValue::try_from(arrow_field.data_type()));
                    scalar?.to_array_of_size(1)
                })
                .collect::<Result<Vec<_>>>()?;
            let batch = RecordBatch::try_new(self.arrow_schema.clone(), columns)?;
            let result = predicate.evaluate(&batch)?.into_array(1)?;
            let result = result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Plan(
                        "Iceberg overwrite predicate did not evaluate to boolean".to_string(),
                    )
                })?;
            if !result.is_null(0) && result.value(0) {
                paths.push(file.file_path);
            }
        }
        paths.sort();
        paths.dedup();
        Ok(paths)
    }

    fn projected_arrow_schema(&self, projection: Option<&Vec<usize>>) -> Result<Arc<ArrowSchema>> {
        match projection {
            Some(projection) => {
                let fields = projection
                    .iter()
                    .map(|idx| self.output_schema.field(*idx).clone())
                    .collect::<Vec<_>>();
                Ok(Arc::new(ArrowSchema::new(fields)))
            }
            None => Ok(self.output_schema.clone()),
        }
    }

    fn create_partitioned_files(
        &self,
        store_ctx: &StoreContext,
        data_files: Vec<DataFile>,
    ) -> Result<Vec<PartitionedFile>> {
        let mut partitioned_files = Vec::new();

        for data_file in data_files {
            let raw_path = data_file.file_path();
            let file_path = store_ctx.resolve_to_absolute_path(raw_path)?;
            log::trace!("Processing data file: {}", file_path);

            log::trace!("Final ObjectPath: {}", file_path);

            let object_meta = ObjectMeta {
                location: file_path,
                last_modified: chrono::Utc::now(),
                size: data_file.file_size_in_bytes(),
                e_tag: None,
                version: None,
            };

            // Convert partition values to ScalarValues
            let partition_values = data_file
                .partition()
                .iter()
                .map(|literal_opt| match literal_opt {
                    Some(Literal::Primitive(prim)) => primitive_to_scalar_default(prim),
                    Some(other) => {
                        log::warn!(
                            "Unexpected non-primitive partition literal {:?}, treating as NULL",
                            other
                        );
                        ScalarValue::Null
                    }
                    None => ScalarValue::Null,
                })
                .collect();

            let mut partitioned_file = PartitionedFile {
                object_meta,
                partition_values,
                range: None,
                statistics: Some(Arc::new({
                    let mut statistics = self.create_file_statistics(&data_file);
                    // Physical expression propagation cannot prove that casts preserve
                    // extrema or null counts. Exact aggregation uses the source facts.
                    statistics.column_statistics = statistics
                        .column_statistics
                        .into_iter()
                        .map(ColumnStatistics::to_inexact)
                        .collect();
                    statistics
                })),
                ordering: None,
                extensions: Default::default(),
                metadata_size_hint: None,
                table_reference: None,
                arrow_schema: None,
            };
            partitioned_file
                .extensions
                .insert(IdentityPartitionDefaults::from_file(
                    &data_file,
                    &self.partition_specs,
                    &self.schema,
                )?);

            partitioned_files.push(partitioned_file);
        }

        Ok(partitioned_files)
    }

    fn deletion_vector_snapshot(&self) -> Option<i64> {
        (self.format_version == crate::spec::FormatVersion::V3)
            .then(|| {
                self.snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.snapshot_id())
            })
            .flatten()
    }

    fn create_merge_partitioned_files(
        &self,
        store_ctx: &StoreContext,
        data_files: Vec<DataFile>,
    ) -> Result<Vec<PartitionedFile>> {
        let file_metadata = data_files
            .iter()
            .map(|file| {
                Ok((
                    file.file_path.clone(),
                    file.partition_spec_id,
                    RowLevelFileMetadata::encode(file, self.deletion_vector_snapshot(), &[])?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut partitioned_files = self.create_partitioned_files(store_ctx, data_files)?;
        for (partitioned_file, (file_path, partition_spec_id, partition_json)) in
            partitioned_files.iter_mut().zip(file_metadata)
        {
            partitioned_file.partition_values = vec![
                ScalarValue::Utf8(Some(file_path)),
                ScalarValue::Int32(Some(partition_spec_id)),
                ScalarValue::Utf8(Some(partition_json)),
            ];
        }
        Ok(partitioned_files)
    }

    /// Create file groups from partitioned files
    fn create_file_groups(&self, partitioned_files: Vec<PartitionedFile>) -> Vec<FileGroup> {
        // Group files by partition values
        let mut file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> = HashMap::new();

        for file in partitioned_files {
            file_groups
                .entry(file.partition_values.clone())
                .or_default()
                .push(file);
        }

        file_groups.into_values().map(FileGroup::from).collect()
    }

    /// Compute the object-store URL for this table, to be passed to
    /// `FileScanConfigBuilder`.
    fn object_store_url(&self) -> Result<ObjectStoreUrl> {
        let table_url = Url::parse(&self.table_uri)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        ObjectStoreUrl::parse(&table_url[..url::Position::BeforePath])
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))
    }

    fn equality_scan(&self, deletes: &[DeleteFileRef]) -> Result<Self> {
        let mut read_scan = self.clone();
        read_scan.schema = crate::equality_schema::equality_read_schema(
            &self.schema,
            &self.schema_history,
            deletes
                .iter()
                .flat_map(|delete| delete.data_file.equality_ids.iter().copied()),
        )?;
        let schema = iceberg_schema_to_arrow(&read_scan.schema)?;
        let schema = Self::reorder_arrow_schema_for_identity_partitions(
            &read_scan.schema,
            &self.partition_specs,
            self.default_spec_id,
            &schema,
        );
        read_scan.arrow_schema = Arc::new(match &self.name_mapping {
            Some(mapping) => {
                crate::datasource::type_converter::apply_name_mapping(&schema, mapping)?
            }
            None => schema,
        });
        read_scan.rebuild_output_schema()?;
        Ok(read_scan)
    }

    fn project_scan_schema(
        &self,
        input: Arc<dyn ExecutionPlan>,
        target: &Arc<ArrowSchema>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = input.schema();
        if &schema == target {
            return Ok(input);
        }
        let expressions = target
            .fields()
            .iter()
            .map(|field| {
                let index = schema.index_of(field.name())?;
                let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), index));
                let column = if schema.field(index).data_type() == field.data_type() {
                    column
                } else {
                    Arc::new(SchemaEvolutionCastColumnExpr::new_with_matching(
                        column,
                        schema.fields()[index].clone(),
                        field.clone(),
                        None,
                        StructFieldMatching::FieldId,
                    ))
                };
                Ok((column, field.name().clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(ProjectionExec::try_new(expressions, input)?))
    }

    fn nan_free(&self, files: &[DataFile]) -> bool {
        self.arrow_schema
            .flattened_fields()
            .iter()
            .filter(|field| field.data_type().is_floating())
            .all(|field| {
                iceberg_field_id(field).ok().flatten().is_some_and(|id| {
                    files
                        .iter()
                        .all(|file| file.nan_value_counts.get(&id) == Some(&0))
                })
            })
    }

    fn build_parquet_source(
        &self,
        session: &dyn Session,
        nan_free: bool,
    ) -> Arc<dyn datafusion::datasource::physical_plan::FileSource> {
        let parquet_options = crate::datasource::parquet::parquet_options(
            &self.arrow_schema,
            nan_free,
            session.config().options().execution.parquet.clone(),
        );
        let schema = TableSchema::builder(self.arrow_schema.clone());
        let schema = if self.file_column_name.is_some() {
            schema.with_virtual_columns(vec![parquet_row_position_field(&self.arrow_schema)])
        } else {
            schema
        };
        Arc::new(ParquetSource::new(schema.build()).with_table_parquet_options(parquet_options))
    }

    fn build_merge_parquet_source(
        &self,
        session: &dyn Session,
        file_column_name: &str,
    ) -> Arc<dyn datafusion::datasource::physical_plan::FileSource> {
        let parquet_options = crate::datasource::parquet::parquet_options(
            &self.arrow_schema,
            false,
            session.config().options().execution.parquet.clone(),
        );
        let table_schema = TableSchema::builder(self.arrow_schema.clone())
            .with_table_partition_cols(vec![
                Arc::new(Field::new(file_column_name, DataType::Utf8, false)),
                Arc::new(Field::new(
                    MERGE_PARTITION_SPEC_ID_COLUMN,
                    DataType::Int32,
                    false,
                )),
                Arc::new(Field::new(
                    MERGE_FILE_METADATA_COLUMN,
                    DataType::Utf8,
                    false,
                )),
            ])
            .with_virtual_columns(vec![parquet_row_position_field(&self.output_schema)])
            .build();
        Arc::new(ParquetSource::new(table_schema).with_table_parquet_options(parquet_options))
    }

    fn expanded_projection(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> Option<Vec<usize>> {
        if let Some(used) = projection {
            let mut cols: Vec<usize> = used.clone();
            if let Some(expr) = conjunction(filters.iter().cloned()) {
                for c in expr.column_refs() {
                    if let Ok(idx) = self.arrow_schema.index_of(c.name.as_str())
                        && !cols.contains(&idx)
                    {
                        cols.push(idx);
                    }
                }
            }
            Some(cols)
        } else {
            None
        }
    }

    /// Aggregate table-level statistics from a list of Iceberg data files
    fn aggregate_statistics<'a>(
        &self,
        data_files: impl IntoIterator<Item = &'a DataFile>,
    ) -> Statistics {
        let statistics = data_files
            .into_iter()
            .map(|file| self.create_file_statistics(file))
            .collect::<Vec<_>>();
        sail_common_datafusion::statistics::aggregate_statistics(&self.arrow_schema, &statistics)
    }

    /// Create file statistics from Iceberg data file metadata
    fn create_file_statistics(&self, data_file: &DataFile) -> Statistics {
        let num_rows = Precision::Exact(data_file.record_count() as usize);
        let total_byte_size = Precision::Exact(data_file.file_size_in_bytes() as usize);

        // Create column statistics from Iceberg metadata
        let column_statistics = self
            .arrow_schema
            .fields()
            .iter()
            .map(|field| {
                let Some(field_id) = iceberg_field_id(field).unwrap_or_default() else {
                    return ColumnStatistics::new_unknown();
                };

                if let Some(spec) = self
                    .partition_specs
                    .iter()
                    .find(|spec| spec.spec_id() == data_file.partition_spec_id)
                    && let Some(index) = spec.fields().iter().position(|partition| {
                        partition.source_id == field_id
                            && partition.transform == Transform::Identity
                    })
                    && let Some(value) = data_file.partition.get(index)
                    && let Some(source) = self.schema.field_by_id(field_id)
                {
                    let scalar = match value {
                        Some(value) => to_scalar(value, &source.field_type).ok(),
                        None => ScalarValue::try_new_null(field.data_type()).ok(),
                    };
                    if let Some(scalar) = scalar {
                        return ColumnStatistics {
                            null_count: Precision::Exact(if scalar.is_null() {
                                data_file.record_count() as usize
                            } else {
                                0
                            }),
                            min_value: if matches!(scalar, ScalarValue::Float32(Some(value)) if value.is_nan()) || matches!(scalar, ScalarValue::Float64(Some(value)) if value.is_nan()) { Precision::Absent } else { Precision::Exact(scalar.clone()) },
                            max_value: if matches!(scalar, ScalarValue::Float32(Some(value)) if value.is_nan()) || matches!(scalar, ScalarValue::Float64(Some(value)) if value.is_nan()) { Precision::Absent } else { Precision::Exact(scalar) },
                            ..ColumnStatistics::new_unknown()
                        };
                    }
                }

                let null_count = data_file
                    .null_value_counts()
                    .get(&field_id)
                    .map(|&count| Precision::Exact(count as usize))
                    .unwrap_or(Precision::Absent);

                let distinct_count = Precision::Absent;

                let min_value = data_file
                    .lower_bounds()
                    .get(&field_id)
                    .and_then(|datum| self.statistic_scalar(data_file, field_id, datum))
                    .map(Self::bound_precision)
                    .unwrap_or(Precision::Absent);

                let max_value = data_file
                    .upper_bounds()
                    .get(&field_id)
                    .and_then(|datum| self.statistic_scalar(data_file, field_id, datum))
                    .map(Self::bound_precision)
                    .unwrap_or(Precision::Absent);

                ColumnStatistics {
                    null_count,
                    max_value,
                    min_value,
                    distinct_count,
                    sum_value: Precision::Absent,
                    byte_size: Precision::Absent,
                }
            })
            .collect();

        Statistics {
            num_rows,
            total_byte_size,
            column_statistics,
        }
    }

    fn constant_projection_scan(
        &self,
        files: &[DataFile],
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.arrow_schema.fields().len()).collect());
        let mut scans = Vec::with_capacity(files.len());
        for file in files {
            let statistics = self.create_file_statistics(file);
            let Some(values) = projection
                .iter()
                .map(|index| {
                    let column = &statistics.column_statistics[*index];
                    if column.null_count == Precision::Exact(file.record_count as usize) {
                        return ScalarValue::try_new_null(
                            self.arrow_schema.field(*index).data_type(),
                        )
                        .ok();
                    }
                    if column.null_count == Precision::Exact(0)
                        && let (Precision::Exact(lower), Precision::Exact(upper)) =
                            (&column.min_value, &column.max_value)
                        && lower == upper
                    {
                        return Some(lower.clone());
                    }
                    None
                })
                .collect::<Option<Vec<_>>>()
            else {
                return Ok(None);
            };
            let Ok(end) = i64::try_from(file.record_count) else {
                return Ok(None);
            };
            let source: Arc<dyn ExecutionPlan> =
                Arc::new(sail_physical_plan::range::RangeExec::try_new(
                    sail_logical_plan::range::Range {
                        start: 0,
                        end,
                        step: 1,
                    },
                    1,
                    Arc::new(ArrowSchema::empty()),
                    vec![],
                )?);
            let expressions = projection
                .iter()
                .zip(values.iter().cloned())
                .map(|(index, value)| {
                    (
                        datafusion::physical_expr::expressions::lit(value),
                        self.arrow_schema.field(*index).name().clone(),
                    )
                })
                .collect::<Vec<_>>();
            scans.push((
                values,
                Arc::new(ProjectionExec::try_new(expressions, source)?) as Arc<dyn ExecutionPlan>,
            ));
        }
        scans.sort_by(|(left, _), (right, _)| {
            left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal)
        });
        let mut scans = scans.into_iter().map(|(_, scan)| scan).collect::<Vec<_>>();
        let scan = if scans.len() == 1 {
            scans.remove(0)
        } else {
            UnionExec::try_new(scans)?
        };
        let scan = match limit {
            Some(limit) => Arc::new(GlobalLimitExec::new(scan, 0, Some(limit))),
            None => scan,
        };
        Ok(Some(Arc::new(IcebergMetadataScanExec::new(scan))))
    }

    fn bound_precision(value: ScalarValue) -> Precision<ScalarValue> {
        // Bounds from older files may be truncated regardless of the current metrics mode.
        if matches!(
            value.data_type(),
            DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
        ) {
            Precision::Inexact(value)
        } else {
            Precision::Exact(value)
        }
    }
}

impl IcebergScan {
    pub fn schema(&self) -> Arc<ArrowSchema> {
        self.output_schema.clone()
    }

    pub(crate) async fn create_physical_plan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        prepared: Option<Arc<IcebergScanPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        log::trace!("Starting scan for table: {}", self.table_uri);

        let Some(_snapshot) = self.snapshot.as_ref() else {
            return Ok(Arc::new(EmptyExec::new(
                self.projected_arrow_schema(projection)?,
            )));
        };

        if self.file_column_name.is_some() || self.row_index_column_name.is_some() {
            return self
                .scan_with_merge_metadata(session, projection, filters, limit)
                .await;
        }

        // The streaming manifest path does not carry partition tuples. Use the
        // manifest-aware scan when missing columns may need identity constants.
        if self.metadata_as_data_read
            && !self.partition_specs.iter().any(|spec| {
                spec.fields()
                    .iter()
                    .any(|field| field.transform == Transform::Identity)
            })
        {
            return self
                .scan_metadata_as_data(session, projection, filters, limit)
                .await;
        }

        let table_url = Url::parse(&self.table_uri)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        let base_store = get_object_store_from_session(session, &table_url)?;
        let store_ctx = StoreContext::new(base_store.clone(), &table_url)?;
        log::trace!("Got object store");

        let planned = match prepared {
            Some(planned) if planned.matches(filters, limit) => planned,
            _ => Arc::new(self.plan_files(session, filters, limit).await?),
        };
        if planned.tasks.is_empty() {
            return Ok(Arc::new(EmptyExec::new(
                self.projected_arrow_schema(projection)?,
            )));
        }
        let (_, parquet_pushdown_filters) = self.separate_filters(&planned.residual_filters());
        let mut clean_files = Vec::new();
        let mut dirty_units = Vec::new();
        let mut all_data_files = Vec::new();
        for task in &planned.tasks {
            let file = task.data_file.clone();
            all_data_files.push(file.clone());
            if task.deletes.is_empty() {
                clean_files.push(file);
            } else {
                dirty_units.push((
                    file,
                    task.deletes.positional.clone(),
                    task.deletes.equality.clone(),
                ));
            }
        }
        let mut table_stats = self.aggregate_statistics(&all_data_files);
        table_stats.column_statistics = table_stats
            .column_statistics
            .into_iter()
            .map(ColumnStatistics::to_inexact)
            .collect();

        // Object-store URL shared by all branches.
        let object_store_url = self.object_store_url()?;

        if dirty_units.is_empty() {
            if parquet_pushdown_filters.is_empty()
                && let Some(scan) =
                    self.constant_projection_scan(&all_data_files, projection, limit)?
            {
                return Ok(scan);
            }
            // Fast path: no deletes apply. Emit the single-DataSourceExec plan that
            // is identical to the pre-delete-integration behavior.
            let nan_free = self.nan_free(&all_data_files);
            let partitioned_files = self.create_partitioned_files(&store_ctx, all_data_files)?;
            let file_groups = self.create_file_groups(partitioned_files);
            let parquet_source = self.build_parquet_source(session, nan_free);
            let expanded_projection =
                self.expanded_projection(projection, &parquet_pushdown_filters);
            let file_scan_config = FileScanConfigBuilder::new(object_store_url, parquet_source)
                .with_file_groups(if file_groups.is_empty() {
                    vec![FileGroup::from(vec![])]
                } else {
                    file_groups
                })
                .with_statistics(table_stats)
                .with_projection_indices(expanded_projection)?
                .with_limit(if parquet_pushdown_filters.is_empty() {
                    limit
                } else {
                    None
                })
                .with_expr_adapter(Some(iceberg_schema_evolution_adapter()))
                .build();
            let mut plan = create_data_scan(file_scan_config)?;
            if let Some(predicate) = conjunction(parquet_pushdown_filters.clone()) {
                let schema = plan.schema().to_dfschema()?;
                let predicate = simplify_expr(session, &schema, predicate)?;
                plan = Arc::new(FilterExec::try_new(predicate, plan)?);
            }
            if let Some(projection) = projection {
                let target = self.projected_arrow_schema(Some(projection))?;
                plan = self.project_scan_schema(plan, &target)?;
            }
            if let Some(limit) = limit {
                plan = Arc::new(GlobalLimitExec::new(plan, 0, Some(limit)));
            }
            return Ok(plan);
        }

        // Delete-aware path: build clean + per-dirty-file branches. We apply
        // predicates, projection, and limit ABOVE the Union so that positional
        // row offsets inside `IcebergDeleteApplyExec` remain aligned with the
        // unfiltered Parquet read of each dirty file.

        let mut branches: Vec<Arc<dyn ExecutionPlan>> = Vec::new();

        // Branch A: clean files scanned as one DataSourceExec. Neither projection nor
        // predicate is pushed down at this level because the upper layers will apply
        // them uniformly across branches.
        if !clean_files.is_empty() {
            let partitioned_files = self.create_partitioned_files(&store_ctx, clean_files)?;
            let file_groups = self.create_file_groups(partitioned_files);
            let parquet_source = self.build_parquet_source(session, false);
            let file_scan_config =
                FileScanConfigBuilder::new(object_store_url.clone(), parquet_source)
                    .with_file_groups(file_groups)
                    .with_expr_adapter(Some(iceberg_schema_evolution_adapter()))
                    .build();
            branches.push(create_data_scan(file_scan_config)?);
        }

        // Branch B: one branch per dirty file.
        for (df, pos_deletes, eq_deletes) in dirty_units {
            let delete_scan = self.equality_scan(&eq_deletes)?;
            let partitioned = delete_scan.create_partitioned_files(&store_ctx, vec![df.clone()])?;
            // Single-file, single-partition scan — preserves row order for positional deletes.
            let parquet_source = delete_scan.build_parquet_source(session, false);
            let file_scan_config =
                FileScanConfigBuilder::new(object_store_url.clone(), parquet_source)
                    .with_file_groups(vec![FileGroup::from(partitioned)])
                    // Position deletes require the original file order and absolute offsets.
                    .with_output_partitioning(Some(Partitioning::UnknownPartitioning(1)))
                    .with_preserve_order(true)
                    .with_expr_adapter(Some(iceberg_schema_evolution_adapter()))
                    .build();
            let data_scan: Arc<dyn ExecutionPlan> = create_data_scan(file_scan_config)?;
            let data_file_raw_path = df.file_path().to_string();
            // Wrap with DeleteApply.
            let apply: Arc<dyn ExecutionPlan> = Arc::new(IcebergDeleteApplyExec::new(
                data_scan,
                data_file_raw_path,
                pos_deletes,
                eq_deletes,
                self.table_uri.clone(),
                delete_scan.schema.clone(),
            ));
            branches.push(self.project_scan_schema(apply, &self.arrow_schema)?);
        }

        // Union the branches.
        let unioned: Arc<dyn ExecutionPlan> = if branches.len() == 1 {
            // SAFETY: length was just checked above.
            branches.into_iter().next().ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(
                    "unreachable: branches.len() == 1 but next() returned None".to_string(),
                )
            })?
        } else {
            UnionExec::try_new(branches)?
        };

        // Apply predicate above (covers both clean & dirty branches).
        let after_filter: Arc<dyn ExecutionPlan> = if !parquet_pushdown_filters.is_empty() {
            let df_schema = self.arrow_schema.clone().to_dfschema()?;
            let pushdown_expr = conjunction(parquet_pushdown_filters.clone()).ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(
                    "conjunction over non-empty filters returned None".to_string(),
                )
            })?;
            let simplified = simplify_expr(session, &df_schema, pushdown_expr)?;
            Arc::new(FilterExec::try_new(simplified, unioned)?)
        } else {
            unioned
        };

        // Apply projection above.
        let after_projection: Arc<dyn ExecutionPlan> = if let Some(proj) = projection {
            let projected_schema = self.arrow_schema.clone();
            let proj_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = proj
                .iter()
                .map(|&idx| {
                    let field = projected_schema.field(idx);
                    let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), idx));
                    (col, field.name().to_string())
                })
                .collect();
            Arc::new(ProjectionExec::try_new(proj_exprs, after_filter)?)
        } else {
            after_filter
        };

        // Apply limit above (may over-scan; correctness is preserved because
        // GlobalLimitExec stops streaming once the row count is reached).
        let final_plan: Arc<dyn ExecutionPlan> = if let Some(lim) = limit {
            Arc::new(GlobalLimitExec::new(after_projection, 0, Some(lim)))
        } else {
            after_projection
        };

        Ok(final_plan)
    }

    pub fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        if self.file_column_name.is_some() || self.row_index_column_name.is_some() {
            return Ok(vec![TableProviderFilterPushDown::Unsupported; filter.len()]);
        }
        if self.metadata_as_data_read
            && !self.partition_specs.iter().any(|spec| {
                spec.fields()
                    .iter()
                    .any(|field| field.transform == Transform::Identity)
            })
        {
            return Ok(vec![TableProviderFilterPushDown::Inexact; filter.len()]);
        }
        Ok(filter
            .iter()
            .map(|e| self.classify_pushdown_for_expr(e))
            .collect())
    }
}

impl IcebergScan {
    async fn scan_with_merge_metadata(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        log::trace!("Starting merge-metadata scan for table: {}", self.table_uri);

        let Some(_snapshot) = self.snapshot.as_ref() else {
            return Ok(Arc::new(EmptyExec::new(
                self.projected_arrow_schema(projection)?,
            )));
        };

        let table_url = Url::parse(&self.table_uri)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        let base_store = get_object_store_from_session(session, &table_url)?;
        let store_ctx = StoreContext::new(base_store.clone(), &table_url)?;

        let planned = self.plan_files(session, filters, limit).await?;
        let (_, parquet_pushdown_filters) = self.separate_filters(filters);
        if planned.tasks.is_empty() {
            return Ok(Arc::new(EmptyExec::new(
                self.projected_arrow_schema(projection)?,
            )));
        }
        let object_store_url = self.object_store_url()?;
        let file_column_name = self.file_column_name.as_ref().ok_or_else(|| {
            datafusion::common::DataFusionError::Internal(
                "Iceberg merge metadata scan requires a file column".to_string(),
            )
        })?;
        let mut clean_files = Vec::new();
        let mut file_lineage = HashMap::new();
        let mut dirty_units = Vec::new();

        for task in planned.tasks {
            let data_file = task.data_file;
            let sequence_number = task.data_sequence_number;
            let matched = task.deletes;
            if matched.is_empty() {
                if self.has_row_lineage() {
                    file_lineage.insert(
                        data_file.file_path.clone(),
                        crate::row_lineage::RowLineage {
                            first_row_id: data_file.first_row_id,
                            data_sequence_number: sequence_number,
                        },
                    );
                }
                clean_files.push(data_file);
            } else {
                dirty_units.push((
                    data_file,
                    sequence_number,
                    matched.positional,
                    matched.equality,
                ));
            }
        }

        let mut branches: Vec<Arc<dyn ExecutionPlan>> =
            Vec::with_capacity(dirty_units.len() + usize::from(!clean_files.is_empty()));

        if !clean_files.is_empty() {
            let partitioned_files = self.create_merge_partitioned_files(&store_ctx, clean_files)?;
            let file_groups = partitioned_files
                .into_iter()
                .map(|file| FileGroup::from(vec![file]))
                .collect::<Vec<_>>();
            let mut delete_scan = self.clone();
            if self.has_row_lineage() {
                delete_scan.arrow_schema = Arc::new(crate::row_lineage::append_lineage_fields(
                    &self.arrow_schema,
                )?);
            }
            let parquet_source =
                delete_scan.build_merge_parquet_source(session, file_column_name.as_str());
            let output_partitioning = output_partitioning_from_partition_fields(
                parquet_source.table_schema().table_schema(),
                parquet_source.table_schema().table_partition_cols(),
                file_groups.len(),
            )
            .unwrap_or_else(|| Partitioning::UnknownPartitioning(file_groups.len()));
            let file_scan_config =
                FileScanConfigBuilder::new(object_store_url.clone(), parquet_source)
                    .with_file_groups(file_groups)
                    // Keep whole-file units for delete routing and per-file metadata.
                    .with_output_partitioning(Some(output_partitioning))
                    .with_preserve_order(true)
                    .with_expr_adapter(Some(iceberg_schema_evolution_adapter()))
                    .build();
            let data_scan = create_data_scan(file_scan_config)?;
            branches.push(Arc::new(
                IcebergMergeMetadataExec::try_new_partitioned_files(
                    data_scan,
                    file_column_name.clone(),
                    self.row_index_column_name.clone(),
                    file_lineage,
                )?,
            ));
        }

        for (df, sequence_number, positional_deletes, equality_deletes) in dirty_units {
            let mut delete_scan = self.equality_scan(&equality_deletes)?;
            let row_lineage = self
                .has_row_lineage()
                .then_some(crate::row_lineage::RowLineage {
                    first_row_id: df.first_row_id,
                    data_sequence_number: sequence_number,
                });
            if row_lineage.is_some() {
                delete_scan.arrow_schema = Arc::new(crate::row_lineage::append_lineage_fields(
                    &delete_scan.arrow_schema,
                )?);
            }
            let partitioned = delete_scan.create_partitioned_files(&store_ctx, vec![df.clone()])?;
            let parquet_source = delete_scan.build_parquet_source(session, false);
            let file_scan_config =
                FileScanConfigBuilder::new(object_store_url.clone(), parquet_source)
                    .with_file_groups(vec![FileGroup::from(partitioned)])
                    // Existing position deletes and MERGE row positions both require the
                    // original file order and absolute offsets.
                    .with_output_partitioning(Some(Partitioning::UnknownPartitioning(1)))
                    .with_preserve_order(true)
                    .with_expr_adapter(Some(iceberg_schema_evolution_adapter()))
                    .build();
            let data_scan: Arc<dyn ExecutionPlan> = create_data_scan(file_scan_config)?;
            let with_metadata: Arc<dyn ExecutionPlan> =
                Arc::new(IcebergMergeMetadataExec::try_new(
                    data_scan,
                    df.file_path.clone(),
                    df.partition_spec_id,
                    RowLevelFileMetadata::encode(
                        &df,
                        self.deletion_vector_snapshot(),
                        &positional_deletes,
                    )?,
                    self.file_column_name.clone(),
                    self.row_index_column_name.clone(),
                    row_lineage,
                )?);

            let apply: Arc<dyn ExecutionPlan> = Arc::new(IcebergDeleteApplyExec::new(
                with_metadata,
                df.file_path.clone(),
                positional_deletes,
                equality_deletes,
                self.table_uri.clone(),
                delete_scan.schema.clone(),
            ));
            branches.push(self.project_scan_schema(apply, &self.output_schema)?);
        }

        let unioned: Arc<dyn ExecutionPlan> = if branches.len() == 1 {
            branches.into_iter().next().ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(
                    "unreachable: branches.len() == 1 but next() returned None".to_string(),
                )
            })?
        } else {
            UnionExec::try_new(branches)?
        };

        let after_filter: Arc<dyn ExecutionPlan> = if !parquet_pushdown_filters.is_empty() {
            let df_schema = self.output_schema.clone().to_dfschema()?;
            let pushdown_expr = conjunction(parquet_pushdown_filters.clone()).ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(
                    "conjunction over non-empty filters returned None".to_string(),
                )
            })?;
            let simplified = simplify_expr(session, &df_schema, pushdown_expr)?;
            Arc::new(FilterExec::try_new(simplified, unioned)?)
        } else {
            unioned
        };

        let after_projection: Arc<dyn ExecutionPlan> = if let Some(proj) = projection {
            let projected_schema = self.output_schema.clone();
            let proj_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = proj
                .iter()
                .map(|&idx| {
                    let field = projected_schema.field(idx);
                    let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), idx));
                    (col, field.name().to_string())
                })
                .collect();
            Arc::new(ProjectionExec::try_new(proj_exprs, after_filter)?)
        } else {
            after_filter
        };

        let final_plan: Arc<dyn ExecutionPlan> = if let Some(lim) = limit {
            Arc::new(GlobalLimitExec::new(after_projection, 0, Some(lim)))
        } else {
            after_projection
        };

        log::trace!(
            "Built merge-metadata scan for snapshot {}",
            _snapshot.snapshot_id()
        );
        Ok(final_plan)
    }

    fn classify_pushdown_for_expr(&self, expr: &Expr) -> TableProviderFilterPushDown {
        if Predicate::new(&self.schema, expr).supported() {
            TableProviderFilterPushDown::Exact
        } else {
            TableProviderFilterPushDown::Inexact
        }
    }

    fn separate_filters(&self, filters: &[Expr]) -> (Vec<Expr>, Vec<Expr>) {
        (
            filters.to_vec(),
            filters
                .iter()
                .filter(|filter| {
                    self.classify_pushdown_for_expr(filter) == TableProviderFilterPushDown::Exact
                })
                .cloned()
                .collect(),
        )
    }

    /// Metadata-as-data scan path: defers manifest scanning to the physical plan.
    /// Instead of eagerly loading file metadata on the driver, an `IcebergManifestScanExec`
    /// node produces file metadata that feeds through `IcebergDiscoveryExec` (tagging) and
    /// into `IcebergScanByDataFilesExec` which dynamically opens Parquet files via a
    /// streaming `try_unfold` loop.
    ///
    /// Physical plan pipeline:
    /// ```text
    /// IcebergManifestScanExec (lazy manifest reading → file metadata stream)
    ///   ↓
    /// IcebergDiscoveryExec (annotate stream with partition_scan flag)
    ///   ↓
    /// IcebergScanByDataFilesExec (consume file paths → dynamic Parquet scan)
    ///   ↓
    /// Data output (actual table rows)
    /// ```
    async fn scan_metadata_as_data(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        log::trace!(
            "Using metadata-as-data scan path for table: {}",
            self.table_uri
        );

        let table_url = Url::parse(&self.table_uri)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        let base_store = get_object_store_from_session(session, &table_url)?;
        let store_ctx = StoreContext::new(base_store, &table_url)?;
        let manifest_list = self.load_manifest_list(&store_ctx).await?;
        if manifest_list
            .entries()
            .iter()
            .any(|mf| mf.content == ManifestContentType::Deletes)
        {
            let mut read_scan = self.clone();
            read_scan.metadata_as_data_read = false;
            return Box::pin(
                read_scan.create_physical_plan(session, projection, filters, limit, None),
            )
            .await;
        }

        let snapshot = self.snapshot.as_ref().ok_or_else(|| {
            datafusion::common::DataFusionError::Plan(
                "Iceberg table has no current snapshot".to_string(),
            )
        })?;
        let manifest_scan: Arc<dyn ExecutionPlan> = Arc::new(IcebergManifestScanExec::new(
            self.table_uri.clone(),
            snapshot.clone(),
            ManifestPruning {
                predicate: Predicate::conjunction(&self.schema, filters),
                limit: limit.filter(|_| filters.is_empty()),
                floating_field_ids: self
                    .arrow_schema
                    .flattened_fields()
                    .iter()
                    .filter(|field| field.data_type().is_floating())
                    .map(|field| iceberg_field_id(field).ok().flatten())
                    .collect(),
                specs: self.partition_specs.clone(),
            },
        ));

        let discovery: Arc<dyn ExecutionPlan> = Arc::new(IcebergDiscoveryExec::new(
            manifest_scan,
            self.table_uri.clone(),
            snapshot.snapshot_id(),
            false, // full data file scan, not partition-only
        )?);

        let predicate = conjunction(filters.iter().cloned())
            .map(|predicate| {
                simplify_expr(
                    session,
                    &self.arrow_schema.clone().to_dfschema()?,
                    predicate,
                )
            })
            .transpose()?;
        let scan_exec: Arc<dyn ExecutionPlan> = Arc::new(
            crate::physical_plan::scan_by_data_files_exec::IcebergScanByDataFilesExec::new(
                discovery,
                self.table_uri.clone(),
                self.arrow_schema.clone(),
                projection.cloned(),
                predicate,
                if filters.is_empty() { limit } else { None },
            )?,
        );

        Ok(scan_exec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::types::{NestedField, PrimitiveLiteral, PrimitiveType, Type};

    fn statistics_fixture() -> Result<(IcebergScan, DataFile)> {
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields([
                Arc::new(NestedField::optional(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "score",
                    Type::Primitive(PrimitiveType::Double),
                )),
            ])
            .build()
            .map_err(|error| datafusion_common::plan_datafusion_err!("{error}"))?;
        let read_scan = IcebergScan::new_empty(
            "file:///tmp/statistics/",
            schema,
            vec![PartitionSpec::unpartitioned_spec()],
            0,
        )?;
        let mut file: DataFile = serde_json::from_value(serde_json::json!({
            "content": "DATA", "file_path": "data.parquet", "file_format": "PARQUET",
            "partition": [], "record_count": 2, "file_size_in_bytes": 100, "partition_spec_id": 0
        }))
        .map_err(|error| datafusion_common::plan_datafusion_err!("{error}"))?;
        file.lower_bounds = HashMap::from([
            (1, Datum::new(PrimitiveType::Int, PrimitiveLiteral::Int(2))),
            (
                2,
                Datum::new(PrimitiveType::Double, PrimitiveLiteral::Double(2.5.into())),
            ),
        ]);
        file.upper_bounds = file.lower_bounds.clone();
        file.null_value_counts = HashMap::from([(1, 0), (2, 0)]);
        Ok((read_scan, file))
    }

    #[test]
    fn floating_bounds_require_a_known_zero_nan_count() -> Result<()> {
        let (read_scan, mut file) = statistics_fixture()?;
        for nan_count in [None, Some(1), Some(0)] {
            file.nan_value_counts = nan_count.map(|count| (2, count)).into_iter().collect();
            let expected = if nan_count == Some(0) {
                Precision::Exact(ScalarValue::Float64(Some(2.5)))
            } else {
                Precision::Absent
            };
            for stats in [
                read_scan.create_file_statistics(&file),
                read_scan.aggregate_statistics(&[file.clone()]),
            ] {
                assert_eq!(stats.column_statistics[1].min_value, expected);
                assert_eq!(stats.column_statistics[1].max_value, expected);
            }
        }
        Ok(())
    }

    #[test]
    fn aggregate_statistics_require_complete_optional_metrics() -> Result<()> {
        let (read_scan, file) = statistics_fixture()?;
        let mut missing = file.clone();
        missing.lower_bounds.clear();
        missing.upper_bounds.clear();
        missing.null_value_counts.clear();
        for files in [[file.clone(), missing.clone()], [missing, file]] {
            let stats = read_scan.aggregate_statistics(&files);
            assert_eq!(stats.num_rows, Precision::Exact(4));
            for column in stats.column_statistics {
                assert_eq!(column.null_count, Precision::Absent);
                assert_eq!(column.min_value, Precision::Absent);
                assert_eq!(column.max_value, Precision::Absent);
            }
        }
        Ok(())
    }

    #[tokio::test]
    #[expect(clippy::expect_used)]
    async fn ineligible_metadata_aggregates_do_not_read_manifests() -> Result<()> {
        use datafusion::functions_aggregate::expr_fn::{count, min, sum};
        use datafusion::logical_expr::expr::NullTreatment;
        use datafusion::logical_expr::{ExprFunctionExt, LogicalPlan, LogicalPlanBuilder};
        use datafusion::prelude::{SessionContext, col, lit};
        use sail_common_datafusion::logical_rewriter::LogicalRewriter;

        use crate::logical::{IcebergMetadataAggregateRewriter, IcebergTableSource};
        use crate::spec::snapshots::{Operation, SnapshotBuilder, Summary};

        let (mut read_scan, _) = statistics_fixture()?;
        read_scan.snapshot = Some(
            SnapshotBuilder::new()
                .with_snapshot_id(1)
                .with_sequence_number(1)
                .with_manifest_list("missing-list.avro")
                .with_summary(Summary::new(Operation::Append))
                .build()
                .expect("snapshot"),
        );
        let context = SessionContext::new();
        context.register_object_store(
            &Url::parse(read_scan.table_uri()).expect("table URL"),
            Arc::new(object_store::memory::InMemory::new()),
        );
        let state = context.state();
        assert!(read_scan.plan_files(&state, &[], None).await.is_err());
        let source = Arc::new(IcebergTableSource::new(Arc::new(read_scan.clone())));
        let input = LogicalPlanBuilder::scan("t", source, None)?.build()?;
        let mut plans = vec![];
        for (name, groups, expressions) in [
            ("grouping", vec![col("id")], vec![count(lit(1))]),
            ("unsupported function", vec![], vec![sum(col("id"))]),
            (
                "filtered aggregate",
                vec![],
                vec![count(lit(1)).filter(col("id").gt(lit(1))).build()?],
            ),
            (
                "ordered aggregate",
                vec![],
                vec![
                    min(col("id"))
                        .order_by(vec![col("id").sort(true, true)])
                        .build()?,
                ],
            ),
            (
                "null treatment",
                vec![],
                vec![
                    min(col("id"))
                        .null_treatment(NullTreatment::IgnoreNulls)
                        .build()?,
                ],
            ),
        ] {
            plans.push((
                name,
                LogicalPlanBuilder::from(input.clone())
                    .aggregate(groups, expressions)?
                    .build()?,
            ));
        }
        let mut limited = input.clone();
        let LogicalPlan::TableScan(scan) = &mut limited else {
            unreachable!("scan fixture");
        };
        scan.fetch = Some(1);
        let filtered = LogicalPlanBuilder::from(input)
            .filter(col("id").gt(lit(1)))?
            .build()?;
        for (name, input) in [("fetch", limited), ("logical filter", filtered)] {
            plans.push((
                name,
                LogicalPlanBuilder::from(input)
                    .aggregate(Vec::<Expr>::new(), vec![count(lit(1))])?
                    .build()?,
            ));
        }
        for (name, scan) in [
            (
                "lazy read",
                read_scan.clone().with_metadata_as_data_read(true),
            ),
            (
                "file metadata",
                read_scan.clone().with_file_column("_file")?,
            ),
            (
                "row metadata",
                read_scan.clone().with_row_index_column("_row")?,
            ),
            (
                "copy-on-write candidates",
                read_scan.select_copy_on_write_candidates(col("id").gt(lit(1))),
            ),
        ] {
            let source = Arc::new(IcebergTableSource::new(Arc::new(scan)));
            plans.push((
                name,
                LogicalPlanBuilder::scan("t", source, None)?
                    .aggregate(Vec::<Expr>::new(), vec![count(lit(1))])?
                    .build()?,
            ));
        }
        for (name, plan) in plans {
            IcebergMetadataAggregateRewriter
                .rewrite(plan, &state)
                .await
                .map_err(|error| error.context(name))?;
        }
        Ok(())
    }

    #[tokio::test]
    #[expect(clippy::expect_used)]
    async fn metadata_aggregate_reuses_file_plan_for_partial_and_fallback() -> Result<()> {
        use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
        use datafusion::functions_aggregate::expr_fn::{count, min, sum};
        use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
        use datafusion::prelude::{SessionContext, col, lit};
        use object_store::ObjectStoreExt;
        use object_store::path::Path;
        use sail_common_datafusion::datasource::MergeCapableSource;
        use sail_common_datafusion::logical_rewriter::LogicalRewriter;

        use crate::logical::{IcebergMetadataAggregateRewriter, IcebergTableSource};
        use crate::spec::manifest::{ManifestMetadata, ManifestWriterBuilder};
        use crate::spec::manifest_list::ManifestListWriter;
        use crate::spec::snapshots::{Operation, SnapshotBuilder, Summary};
        use crate::spec::{FormatVersion, ManifestContentType};

        let (mut read_scan, file) = statistics_fixture()?;
        let context = SessionContext::new();
        let table_url = Url::parse(read_scan.table_uri()).expect("table URL");
        let store = Arc::new(object_store::memory::InMemory::new());
        context.register_object_store(&table_url, store.clone());
        let store_context = StoreContext::new(store, &table_url)?;
        let mut writer = ManifestWriterBuilder::new(
            Some(1),
            None,
            ManifestMetadata::new(
                Arc::new(read_scan.schema.clone()),
                0,
                PartitionSpec::unpartitioned_spec(),
                FormatVersion::V2,
                ManifestContentType::Data,
            ),
        )
        .build();
        writer.add(file);
        let bytes = writer.to_avro_bytes_v2().expect("manifest bytes");
        store_context
            .prefixed
            .put(&Path::from("manifest.avro"), bytes.into())
            .await?;
        let mut list = ManifestListWriter::new();
        list.append(
            writer
                .into_manifest_file("manifest.avro".into(), 1, 1)
                .expect("manifest file"),
        );
        let bytes = list.to_bytes(FormatVersion::V2).expect("manifest list");
        store_context
            .prefixed
            .put(&Path::from("list.avro"), bytes.into())
            .await?;
        read_scan.snapshot = Some(
            SnapshotBuilder::new()
                .with_snapshot_id(1)
                .with_sequence_number(1)
                .with_manifest_list("list.avro")
                .with_summary(Summary::new(Operation::Append))
                .build()
                .expect("snapshot"),
        );
        let read_scan = Arc::new(read_scan);
        let source = Arc::new(IcebergTableSource::new(Arc::clone(&read_scan)));
        let state = context.state();
        let mut rewritten_scans = vec![];
        for expressions in [
            vec![count(lit(1)), sum(col("score"))],
            vec![min(col("score"))],
        ] {
            let plan = LogicalPlanBuilder::scan("t", source.clone(), None)?
                .aggregate(Vec::<Expr>::new(), expressions)?
                .build()?;
            let rewritten = IcebergMetadataAggregateRewriter
                .rewrite(plan, &state)
                .await?;
            rewritten.data.apply(|node| {
                if let LogicalPlan::TableScan(scan) = node {
                    rewritten_scans.push(scan.clone());
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        assert_eq!(rewritten_scans.len(), 2);
        let aggregate = LogicalPlanBuilder::scan("t", source, None)?
            .aggregate(Vec::<Expr>::new(), vec![min(col("score"))])?
            .build()?;
        let repeated = LogicalPlanBuilder::from(aggregate.clone())
            .union(aggregate)?
            .build()?;
        let rewritten = IcebergMetadataAggregateRewriter
            .rewrite(repeated, &state)
            .await?;
        let mut prepared_requests = vec![];
        rewritten.data.apply(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                let source = scan
                    .source
                    .downcast_ref::<IcebergTableSource>()
                    .expect("Iceberg source");
                prepared_requests.push(
                    source
                        .prepared(&scan.filters, scan.fetch)
                        .expect("prepared request"),
                );
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert_eq!(prepared_requests.len(), 2);
        assert!(Arc::ptr_eq(&prepared_requests[0], &prepared_requests[1]));
        store_context
            .prefixed
            .delete(&Path::from("manifest.avro"))
            .await?;
        store_context
            .prefixed
            .delete(&Path::from("list.avro"))
            .await?;
        assert!(read_scan.plan_files(&state, &[], None).await.is_err());
        for scan in rewritten_scans {
            let source = scan
                .source
                .downcast_ref::<IcebergTableSource>()
                .expect("Iceberg source");
            let prepared = source
                .prepared(&scan.filters, scan.fetch)
                .expect("prepared file plan");
            assert_eq!(prepared.tasks.len(), 1);
            assert!(source.prepared(&[col("id").gt(lit(2))], None).is_none());
            assert!(source.prepared(&[], Some(1)).is_none());
            for refined in [
                source.with_file_column("_file")?,
                source.with_row_index_column("_row")?,
            ] {
                let refined = refined
                    .downcast_ref::<IcebergTableSource>()
                    .expect("refined Iceberg source");
                assert!(refined.prepared(&scan.filters, scan.fetch).is_none());
            }
            source
                .scan()
                .create_physical_plan(
                    &state,
                    scan.projection.as_ref(),
                    &scan.filters,
                    scan.fetch,
                    Some(prepared),
                )
                .await?;
        }
        Ok(())
    }
}
