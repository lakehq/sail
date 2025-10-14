use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::Session;
use datafusion::common::scalar::ScalarValue;
use datafusion::common::stats::{ColumnStatistics, Precision, Statistics};
use datafusion::common::{Result as DataFusionResult, ToDFSchema};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{
    BinaryExpr, Expr, LogicalPlan, Operator, TableProviderFilterPushDown,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::ExecutionPlan;
use object_store::path::Path as ObjectPath;
use object_store::ObjectMeta;
use url::Url;

use crate::arrow_conversion::iceberg_schema_to_arrow;
use crate::datasource::expr_adapter::IcebergPhysicalExprAdapterFactory;
use crate::datasource::expressions::simplify_expr;
use crate::datasource::pruning::{prune_files, prune_manifests_by_partition_summaries};
use crate::spec::types::values::{Literal, PrimitiveLiteral};
use crate::spec::{
    DataFile, FormatVersion, Manifest, ManifestContentType, ManifestList, ManifestStatus,
    PartitionSpec, Schema, Snapshot,
};

/// Iceberg table provider for DataFusion
#[derive(Debug)]
pub struct IcebergTableProvider {
    /// The table location (URI)
    table_uri: String,
    /// The current schema of the table
    schema: Schema,
    /// The current snapshot of the table
    snapshot: Snapshot,
    /// All partition specs referenced by the table
    #[allow(unused)]
    partition_specs: Vec<PartitionSpec>,
    /// Arrow schema for DataFusion
    arrow_schema: Arc<ArrowSchema>,
}

impl IcebergTableProvider {
    /// Create a new Iceberg table provider
    pub fn new(
        table_uri: impl ToString,
        schema: Schema,
        snapshot: Snapshot,
        partition_specs: Vec<PartitionSpec>,
    ) -> DataFusionResult<Self> {
        let table_uri_str = table_uri.to_string();
        log::trace!("Creating table provider for: {}", table_uri_str);

        let arrow_schema = Arc::new(iceberg_schema_to_arrow(&schema).map_err(|e| {
            log::trace!("Failed to convert schema to Arrow: {:?}", e);
            e
        })?);

        log::trace!(
            "Converted schema to Arrow with {} fields",
            arrow_schema.fields().len()
        );

        Ok(Self {
            table_uri: table_uri_str,
            schema,
            snapshot,
            partition_specs,
            arrow_schema,
        })
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
    pub fn current_snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// Get object store from DataFusion session
    fn get_object_store(
        &self,
        session: &dyn Session,
    ) -> DataFusionResult<Arc<dyn object_store::ObjectStore>> {
        let table_url = Url::parse(&self.table_uri)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        session
            .runtime_env()
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))
    }

    /// Load manifest list from snapshot
    async fn load_manifest_list(
        &self,
        object_store: &Arc<dyn object_store::ObjectStore>,
    ) -> DataFusionResult<ManifestList> {
        let manifest_list_str = self.snapshot.manifest_list();
        log::trace!("Manifest list path: {}", manifest_list_str);

        let manifest_list_path = if let Ok(url) = Url::parse(manifest_list_str) {
            log::trace!("Parsed manifest list as URL, path: {}", url.path());
            ObjectPath::from(url.path())
        } else {
            ObjectPath::from(manifest_list_str)
        };

        let manifest_list_data = object_store
            .get(&manifest_list_path)
            .await
            .map_err(|e| {
                log::trace!("Failed to get manifest list: {:?}", e);
                datafusion::common::DataFusionError::External(Box::new(e))
            })?
            .bytes()
            .await
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        log::trace!("Read {} bytes from manifest list", manifest_list_data.len());

        ManifestList::parse_with_version(&manifest_list_data, FormatVersion::V2)
            .map_err(datafusion::common::DataFusionError::Execution)
    }

    /// Load data files from manifests
    async fn load_data_files(
        &self,
        session: &dyn Session,
        filters: &[Expr],
        object_store: &Arc<dyn object_store::ObjectStore>,
        manifest_list: &ManifestList,
    ) -> DataFusionResult<Vec<DataFile>> {
        let mut data_files = Vec::new();

        // Build partition spec map for summary pruning
        let spec_map: HashMap<i32, PartitionSpec> = self
            .partition_specs
            .iter()
            .map(|s| (s.spec_id(), s.clone()))
            .collect();
        let manifest_files =
            prune_manifests_by_partition_summaries(manifest_list, &self.schema, &spec_map, filters);

        for manifest_file in manifest_files {
            // TODO: Support delete manifests
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }

            let manifest_path_str = manifest_file.manifest_path.as_str();
            log::trace!("Loading manifest: {}", manifest_path_str);

            let manifest_path = if let Ok(url) = Url::parse(manifest_path_str) {
                log::trace!("Parsed manifest as URL, path: {}", url.path());
                ObjectPath::from(url.path())
            } else {
                ObjectPath::from(manifest_path_str)
            };

            let manifest_data = object_store
                .get(&manifest_path)
                .await
                .map_err(|e| {
                    log::trace!("Failed to get manifest: {:?}", e);
                    datafusion::common::DataFusionError::External(Box::new(e))
                })?
                .bytes()
                .await
                .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

            log::trace!("Read {} bytes from manifest", manifest_data.len());

            let manifest = Manifest::parse_avro(&manifest_data)
                .map_err(datafusion::common::DataFusionError::Execution)?;

            // Get partition_spec_id from manifest file
            let partition_spec_id = manifest_file.partition_spec_id;

            // Collect data files for this manifest
            let manifest_data_files: Vec<DataFile> = manifest
                .entries()
                .iter()
                .filter_map(|entry_ref| {
                    let entry = entry_ref.as_ref();
                    if matches!(
                        entry.status,
                        ManifestStatus::Added | ManifestStatus::Existing
                    ) {
                        let mut df = entry.data_file.clone();
                        df.partition_spec_id = partition_spec_id;
                        Some(df)
                    } else {
                        None
                    }
                })
                .collect();

            // Early prune at manifest entry level using DataFusion predicate over metrics
            if !filters.is_empty() {
                let (kept, _mask) = crate::datasource::pruning::prune_files(
                    session,
                    filters,
                    None,
                    self.arrow_schema.clone(),
                    manifest_data_files,
                    &self.schema,
                )?;
                data_files.extend(kept);
            } else {
                data_files.extend(manifest_data_files);
            }
        }

        Ok(data_files)
    }

    /// Create partitioned files for DataFusion from Iceberg data files
    fn create_partitioned_files(
        &self,
        data_files: Vec<DataFile>,
    ) -> DataFusionResult<Vec<PartitionedFile>> {
        let mut partitioned_files = Vec::new();

        let table_url = Url::parse(&self.table_uri)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        let table_base_path = table_url.path();

        for data_file in data_files {
            let file_path_str = data_file.file_path();
            log::trace!("Processing data file: {}", file_path_str);

            let file_path = if let Ok(url) = Url::parse(file_path_str) {
                ObjectPath::from(url.path())
            } else {
                ObjectPath::from(format!(
                    "{}{}{}",
                    table_base_path,
                    object_store::path::DELIMITER,
                    file_path_str
                ))
            };

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
                    Some(literal) => self.literal_to_scalar_value(literal),
                    None => ScalarValue::Null,
                })
                .collect();

            let partitioned_file = PartitionedFile {
                object_meta,
                partition_values,
                range: None,
                statistics: Some(Arc::new(self.create_file_statistics(&data_file))),
                extensions: None,
                metadata_size_hint: None,
            };

            partitioned_files.push(partitioned_file);
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

    /// Aggregate table-level statistics from a list of Iceberg data files
    fn aggregate_statistics(&self, data_files: &[DataFile]) -> Statistics {
        if data_files.is_empty() {
            return Statistics::new_unknown(&self.arrow_schema);
        }

        let mut total_rows: usize = 0;
        let mut total_bytes: usize = 0;

        // Pre-compute field id per column index
        let field_ids: Vec<i32> = self.schema.fields().iter().map(|f| f.id).collect();

        // Initialize accumulators per column
        let mut min_scalars: Vec<Option<ScalarValue>> =
            vec![None; self.arrow_schema.fields().len()];
        let mut max_scalars: Vec<Option<ScalarValue>> =
            vec![None; self.arrow_schema.fields().len()];
        let mut null_counts: Vec<usize> = vec![0; self.arrow_schema.fields().len()];

        for df in data_files {
            total_rows = total_rows.saturating_add(df.record_count() as usize);
            total_bytes = total_bytes.saturating_add(df.file_size_in_bytes() as usize);

            for (col_idx, field_id) in field_ids.iter().enumerate() {
                // null counts
                if let Some(c) = df.null_value_counts().get(field_id) {
                    null_counts[col_idx] = null_counts[col_idx].saturating_add(*c as usize);
                }

                // min
                if let Some(d) = df.lower_bounds().get(field_id) {
                    let v = Literal::Primitive(d.literal.clone());
                    let sv = self.literal_to_scalar_value(&v);
                    min_scalars[col_idx] = match (&min_scalars[col_idx], &sv) {
                        (None, s) => Some(s.clone()),
                        (Some(existing), s) => Some(if s < existing {
                            s.clone()
                        } else {
                            existing.clone()
                        }),
                    };
                }

                // max
                if let Some(d) = df.upper_bounds().get(field_id) {
                    let v = Literal::Primitive(d.literal.clone());
                    let sv = self.literal_to_scalar_value(&v);
                    max_scalars[col_idx] = match (&max_scalars[col_idx], &sv) {
                        (None, s) => Some(s.clone()),
                        (Some(existing), s) => Some(if s > existing {
                            s.clone()
                        } else {
                            existing.clone()
                        }),
                    };
                }
            }
        }

        let column_statistics = (0..self.arrow_schema.fields().len())
            .map(|i| ColumnStatistics {
                null_count: Precision::Exact(null_counts[i]),
                max_value: max_scalars[i]
                    .clone()
                    .map(Precision::Exact)
                    .unwrap_or(Precision::Absent),
                min_value: min_scalars[i]
                    .clone()
                    .map(Precision::Exact)
                    .unwrap_or(Precision::Absent),
                distinct_count: Precision::Absent,
                sum_value: Precision::Absent,
            })
            .collect();

        Statistics {
            num_rows: Precision::Exact(total_rows),
            total_byte_size: Precision::Exact(total_bytes),
            column_statistics,
        }
    }

    /// Convert Iceberg Literal to DataFusion ScalarValue
    fn literal_to_scalar_value(&self, literal: &Literal) -> ScalarValue {
        match literal {
            Literal::Primitive(primitive) => match primitive {
                PrimitiveLiteral::Boolean(v) => ScalarValue::Boolean(Some(*v)),
                PrimitiveLiteral::Int(v) => ScalarValue::Int32(Some(*v)),
                PrimitiveLiteral::Long(v) => ScalarValue::Int64(Some(*v)),
                PrimitiveLiteral::Float(v) => ScalarValue::Float32(Some(v.into_inner())),
                PrimitiveLiteral::Double(v) => ScalarValue::Float64(Some(v.into_inner())),
                PrimitiveLiteral::String(v) => ScalarValue::Utf8(Some(v.clone())),
                PrimitiveLiteral::Binary(v) => ScalarValue::Binary(Some(v.clone())),
                PrimitiveLiteral::Int128(v) => ScalarValue::Decimal128(Some(*v), 38, 0),
                PrimitiveLiteral::UInt128(v) => {
                    if *v <= i128::MAX as u128 {
                        ScalarValue::Decimal128(Some(*v as i128), 38, 0)
                    } else {
                        ScalarValue::Utf8(Some(v.to_string()))
                    }
                }
            },
            Literal::Struct(fields) => {
                let json_repr = serde_json::to_string(fields).unwrap_or_default();
                ScalarValue::Utf8(Some(json_repr))
            }
            Literal::List(items) => {
                let json_repr = serde_json::to_string(items).unwrap_or_default();
                ScalarValue::Utf8(Some(json_repr))
            }
            Literal::Map(pairs) => {
                let json_repr = serde_json::to_string(pairs).unwrap_or_default();
                ScalarValue::Utf8(Some(json_repr))
            }
        }
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
            .enumerate()
            .map(|(i, _field)| {
                let field_id = self
                    .schema
                    .fields()
                    .get(i)
                    .map(|f| f.id)
                    .unwrap_or(i as i32 + 1);

                let null_count = data_file
                    .null_value_counts()
                    .get(&field_id)
                    .map(|&count| Precision::Exact(count as usize))
                    .unwrap_or(Precision::Absent);

                let distinct_count = Precision::Absent;

                let min_value = data_file
                    .lower_bounds()
                    .get(&field_id)
                    .map(|datum| {
                        // convert Datum -> Literal for existing scalar conversion
                        let lit = Literal::Primitive(datum.literal.clone());
                        self.literal_to_scalar_value(&lit)
                    })
                    .map(Precision::Exact)
                    .unwrap_or(Precision::Absent);

                let max_value = data_file
                    .upper_bounds()
                    .get(&field_id)
                    .map(|datum| {
                        let lit = Literal::Primitive(datum.literal.clone());
                        self.literal_to_scalar_value(&lit)
                    })
                    .map(Precision::Exact)
                    .unwrap_or(Precision::Absent);

                ColumnStatistics {
                    null_count,
                    max_value,
                    min_value,
                    distinct_count,
                    sum_value: Precision::Absent,
                }
            })
            .collect();

        Statistics {
            num_rows,
            total_byte_size,
            column_statistics,
        }
    }
}

#[async_trait]
impl TableProvider for IcebergTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.arrow_schema.clone()
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
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        log::trace!("Starting scan for table: {}", self.table_uri);

        let object_store = self.get_object_store(session)?;
        log::trace!("Got object store");

        log::trace!(
            "Loading manifest list from: {}",
            self.snapshot.manifest_list()
        );
        let manifest_list = self.load_manifest_list(&object_store).await?;
        log::trace!("Loaded {} manifest files", manifest_list.entries().len());

        // Classify & split filters for pruning vs parquet pushdown
        let (pruning_filters, parquet_pushdown_filters) = self.separate_filters(filters);

        log::trace!("Loading data files from manifests...");
        let mut data_files = self
            .load_data_files(session, &pruning_filters, &object_store, &manifest_list)
            .await?;
        log::trace!("Loaded {} data files", data_files.len());

        // Build filter conjunction and run DataFusion-based pruning on Iceberg metrics
        let filter_expr = conjunction(pruning_filters.iter().cloned());
        let mut _pruning_mask: Option<Vec<bool>> = None;
        if filter_expr.is_some() || limit.is_some() {
            let (kept, mask) = prune_files(
                session,
                &pruning_filters,
                limit,
                self.rebuild_logical_schema_for_filters(projection, filters),
                data_files,
                &self.schema,
            )?;
            _pruning_mask = mask;
            data_files = kept;
            log::trace!("Pruned data files, remaining: {}", data_files.len());
        }

        log::trace!("Creating partitioned files...");
        let partitioned_files = self.create_partitioned_files(data_files.clone())?;
        log::trace!("Created {} partitioned files", partitioned_files.len());

        // Step 4: Create file groups
        let file_groups = self.create_file_groups(partitioned_files);

        // Step 5: Create file scan configuration
        let file_schema = self.arrow_schema.clone();
        let table_url = Url::parse(&self.table_uri)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        let base_url = format!("{}://{}", table_url.scheme(), table_url.authority());
        let base_url_parsed = Url::parse(&base_url)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        let object_store_url = ObjectStoreUrl::parse(base_url_parsed)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        let parquet_options = TableParquetOptions {
            global: session.config().options().execution.parquet.clone(),
            ..Default::default()
        };

        let mut parquet_source = ParquetSource::new(parquet_options);
        // Prepare pushdown filter for Parquet
        let pushdown_filter: Option<Arc<dyn PhysicalExpr>> = if !parquet_pushdown_filters.is_empty()
        {
            let logical_schema = self.rebuild_logical_schema_for_filters(projection, filters);
            let df_schema = logical_schema.to_dfschema()?;
            let pushdown_expr = conjunction(parquet_pushdown_filters);
            pushdown_expr.map(|expr| simplify_expr(session, &df_schema, expr))
        } else {
            None
        };
        if let Some(pred) = pushdown_filter {
            // TODO: Consider expression adapter for Parquet pushdown
            parquet_source = parquet_source.with_predicate(pred);
        }
        let parquet_source = Arc::new(parquet_source);

        // Build table statistics from pruned files
        let table_stats = self.aggregate_statistics(&data_files);

        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url, file_schema, parquet_source)
                .with_file_groups(if file_groups.is_empty() {
                    vec![FileGroup::from(vec![])]
                } else {
                    file_groups
                })
                .with_statistics(table_stats)
                .with_projection(projection.cloned())
                .with_limit(limit)
                .with_expr_adapter(Some(Arc::new(IcebergPhysicalExprAdapterFactory {})
                    as Arc<dyn PhysicalExprAdapterFactory>))
                .build();

        Ok(DataSourceExec::from_data_source(file_scan_config))
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filter
            .iter()
            .map(|e| self.classify_pushdown_for_expr(e))
            .collect())
    }
}

impl IcebergTableProvider {
    fn classify_pushdown_for_expr(&self, expr: &Expr) -> TableProviderFilterPushDown {
        use TableProviderFilterPushDown as FP;
        // Identity partition columns get Exact (Eq/IN) or Inexact (ranges)
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let (l, r) = (Self::strip_expr(left), Self::strip_expr(right));
                match op {
                    Operator::Eq => {
                        if let (Some(col), true) =
                            (self.expr_as_column_name(l), self.expr_is_literal(r))
                        {
                            if self.is_identity_partition_col(&col) {
                                return FP::Exact;
                            }
                        }
                        if let (Some(col), true) =
                            (self.expr_as_column_name(r), self.expr_is_literal(l))
                        {
                            if self.is_identity_partition_col(&col) {
                                return FP::Exact;
                            }
                        }
                        FP::Unsupported
                    }
                    Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq => {
                        if let Some(col) = self.expr_as_column_name(l) {
                            if self.expr_is_literal(r) && self.is_identity_partition_col(&col) {
                                return FP::Inexact;
                            }
                        }
                        FP::Unsupported
                    }
                    _ => FP::Unsupported,
                }
            }
            Expr::InList(in_list) if !in_list.negated => {
                let e = Self::strip_expr(&in_list.expr);
                if let Some(col) = self.expr_as_column_name(e) {
                    let all_literals = in_list.list.iter().all(|it| self.expr_is_literal(it));
                    if all_literals && self.is_identity_partition_col(&col) {
                        TableProviderFilterPushDown::Exact
                    } else {
                        TableProviderFilterPushDown::Unsupported
                    }
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            }
            _ => TableProviderFilterPushDown::Unsupported,
        }
    }

    fn separate_filters(&self, filters: &[Expr]) -> (Vec<Expr>, Vec<Expr>) {
        let mut pruning_filters = Vec::new();
        let mut parquet_pushdown_filters = Vec::new();
        for f in filters.iter() {
            match self.classify_pushdown_for_expr(f) {
                TableProviderFilterPushDown::Exact => {
                    pruning_filters.push(f.clone());
                }
                TableProviderFilterPushDown::Inexact => {
                    pruning_filters.push(f.clone());
                    parquet_pushdown_filters.push(f.clone());
                }
                TableProviderFilterPushDown::Unsupported => {}
            }
        }
        (pruning_filters, parquet_pushdown_filters)
    }

    fn strip_expr(expr: &Expr) -> &Expr {
        match expr {
            Expr::Cast(c) => Self::strip_expr(&c.expr),
            Expr::Alias(a) => Self::strip_expr(&a.expr),
            _ => expr,
        }
    }

    fn expr_as_column_name(&self, expr: &Expr) -> Option<String> {
        if let Expr::Column(c) = expr {
            return Some(c.name.clone());
        }
        None
    }

    fn expr_is_literal(&self, expr: &Expr) -> bool {
        matches!(expr, Expr::Literal(_, _))
    }

    fn is_identity_partition_col(&self, col_name: &str) -> bool {
        // Map identity partition source_id to schema field names
        let mut names = std::collections::HashSet::new();
        for spec in &self.partition_specs {
            for pf in spec.fields().iter() {
                if matches!(pf.transform, crate::spec::transform::Transform::Identity) {
                    if let Some(field) = self.schema.field_by_id(pf.source_id) {
                        names.insert(field.name.clone());
                    }
                }
            }
        }
        names.contains(col_name)
    }

    fn rebuild_logical_schema_for_filters(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> Arc<ArrowSchema> {
        if let Some(used) = projection {
            let mut fields: Vec<arrow_schema::FieldRef> = Vec::new();
            for idx in used {
                fields.push(Arc::new(self.arrow_schema.field(*idx).clone()));
            }
            if let Some(expr) = conjunction(filters.iter().cloned()) {
                for c in expr.column_refs() {
                    if let Ok(idx) = self.arrow_schema.index_of(c.name.as_str()) {
                        if !used.contains(&idx)
                            && !fields.iter().any(|f| f.name() == c.name.as_str())
                        {
                            fields.push(Arc::new(self.arrow_schema.field(idx).clone()));
                        }
                    }
                }
            }
            Arc::new(ArrowSchema::new(fields))
        } else {
            self.arrow_schema.clone()
        }
    }
}
