use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::Session;
use datafusion::common::stats::{ColumnStatistics, Precision, Statistics};
use datafusion::common::ToDFSchema;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use object_store::path::Path as ObjectStorePath;
use url::Url;

use crate::datasource::arrow::columns_to_arrow_schema;
use crate::datasource::expressions::{get_pushdown_filters, simplify_expr};
use crate::datasource::pruning::prune_files;
use crate::metadata::{DuckLakeMetaStore, DuckLakeTable, ListDataFilesRequest};
use crate::options::DuckLakeOptions;
use crate::spec::{ColumnInfo, FieldIndex, FileInfo, PartitionFieldInfo, PartitionFilter};

pub struct DuckLakeTableProvider {
    table: DuckLakeTable,
    schema: ArrowSchemaRef,
    base_path: String,
    snapshot_id: Option<u64>,
    meta_store: Arc<dyn DuckLakeMetaStore>,
}

impl DuckLakeTableProvider {
    pub async fn new(
        _ctx: &dyn Session,
        meta_store: Arc<dyn DuckLakeMetaStore>,
        opts: DuckLakeOptions,
    ) -> DataFusionResult<Self> {
        let parts: Vec<&str> = opts.table.split('.').collect();
        let (schema_name, table_name) = match parts.as_slice() {
            [table] => (None, *table),
            [schema, table] => (Some(*schema), *table),
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "Invalid table name format: {}",
                    opts.table
                )))
            }
        };

        let table = meta_store.load_table(table_name, schema_name).await?;
        let schema = Arc::new(columns_to_arrow_schema(&table.columns)?);

        log::trace!(
            "Loaded DuckLake table: {}.{} with {} columns",
            table.schema_info.schema_name,
            table.table_info.table_name,
            table.columns.len()
        );

        Ok(Self {
            table,
            schema,
            base_path: opts.base_path,
            snapshot_id: opts.snapshot_id,
            meta_store,
        })
    }
}

impl std::fmt::Debug for DuckLakeTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckLakeTableProvider")
            .field("table", &self.table.table_info.table_name)
            .field("schema", &self.table.schema_info.schema_name)
            .finish()
    }
}

#[async_trait]
impl TableProvider for DuckLakeTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(get_pushdown_filters(filters, &[]))
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        log::trace!(
            "Scanning DuckLake table: {}.{}",
            self.table.schema_info.schema_name,
            self.table.table_info.table_name
        );

        let (partition_filters, remaining_filters) = Self::extract_partition_filters(
            filters,
            &self.table.columns,
            &self.table.partition_fields,
        );
        let (pruning_filters, pushdown_filters) = self.separate_filters(&remaining_filters);

        let prune_schema = self.build_prune_schema(projection, &pruning_filters);
        let required_stats = self.collect_required_stat_fields(prune_schema.as_ref());
        let request = self.build_data_file_request(partition_filters, required_stats);
        let files = self.meta_store.list_data_files(request).await?;

        log::trace!("Found {} data files", files.len());

        let (files, _mask) = prune_files(
            session,
            &pruning_filters,
            limit,
            prune_schema.clone(),
            files,
            &self.table.columns,
            &self.table.partition_fields,
        )?;

        let (object_store_url, table_prefix) = self.build_object_store_context()?;
        let table_stats = self.aggregate_statistics(self.schema.as_ref(), &files);
        let file_groups = self.build_file_groups(files, &table_prefix)?;

        let parquet_options = TableParquetOptions {
            global: session.config().options().execution.parquet.clone(),
            ..Default::default()
        };

        let mut parquet_source = ParquetSource::new(parquet_options);
        let pushdown_filter: Option<Arc<dyn PhysicalExpr>> = if !pushdown_filters.is_empty() {
            let df_schema = prune_schema.clone().to_dfschema()?;
            let pushdown_expr = datafusion::logical_expr::utils::conjunction(pushdown_filters);
            pushdown_expr.map(|expr| simplify_expr(session, &df_schema, expr))
        } else {
            None
        };
        if let Some(pred) = pushdown_filter {
            parquet_source = parquet_source.with_predicate(pred);
        }
        let parquet_source = Arc::new(parquet_source);

        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url, self.schema.clone(), parquet_source)
                .with_file_groups(file_groups)
                .with_statistics(table_stats)
                .with_projection_indices(projection.cloned())
                .with_limit(limit)
                .build();

        Ok(DataSourceExec::from_data_source(file_scan_config))
    }
}

impl DuckLakeTableProvider {
    fn build_prune_schema(
        &self,
        projection: Option<&Vec<usize>>,
        pruning_filters: &[Expr],
    ) -> Arc<datafusion::arrow::datatypes::Schema> {
        if let Some(used_columns) = projection {
            let mut fields = used_columns
                .iter()
                .map(|idx| self.schema.field(*idx).to_owned())
                .collect::<Vec<_>>();

            if let Some(expr) =
                datafusion::logical_expr::utils::conjunction(pruning_filters.iter().cloned())
            {
                for c in expr.column_refs() {
                    if let Ok(idx) = self.schema.index_of(c.name.as_str()) {
                        if !used_columns.contains(&idx)
                            && !fields.iter().any(|f| f.name() == c.name.as_str())
                        {
                            fields.push(self.schema.field(idx).to_owned());
                        }
                    }
                }
            }

            Arc::new(datafusion::arrow::datatypes::Schema::new(fields))
        } else {
            self.schema.clone()
        }
    }

    fn collect_required_stat_fields(
        &self,
        schema: &datafusion::arrow::datatypes::Schema,
    ) -> Vec<FieldIndex> {
        let mut name_to_id: HashMap<String, FieldIndex> = HashMap::new();
        for column in &self.table.columns {
            name_to_id.insert(column.column_name.clone(), column.column_id);
        }

        let mut seen = HashSet::new();
        let mut required = Vec::new();
        for field in schema.fields() {
            if let Some(field_id) = name_to_id.get(field.name()) {
                if seen.insert(*field_id) {
                    required.push(*field_id);
                }
            }
        }
        required
    }

    fn build_data_file_request(
        &self,
        partition_filters: Vec<PartitionFilter>,
        required_stats: Vec<FieldIndex>,
    ) -> ListDataFilesRequest {
        ListDataFilesRequest {
            table_id: self.table.table_info.table_id,
            snapshot_id: self.snapshot_id,
            partition_filters: (!partition_filters.is_empty()).then_some(partition_filters),
            required_column_stats: (!required_stats.is_empty()).then_some(required_stats),
        }
    }

    fn build_object_store_context(&self) -> DataFusionResult<(ObjectStoreUrl, ObjectStorePath)> {
        let base_url =
            Url::parse(&self.base_path).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut object_store_base = base_url.clone();
        object_store_base.set_query(None);
        object_store_base.set_fragment(None);
        object_store_base.set_path("/");
        let object_store_url = ObjectStoreUrl::parse(object_store_base.as_str())?;

        let mut table_prefix = ObjectStorePath::parse(base_url.path())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        table_prefix = table_prefix
            .child(self.table.schema_info.schema_name.as_str())
            .child(self.table.table_info.table_name.as_str());

        Ok((object_store_url, table_prefix))
    }

    fn build_file_groups(
        &self,
        files: Vec<FileInfo>,
        table_prefix: &ObjectStorePath,
    ) -> DataFusionResult<Vec<FileGroup>> {
        let mut file_groups: HashMap<Option<u64>, Vec<PartitionedFile>> = HashMap::new();

        for file in files {
            let object_path = Self::resolve_file_path(table_prefix, &file)?;
            let partitioned_file = PartitionedFile::new(object_path, file.file_size_bytes);
            let partition_key = file.partition_id.map(|p| p.0);
            file_groups
                .entry(partition_key)
                .or_default()
                .push(partitioned_file);
        }

        log::trace!(
            "Created {} file groups from {} files",
            file_groups.len(),
            file_groups.values().map(|v| v.len()).sum::<usize>()
        );

        if file_groups.is_empty() {
            log::warn!("No data files found for table");
            Ok(vec![FileGroup::from(vec![])])
        } else {
            Ok(file_groups.into_values().map(FileGroup::from).collect())
        }
    }

    fn resolve_file_path(
        base_prefix: &ObjectStorePath,
        file: &FileInfo,
    ) -> DataFusionResult<ObjectStorePath> {
        if file.path_is_relative {
            let relative = ObjectStorePath::parse(&file.path)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok(Self::append_relative_path(base_prefix, &relative))
        } else if let Ok(path_url) = Url::parse(&file.path) {
            ObjectStorePath::from_url_path(path_url.path())
                .map_err(|e| DataFusionError::External(Box::new(e)))
        } else {
            ObjectStorePath::parse(&file.path).map_err(|e| DataFusionError::External(Box::new(e)))
        }
    }

    fn append_relative_path(
        base_prefix: &ObjectStorePath,
        relative: &ObjectStorePath,
    ) -> ObjectStorePath {
        relative
            .parts()
            .fold(base_prefix.clone(), |acc, part| acc.child(part))
    }

    fn separate_filters(&self, filters: &[Expr]) -> (Vec<Expr>, Vec<Expr>) {
        let predicates: Vec<&Expr> = filters.iter().collect();
        let pushdown_kinds = get_pushdown_filters(&predicates, &[]);
        let mut pruning_filters = Vec::new();
        let mut parquet_pushdown_filters = Vec::new();
        for (filter, kind) in filters.iter().zip(pushdown_kinds) {
            match kind {
                TableProviderFilterPushDown::Exact => {
                    pruning_filters.push(filter.clone());
                }
                TableProviderFilterPushDown::Inexact => {
                    pruning_filters.push(filter.clone());
                    parquet_pushdown_filters.push(filter.clone());
                }
                TableProviderFilterPushDown::Unsupported => {}
            }
        }
        (pruning_filters, parquet_pushdown_filters)
    }

    // TODO: Add extraction of stats-based filters for column statistics pushdown.

    fn extract_partition_filters(
        filters: &[Expr],
        columns: &[ColumnInfo],
        partition_fields: &[PartitionFieldInfo],
    ) -> (Vec<PartitionFilter>, Vec<Expr>) {
        let mut name_to_partition_key: HashMap<String, u64> = HashMap::new();
        for field in partition_fields {
            if let Some(col) = columns.iter().find(|c| c.column_id == field.column_id) {
                if field.transform.trim().eq_ignore_ascii_case("identity") {
                    name_to_partition_key
                        .insert(col.column_name.clone(), field.partition_key_index);
                }
            }
        }

        let mut partition_values: HashMap<u64, Vec<String>> = HashMap::new();
        let mut remaining_filters = Vec::new();

        for expr in filters.iter().cloned() {
            if let Some((col_name, values)) = Self::extract_values_from_expr(&expr) {
                if let Some(partition_key_index) = name_to_partition_key.get(&col_name).copied() {
                    let entry = partition_values.entry(partition_key_index).or_default();
                    for v in values {
                        if !entry.contains(&v) {
                            entry.push(v);
                        }
                    }
                    remaining_filters.push(expr);
                    continue;
                }
            }
            remaining_filters.push(expr);
        }

        let mut out_filters = Vec::new();
        for (partition_key_index, values) in partition_values {
            if !values.is_empty() {
                out_filters.push(PartitionFilter {
                    partition_key_index,
                    values,
                });
            }
        }

        (out_filters, remaining_filters)
    }

    fn extract_values_from_expr(expr: &Expr) -> Option<(String, Vec<String>)> {
        match expr {
            Expr::BinaryExpr(be) => {
                use datafusion::logical_expr::Operator;
                match be.op {
                    Operator::Eq => {
                        if let (Some(col), Some(value)) = (
                            Self::column_name(&be.left),
                            Self::literal_to_string(&be.right),
                        ) {
                            return Some((col, vec![value]));
                        }
                        if let (Some(col), Some(value)) = (
                            Self::column_name(&be.right),
                            Self::literal_to_string(&be.left),
                        ) {
                            return Some((col, vec![value]));
                        }
                        None
                    }
                    _ => None,
                }
            }
            Expr::InList(in_list) if !in_list.negated => {
                if let Some(col) = Self::column_name(&in_list.expr) {
                    let mut values = Vec::new();
                    for v in &in_list.list {
                        if let Some(s) = Self::literal_to_string(v) {
                            values.push(s);
                        } else {
                            return None;
                        }
                    }
                    if values.is_empty() {
                        None
                    } else {
                        Some((col, values))
                    }
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn column_name(expr: &Expr) -> Option<String> {
        if let Expr::Column(c) = expr {
            Some(c.name.clone())
        } else {
            None
        }
    }

    fn literal_to_string(expr: &Expr) -> Option<String> {
        if let Expr::Literal(value, _) = expr {
            if value.is_null() {
                return None;
            }
            let s = value.to_string();
            if let Some(stripped) = s
                .strip_prefix('\'')
                .and_then(|rest| rest.strip_suffix('\''))
            {
                Some(stripped.to_string())
            } else {
                Some(s)
            }
        } else {
            None
        }
    }

    fn aggregate_statistics(
        &self,
        schema: &datafusion::arrow::datatypes::Schema,
        files: &[FileInfo],
    ) -> Statistics {
        let column_statistics = (0..schema.fields().len())
            .map(|_| ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Absent,
                min_value: Precision::Absent,
                distinct_count: Precision::Absent,
                sum_value: Precision::Absent,
            })
            .collect();
        let total_rows = files
            .iter()
            .fold(0u64, |acc, file| acc.saturating_add(file.record_count));
        let total_bytes = files
            .iter()
            .fold(0u64, |acc, file| acc.saturating_add(file.file_size_bytes));
        let num_rows = total_rows.min(usize::MAX as u64) as usize;
        let total_byte_size = total_bytes.min(usize::MAX as u64) as usize;
        Statistics {
            num_rows: Precision::Exact(num_rows),
            total_byte_size: Precision::Exact(total_byte_size),
            column_statistics,
        }
    }
}
