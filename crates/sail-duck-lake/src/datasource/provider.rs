use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::common::ToDFSchema;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{DataFusionError, Result as DataFusionResult};

use crate::datasource::arrow::{field_column_id, schema_column_name_by_id};
use crate::datasource::expressions::{get_pushdown_filters, simplify_expr};
use crate::metadata::{DuckLakeMetaStore, DuckLakeTable, ListDataFilesRequest};
use crate::options::DuckLakeOptions;
use crate::physical_plan::{DuckLakeMetadataScanExec, DuckLakePruningExec, DuckLakeScanExec};
use crate::spec::{FieldIndex, PartitionFieldInfo, PartitionFilter};

pub struct DuckLakeTableProvider {
    table: DuckLakeTable,
    schema: ArrowSchemaRef,
    base_path: String,
    metastore_url: String,
    snapshot_id: Option<u64>,
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
        let schema = table.schema.clone();

        log::trace!(
            "Loaded DuckLake table: {}.{} with {} columns",
            table.schema_info.schema_name,
            table.table_info.table_name,
            schema.fields().len()
        );

        Ok(Self {
            table,
            schema,
            base_path: opts.base_path,
            metastore_url: opts.url,
            snapshot_id: opts.snapshot_id,
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
            self.schema.as_ref(),
            &self.table.partition_fields,
        );
        let (pruning_filters, pushdown_filters) = self.separate_filters(&remaining_filters);

        let prune_schema = self.build_prune_schema(projection, &pruning_filters);
        let required_stats = self.collect_required_stat_fields(prune_schema.as_ref());
        let request = self.build_data_file_request(partition_filters, required_stats);
        // Build pruning predicate (for metadata pruning) and pushdown predicate (for Parquet scan).
        let pruning_predicate: Option<Arc<dyn PhysicalExpr>> =
            if let Some(expr) = datafusion::logical_expr::utils::conjunction(pruning_filters) {
                let df_schema = prune_schema.clone().to_dfschema()?;
                Some(session.create_physical_expr(expr, &df_schema)?)
            } else {
                None
            };
        let pushdown_predicate: Option<Arc<dyn PhysicalExpr>> = if !pushdown_filters.is_empty() {
            let df_schema = prune_schema.clone().to_dfschema()?;
            let pushdown_expr = datafusion::logical_expr::utils::conjunction(pushdown_filters);
            pushdown_expr.map(|expr| simplify_expr(session, &df_schema, expr))
        } else {
            None
        };

        // TODO: fetch and apply delete files (row-level deletes) alongside data files
        // TODO: load name mappings (mapping_id) and adapt physical<->logical schema for schema evolution
        // TODO: union inlined_data_tables into the scan (MemoryExec) so unflushed inserts are visible

        // Source: stream metadata from metastore as Arrow batches.
        let metadata_batch_size = 10_000usize;
        let metadata_exec = Arc::new(DuckLakeMetadataScanExec::try_new(
            self.metastore_url.clone(),
            request,
            metadata_batch_size,
        )?);

        // Prune metadata using DataFusion pruning rules.
        let pruning_exec = Arc::new(DuckLakePruningExec::try_new(
            metadata_exec,
            pruning_predicate,
            prune_schema,
            self.table.partition_fields.clone(),
            limit,
        )?);

        // Sink: consume pruned file list and scan Parquet data.
        let scan_exec = Arc::new(DuckLakeScanExec::try_new(
            pruning_exec,
            self.base_path.clone(),
            self.table.schema_info.schema_name.clone(),
            self.table.table_info.table_name.clone(),
            self.schema.clone(),
            projection.cloned(),
            limit,
            pushdown_predicate,
        )?);

        Ok(scan_exec)
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
        for field in schema.fields() {
            if let Some(column_id) = field_column_id(field) {
                name_to_id.insert(field.name().clone(), column_id);
            }
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
        schema: &datafusion::arrow::datatypes::Schema,
        partition_fields: &[PartitionFieldInfo],
    ) -> (Vec<PartitionFilter>, Vec<Expr>) {
        // TODO: support non-identity transforms and richer predicates for partition pruning/pushdown
        let mut name_to_partition_key: HashMap<String, u64> = HashMap::new();
        for field in partition_fields {
            if field.transform.trim().eq_ignore_ascii_case("identity") {
                if let Some(column_name) = schema_column_name_by_id(schema, field.column_id) {
                    name_to_partition_key.insert(column_name, field.partition_key_index);
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

    // NOTE: file scan statistics and file-group construction now happen inside `DuckLakeScanExec`.
}
