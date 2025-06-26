use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef,
};
use datafusion::catalog::Session;
use datafusion::datasource::physical_plan::{
    wrap_partition_type_in_dict, FileGroup, FileScanConfig, FileScanConfigBuilder, ParquetSource,
};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::not_impl_err;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, ToDFSchema};
use deltalake::arrow::error::ArrowError;
use deltalake::kernel::StructType;
use deltalake::{DeltaTable, DeltaTableError};
use sail_plan::error::{PlanError, PlanResult};

use crate::scan::partitioned_file_from_action;

#[derive(Debug)]
pub struct DeltaTableProvider {
    table: DeltaTable,
    schema: SchemaRef,
}

fn to_plan_err(e: DeltaTableError) -> PlanError {
    match e {
        DeltaTableError::Arrow { source } => PlanError::ArrowError(source),
        _ => PlanError::invalid(e.to_string()),
    }
}

impl DeltaTableProvider {
    pub fn new(table: DeltaTable) -> PlanResult<Self> {
        let schema = Self::create_arrow_schema(&table)?;
        Ok(Self { table, schema })
    }

    /// Replicate the schema conversion logic from `delta-rs`.
    /// This function converts the DeltaTable's schema into an Arrow schema, correctly handling
    /// partition columns by placing them at the end.
    fn create_arrow_schema(table: &DeltaTable) -> PlanResult<SchemaRef> {
        let metadata = table.metadata().map_err(to_plan_err)?;
        let schema = metadata.schema().map_err(to_plan_err)?;
        let partition_columns = &metadata.partition_columns;

        // Non-partitioned columns
        let mut fields: Vec<Arc<ArrowField>> = schema
            .get_fields()
            .iter()
            .filter(|f| !partition_columns.contains(f.get_name()))
            .map(|f| f.try_into().map(Arc::new))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| PlanError::ArrowError(e))?;

        // Partitioned columns (at the end)
        for part_col in partition_columns {
            let field = schema
                .get_field(part_col)
                .map_err(|e| PlanError::invalid(e.to_string()))?;
            let mut arrow_field: ArrowField = field
                .try_into()
                .map_err(|e: ArrowError| PlanError::ArrowError(e))?;

            // DataFusion dictionary-encodes partition columns to save memory.
            let dict_type = wrap_partition_type_in_dict(arrow_field.data_type().clone());
            arrow_field.set_data_type(dict_type);
            fields.push(Arc::new(arrow_field));
        }

        Ok(Arc::new(ArrowSchema::new(fields)))
    }
}

#[async_trait]
impl TableProvider for DeltaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let snapshot = self.table.snapshot().map_err(to_plan_err)?;

        // 1. Get the list of files
        let files = if let Some(predicate) = conjunction(_filters.iter().cloned()) {
            let df_schema = self.schema().to_dfschema()?;
            let pruning_predicate = PruningPredicate::try_new(&predicate, self.schema.clone())?;

            let files_to_prune = pruning_predicate.prune(snapshot)?;
            snapshot
                .file_actions()
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?
                .into_iter()
                .zip(files_to_prune.into_iter())
                .filter_map(|(action, keep)| if keep { Some(action) } else { None })
                .collect()
        } else {
            snapshot
                .file_actions()
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?
        };

        // 2. Group files by partition
        let mut file_groups: HashMap<Vec<datafusion::common::ScalarValue>, Vec<_>> = HashMap::new();
        let partition_cols = &snapshot.metadata().partition_columns;

        for action in files {
            let part = partitioned_file_from_action(&action, partition_cols, self.schema.as_ref())
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
            file_groups
                .entry(part.partition_values.clone())
                .or_default()
                .push(part);
        }

        // 3. Create a FileScanConfig
        let file_schema = Arc::new(ArrowSchema::new(
            self.schema
                .fields()
                .iter()
                .filter(|f| !partition_cols.contains(f.name()))
                .cloned()
                .collect::<Vec<_>>(),
        ));

        let table_partition_cols = self
            .schema
            .fields()
            .iter()
            .filter(|f| partition_cols.contains(f.name()))
            .cloned()
            .collect::<Vec<_>>();

        let scan_config = FileScanConfigBuilder::new(
            self.table.object_store().get_url(),
            file_schema,
            Arc::new(ParquetSource::default()),
        )
        .with_file_groups(file_groups.into_values().map(FileGroup::from).collect())
        .with_projection(_projection.cloned())
        .with_limit(_limit)
        .with_table_partition_cols(table_partition_cols)
        .build();

        Ok(Arc::new(scan_config.create_physical_plan()?))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let partition_cols = &self.table.metadata().unwrap().partition_columns;
        let partition_cols: std::collections::HashSet<&String> = partition_cols.iter().collect();

        Ok(filters
            .iter()
            .map(|filter| {
                let mut cols = std::collections::HashSet::new();
                if datafusion_common::utils::expr_collect_columns(filter, &mut cols).is_err() {
                    return TableProviderFilterPushDown::Unsupported;
                }

                if cols.iter().all(|c| partition_cols.contains(&c.name)) {
                    // All columns in the filter are partition columns. We can evaluate this filter
                    // exactly at the file level.
                    TableProviderFilterPushDown::Exact
                } else if cols.iter().any(|c| partition_cols.contains(&c.name)) {
                    // Some columns are partition columns, some are not. We can do file pruning, but
                    // the filter must also be evaluated at the row level after scanning.
                    TableProviderFilterPushDown::Inexact
                } else {
                    // No partition columns are involved. The filter is not useful for file pruning.
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}
