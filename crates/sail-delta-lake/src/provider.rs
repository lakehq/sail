use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::TableProvider;
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::config::TableParquetOptions;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::Result as DataFusionResult;
use datafusion_execution::object_store::ObjectStoreUrl;
use deltalake::arrow::error::ArrowError;
use deltalake::{DeltaTable, DeltaTableError};

use crate::error::DeltaResult;
use crate::scan::partitioned_file_from_action;

#[derive(Debug)]
pub struct DeltaTableProvider {
    table: DeltaTable,
    schema: SchemaRef,
}

impl DeltaTableProvider {
    pub fn new(table: DeltaTable) -> DeltaResult<Self> {
        let schema = Self::create_arrow_schema(&table)?;
        Ok(Self { table, schema })
    }

    /// Replicate the schema conversion logic from `delta-rs`.
    /// This function converts the DeltaTable's schema into an Arrow schema, correctly handling
    /// partition columns by placing them at the end.
    fn create_arrow_schema(table: &DeltaTable) -> DeltaResult<SchemaRef> {
        let metadata = table.metadata()?;
        let schema = metadata.schema()?;
        let partition_columns = &metadata.partition_columns;

        // Non-partitioned columns
        let mut fields: Vec<Arc<ArrowField>> = schema
            .fields()
            .filter(|f| !partition_columns.contains(f.name()))
            .map(|f| f.try_into().map(Arc::new))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e: ArrowError| DeltaTableError::Arrow { source: e })?;

        // Partitioned columns (at the end)
        for part_col in partition_columns {
            let field = schema
                .fields()
                .find(|f| f.name() == part_col)
                .ok_or_else(|| {
                    DeltaTableError::Generic(format!(
                        "Partition column '{}' not found in schema",
                        part_col
                    ))
                })?;
            let arrow_field: ArrowField = field
                .try_into()
                .map_err(|e: ArrowError| DeltaTableError::Arrow { source: e })?;

            // DataFusion dictionary-encodes partition columns to save memory.
            let dict_type = datafusion::datasource::physical_plan::wrap_partition_type_in_dict(
                arrow_field.data_type().clone(),
            );
            let new_arrow_field =
                ArrowField::new(arrow_field.name(), dict_type, arrow_field.is_nullable());
            fields.push(Arc::new(new_arrow_field));
        }

        Ok(Arc::new(ArrowSchema::new(fields)))
    }

    /// Get the list of partitioned files from the Delta table
    fn get_partitioned_files(&self) -> DeltaResult<Vec<PartitionedFile>> {
        let snapshot = self.table.snapshot()?;
        let partition_columns = self.table.metadata()?.partition_columns.clone();
        let schema = snapshot.schema();

        let mut files = Vec::new();

        // Use the proper iterator over Add actions from delta-rs
        for add_action in snapshot.file_actions()? {
            let partitioned_file =
                partitioned_file_from_action(&add_action, &partition_columns, schema)?;
            files.push(partitioned_file);
        }

        Ok(files)
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
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Apply partition filters first for file pruning
        let partition_columns = match self.table.metadata() {
            Ok(metadata) => metadata.partition_columns.clone(),
            Err(_) => vec![],
        };

        // Separate partition filters from data filters
        let (partition_filters, remaining_filters): (Vec<&Expr>, Vec<&Expr>) = filters
            .iter()
            .partition(|filter| is_exact_predicate_for_partition_cols(&partition_columns, filter));

        // Get the partitioned files from Delta table with partition pruning
        let files = if partition_filters.is_empty() {
            self.get_partitioned_files()
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?
        } else {
            // Apply partition pruning - this would need more complex logic
            // For now, get all files and let DataFusion handle filtering
            self.get_partitioned_files()
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?
        };

        // Parse the table URI to get the object store URL
        let object_store_url = ObjectStoreUrl::parse(self.table.table_uri())
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

        // Create ParquetSource with default options
        let parquet_options = TableParquetOptions::default();
        let source = Arc::new(ParquetSource::new(parquet_options));

        // Create FileScanConfig using the builder with the object store URL
        let mut builder = FileScanConfigBuilder::new(object_store_url, self.schema.clone(), source);

        // Add files to the builder
        for file in files {
            builder = builder.with_file(file);
        }

        // Apply projection if provided
        if let Some(projection) = projection {
            builder = builder.with_projection(Some(projection.clone()));
        }

        // Apply limit if provided
        if let Some(limit) = limit {
            builder = builder.with_limit(Some(limit));
        }

        let config = builder.build();

        // Create DataSourceExec from the config
        let exec = DataSourceExec::from_data_source(config);

        // Apply remaining filters that couldn't be pushed to partition level
        if !remaining_filters.is_empty() {
            use datafusion::physical_expr::create_physical_expr;
            use datafusion::physical_expr::execution_props::ExecutionProps;
            use datafusion::physical_plan::filter::FilterExec;
            use datafusion_common::DFSchema;

            // Combine remaining filters with AND
            let combined_filter = remaining_filters
                .into_iter()
                .cloned()
                .reduce(|acc, filter| {
                    use datafusion::logical_expr::{BinaryExpr, Operator};
                    Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(acc),
                        Operator::And,
                        Box::new(filter),
                    ))
                });

            if let Some(filter_expr) = combined_filter {
                // Convert Arrow schema to DFSchema for create_physical_expr
                let df_schema = DFSchema::try_from(exec.schema().as_ref().clone())?;
                let physical_filter =
                    create_physical_expr(&filter_expr, &df_schema, &ExecutionProps::new())?;

                let filtered_exec = Arc::new(FilterExec::try_new(physical_filter, exec)?);
                Ok(filtered_exec as Arc<dyn ExecutionPlan>)
            } else {
                Ok(exec as Arc<dyn ExecutionPlan>)
            }
        } else {
            Ok(exec as Arc<dyn ExecutionPlan>)
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // Get partition columns from Delta table metadata
        let partition_columns = match self.table.metadata() {
            Ok(metadata) => metadata.partition_columns.clone(),
            Err(_) => {
                return Ok(vec![
                    TableProviderFilterPushDown::Unsupported;
                    filters.len()
                ])
            }
        };

        // Evaluate each filter to determine pushdown capability
        Ok(filters
            .iter()
            .map(|filter| {
                // Check if this filter can be exactly evaluated using partition columns only
                if is_exact_predicate_for_partition_cols(&partition_columns, filter) {
                    // Filter only references partition columns and supported operators
                    // This can be handled exactly during file pruning
                    TableProviderFilterPushDown::Exact
                } else if has_supported_column_refs(filter) {
                    // Filter references some columns but may not be limited to partition columns
                    // Can be pushed down but may not be completely exact (needs additional filtering)
                    TableProviderFilterPushDown::Inexact
                } else {
                    // Filter cannot be pushed down (e.g., complex expressions, unsupported functions)
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

/// Check if a filter expression can be exactly evaluated using only partition columns
/// Based on delta-rs implementation of expr_is_exact_predicate_for_cols
fn is_exact_predicate_for_partition_cols(partition_cols: &[String], expr: &Expr) -> bool {
    use datafusion::logical_expr::{BinaryExpr, Operator};

    let mut is_exact = true;

    let _ = expr.apply(|expr| match expr {
        Expr::Column(col) => {
            // All column references must be partition columns
            is_exact &= partition_cols.contains(&col.name);
            if is_exact {
                Ok(TreeNodeRecursion::Jump) // Skip children, we found a valid column
            } else {
                Ok(TreeNodeRecursion::Stop) // Stop traversal, found invalid column
            }
        }
        Expr::BinaryExpr(BinaryExpr { op, .. }) => {
            // Only support specific binary operators for exact pushdown
            is_exact &= matches!(
                op,
                Operator::And
                    | Operator::Or
                    | Operator::NotEq
                    | Operator::Eq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::Lt
                    | Operator::LtEq
            );
            if is_exact {
                Ok(TreeNodeRecursion::Continue) // Continue checking children
            } else {
                Ok(TreeNodeRecursion::Stop) // Stop traversal, unsupported operator
            }
        }
        // These expression types are supported for exact pushdown
        Expr::Literal(_, _)
        | Expr::Not(_)
        | Expr::IsNotNull(_)
        | Expr::IsNull(_)
        | Expr::Between(_)
        | Expr::InList(_) => Ok(TreeNodeRecursion::Continue),
        // All other expression types are not supported for exact pushdown
        _ => {
            is_exact = false;
            Ok(TreeNodeRecursion::Stop)
        }
    });

    is_exact
}

/// Check if a filter expression has column references that could potentially be pushed down
fn has_supported_column_refs(expr: &Expr) -> bool {
    // If the expression has any column references, it might be pushable as inexact
    !expr.column_refs().is_empty()
}
