use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::catalog::Session;
use datafusion::common::{Result, ToDFSchema};
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use delta_kernel::table_features::ColumnMappingMode;
use sail_common_datafusion::rename::physical_plan::rename_projected_physical_plan;

use crate::datasource::scan::{build_file_scan_config, FileScanParams};
use crate::datasource::{
    df_logical_schema, prune_files, simplify_expr, DataFusionMixins, DeltaScanConfig,
};
use crate::kernel::models::Add;
use crate::schema::get_physical_schema;
use crate::storage::LogStoreRef;
use crate::table::DeltaTableState;

pub(crate) async fn plan_delta_scan(
    session: &dyn Session,
    snapshot: &DeltaTableState,
    log_store: &LogStoreRef,
    config: &DeltaScanConfig,
    files: Option<Arc<Vec<Add>>>,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let config = config.clone();

    let schema = match config.schema.clone() {
        Some(value) => Ok(value),
        // Change from `arrow_schema` to input_schema for Spark compatibility
        None => snapshot.input_schema(),
    }?;

    let full_logical_schema = df_logical_schema(
        snapshot,
        &config.file_column_name,
        &config.commit_version_column_name,
        &config.commit_timestamp_column_name,
        Some(schema.clone()),
    )?;

    let logical_schema = if let Some(used_columns) = projection {
        let mut fields = vec![];
        for idx in used_columns {
            fields.push(full_logical_schema.field(*idx).to_owned());
        }
        // partition filters with Exact pushdown were removed from projection by DF optimizer,
        // we need to add them back for the predicate pruning to work
        let filter_expr = conjunction(filters.iter().cloned());
        if let Some(expr) = &filter_expr {
            for c in expr.column_refs() {
                let idx = full_logical_schema.index_of(c.name.as_str())?;
                if !used_columns.contains(&idx) {
                    fields.push(full_logical_schema.field(idx).to_owned());
                }
            }
        }
        // Ensure all partition columns are included in logical schema
        let table_partition_cols = snapshot.metadata().partition_columns();
        for partition_col in table_partition_cols.iter() {
            if let Ok(idx) = full_logical_schema.index_of(partition_col.as_str()) {
                if !used_columns.contains(&idx) && !fields.iter().any(|f| f.name() == partition_col)
                {
                    fields.push(full_logical_schema.field(idx).to_owned());
                }
            }
        }
        Arc::new(ArrowSchema::new(fields))
    } else {
        Arc::clone(&full_logical_schema)
    };

    // Separate filters for pruning vs pushdown.
    //
    // Exact and Inexact filters are used for pruning; Inexact are additionally pushed down.
    let partition_cols = snapshot.metadata().partition_columns();
    let predicates: Vec<&Expr> = filters.iter().collect();
    let pushdown_filters =
        crate::datasource::get_pushdown_filters(&predicates, partition_cols.as_slice());

    let mut pruning_filters = Vec::new();
    let mut parquet_pushdown_filters = Vec::new();
    for (filter, pushdown) in filters.iter().zip(pushdown_filters) {
        match pushdown {
            datafusion::logical_expr::TableProviderFilterPushDown::Exact => {
                pruning_filters.push(filter.clone());
            }
            datafusion::logical_expr::TableProviderFilterPushDown::Inexact => {
                pruning_filters.push(filter.clone());
                parquet_pushdown_filters.push(filter.clone());
            }
            datafusion::logical_expr::TableProviderFilterPushDown::Unsupported => {}
        }
    }

    let (files, pruning_mask) = match files {
        Some(files) => (files, None),
        None => {
            let result = prune_files(
                snapshot,
                log_store,
                session,
                &pruning_filters,
                limit,
                logical_schema.clone(),
            )
            .await?;
            (Arc::new(result.files), result.pruning_mask)
        }
    };

    // Prepare pushdown filter for Parquet.
    let pushdown_filter = if !parquet_pushdown_filters.is_empty() {
        let df_schema = logical_schema.clone().to_dfschema()?;
        let pushdown_expr = conjunction(parquet_pushdown_filters);
        pushdown_expr
            .map(|expr| simplify_expr(session, &df_schema, expr))
            .transpose()?
    } else {
        None
    };

    // Build physical file schema (non-partition columns)
    let table_partition_cols = snapshot.metadata().partition_columns();
    let kmode: ColumnMappingMode = snapshot.effective_column_mapping_mode();
    let kschema_arc = snapshot.snapshot().table_configuration().schema();
    let physical_arrow: ArrowSchema = get_physical_schema(&kschema_arc, kmode);
    let physical_partition_cols: HashSet<String> = table_partition_cols
        .iter()
        .map(|col| {
            kschema_arc
                .field(col)
                .map(|f| f.physical_name(kmode).to_string())
                .unwrap_or_else(|| col.clone())
        })
        .collect();

    let file_fields = physical_arrow
        .fields()
        .iter()
        .filter(|f| !physical_partition_cols.contains(f.name()))
        .cloned()
        .collect::<Vec<_>>();
    let file_schema = Arc::new(ArrowSchema::new(file_fields));

    let file_scan_config = build_file_scan_config(
        snapshot,
        log_store,
        &files,
        &config,
        FileScanParams {
            pruning_mask: pruning_mask.as_deref(),
            projection,
            limit,
            pushdown_filter,
            sort_order: None,
        },
        session,
        file_schema,
    )?;

    let scan_exec = DataSourceExec::from_data_source(file_scan_config);

    // Rename columns from physical back to logical names expected by `schema`
    let logical_names = full_logical_schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect::<Vec<_>>();
    let renamed = rename_projected_physical_plan(scan_exec, &logical_names, projection)?;
    Ok(renamed)
}
