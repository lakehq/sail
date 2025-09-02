use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::Session;
use datafusion::common::scalar::ScalarValue;
use datafusion::common::stats::Statistics;
use datafusion::common::{Result, ToDFSchema};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    wrap_partition_type_in_dict, wrap_partition_value_in_dict, FileGroup, FileScanConfigBuilder,
    ParquetSource,
};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::utils::{conjunction, split_conjunction};
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::pruning::PruningStatistics;
use deltalake::errors::DeltaResult;
use deltalake::kernel::Add;
use deltalake::logstore::LogStoreRef;
use futures::TryStreamExt;
use object_store::path::Path;

use crate::delta_datafusion::schema_rewriter::DeltaPhysicalExprAdapterFactory;
use crate::delta_datafusion::{
    create_object_store_url, delta_to_datafusion_error, df_logical_schema, get_pushdown_filters,
    partitioned_file_from_action, DataFusionMixins, DeltaScanConfig, DeltaTableStateExt,
};
use crate::table::DeltaTableState;

// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/delta_datafusion/mod.rs>

/// A Delta table provider that enables additional metadata columns to be included during the scan
#[derive(Debug)]
pub struct DeltaTableProvider {
    snapshot: DeltaTableState,
    log_store: LogStoreRef,
    config: DeltaScanConfig,
    schema: Arc<ArrowSchema>,
    files: Option<Vec<Add>>,
}

impl DeltaTableProvider {
    pub fn try_new(
        snapshot: DeltaTableState,
        log_store: LogStoreRef,
        config: DeltaScanConfig,
    ) -> DeltaResult<Self> {
        Ok(DeltaTableProvider {
            schema: df_logical_schema(&snapshot, &config.file_column_name, config.schema.clone())?,
            snapshot,
            log_store,
            config,
            files: None,
        })
    }

    pub fn with_files(mut self, files: Vec<Add>) -> DeltaTableProvider {
        self.files = Some(files);
        self
    }
}

fn simplify_expr(
    runtime_env: Arc<RuntimeEnv>,
    df_schema: &datafusion::common::DFSchema,
    expr: Expr,
) -> Arc<dyn PhysicalExpr> {
    let props = ExecutionProps::new();
    let simplify_context = SimplifyContext::new(&props).with_schema(df_schema.clone().into());
    let simplifier = ExprSimplifier::new(simplify_context).with_max_cycles(10);
    #[allow(clippy::expect_used)]
    let simplified = simplifier
        .simplify(expr)
        .expect("Failed to simplify expression");

    let session_state = SessionStateBuilder::new()
        .with_runtime_env(runtime_env)
        .build();

    #[allow(clippy::expect_used)]
    session_state
        .create_physical_expr(simplified, df_schema)
        .expect("Failed to create physical expression")
}

#[async_trait]
impl TableProvider for DeltaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filter_expr = conjunction(filters.iter().cloned());
        let config = self.config.clone();

        let schema = match config.schema.clone() {
            Some(value) => Ok(value),
            // Change from `arrow_schema` to input_schema for Spark compatibility
            None => self.snapshot.input_schema(),
        }
        .map_err(delta_to_datafusion_error)?;

        let logical_schema = df_logical_schema(
            &self.snapshot,
            &config.file_column_name,
            Some(schema.clone()),
        )
        .map_err(delta_to_datafusion_error)?;

        let logical_schema = if let Some(used_columns) = projection {
            let mut fields = vec![];
            for idx in used_columns {
                fields.push(logical_schema.field(*idx).to_owned());
            }
            // partition filters with Exact pushdown were removed from projection by DF optimizer,
            // we need to add them back for the predicate pruning to work
            if let Some(expr) = &filter_expr {
                for c in expr.column_refs() {
                    let idx = logical_schema.index_of(c.name.as_str())?;
                    if !used_columns.contains(&idx) {
                        fields.push(logical_schema.field(idx).to_owned());
                    }
                }
            }
            // Ensure all partition columns are included in logical schema
            let table_partition_cols = self.snapshot.metadata().partition_columns();
            for partition_col in table_partition_cols.iter() {
                if let Ok(idx) = logical_schema.index_of(partition_col.as_str()) {
                    if !used_columns.contains(&idx)
                        && !fields.iter().any(|f| f.name() == partition_col)
                    {
                        fields.push(logical_schema.field(idx).to_owned());
                    }
                }
            }
            Arc::new(ArrowSchema::new(fields))
        } else {
            logical_schema
        };

        let df_schema = logical_schema.clone().to_dfschema()?;

        let logical_filter = filter_expr
            .clone()
            .map(|expr| simplify_expr(session.runtime_env().clone(), &df_schema, expr));

        let pushdown_filter = filter_expr
            .clone()
            .and_then(|expr| {
                let predicates = split_conjunction(&expr);
                let pushdown_filters = get_pushdown_filters(
                    &predicates,
                    self.snapshot.metadata().partition_columns().as_slice(),
                );

                let filtered_predicates = predicates.into_iter().zip(pushdown_filters).filter_map(
                    |(filter, pushdown)| {
                        if pushdown == TableProviderFilterPushDown::Inexact {
                            Some((*filter).clone())
                        } else {
                            None
                        }
                    },
                );
                conjunction(filtered_predicates)
            })
            .map(|expr| simplify_expr(session.runtime_env().clone(), &df_schema, expr));

        let table_partition_cols = self.snapshot.metadata().partition_columns();
        let file_schema = Arc::new(ArrowSchema::new(
            schema
                .fields()
                .iter()
                .filter(|f| !table_partition_cols.contains(f.name()))
                .cloned()
                .collect::<Vec<_>>(),
        ));

        let log_data = self.snapshot.snapshot().log_data();

        let (files, pruning_mask) = match &self.files {
            Some(files) => {
                let files = files.to_owned();
                (files, None)
            }
            None => {
                // early return in case we have no push down filters or limit
                if logical_filter.is_none() && limit.is_none() {
                    let files: Vec<Add> = self
                        .snapshot
                        .file_actions_iter(&self.log_store)
                        .try_collect()
                        .await
                        .map_err(delta_to_datafusion_error)?;
                    (files, None)
                } else {
                    let num_containers = log_data.num_containers();

                    let files_to_prune = if let Some(predicate) = &logical_filter {
                        let pruning_predicate =
                            PruningPredicate::try_new(predicate.clone(), logical_schema.clone())?;
                        pruning_predicate.prune(&log_data)?
                    } else {
                        vec![true; num_containers]
                    };

                    // For now, collect all files and apply pruning logic
                    let all_files: Vec<Add> = self
                        .snapshot
                        .file_actions_iter(&self.log_store)
                        .try_collect()
                        .await
                        .map_err(delta_to_datafusion_error)?;

                    // needed to enforce limit and deal with missing statistics
                    let mut pruned_without_stats = vec![];
                    let mut rows_collected = 0;
                    let mut files = vec![];

                    for (action, keep) in all_files.iter().zip(files_to_prune.iter()) {
                        // prune file based on predicate pushdown
                        if *keep {
                            // prune file based on limit pushdown
                            if let Some(limit) = limit {
                                if let Some(stats) = action.get_stats().map_err(|e| {
                                    datafusion::common::DataFusionError::External(Box::new(e))
                                })? {
                                    if rows_collected <= limit as i64 {
                                        rows_collected += stats.num_records;
                                        files.push(action.to_owned());
                                    } else {
                                        break;
                                    }
                                } else {
                                    // some files are missing stats; skipping but storing them
                                    // in a list in case we can't reach the target limit
                                    pruned_without_stats.push(action.to_owned());
                                }
                            } else {
                                files.push(action.to_owned());
                            }
                        }
                    }

                    if let Some(limit) = limit {
                        if rows_collected < limit as i64 {
                            files.extend(pruned_without_stats);
                        }
                    }

                    (files, Some(files_to_prune))
                }
            }
        };

        // Calculate metrics for files scanned and pruned
        let num_containers = if let Some(files) = &self.files {
            files.len()
        } else {
            log_data.num_containers()
        };
        let files_scanned = files.len();
        let _files_pruned = num_containers - files_scanned;

        // TODO we group files together by their partition values. If the table is partitioned
        // we may be able to reduce the number of groups by combining groups with the same partition values
        let mut file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> = HashMap::new();
        let table_partition_cols = &self.snapshot.metadata().partition_columns();

        for action in files.iter() {
            let mut part = partitioned_file_from_action(action, table_partition_cols, &schema);

            if config.file_column_name.is_some() {
                let partition_value = if config.wrap_partition_values {
                    wrap_partition_value_in_dict(ScalarValue::Utf8(Some(action.path.clone())))
                } else {
                    ScalarValue::Utf8(Some(action.path.clone()))
                };
                part.partition_values.push(partition_value);
            }

            file_groups
                .entry(part.partition_values.clone())
                .or_default()
                .push(part);
        }

        // Rewrite the file groups so that the file paths are prepended with
        // the Delta table location.
        file_groups.iter_mut().for_each(|(_, files)| {
            files.iter_mut().for_each(|file| {
                file.object_meta.location = Path::from(format!(
                    "{}{}{}",
                    self.log_store.config().location.path(),
                    object_store::path::DELIMITER,
                    file.object_meta.location
                ));
            });
        });

        let mut table_partition_cols = table_partition_cols
            .iter()
            .map(|col| {
                #[allow(clippy::expect_used)]
                let field = schema
                    .field_with_name(col)
                    .expect("Column should exist in schema");
                let corrected = if config.wrap_partition_values {
                    match field.data_type() {
                        ArrowDataType::Utf8
                        | ArrowDataType::LargeUtf8
                        | ArrowDataType::Binary
                        | ArrowDataType::LargeBinary => {
                            wrap_partition_type_in_dict(field.data_type().clone())
                        }
                        _ => field.data_type().clone(),
                    }
                } else {
                    field.data_type().clone()
                };
                Field::new(col.clone(), corrected, true)
            })
            .collect::<Vec<_>>();

        if let Some(file_column_name) = &config.file_column_name {
            let field_name_datatype = if config.wrap_partition_values {
                wrap_partition_type_in_dict(ArrowDataType::Utf8)
            } else {
                ArrowDataType::Utf8
            };
            table_partition_cols.push(Field::new(
                file_column_name.clone(),
                field_name_datatype,
                false,
            ));
        }

        let stats = self
            .snapshot
            .datafusion_table_statistics(pruning_mask.as_deref())
            .unwrap_or_else(|| Statistics::new_unknown(&schema));

        let parquet_options = TableParquetOptions {
            global: session.config().options().execution.parquet.clone(),
            ..Default::default()
        };

        // Create the base ParquetSource and apply predicate if needed
        let mut parquet_source = ParquetSource::new(parquet_options);

        if let Some(predicate) = pushdown_filter {
            if config.enable_parquet_pushdown {
                parquet_source = parquet_source.with_predicate(predicate);
            }
        }

        let file_source = Arc::new(parquet_source);

        let object_store_url = create_object_store_url(&self.log_store.config().location)
            .map_err(delta_to_datafusion_error)?;
        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url, file_schema, file_source)
                .with_file_groups(
                    // If all files were filtered out, we still need to emit at least one partition to
                    // pass datafusion sanity checks.
                    //
                    // See https://github.com/apache/datafusion/issues/11322
                    if file_groups.is_empty() {
                        vec![FileGroup::from(vec![])]
                    } else {
                        file_groups.into_values().map(FileGroup::from).collect()
                    },
                )
                .with_statistics(stats)
                .with_projection(projection.cloned())
                .with_limit(limit)
                .with_table_partition_cols(table_partition_cols)
                .with_expr_adapter(Some(Arc::new(DeltaPhysicalExprAdapterFactory {})))
                .build();
        let _metrics = ExecutionPlanMetricsSet::new();
        // MetricBuilder::new(&metrics).global_counter("files_scanned").add(files_scanned);
        // MetricBuilder::new(&metrics).global_counter("files_pruned").add(files_pruned);

        // TODO: Properly expose these metrics

        Ok(DataSourceExec::from_data_source(file_scan_config))
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let partition_cols = self.snapshot.metadata().partition_columns().as_slice();
        Ok(get_pushdown_filters(filter, partition_cols))
    }

    fn statistics(&self) -> Option<Statistics> {
        self.snapshot.datafusion_table_statistics(None)
    }
}
