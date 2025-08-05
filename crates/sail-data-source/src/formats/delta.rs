use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::sink::DataSinkExec;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_expr_common::physical_expr::{fmt_sql, PhysicalExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::sqlparser::ast::escape_quoted_string;
use datafusion_common::{not_impl_err, plan_err, DataFusionError, Result, ScalarValue, ToDFSchema};
use deltalake::kernel::{Action, Remove};
use deltalake::protocol::{DeltaOperation, SaveMode};
use sail_common_datafusion::datasource::{PhysicalSinkMode, SinkInfo, SourceInfo, TableFormat};
use sail_delta_lake::create_delta_provider;
use sail_delta_lake::delta_datafusion::{parse_predicate_expression, DataFusionMixins};
use sail_delta_lake::delta_format::DeltaDataSink;
use sail_delta_lake::operations::write::execution::{prepare_predicate_actions, WriterStatsConfig};
use sail_delta_lake::table::open_table_with_object_store;
use url::Url;
use uuid::Uuid;

use crate::options::DataSourceOptionsResolver;

#[derive(Debug, Default)]
pub struct DeltaTableFormat;

#[async_trait]
impl TableFormat for DeltaTableFormat {
    fn name(&self) -> &str {
        "delta"
    }

    async fn create_provider(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>> {
        let SourceInfo {
            paths,
            schema,
            constraints: _,
            partition_by: _,
            bucket_by: _,
            sort_order: _,
            options,
        } = info;
        let table_url = Self::parse_table_url(ctx, paths).await?;
        create_delta_provider(ctx, table_url, schema, &options).await
    }

    async fn create_writer(
        &self,
        ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let SinkInfo {
            input,
            path,
            mode,
            partition_by,
            bucket_by,
            sort_order,
            options,
        } = info;

        if bucket_by.is_some() {
            return not_impl_err!("bucketing for Delta format");
        }

        let table_url = Self::parse_table_url(ctx, vec![path]).await?;
        let resolver = DataSourceOptionsResolver::new(ctx);
        let delta_options = resolver.resolve_delta_write_options(options)?;

        let mut initial_actions: Vec<Action> = Vec::new();
        let mut operation: Option<DeltaOperation> = None;
        let mut predicate_str: Option<String> = None;

        let object_store = ctx
            .runtime_env()
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let table_result =
            open_table_with_object_store(table_url.clone(), object_store, Default::default()).await;

        let table_exists = table_result.is_ok();
        #[allow(clippy::unwrap_used)]
        let table = if table_exists {
            Some(table_result.unwrap())
        } else {
            None
        };

        match mode {
            PhysicalSinkMode::Append => {
                operation = Some(DeltaOperation::Write {
                    mode: SaveMode::Append,
                    partition_by: if partition_by.is_empty() {
                        None
                    } else {
                        Some(partition_by.clone())
                    },
                    predicate: None,
                });
            }
            PhysicalSinkMode::Overwrite => {
                if let Some(table) = &table {
                    if let Some(replace_where) = delta_options.replace_where.clone() {
                        let snapshot = table
                            .snapshot()
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        let df_schema = snapshot
                            .arrow_schema()
                            .map_err(|e| DataFusionError::External(Box::new(e)))?
                            .to_dfschema()?;
                        let session_state = SessionStateBuilder::new()
                            .with_runtime_env(ctx.runtime_env().clone())
                            .build();
                        let predicate_expr =
                            parse_predicate_expression(&df_schema, &replace_where, &session_state)
                                .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        predicate_str = Some(replace_where);

                        #[allow(clippy::unwrap_used)]
                        let (remove_actions, _) = prepare_predicate_actions(
                            predicate_expr,
                            table.log_store(),
                            snapshot,
                            session_state,
                            partition_by.clone(),
                            None,
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as i64,
                            WriterStatsConfig::new(32, None),
                            Uuid::new_v4(),
                        )
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        initial_actions.extend(remove_actions);
                    } else {
                        // Full overwrite
                        let snapshot = table
                            .snapshot()
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        let remove_actions: Vec<Action> = snapshot
                            .file_actions()
                            .map_err(|e| DataFusionError::External(Box::new(e)))?
                            .into_iter()
                            .map(|add| {
                                #[allow(clippy::unwrap_used)]
                                Action::Remove(Remove {
                                    path: add.path.clone(),
                                    deletion_timestamp: Some(
                                        SystemTime::now()
                                            .duration_since(UNIX_EPOCH)
                                            .unwrap()
                                            .as_millis()
                                            as i64,
                                    ),
                                    data_change: true,
                                    ..Default::default()
                                })
                            })
                            .collect();
                        initial_actions.extend(remove_actions);
                    }
                }
                operation = Some(DeltaOperation::Write {
                    mode: SaveMode::Overwrite,
                    partition_by: if partition_by.is_empty() {
                        None
                    } else {
                        Some(partition_by.clone())
                    },
                    predicate: predicate_str.clone(),
                });
            }
            PhysicalSinkMode::OverwriteIf { condition } => {
                // V2 Overwrite with condition
                if let Some(table) = &table {
                    let snapshot = table
                        .snapshot()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    let df_schema = snapshot
                        .arrow_schema()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?
                        .to_dfschema()?;
                    let session_state = SessionStateBuilder::new()
                        .with_runtime_env(ctx.runtime_env().clone())
                        .build();

                    // Convert PhysicalExpr to sql string
                    let condition_str = physical_expr_to_sql_string(condition.as_ref());
                    let logical_expr =
                        parse_predicate_expression(&df_schema, &condition_str, &session_state)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    predicate_str = Some(condition_str);

                    #[allow(clippy::unwrap_used)]
                    let (remove_actions, _) = prepare_predicate_actions(
                        logical_expr,
                        table.log_store(),
                        snapshot,
                        session_state,
                        partition_by.clone(),
                        None,
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as i64,
                        WriterStatsConfig::new(32, None),
                        Uuid::new_v4(),
                    )
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    initial_actions.extend(remove_actions);
                }
                operation = Some(DeltaOperation::Write {
                    mode: SaveMode::Overwrite,
                    partition_by: if partition_by.is_empty() {
                        None
                    } else {
                        Some(partition_by.clone())
                    },
                    predicate: predicate_str.clone(),
                });
            }
            PhysicalSinkMode::ErrorIfExists => {
                if table_exists {
                    return plan_err!("Delta table already exists at path: {table_url}");
                }
                // Operation will be Create for new table inside sink
            }
            PhysicalSinkMode::IgnoreIfExists => {
                if table_exists {
                    // If table exists, do nothing. We can return an empty plan.
                    return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                        input.schema(),
                    )));
                }
                // Operation will be Create for new table inside sink
            }
            PhysicalSinkMode::OverwritePartitions => {
                return not_impl_err!("unsupported sink mode for Delta: {mode:?}")
            }
        };

        let sink = Arc::new(DeltaDataSink::new(
            table_url,
            delta_options,
            input.schema(),
            partition_by,
            initial_actions,
            operation,
            table_exists,
        ));

        Ok(Arc::new(DataSinkExec::new(input, sink, sort_order)))
    }
}

impl DeltaTableFormat {
    async fn parse_table_url(ctx: &dyn Session, paths: Vec<String>) -> Result<Url> {
        let mut urls = crate::url::resolve_listing_urls(ctx, paths.clone()).await?;
        match (urls.pop(), urls.is_empty()) {
            (Some(path), true) => Ok(<ListingTableUrl as AsRef<Url>>::as_ref(&path).clone()),
            _ => plan_err!("expected a single path for Delta table sink: {paths:?}"),
        }
    }
}

/// Convert a PhysicalExpr to a properly formatted SQL string
/// This function handles string literals correctly by adding quotes
fn physical_expr_to_sql_string(expr: &dyn PhysicalExpr) -> String {
    // Try to downcast to a BinaryExpr first
    if let Some(binary) = expr
        .as_any()
        .downcast_ref::<datafusion::physical_plan::expressions::BinaryExpr>()
    {
        let left = physical_expr_to_sql_string(binary.left().as_ref());
        let right = physical_expr_to_sql_string(binary.right().as_ref());
        let op = binary.op();
        return format!("{left} {op} {right}");
    }

    // Try to downcast to a Literal expression
    if let Some(literal) = expr
        .as_any()
        .downcast_ref::<datafusion::physical_plan::expressions::Literal>()
    {
        // Handle string literals specially by adding quotes
        match literal.value() {
            ScalarValue::Utf8(Some(s))
            | ScalarValue::LargeUtf8(Some(s))
            | ScalarValue::Utf8View(Some(s)) => {
                format!("'{}'", escape_quoted_string(s, '\''))
            }
            _ => literal.value().to_string(),
        }
    } else if let Some(column) = expr
        .as_any()
        .downcast_ref::<datafusion::physical_plan::expressions::Column>()
    {
        column.name().to_string()
    } else {
        // For other expressions, use the default fmt_sql
        fmt_sql(expr).to_string()
    }
}
