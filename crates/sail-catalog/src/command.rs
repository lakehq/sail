use std::collections::HashSet;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use sail_common_datafusion::array::serde::ArrowSerializer;
use sail_common_datafusion::catalog::{
    CatalogPartitionField, PartitionTransform, TableKind, TableStatus,
};
use sail_common_datafusion::datasource::{
    is_lakehouse_format, is_relative_location, TableFormatRegistry,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use serde::{Deserialize, Serialize};

use crate::error::{CatalogError, CatalogResult};
use crate::manager::tracker::{CatalogFunctionId, CatalogLogicalPlanId};
use crate::manager::CatalogManager;
use crate::metadata_compat::{
    describe_extended_metadata_rows, redact_property_value, show_table_extended_information,
    show_tblproperties_rows,
};
use crate::provider::{
    AlterTableOptions, CreateDatabaseOptions, CreateTableOptions, CreateTemporaryViewOptions,
    CreateViewOptions, DropDatabaseOptions, DropTableOptions, DropTemporaryViewOptions,
    DropViewOptions,
};
use crate::utils::{quote_names_if_needed, quote_namespace_if_needed};

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
pub enum CatalogCommand {
    CurrentCatalog,
    SetCurrentCatalog {
        catalog: String,
    },
    ListCatalogs {
        pattern: Option<String>,
    },
    CurrentDatabase,
    SetCurrentDatabase {
        database: Vec<String>,
    },
    CreateDatabase {
        database: Vec<String>,
        options: CreateDatabaseOptions,
    },
    DatabaseExists {
        database: Vec<String>,
    },
    GetDatabase {
        database: Vec<String>,
    },
    ListDatabases {
        qualifier: Vec<String>,
        pattern: Option<String>,
    },
    DropDatabase {
        database: Vec<String>,
        options: DropDatabaseOptions,
    },
    CreateTable {
        table: Vec<String>,
        options: CreateTableOptions,
    },
    TableExists {
        table: Vec<String>,
    },
    GetTable {
        table: Vec<String>,
    },
    ShowTables {
        database: Vec<String>,
        pattern: Option<String>,
    },
    ShowTableExtended {
        database: Vec<String>,
        pattern: String,
    },
    ShowTableProperties {
        table: Vec<String>,
        property_key: Option<String>,
    },
    ListTables {
        database: Vec<String>,
        pattern: Option<String>,
    },
    ListViews {
        database: Vec<String>,
        pattern: Option<String>,
    },
    DropTable {
        table: Vec<String>,
        options: DropTableOptions,
    },
    AlterTable {
        table: Vec<String>,
        if_exists: bool,
        options: AlterTableOptions,
    },
    ListColumns {
        table: Vec<String>,
    },
    FunctionExists {
        function: Vec<String>,
    },
    GetFunction {
        function: Vec<String>,
    },
    ListFunctions {
        database: Vec<String>,
        pattern: Option<String>,
    },
    DropFunction {
        function: Vec<String>,
        if_exists: bool,
        is_temporary: bool,
    },
    RegisterFunction {
        udf: CatalogFunctionId,
    },
    RegisterTableFunction {
        name: String,
        // We have to be explicit about the UDTF types we support.
        // We cannot use `Arc<dyn TableFunctionImpl>` because it does not implement `Eq` and `Hash`.
        udtf: CatalogTableFunction,
    },
    DropTemporaryView {
        view: String,
        is_global: bool,
        options: DropTemporaryViewOptions,
    },
    DropView {
        view: Vec<String>,
        options: DropViewOptions,
    },
    CreateTemporaryView {
        view: String,
        is_global: bool,
        options: CreateTemporaryViewOptions<CatalogLogicalPlanId>,
    },
    CreateView {
        view: Vec<String>,
        options: CreateViewOptions,
    },
    DescribeTable {
        table: Vec<String>,
        extended: bool,
    },
    DescribeDatabase {
        database: Vec<String>,
        extended: bool,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Serialize, Deserialize)]
pub enum CatalogTableFunction {
    // We do not support any kind of table functions yet.
    // PySpark UDTF is registered as a scalar UDF.
}

impl CatalogCommand {
    pub fn name(&self) -> &str {
        match self {
            CatalogCommand::CurrentCatalog => "CurrentCatalog",
            CatalogCommand::SetCurrentCatalog { .. } => "SetCurrentCatalog",
            CatalogCommand::ListCatalogs { .. } => "ListCatalogs",
            CatalogCommand::CurrentDatabase => "CurrentDatabase",
            CatalogCommand::SetCurrentDatabase { .. } => "SetCurrentDatabase",
            CatalogCommand::CreateDatabase { .. } => "CreateDatabase",
            CatalogCommand::DatabaseExists { .. } => "DatabaseExists",
            CatalogCommand::GetDatabase { .. } => "GetDatabase",
            CatalogCommand::ListDatabases { .. } => "ListDatabases",
            CatalogCommand::DropDatabase { .. } => "DropDatabase",
            CatalogCommand::CreateTable { .. } => "CreateTable",
            CatalogCommand::TableExists { .. } => "TableExists",
            CatalogCommand::GetTable { .. } => "GetTable",
            CatalogCommand::ShowTables { .. } => "ShowTables",
            CatalogCommand::ShowTableExtended { .. } => "ShowTableExtended",
            CatalogCommand::ShowTableProperties { .. } => "ShowTableProperties",
            CatalogCommand::ListTables { .. } => "ListTables",
            CatalogCommand::ListViews { .. } => "ListViews",
            CatalogCommand::DropTable { .. } => "DropTable",
            CatalogCommand::AlterTable { .. } => "AlterTable",
            CatalogCommand::ListColumns { .. } => "ListColumns",
            CatalogCommand::FunctionExists { .. } => "FunctionExists",
            CatalogCommand::GetFunction { .. } => "GetFunction",
            CatalogCommand::ListFunctions { .. } => "ListFunctions",
            CatalogCommand::RegisterFunction { .. } => "RegisterFunction",
            CatalogCommand::RegisterTableFunction { .. } => "RegisterTableFunction",
            CatalogCommand::DropFunction { .. } => "DropFunction",
            CatalogCommand::DropTemporaryView { .. } => "DropTemporaryView",
            CatalogCommand::DropView { .. } => "DropView",
            CatalogCommand::CreateTemporaryView { .. } => "CreateTemporaryView",
            CatalogCommand::CreateView { .. } => "CreateView",
            CatalogCommand::DescribeTable { .. } => "DescribeTable",
            CatalogCommand::DescribeDatabase { .. } => "DescribeDatabase",
        }
    }

    pub fn schema<C: SessionExtensionAccessor>(&self, ctx: &C) -> CatalogResult<SchemaRef> {
        let service = ctx.extension::<PlanService>()?;
        let display = service.catalog_display();
        let schema = match self {
            CatalogCommand::ListCatalogs { .. } => display.catalogs().schema()?,
            CatalogCommand::GetDatabase { .. } | CatalogCommand::ListDatabases { .. } => {
                display.databases().schema()?
            }
            CatalogCommand::GetTable { .. }
            | CatalogCommand::ListTables { .. }
            | CatalogCommand::ListViews { .. } => display.tables().schema()?,
            CatalogCommand::ShowTables { .. } => {
                ArrowSerializer::default().schema::<ShowTablesRow>()?
            }
            CatalogCommand::ShowTableExtended { .. } => {
                ArrowSerializer::default().schema::<ShowTableExtendedRow>()?
            }
            CatalogCommand::ShowTableProperties { .. } => {
                ArrowSerializer::default().schema::<ShowTablePropertyRow>()?
            }
            CatalogCommand::ListColumns { .. } => display.table_columns().schema()?,
            CatalogCommand::GetFunction { .. } | CatalogCommand::ListFunctions { .. } => {
                display.functions().schema()?
            }
            CatalogCommand::SetCurrentCatalog { .. }
            | CatalogCommand::SetCurrentDatabase { .. }
            | CatalogCommand::RegisterFunction { .. }
            | CatalogCommand::RegisterTableFunction { .. } => display.empty().schema()?,
            CatalogCommand::CurrentCatalog | CatalogCommand::CurrentDatabase => {
                display.strings().schema()?
            }
            CatalogCommand::DescribeTable { .. } => {
                ArrowSerializer::default().schema::<DescribeTableRow>()?
            }
            CatalogCommand::DescribeDatabase { .. } => {
                ArrowSerializer::default().schema::<DescribeDatabaseRow>()?
            }
            CatalogCommand::DatabaseExists { .. }
            | CatalogCommand::TableExists { .. }
            | CatalogCommand::FunctionExists { .. }
            | CatalogCommand::CreateDatabase { .. }
            | CatalogCommand::CreateTable { .. }
            | CatalogCommand::CreateTemporaryView { .. }
            | CatalogCommand::CreateView { .. }
            | CatalogCommand::DropDatabase { .. }
            | CatalogCommand::DropTable { .. }
            | CatalogCommand::AlterTable { .. }
            | CatalogCommand::DropFunction { .. }
            | CatalogCommand::DropTemporaryView { .. }
            | CatalogCommand::DropView { .. } => display.bools().schema()?,
        };
        Ok(schema)
    }

    pub async fn execute<C: SessionExtensionAccessor>(
        self,
        ctx: &C,
        manager: &CatalogManager,
    ) -> CatalogResult<RecordBatch> {
        // TODO: make sure we return the same schema as Spark for each command
        let service = ctx.extension::<PlanService>()?;
        let display = service.catalog_display();
        let batch = match self {
            CatalogCommand::CurrentCatalog => {
                let value = manager.default_catalog()?;
                display.strings().to_record_batch(vec![value.to_string()])?
            }
            CatalogCommand::SetCurrentCatalog { catalog } => {
                manager.set_default_catalog(catalog)?;
                display.empty().to_record_batch(vec![])?
            }
            CatalogCommand::ListCatalogs { pattern } => {
                let rows = manager
                    .list_catalogs(pattern.as_deref())?
                    .into_iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>();
                display.catalogs().to_record_batch(rows)?
            }
            CatalogCommand::CurrentDatabase => {
                let value = manager.default_database()?;
                let value = quote_namespace_if_needed(&value);
                display.strings().to_record_batch(vec![value])?
            }
            CatalogCommand::SetCurrentDatabase { database } => {
                manager.set_default_database(database).await?;
                display.empty().to_record_batch(vec![])?
            }
            CatalogCommand::CreateDatabase { database, options } => {
                manager.create_database(&database, options).await?;
                display.bools().to_record_batch(vec![true])?
            }
            CatalogCommand::DatabaseExists { database } => {
                let value = match manager.get_database(&database).await {
                    Ok(_) => true,
                    Err(CatalogError::NotFound(_, _)) => false,
                    Err(e) => return Err(e),
                };
                display.bools().to_record_batch(vec![value])?
            }
            CatalogCommand::GetDatabase { database } => {
                let rows = match manager.get_database(&database).await {
                    Ok(x) => vec![x],
                    Err(CatalogError::NotFound(_, _)) => vec![],
                    Err(e) => return Err(e),
                };
                display.databases().to_record_batch(rows)?
            }
            CatalogCommand::ListDatabases { qualifier, pattern } => {
                let rows = match manager.list_databases(&qualifier, pattern.as_deref()).await {
                    Ok(rows) => rows,
                    Err(CatalogError::NotFound(_, _)) => vec![],
                    Err(e) => return Err(e),
                };
                display.databases().to_record_batch(rows)?
            }
            CatalogCommand::DropDatabase { database, options } => {
                manager.drop_database(&database, options).await?;
                display.bools().to_record_batch(vec![true])?
            }
            CatalogCommand::CreateTable { table, options } => {
                manager.create_table(&table, options).await?;
                display.bools().to_record_batch(vec![true])?
            }
            CatalogCommand::TableExists { table } => {
                let value = match manager.get_table_or_view(&table).await {
                    Ok(_) => true,
                    Err(CatalogError::NotFound(_, _)) => false,
                    Err(e) => return Err(e),
                };
                display.bools().to_record_batch(vec![value])?
            }
            CatalogCommand::GetTable { table } => {
                // We are supposed to return an error if the table or view does not exist.
                let table = manager.get_table_or_view(&table).await?;
                display.tables().to_record_batch(vec![table])?
            }
            CatalogCommand::ShowTables { database, pattern } => {
                let rows = manager
                    .list_tables_and_views(&database, pattern.as_deref())
                    .await?;
                let rows = rows
                    .into_iter()
                    .map(|row| ShowTablesRow {
                        database: quote_names_if_needed(&row.database),
                        table_name: row.name,
                        is_temporary: row.kind.is_temporary(),
                    })
                    .collect::<Vec<_>>();
                ArrowSerializer::default().build_record_batch(&rows)?
            }
            CatalogCommand::ShowTableExtended { database, pattern } => {
                let rows = manager
                    .list_tables_and_views(&database, Some(pattern.as_str()))
                    .await?;
                let formatter = service.plan_formatter();
                let hydrated = futures::future::try_join_all(
                    rows.iter().map(|row| hydrate_table_status(ctx, row)),
                )
                .await?;
                let mut output = Vec::with_capacity(hydrated.len());
                for row in hydrated {
                    output.push(ShowTableExtendedRow {
                        database: quote_names_if_needed(&row.database),
                        table_name: row.name.clone(),
                        is_temporary: row.kind.is_temporary(),
                        information: show_table_extended_information(&row, formatter)?,
                    });
                }
                ArrowSerializer::default().build_record_batch(&output)?
            }
            CatalogCommand::ShowTableProperties {
                table,
                property_key,
            } => {
                let table_status = manager.get_table_or_view(&table).await?;
                let table_status = hydrate_table_status(ctx, &table_status).await?;
                if table_status.kind.is_temporary() {
                    let rows: Vec<ShowTablePropertyRow> = Vec::new();
                    ArrowSerializer::default().build_record_batch(&rows)?
                } else {
                    let table_name = qualified_table_name(&table_status);
                    let rows = show_tblproperties_rows(
                        table_name.as_str(),
                        table_status.kind.properties(),
                        property_key.as_deref(),
                    );
                    let pairs = rows
                        .into_iter()
                        .map(|(key, value)| ShowTablePropertyRow { key, value })
                        .collect::<Vec<_>>();
                    ArrowSerializer::default().build_record_batch(&pairs)?
                }
            }
            CatalogCommand::ListTables { database, pattern } => {
                let rows = manager
                    .list_tables_and_views(&database, pattern.as_deref())
                    .await?;
                display.tables().to_record_batch(rows)?
            }
            CatalogCommand::ListViews { database, pattern } => {
                let rows = manager
                    .list_views_and_temporary_views(&database, pattern.as_deref())
                    .await?;
                display.tables().to_record_batch(rows)?
            }
            CatalogCommand::DropTable { table, options } => {
                manager.drop_table(&table, options).await?;
                display.bools().to_record_batch(vec![true])?
            }
            CatalogCommand::AlterTable {
                table,
                if_exists,
                options,
            } => {
                // Fetch the table status before altering so we can propagate
                // property changes to the underlying storage (e.g. Delta log).
                let table_status = match manager.get_table_or_view(&table).await {
                    Ok(status) => status,
                    Err(CatalogError::NotFound(_, _)) if if_exists => {
                        return Ok(display.bools().to_record_batch(vec![true])?);
                    }
                    Err(e) => return Err(e),
                };

                let (location, format) = match &table_status.kind {
                    sail_common_datafusion::catalog::TableKind::Table {
                        location: Some(loc),
                        format,
                        ..
                    } => (Some(loc.clone()), Some(format.clone())),
                    sail_common_datafusion::catalog::TableKind::Table { format, .. } => {
                        (None, Some(format.clone()))
                    }
                    _ => (None, None),
                };

                let is_lakehouse = format.as_deref().is_some_and(is_lakehouse_format);
                let is_property_alter = matches!(
                    options,
                    AlterTableOptions::SetTableProperties { .. }
                        | AlterTableOptions::UnsetTableProperties { .. }
                );

                if is_lakehouse {
                    let format_name = format.as_deref().ok_or_else(|| {
                        CatalogError::External(
                            "missing table format for storage-backed ALTER TABLE".to_string(),
                        )
                    })?;
                    if location.is_none() {
                        return Err(CatalogError::External(format!(
                            "missing table location for storage-backed ALTER TABLE on format '{format_name}'"
                        )));
                    }
                    if let (Some(location), Some(format)) = (location, format) {
                        // Persist changes to storage first (source of truth for lakehouse
                        // formats). Only attempt catalog metadata update after storage succeeds.
                        let registry = ctx.extension::<TableFormatRegistry>().map_err(|e| {
                            CatalogError::External(format!(
                                "missing TableFormatRegistry for storage-backed ALTER TABLE on format '{format}': {e}"
                            ))
                        })?;
                        let table_format = registry.get(&format).map_err(|e| {
                            CatalogError::External(format!(
                                "unknown table format '{format}' for storage-backed ALTER TABLE: {e}"
                            ))
                        })?;
                        let runtime = ctx.runtime_env();
                        match &options {
                            AlterTableOptions::SetTableProperties { properties } => {
                                let changes = properties
                                    .iter()
                                    .map(|(k, v)| (k.clone(), Some(v.clone())))
                                    .collect::<Vec<_>>();
                                table_format
                                    .alter_table_properties(runtime, &location, changes, false)
                                    .await
                                    .map_err(|e| CatalogError::External(e.to_string()))?;
                            }
                            AlterTableOptions::UnsetTableProperties { keys, if_exists } => {
                                let changes =
                                    keys.iter().map(|k| (k.clone(), None)).collect::<Vec<_>>();
                                table_format
                                    .alter_table_properties(runtime, &location, changes, *if_exists)
                                    .await
                                    .map_err(|e| CatalogError::External(e.to_string()))?;
                            }
                            AlterTableOptions::AlterColumnType { name, data_type } => {
                                table_format
                                    .alter_table_column_type(
                                        runtime,
                                        &location,
                                        name.clone(),
                                        data_type.clone(),
                                    )
                                    .await
                                    .map_err(|e| CatalogError::External(e.to_string()))?;
                            }
                        }
                    }
                    if is_property_alter {
                        // Spark-compatible behavior for property updates: after storage commit,
                        // treat catalog metadata refresh as best-effort.
                        if let Err(error) = manager.alter_table(&table, options).await {
                            log::warn!(
                                "best-effort catalog update failed after storage-backed ALTER TABLE for {:?}: {}",
                                table,
                                error
                            );
                        }
                    } else {
                        // Keep strict behavior for non-property ALTER operations.
                        manager.alter_table(&table, options).await?;
                    }
                } else {
                    manager.alter_table(&table, options).await?;
                }
                display.bools().to_record_batch(vec![true])?
            }
            CatalogCommand::ListColumns { table } => {
                let table_status = manager.get_table_or_view(&table).await?;
                let table_status = hydrate_table_status(ctx, &table_status).await?;
                let rows = table_status.kind.columns();
                display.table_columns().to_record_batch(rows)?
            }
            CatalogCommand::DescribeTable { table, extended } => {
                let table_status = manager.get_table_or_view(&table).await?;
                let table_status = hydrate_table_status(ctx, &table_status).await?;
                let formatter = service.plan_formatter();
                let serializer = ArrowSerializer::default();

                let mut rows: Vec<DescribeTableRow> = Vec::new();

                for col in &table_status.kind.columns() {
                    rows.push(DescribeTableRow {
                        col_name: col.name.clone(),
                        data_type: formatter
                            .data_type_to_simple_string(&col.data_type)
                            .unwrap_or_else(|_| "invalid".to_string()),
                        comment: col.comment.clone(),
                    });
                }

                if extended {
                    match describe_partition_section(&table_status, formatter) {
                        PartitionSection::None => {}
                        PartitionSection::Identity(partition_rows) => {
                            rows.push(DescribeTableRow {
                                col_name: "# Partition Information".to_string(),
                                data_type: String::new(),
                                comment: None,
                            });
                            rows.push(DescribeTableRow {
                                col_name: "# col_name".to_string(),
                                data_type: "data_type".to_string(),
                                comment: Some("comment".to_string()),
                            });
                            rows.extend(partition_rows);
                        }
                        PartitionSection::Transformed(partition_rows) => {
                            rows.push(DescribeTableRow {
                                col_name: String::new(),
                                data_type: String::new(),
                                comment: None,
                            });
                            rows.push(DescribeTableRow {
                                col_name: "# Partitioning".to_string(),
                                data_type: String::new(),
                                comment: None,
                            });
                            rows.extend(partition_rows);
                        }
                    }

                    rows.push(DescribeTableRow {
                        col_name: String::new(),
                        data_type: String::new(),
                        comment: None,
                    });
                    rows.push(DescribeTableRow {
                        col_name: "# Detailed Table Information".to_string(),
                        data_type: String::new(),
                        comment: None,
                    });

                    for (key, value) in describe_extended_metadata_rows(&table_status) {
                        rows.push(DescribeTableRow {
                            col_name: key,
                            data_type: value,
                            comment: None,
                        });
                    }
                }

                serializer.build_record_batch(&rows)?
            }
            CatalogCommand::FunctionExists { .. } => {
                return Err(CatalogError::NotSupported("function exists".to_string()));
            }
            CatalogCommand::GetFunction { .. } => {
                return Err(CatalogError::NotSupported("get function".to_string()));
            }
            CatalogCommand::ListFunctions { .. } => {
                return Err(CatalogError::NotSupported("list functions".to_string()));
            }
            CatalogCommand::DropFunction {
                function,
                if_exists,
                is_temporary,
            } => {
                manager
                    .deregister_function(&function, if_exists, is_temporary)
                    .await?;
                display.bools().to_record_batch(vec![true])?
            }
            CatalogCommand::RegisterFunction { udf } => {
                let udf = manager.get_tracked_function(udf)?;
                manager.register_function(udf)?;
                display.empty().to_record_batch(vec![])?
            }
            CatalogCommand::RegisterTableFunction { name, udtf } => {
                manager.register_table_function(name, udtf)?;
                display.empty().to_record_batch(vec![])?
            }
            CatalogCommand::DropTemporaryView {
                view,
                is_global,
                options,
            } => {
                if is_global {
                    manager.drop_global_temporary_view(&view, options).await?;
                } else {
                    manager.drop_temporary_view(&view, options).await?;
                }
                display.bools().to_record_batch(vec![true])?
            }
            CatalogCommand::DropView { view, options } => {
                manager.drop_maybe_temporary_view(&view, options).await?;
                display.bools().to_record_batch(vec![true])?
            }
            CatalogCommand::CreateTemporaryView {
                view,
                is_global,
                options,
            } => {
                let input = manager.get_tracked_logical_plan(options.input)?;
                let options = CreateTemporaryViewOptions {
                    input,
                    columns: options.columns,
                    if_not_exists: options.if_not_exists,
                    replace: options.replace,
                    comment: options.comment,
                    properties: options.properties,
                };
                if is_global {
                    manager.create_global_temporary_view(&view, options).await?;
                } else {
                    manager.create_temporary_view(&view, options).await?;
                }
                display.bools().to_record_batch(vec![true])?
            }
            CatalogCommand::CreateView { view, options } => {
                manager.create_view(&view, options).await?;
                display.bools().to_record_batch(vec![true])?
            }
            CatalogCommand::DescribeDatabase { database, extended } => {
                let status = manager.get_database(&database).await?;
                let serializer = ArrowSerializer::default();

                let mut rows: Vec<DescribeDatabaseRow> = vec![
                    DescribeDatabaseRow {
                        info_name: "Namespace Name".to_string(),
                        info_value: quote_names_if_needed(&status.database),
                    },
                    DescribeDatabaseRow {
                        info_name: "Comment".to_string(),
                        info_value: status.comment.unwrap_or_default(),
                    },
                    DescribeDatabaseRow {
                        info_name: "Location".to_string(),
                        info_value: status.location.unwrap_or_default(),
                    },
                ];

                if extended {
                    let props = if status.properties.is_empty() {
                        String::new()
                    } else {
                        let mut sorted_props = status.properties.clone();
                        sorted_props.sort_by(|(a, _), (b, _)| a.cmp(b));
                        let entries: Vec<String> = sorted_props
                            .iter()
                            .map(|(k, v)| format!("({},{})", k, redact_property_value(k, v)))
                            .collect();
                        format!("({})", entries.join(","))
                    };
                    rows.push(DescribeDatabaseRow {
                        info_name: "Properties".to_string(),
                        info_value: props,
                    });
                }

                serializer.build_record_batch(&rows)?
            }
        };
        Ok(batch)
    }
}

async fn hydrate_table_status<C: SessionExtensionAccessor>(
    ctx: &C,
    table: &TableStatus,
) -> CatalogResult<TableStatus> {
    let (format, location) = match &table.kind {
        TableKind::Table {
            format, location, ..
        } => (format.as_str(), location.as_deref()),
        _ => return Ok(table.clone()),
    };

    if !is_lakehouse_format(format) {
        return Ok(table.clone());
    }
    let location = location.ok_or_else(|| {
        CatalogError::External(format!(
            "missing table location for storage-backed metadata hydration on format '{format}'"
        ))
    })?;
    if is_relative_location(location) {
        log::warn!(
            "skipping storage-backed metadata hydration for {:?} because lakehouse location '{}' is relative",
            qualified_table_name(table),
            location,
        );
        return Ok(table.clone());
    }

    let registry = ctx.extension::<TableFormatRegistry>().map_err(|e| {
        CatalogError::External(format!(
            "missing TableFormatRegistry for storage-backed metadata hydration: {e}"
        ))
    })?;
    let table_format = registry.get(format).map_err(|e| {
        CatalogError::External(format!(
            "unknown table format '{format}' for storage-backed metadata hydration: {e}"
        ))
    })?;
    let metadata = table_format
        .load_storage_table_metadata(ctx.runtime_env(), location)
        .await
        .map_err(|e| CatalogError::External(e.to_string()))?;

    let TableKind::Table {
        constraints,
        sort_by,
        bucket_by,
        is_external,
        ..
    } = &table.kind
    else {
        return Ok(table.clone());
    };

    let columns = normalize_partition_flags(metadata.columns, &metadata.partition_columns);

    Ok(TableStatus {
        catalog: table.catalog.clone(),
        database: table.database.clone(),
        name: table.name.clone(),
        kind: TableKind::Table {
            columns,
            comment: metadata.comment,
            constraints: constraints.clone(),
            location: metadata.location,
            format: metadata.provider,
            partition_by: metadata.partition_columns,
            sort_by: sort_by.clone(),
            bucket_by: bucket_by.clone(),
            properties: metadata.properties,
            is_external: *is_external,
        },
    })
}

fn normalize_partition_flags(
    mut columns: Vec<sail_common_datafusion::catalog::TableColumnStatus>,
    partition_columns: &[CatalogPartitionField],
) -> Vec<sail_common_datafusion::catalog::TableColumnStatus> {
    let identity_partition_columns: HashSet<String> = partition_columns
        .iter()
        .filter(|field| is_identity_partition_transform(field.transform))
        .map(|field| field.column.to_ascii_lowercase())
        .collect();
    for column in &mut columns {
        column.is_partition =
            identity_partition_columns.contains(&column.name.to_ascii_lowercase());
    }
    columns
}

fn describe_partition_section(
    table: &TableStatus,
    formatter: &dyn sail_common_datafusion::session::plan::PlanFormatter,
) -> PartitionSection {
    match &table.kind {
        TableKind::Table {
            columns,
            partition_by,
            ..
        } if partition_by.is_empty() => PartitionSection::None,
        TableKind::Table {
            columns,
            partition_by,
            ..
        } => {
            if partition_by
                .iter()
                .all(|field| is_identity_partition_transform(field.transform))
            {
                PartitionSection::Identity(
                    partition_by
                        .iter()
                        .map(|field| {
                            let source_column = columns
                                .iter()
                                .find(|column| column.name.eq_ignore_ascii_case(&field.column));
                            DescribeTableRow {
                                col_name: field.column.clone(),
                                data_type: source_column
                                    .map(|column| {
                                        formatter
                                            .data_type_to_simple_string(&column.data_type)
                                            .unwrap_or_else(|_| "invalid".to_string())
                                    })
                                    .unwrap_or_default(),
                                comment: source_column.and_then(|column| column.comment.clone()),
                            }
                        })
                        .collect(),
                )
            } else {
                PartitionSection::Transformed(
                    partition_by
                        .iter()
                        .enumerate()
                        .map(|(index, field)| DescribeTableRow {
                            col_name: format!("Part {index}"),
                            data_type: format_partition_field(field),
                            comment: Some(String::new()),
                        })
                        .collect(),
                )
            }
        }
        _ => PartitionSection::None,
    }
}

fn format_partition_field(field: &CatalogPartitionField) -> String {
    match field.transform.unwrap_or(PartitionTransform::Identity) {
        PartitionTransform::Identity => field.column.clone(),
        PartitionTransform::Year => format!("years({})", field.column),
        PartitionTransform::Month => format!("months({})", field.column),
        PartitionTransform::Day => format!("days({})", field.column),
        PartitionTransform::Hour => format!("hours({})", field.column),
        PartitionTransform::Bucket(n) => format!("bucket({n}, {})", field.column),
        PartitionTransform::Truncate(width) => format!("truncate({width}, {})", field.column),
    }
}

fn is_identity_partition_transform(transform: Option<PartitionTransform>) -> bool {
    matches!(
        transform.unwrap_or(PartitionTransform::Identity),
        PartitionTransform::Identity
    )
}

enum PartitionSection {
    None,
    Identity(Vec<DescribeTableRow>),
    Transformed(Vec<DescribeTableRow>),
}

#[derive(Serialize, Deserialize)]
struct DescribeTableRow {
    col_name: String,
    data_type: String,
    comment: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct DescribeDatabaseRow {
    info_name: String,
    info_value: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ShowTablesRow {
    database: String,
    #[serde(rename = "tableName")]
    table_name: String,
    is_temporary: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ShowTableExtendedRow {
    database: String,
    #[serde(rename = "tableName")]
    table_name: String,
    is_temporary: bool,
    information: String,
}

#[derive(Serialize, Deserialize)]
struct ShowTablePropertyRow {
    key: String,
    value: String,
}

fn qualified_table_name(table: &sail_common_datafusion::catalog::TableStatus) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(catalog) = &table.catalog {
        parts.push(catalog.clone());
    }
    parts.extend(table.database.clone());
    parts.push(table.name.clone());
    parts.join(".")
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::array::Array;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::catalog::Session;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::logical_expr::TableSource;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion_common::{internal_datafusion_err, not_impl_err, Result, ScalarValue};
    use sail_common_datafusion::catalog::display::{CatalogObjectDisplay, DefaultCatalogDisplay};
    use sail_common_datafusion::catalog::{
        CatalogPartitionField, DatabaseStatus, PartitionTransform, TableColumnStatus, TableKind,
        TableStatus,
    };
    use sail_common_datafusion::datasource::{
        SinkInfo, SourceInfo, StorageTableMetadata, TableFormat, TableFormatRegistry,
    };
    use sail_common_datafusion::extension::{SessionExtension, SessionExtensionAccessor};
    use sail_common_datafusion::session::plan::{PlanFormatter, PlanService};
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::manager::CatalogManagerOptions;
    use crate::provider::{
        CatalogProvider, CreateDatabaseOptions, CreateTableOptions, CreateViewOptions,
        DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace,
    };

    #[tokio::test]
    async fn list_columns_shows_storage_schema_for_delta_tables() -> CatalogResult<()> {
        let manager = test_manager(default_catalog_table("delta"))?;
        let ctx = test_session(Arc::new(TestTableFormat::new(
            "delta",
            StorageTableMetadata {
                columns: vec![table_column("storage_col")],
                comment: None,
                location: Some("file:///tmp/storage".to_string()),
                provider: "delta".to_string(),
                partition_columns: vec![],
                properties: vec![],
            },
        )))?;

        let batch = CatalogCommand::ListColumns {
            table: vec!["t".to_string()],
        }
        .execute(&ctx, &manager)
        .await?;

        let name = array_value_to_string(batch.column(0), 0)
            .map_err(|e| CatalogError::Internal(e.to_string()))?;
        assert_eq!(name, "storage_col");
        Ok(())
    }

    #[tokio::test]
    async fn describe_extended_uses_partition_transform_names_for_hidden_iceberg_partitions(
    ) -> CatalogResult<()> {
        let manager = test_manager(default_catalog_table("iceberg"))?;
        let mut transformed_source_column = table_column("ts");
        transformed_source_column.is_partition = true;
        let ctx = test_session(Arc::new(TestTableFormat::new(
            "iceberg",
            StorageTableMetadata {
                columns: vec![transformed_source_column, table_column("value")],
                comment: None,
                location: Some("file:///tmp/storage".to_string()),
                provider: "iceberg".to_string(),
                partition_columns: vec![CatalogPartitionField {
                    column: "ts".to_string(),
                    transform: Some(PartitionTransform::Day),
                }],
                properties: vec![],
            },
        )))?;

        let batch = CatalogCommand::DescribeTable {
            table: vec!["t".to_string()],
            extended: true,
        }
        .execute(&ctx, &manager)
        .await?;

        let names = string_column(&batch, 0)?;
        let partitioning_header = names
            .iter()
            .position(|name| name == "# Partitioning")
            .ok_or_else(|| {
                CatalogError::Internal("partitioning section header not found".to_string())
            })?;
        assert_eq!(
            names.get(partitioning_header + 1),
            Some(&"Part 0".to_string())
        );

        let values = string_column(&batch, 1)?;
        assert_eq!(
            values.get(partitioning_header + 1),
            Some(&"days(ts)".to_string())
        );
        Ok(())
    }

    #[tokio::test]
    async fn list_columns_does_not_mark_hidden_iceberg_transform_source_columns_as_partitions(
    ) -> CatalogResult<()> {
        let manager = test_manager(default_catalog_table("iceberg"))?;
        let mut transformed_source_column = table_column("ts");
        transformed_source_column.is_partition = true;
        let ctx = test_session(Arc::new(TestTableFormat::new(
            "iceberg",
            StorageTableMetadata {
                columns: vec![transformed_source_column, table_column("value")],
                comment: None,
                location: Some("file:///tmp/storage".to_string()),
                provider: "iceberg".to_string(),
                partition_columns: vec![CatalogPartitionField {
                    column: "ts".to_string(),
                    transform: Some(PartitionTransform::Day),
                }],
                properties: vec![],
            },
        )))?;

        let batch = CatalogCommand::ListColumns {
            table: vec!["t".to_string()],
        }
        .execute(&ctx, &manager)
        .await?;

        let names = string_column(&batch, 0)?;
        let partition_flags = string_column(&batch, 1)?;
        let columns = names
            .into_iter()
            .zip(partition_flags)
            .collect::<HashMap<_, _>>();

        assert_eq!(columns.get("ts").map(String::as_str), Some("false"));
        Ok(())
    }

    #[tokio::test]
    async fn show_table_properties_with_key_keeps_key_value_shape() -> CatalogResult<()> {
        let mut table = default_catalog_table("parquet");
        let TableKind::Table { properties, .. } = &mut table.kind else {
            unreachable!();
        };
        properties.push(("custom.key".to_string(), "hello".to_string()));
        let manager = test_manager(table)?;
        let ctx = test_session(Arc::new(TestTableFormat::new(
            "parquet",
            StorageTableMetadata {
                columns: vec![table_column("storage_col")],
                comment: None,
                location: Some("file:///tmp/storage".to_string()),
                provider: "parquet".to_string(),
                partition_columns: vec![],
                properties: vec![],
            },
        )))?;

        let batch = CatalogCommand::ShowTableProperties {
            table: vec!["t".to_string()],
            property_key: Some("custom.key".to_string()),
        }
        .execute(&ctx, &manager)
        .await?;

        assert_eq!(batch.num_columns(), 2);
        assert_eq!(string_column(&batch, 0)?, vec!["custom.key".to_string()]);
        assert_eq!(string_column(&batch, 1)?, vec!["hello".to_string()]);
        Ok(())
    }

    #[tokio::test]
    async fn alter_table_property_update_on_lakehouse_without_location_returns_error(
    ) -> CatalogResult<()> {
        let mut table = default_catalog_table("delta");
        let TableKind::Table { location, .. } = &mut table.kind else {
            unreachable!();
        };
        *location = None;
        let manager = test_manager(table)?;
        let ctx = test_session(Arc::new(TestTableFormat::new(
            "delta",
            StorageTableMetadata {
                columns: vec![table_column("storage_col")],
                comment: None,
                location: Some("file:///tmp/storage".to_string()),
                provider: "delta".to_string(),
                partition_columns: vec![],
                properties: vec![],
            },
        )))?;

        let result = CatalogCommand::AlterTable {
            table: vec!["t".to_string()],
            if_exists: false,
            options: AlterTableOptions::SetTableProperties {
                properties: vec![("k".to_string(), "v".to_string())],
            },
        }
        .execute(&ctx, &manager)
        .await;

        let Err(CatalogError::External(message)) = result else {
            return Err(CatalogError::Internal(
                "expected missing-location error for lakehouse ALTER TABLE".to_string(),
            ));
        };
        assert!(message.contains("missing table location for storage-backed ALTER TABLE on format"));
        Ok(())
    }

    #[tokio::test]
    async fn list_columns_falls_back_to_catalog_metadata_for_relative_lakehouse_locations(
    ) -> CatalogResult<()> {
        let mut table = default_catalog_table("delta");
        let TableKind::Table {
            location, columns, ..
        } = &mut table.kind
        else {
            unreachable!();
        };
        *location = Some("relative/delta".to_string());
        *columns = vec![table_column("catalog_col")];
        let manager = test_manager(table)?;
        let ctx = test_session(Arc::new(TestTableFormat::new_failing(
            "delta",
            "storage metadata load should not run for relative locations",
        )))?;

        let batch = CatalogCommand::ListColumns {
            table: vec!["t".to_string()],
        }
        .execute(&ctx, &manager)
        .await?;

        let names = string_column(&batch, 0)?;
        assert_eq!(names, vec!["catalog_col".to_string()]);
        Ok(())
    }

    #[tokio::test]
    async fn list_columns_marks_identity_partition_columns_case_insensitively() -> CatalogResult<()>
    {
        let manager = test_manager(default_catalog_table("delta"))?;
        let ctx = test_session(Arc::new(TestTableFormat::new(
            "delta",
            StorageTableMetadata {
                columns: vec![table_column("Ts"), table_column("value")],
                comment: None,
                location: Some("file:///tmp/storage".to_string()),
                provider: "delta".to_string(),
                partition_columns: vec![CatalogPartitionField {
                    column: "ts".to_string(),
                    transform: Some(PartitionTransform::Identity),
                }],
                properties: vec![],
            },
        )))?;

        let batch = CatalogCommand::ListColumns {
            table: vec!["t".to_string()],
        }
        .execute(&ctx, &manager)
        .await?;

        let names = string_column(&batch, 0)?;
        let partition_flags = string_column(&batch, 1)?;
        let columns = names
            .into_iter()
            .zip(partition_flags)
            .collect::<HashMap<_, _>>();

        assert_eq!(columns.get("Ts").map(String::as_str), Some("true"));
        Ok(())
    }

    #[tokio::test]
    async fn describe_extended_shows_external_type_for_external_delta_tables() -> CatalogResult<()>
    {
        let mut table = default_catalog_table("delta");
        let TableKind::Table { is_external, .. } = &mut table.kind else {
            unreachable!();
        };
        *is_external = true;
        let manager = test_manager(table)?;
        let ctx = test_session(Arc::new(TestTableFormat::new(
            "delta",
            StorageTableMetadata {
                columns: vec![table_column("storage_col")],
                comment: None,
                location: Some("file:///tmp/storage".to_string()),
                provider: "delta".to_string(),
                partition_columns: vec![],
                properties: vec![],
            },
        )))?;

        let batch = CatalogCommand::DescribeTable {
            table: vec!["t".to_string()],
            extended: true,
        }
        .execute(&ctx, &manager)
        .await?;

        let names = string_column(&batch, 0)?;
        let values = string_column(&batch, 1)?;
        let rows = names.into_iter().zip(values).collect::<HashMap<_, _>>();
        assert_eq!(rows.get("Type").map(String::as_str), Some("EXTERNAL"));
        Ok(())
    }

    #[tokio::test]
    async fn describe_database_extended_redacts_sensitive_properties() -> CatalogResult<()> {
        let manager = test_manager_with_database(DatabaseStatus {
            catalog: "spark_catalog".to_string(),
            database: vec!["default".to_string()],
            comment: None,
            location: Some("file:///tmp/wh".to_string()),
            properties: vec![
                ("password".to_string(), "hunter2".to_string()),
                ("endpoint".to_string(), "https://example.test".to_string()),
            ],
        })?;
        let ctx = test_session(Arc::new(TestTableFormat::new(
            "parquet",
            StorageTableMetadata {
                columns: vec![],
                comment: None,
                location: None,
                provider: "parquet".to_string(),
                partition_columns: vec![],
                properties: vec![],
            },
        )))?;

        let batch = CatalogCommand::DescribeDatabase {
            database: vec!["default".to_string()],
            extended: true,
        }
        .execute(&ctx, &manager)
        .await?;

        let names = string_column(&batch, 0)?;
        let values = string_column(&batch, 1)?;
        let rows: HashMap<_, _> = names.into_iter().zip(values).collect();
        let props = rows
            .get("Properties")
            .ok_or_else(|| CatalogError::Internal("Properties row missing".to_string()))?;
        assert!(
            props.contains("*********(redacted)"),
            "password value should be redacted, got: {props}"
        );
        assert!(
            !props.contains("hunter2"),
            "raw password should not appear, got: {props}"
        );
        assert!(
            props.contains("https://example.test"),
            "non-sensitive endpoint should appear verbatim, got: {props}"
        );
        Ok(())
    }

    struct TestSession {
        registry: Arc<TableFormatRegistry>,
        plan_service: Arc<PlanService>,
        runtime_env: Arc<RuntimeEnv>,
    }

    impl SessionExtensionAccessor for TestSession {
        fn extension<T: SessionExtension>(&self) -> Result<Arc<T>> {
            if T::name() == TableFormatRegistry::name() {
                let value: Arc<dyn Any + Send + Sync> = self.registry.clone();
                value
                    .downcast::<T>()
                    .map_err(|_| internal_datafusion_err!("extension type mismatch: {}", T::name()))
            } else if T::name() == PlanService::name() {
                let value: Arc<dyn Any + Send + Sync> = self.plan_service.clone();
                value
                    .downcast::<T>()
                    .map_err(|_| internal_datafusion_err!("extension type mismatch: {}", T::name()))
            } else {
                Err(internal_datafusion_err!(
                    "session extension not found: {}",
                    T::name()
                ))
            }
        }

        fn runtime_env(&self) -> Arc<RuntimeEnv> {
            self.runtime_env.clone()
        }
    }

    struct TestTableFormat {
        name: &'static str,
        metadata: Option<StorageTableMetadata>,
        error: Option<String>,
    }

    impl TestTableFormat {
        fn new(name: &'static str, metadata: StorageTableMetadata) -> Self {
            Self {
                name,
                metadata: Some(metadata),
                error: None,
            }
        }

        fn new_failing(name: &'static str, message: &str) -> Self {
            Self {
                name,
                metadata: None,
                error: Some(message.to_string()),
            }
        }
    }

    #[async_trait::async_trait]
    impl TableFormat for TestTableFormat {
        fn name(&self) -> &str {
            self.name
        }

        async fn create_source(
            &self,
            ctx: &dyn Session,
            info: SourceInfo,
        ) -> Result<Arc<dyn TableSource>> {
            let _ = (ctx, info);
            not_impl_err!("test table format does not create sources")
        }

        async fn create_writer(
            &self,
            ctx: &dyn Session,
            info: SinkInfo,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            let _ = (ctx, info);
            not_impl_err!("test table format does not create writers")
        }

        async fn load_storage_table_metadata(
            &self,
            runtime_env: Arc<RuntimeEnv>,
            path: &str,
        ) -> Result<StorageTableMetadata> {
            let _ = (runtime_env, path);
            if let Some(metadata) = &self.metadata {
                Ok(metadata.clone())
            } else {
                let message = self
                    .error
                    .as_deref()
                    .unwrap_or("test table format metadata load failed");
                datafusion_common::plan_err!("{message}")
            }
        }
    }

    struct TestCatalogProvider {
        table: TableStatus,
        database: Option<DatabaseStatus>,
    }

    impl TestCatalogProvider {
        fn new(table: TableStatus) -> Self {
            Self {
                table,
                database: None,
            }
        }

        fn with_database(mut self, database: DatabaseStatus) -> Self {
            self.database = Some(database);
            self
        }
    }

    #[async_trait::async_trait]
    impl CatalogProvider for TestCatalogProvider {
        fn get_name(&self) -> &str {
            "spark_catalog"
        }

        async fn create_database(
            &self,
            database: &Namespace,
            options: CreateDatabaseOptions,
        ) -> CatalogResult<DatabaseStatus> {
            let _ = (database, options);
            Err(CatalogError::NotSupported("test provider".to_string()))
        }

        async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
            let _ = database;
            match &self.database {
                Some(db) => Ok(db.clone()),
                None => Err(CatalogError::NotSupported("test provider".to_string())),
            }
        }

        async fn list_databases(
            &self,
            prefix: Option<&Namespace>,
        ) -> CatalogResult<Vec<DatabaseStatus>> {
            let _ = prefix;
            Err(CatalogError::NotSupported("test provider".to_string()))
        }

        async fn drop_database(
            &self,
            database: &Namespace,
            options: DropDatabaseOptions,
        ) -> CatalogResult<()> {
            let _ = (database, options);
            Err(CatalogError::NotSupported("test provider".to_string()))
        }

        async fn create_table(
            &self,
            database: &Namespace,
            table: &str,
            options: CreateTableOptions,
        ) -> CatalogResult<TableStatus> {
            let _ = (database, table, options);
            Err(CatalogError::NotSupported("test provider".to_string()))
        }

        async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
            let _ = database;
            if table == "t" {
                Ok(self.table.clone())
            } else {
                Err(CatalogError::NotFound(
                    crate::error::CatalogObject::Table,
                    table.to_string(),
                ))
            }
        }

        async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
            let _ = database;
            Err(CatalogError::NotSupported("test provider".to_string()))
        }

        async fn drop_table(
            &self,
            database: &Namespace,
            table: &str,
            options: DropTableOptions,
        ) -> CatalogResult<()> {
            let _ = (database, table, options);
            Err(CatalogError::NotSupported("test provider".to_string()))
        }

        async fn alter_table(
            &self,
            database: &Namespace,
            table: &str,
            options: AlterTableOptions,
        ) -> CatalogResult<()> {
            let _ = (database, table, options);
            Err(CatalogError::NotSupported("test provider".to_string()))
        }

        async fn create_view(
            &self,
            database: &Namespace,
            view: &str,
            options: CreateViewOptions,
        ) -> CatalogResult<TableStatus> {
            let _ = (database, view, options);
            Err(CatalogError::NotSupported("test provider".to_string()))
        }

        async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus> {
            let _ = database;
            Err(CatalogError::NotFound(
                crate::error::CatalogObject::View,
                view.to_string(),
            ))
        }

        async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
            let _ = database;
            Err(CatalogError::NotSupported("test provider".to_string()))
        }

        async fn drop_view(
            &self,
            database: &Namespace,
            view: &str,
            options: DropViewOptions,
        ) -> CatalogResult<()> {
            let _ = (database, view, options);
            Err(CatalogError::NotSupported("test provider".to_string()))
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
    struct TestPlanFormatter;

    impl PlanFormatter for TestPlanFormatter {
        fn data_type_to_simple_string(&self, data_type: &DataType) -> Result<String> {
            Ok(format!("{data_type:?}"))
        }

        fn literal_to_string(
            &self,
            literal: &ScalarValue,
            display_timezone: &str,
        ) -> Result<String> {
            let _ = display_timezone;
            Ok(format!("{literal:?}"))
        }

        fn function_to_string(
            &self,
            name: &str,
            arguments: Vec<&str>,
            is_distinct: bool,
        ) -> Result<String> {
            let _ = (arguments, is_distinct);
            Ok(name.to_string())
        }
    }

    #[derive(Default)]
    struct TestCatalogObjectDisplay;

    impl CatalogObjectDisplay for TestCatalogObjectDisplay {
        type Catalog = TestCatalogRow;
        type Database = TestDatabaseRow;
        type Table = TestTableRow;
        type TableColumn = TestTableColumnRow;
        type Function = TestFunctionRow;

        fn catalog(name: String) -> Self::Catalog {
            TestCatalogRow { name }
        }

        fn database(status: DatabaseStatus) -> Self::Database {
            TestDatabaseRow {
                name: status.database.join("."),
            }
        }

        fn table(status: TableStatus) -> Self::Table {
            TestTableRow { name: status.name }
        }

        fn table_column(status: TableColumnStatus) -> Self::TableColumn {
            TestTableColumnRow {
                name: status.name,
                is_partition: status.is_partition,
            }
        }

        fn function(name: String) -> Self::Function {
            TestFunctionRow { name }
        }
    }

    #[derive(Serialize, Deserialize)]
    struct TestCatalogRow {
        name: String,
    }

    #[derive(Serialize, Deserialize)]
    struct TestDatabaseRow {
        name: String,
    }

    #[derive(Serialize, Deserialize)]
    struct TestTableRow {
        name: String,
    }

    #[derive(Serialize, Deserialize)]
    struct TestTableColumnRow {
        name: String,
        is_partition: bool,
    }

    #[derive(Serialize, Deserialize)]
    struct TestFunctionRow {
        name: String,
    }

    fn table_column(name: &str) -> TableColumnStatus {
        TableColumnStatus {
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: true,
            comment: None,
            default: None,
            generated_always_as: None,
            is_partition: false,
            is_bucket: false,
            is_cluster: false,
        }
    }

    fn default_catalog_table(format: &str) -> TableStatus {
        TableStatus {
            catalog: Some("spark_catalog".to_string()),
            database: vec!["default".to_string()],
            name: "t".to_string(),
            kind: TableKind::Table {
                columns: vec![table_column("catalog_col")],
                comment: None,
                constraints: vec![],
                location: Some("file:///tmp/catalog".to_string()),
                format: format.to_string(),
                partition_by: vec![],
                sort_by: vec![],
                bucket_by: None,
                properties: vec![],
                is_external: false,
            },
        }
    }

    fn test_manager(table: TableStatus) -> CatalogResult<CatalogManager> {
        CatalogManager::try_new(CatalogManagerOptions {
            catalogs: HashMap::from([(
                "spark_catalog".to_string(),
                Arc::new(TestCatalogProvider::new(table)) as Arc<dyn CatalogProvider>,
            )]),
            default_catalog: "spark_catalog".to_string(),
            default_database: vec!["default".to_string()],
            global_temporary_database: vec!["global_temp".to_string()],
        })
    }

    fn test_manager_with_database(database: DatabaseStatus) -> CatalogResult<CatalogManager> {
        CatalogManager::try_new(CatalogManagerOptions {
            catalogs: HashMap::from([(
                "spark_catalog".to_string(),
                Arc::new(
                    TestCatalogProvider::new(default_catalog_table("parquet"))
                        .with_database(database),
                ) as Arc<dyn CatalogProvider>,
            )]),
            default_catalog: "spark_catalog".to_string(),
            default_database: vec!["default".to_string()],
            global_temporary_database: vec!["global_temp".to_string()],
        })
    }

    fn test_session(format: Arc<dyn TableFormat>) -> CatalogResult<TestSession> {
        let registry = Arc::new(TableFormatRegistry::new());
        registry.register(format)?;
        Ok(TestSession {
            registry,
            plan_service: Arc::new(PlanService::new(
                Box::new(DefaultCatalogDisplay::<TestCatalogObjectDisplay>::default()),
                Box::new(TestPlanFormatter),
            )),
            runtime_env: Arc::new(RuntimeEnv::default()),
        })
    }

    fn string_column(
        batch: &datafusion::arrow::array::RecordBatch,
        index: usize,
    ) -> CatalogResult<Vec<String>> {
        let column = batch.column(index);
        (0..column.len())
            .map(|row| {
                array_value_to_string(column, row)
                    .map_err(|e| CatalogError::Internal(e.to_string()))
            })
            .collect()
    }
}
