use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use sail_common_datafusion::array::serde::ArrowSerializer;
use sail_common_datafusion::datasource::{
    is_lakehouse_format, CreateTableOperation, CreateTableStorageAction, TableFormatRegistry,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use serde::{Deserialize, Serialize};

use crate::error::{CatalogError, CatalogResult};
use crate::manager::table::CreateTableMaterialization;
use crate::manager::tracker::{CatalogFunctionId, CatalogLogicalPlanId};
use crate::manager::CatalogManager;
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
                // For lakehouse formats (Delta/Iceberg), materialize the physical
                // table (write the initial transaction log) as part of catalog
                // creation. Catalog creates register the catalog entry first and
                // roll it back if storage initialization fails; catalog replaces
                // materialize first so a storage failure does not overwrite catalog
                // metadata.
                //
                // Known limitation: the REPLACE path is not fully atomic across the
                // storage and catalog layers. If the catalog update fails AFTER
                // storage was tombstoned, the previous table contents are gone but
                // the catalog still references the (now-replaced) location. A full
                // fix requires two-phase commit on the catalog side; today we accept
                // this as the symmetric cost of writing a real v0 commit first.
                //
                // `defer_materialize=true` means a subsequent writer (CTAS, first
                // INSERT through the write-to-create path) will atomically create
                // the physical table in a single commit, so we skip the metadata-
                // only pre-commit here.
                //
                // The catalog decides which formats may use generic
                // materialization. Path-style catalogs can support Delta and
                // Iceberg here, while provider-side catalogs such as Iceberg
                // REST/Glue create Iceberg metadata through their own APIs.
                let format = options.format.trim();
                let materialization = if !options.defer_materialize
                    && manager
                        .table_supports_generic_create_table_materialization(&table, format)?
                {
                    let materialization_mode = if !format.is_empty() && is_lakehouse_format(format)
                    {
                        manager
                            .create_table_materialization(
                                &table,
                                options.if_not_exists,
                                options.replace,
                            )
                            .await?
                    } else {
                        CreateTableMaterialization::Skip
                    };
                    match (options.location.clone(), materialization_mode) {
                        (_, CreateTableMaterialization::Skip) | (None, _) => None,
                        (Some(location), materialization_mode) => {
                            let operation = match (options.replace, options.if_not_exists) {
                                (true, _) => CreateTableOperation::CreateOrReplace {
                                    storage_action: if matches!(
                                        materialization_mode,
                                        CreateTableMaterialization::Replace
                                    ) {
                                        CreateTableStorageAction::Replace
                                    } else {
                                        CreateTableStorageAction::CreateOrAdopt
                                    },
                                },
                                (false, true) => CreateTableOperation::CreateIfNotExists,
                                (false, false) => CreateTableOperation::Create,
                            };
                            let registry = ctx.extension::<TableFormatRegistry>().map_err(|e| {
                                CatalogError::External(format!(
                                    "missing TableFormatRegistry for storage-backed CREATE TABLE on format '{format}': {e}"
                                ))
                            })?;
                            let table_format = registry.get(format).map_err(|e| {
                                CatalogError::External(format!(
                                    "unknown table format '{format}' for storage-backed CREATE TABLE: {e}"
                                ))
                            })?;
                            let schema = create_table_schema_from_columns(&options.columns);
                            let partition_by = options.partition_by.clone();
                            let properties = create_table_physical_properties(&options.properties);
                            let generated_columns: std::collections::HashMap<String, String> =
                                options
                                    .columns
                                    .iter()
                                    .filter_map(|c| {
                                        c.generated_always_as
                                            .as_ref()
                                            .map(|expr| (c.name.clone(), expr.clone()))
                                    })
                                    .collect();
                            let info = sail_common_datafusion::datasource::CreateTableInfo {
                                path: location,
                                schema,
                                partition_by,
                                properties,
                                operation,
                                generated_columns,
                            };
                            Some((table_format, info))
                        }
                    }
                } else {
                    None
                };
                if let Some((table_format, info)) = materialization {
                    if info.operation.replaces_existing_storage() {
                        let physical_location = info.path.clone();
                        table_format
                            .create_table(ctx.runtime_env(), info)
                            .await
                            .map_err(|e| CatalogError::External(e.to_string()))?;
                        if let Err(e) = manager.create_table(&table, options).await {
                            return Err(CatalogError::External(format!(
                                "failed to register catalog table after materializing storage-backed \
                                 CREATE OR REPLACE TABLE at {physical_location}; physical table \
                                 data or metadata may have been created or overwritten, leaving \
                                 storage and catalog out of sync: {e}"
                            )));
                        }
                    } else {
                        manager.create_table(&table, options).await?;
                        if let Err(e) = table_format.create_table(ctx.runtime_env(), info).await {
                            let storage_error = e.to_string();
                            if let Err(rollback_error) = manager
                                .drop_table(
                                    &table,
                                    DropTableOptions {
                                        if_exists: true,
                                        purge: false,
                                    },
                                )
                                .await
                            {
                                return Err(CatalogError::External(format!(
                                    "failed to materialize storage-backed CREATE TABLE: {storage_error}; \
                                     additionally failed to roll back catalog table: {rollback_error}"
                                )));
                            }
                            return Err(CatalogError::External(storage_error));
                        }
                    }
                } else {
                    manager.create_table(&table, options).await?;
                }
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
                let rows = rows
                    .into_iter()
                    .map(|row| {
                        Ok(ShowTableExtendedRow {
                            database: quote_names_if_needed(&row.database),
                            table_name: row.name.clone(),
                            is_temporary: row.kind.is_temporary(),
                            information: row.show_table_extended_information(formatter)?,
                        })
                    })
                    .collect::<CatalogResult<Vec<_>>>()?;
                ArrowSerializer::default().build_record_batch(&rows)?
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

                // Persist changes to storage first (source of truth for table formats
                // such as Delta Lake). Only after storage commits successfully do we
                // update the catalog metadata, so we never end up with the two layers
                // out of sync.
                if let (Some(location), Some(format)) = (location, format) {
                    // Non-lakehouse formats (e.g., plain Parquet/Hive tables) have no
                    // storage-layer metadata — the catalog is the sole source of truth.
                    // Skip straight to catalog-only update.
                    // This mirrors Spark+Delta where DeltaCatalog intercepts ALTER TABLE
                    // for Delta tables but plain tables fall through to SessionCatalog.
                    if !is_lakehouse_format(&format) {
                        manager.alter_table(&table, options).await?;
                        return Ok(display.bools().to_record_batch(vec![true])?);
                    }
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
                    };

                    // Storage is the source of truth for lakehouse ALTER TABLE
                    // operations, but metadata reads still flow through the
                    // catalog. Surface catalog sync failures so callers do not
                    // observe successful storage mutation followed by stale
                    // DESCRIBE/SHOW metadata.
                    manager.alter_table(&table, options).await?;
                    return Ok(display.bools().to_record_batch(vec![true])?);
                }

                manager.alter_table(&table, options).await?;
                display.bools().to_record_batch(vec![true])?
            }
            CatalogCommand::ListColumns { table } => {
                let rows = manager.get_table_or_view(&table).await?.kind.columns();
                display.table_columns().to_record_batch(rows)?
            }
            CatalogCommand::DescribeTable { table, extended } => {
                let table_status = manager.get_table_or_view(&table).await?;
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
                    let partition_cols = table_status.kind.partition_columns();
                    if !partition_cols.is_empty() {
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
                        for col in &partition_cols {
                            rows.push(DescribeTableRow {
                                col_name: col.name.clone(),
                                data_type: formatter
                                    .data_type_to_simple_string(&col.data_type)
                                    .unwrap_or_else(|_| "invalid".to_string()),
                                comment: col.comment.clone(),
                            });
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

                    for (key, value) in table_status.describe_extended_metadata() {
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
                            .map(|(k, v)| format!("({k},{v})"))
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

/// Build an Arrow [`SchemaRef`] from the catalog column descriptors used by
/// `CREATE TABLE`. The resulting schema is passed to
/// [`sail_common_datafusion::datasource::TableFormat::create_table`] so that
/// lakehouse formats can materialize the initial table metadata commit.
fn create_table_schema_from_columns(
    columns: &[crate::provider::CreateTableColumnOptions],
) -> SchemaRef {
    use datafusion::arrow::datatypes::{Field, Schema};

    let fields: Vec<Field> = columns
        .iter()
        .map(|c| {
            Field::new(&c.name, c.data_type.clone(), c.nullable).with_metadata(
                c.metadata
                    .iter()
                    .cloned()
                    .collect::<std::collections::HashMap<_, _>>(),
            )
        })
        .collect();
    std::sync::Arc::new(Schema::new(fields))
}

fn create_table_physical_properties(
    properties: &[(String, String)],
) -> std::collections::HashMap<String, String> {
    properties
        .iter()
        .filter(|(key, _)| !is_catalog_encoded_option_property(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn is_catalog_encoded_option_property(key: &str) -> bool {
    key.starts_with("option.")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use datafusion::catalog::Session;
    use datafusion::execution::context::SessionConfig;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use datafusion_common::not_impl_err;
    use datafusion_expr::TableSource;
    use sail_common_datafusion::catalog::display::{CatalogObjectDisplay, DefaultCatalogDisplay};
    use sail_common_datafusion::catalog::{
        DatabaseStatus, TableColumnStatus, TableKind, TableStatus,
    };
    use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat};
    use sail_common_datafusion::session::plan::{PlanFormatter, PlanService};
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::provider::CatalogProvider;

    #[derive(Serialize, Deserialize)]
    struct TestCatalogOutput {
        value: String,
    }

    #[derive(Default)]
    struct TestCatalogObjectDisplay;

    impl CatalogObjectDisplay for TestCatalogObjectDisplay {
        type Catalog = TestCatalogOutput;
        type Database = TestCatalogOutput;
        type Table = TestCatalogOutput;
        type TableColumn = TestCatalogOutput;
        type Function = TestCatalogOutput;

        fn catalog(name: String) -> Self::Catalog {
            TestCatalogOutput { value: name }
        }

        fn database(status: DatabaseStatus) -> Self::Database {
            TestCatalogOutput {
                value: status.database.join("."),
            }
        }

        fn table(status: TableStatus) -> Self::Table {
            TestCatalogOutput { value: status.name }
        }

        fn table_column(status: TableColumnStatus) -> Self::TableColumn {
            TestCatalogOutput { value: status.name }
        }

        fn function(name: String) -> Self::Function {
            TestCatalogOutput { value: name }
        }
    }

    #[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
    struct TestPlanFormatter;

    impl PlanFormatter for TestPlanFormatter {
        fn data_type_to_simple_string(
            &self,
            _data_type: &datafusion::arrow::datatypes::DataType,
        ) -> datafusion_common::Result<String> {
            Ok("int".to_string())
        }

        fn literal_to_string(
            &self,
            _literal: &datafusion_common::ScalarValue,
            _display_timezone: &str,
        ) -> datafusion_common::Result<String> {
            not_impl_err!("unused in test")
        }

        fn function_to_string(
            &self,
            _name: &str,
            _arguments: Vec<&str>,
            _is_distinct: bool,
        ) -> datafusion_common::Result<String> {
            not_impl_err!("unused in test")
        }
    }

    struct TestProvider {
        table_status: TableStatus,
        table_exists: bool,
        create_error: Option<String>,
        create_table_calls: Arc<AtomicUsize>,
        drop_table_calls: Arc<AtomicUsize>,
        alter_error: Option<String>,
        supports_materialization: bool,
    }

    #[async_trait]
    impl CatalogProvider for TestProvider {
        fn get_name(&self) -> &str {
            "test"
        }

        fn supports_generic_create_table_materialization(&self, _format: &str) -> bool {
            self.supports_materialization
        }

        async fn create_database(
            &self,
            _database: &crate::provider::Namespace,
            _options: CreateDatabaseOptions,
        ) -> CatalogResult<sail_common_datafusion::catalog::DatabaseStatus> {
            unreachable!()
        }

        async fn get_database(
            &self,
            database: &crate::provider::Namespace,
        ) -> CatalogResult<sail_common_datafusion::catalog::DatabaseStatus> {
            Ok(DatabaseStatus {
                catalog: "test".to_string(),
                database: database.clone().into(),
                comment: None,
                location: None,
                properties: vec![],
            })
        }

        async fn list_databases(
            &self,
            _prefix: Option<&crate::provider::Namespace>,
        ) -> CatalogResult<Vec<sail_common_datafusion::catalog::DatabaseStatus>> {
            unreachable!()
        }

        async fn drop_database(
            &self,
            _database: &crate::provider::Namespace,
            _options: DropDatabaseOptions,
        ) -> CatalogResult<()> {
            unreachable!()
        }

        async fn create_table(
            &self,
            _database: &crate::provider::Namespace,
            _table: &str,
            _options: CreateTableOptions,
        ) -> CatalogResult<TableStatus> {
            self.create_table_calls.fetch_add(1, Ordering::SeqCst);
            match &self.create_error {
                Some(message) => Err(CatalogError::External(message.clone())),
                None => Ok(self.table_status.clone()),
            }
        }

        async fn get_table(
            &self,
            _database: &crate::provider::Namespace,
            table: &str,
        ) -> CatalogResult<TableStatus> {
            if self.table_exists {
                Ok(self.table_status.clone())
            } else {
                Err(CatalogError::NotFound(
                    crate::error::CatalogObject::Table,
                    table.to_string(),
                ))
            }
        }

        async fn list_tables(
            &self,
            _database: &crate::provider::Namespace,
        ) -> CatalogResult<Vec<TableStatus>> {
            unreachable!()
        }

        async fn drop_table(
            &self,
            _database: &crate::provider::Namespace,
            _table: &str,
            _options: DropTableOptions,
        ) -> CatalogResult<()> {
            self.drop_table_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn alter_table(
            &self,
            _database: &crate::provider::Namespace,
            _table: &str,
            _options: AlterTableOptions,
        ) -> CatalogResult<()> {
            match &self.alter_error {
                Some(message) => Err(CatalogError::External(message.clone())),
                None => Ok(()),
            }
        }

        async fn create_view(
            &self,
            _database: &crate::provider::Namespace,
            _view: &str,
            _options: CreateViewOptions,
        ) -> CatalogResult<TableStatus> {
            unreachable!()
        }

        async fn get_view(
            &self,
            _database: &crate::provider::Namespace,
            _view: &str,
        ) -> CatalogResult<TableStatus> {
            unreachable!()
        }

        async fn list_views(
            &self,
            _database: &crate::provider::Namespace,
        ) -> CatalogResult<Vec<TableStatus>> {
            unreachable!()
        }

        async fn drop_view(
            &self,
            _database: &crate::provider::Namespace,
            _view: &str,
            _options: DropViewOptions,
        ) -> CatalogResult<()> {
            unreachable!()
        }
    }

    struct TestTableFormat {
        create_table_calls: Arc<AtomicUsize>,
        create_error: Option<String>,
        operations: Arc<Mutex<Vec<CreateTableOperation>>>,
    }

    type TestFormatRecorder = (
        SessionContext,
        Arc<AtomicUsize>,
        Arc<Mutex<Vec<CreateTableOperation>>>,
    );

    #[async_trait]
    impl TableFormat for TestTableFormat {
        fn name(&self) -> &str {
            "delta"
        }

        async fn create_source(
            &self,
            _ctx: &dyn Session,
            _info: SourceInfo,
        ) -> datafusion_common::Result<Arc<dyn TableSource>> {
            not_impl_err!("unused in test")
        }

        async fn create_writer(
            &self,
            _ctx: &dyn Session,
            _info: SinkInfo,
        ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
            not_impl_err!("unused in test")
        }

        async fn alter_table_properties(
            &self,
            _runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
            _path: &str,
            _changes: Vec<(String, Option<String>)>,
            _if_exists: bool,
        ) -> datafusion_common::Result<()> {
            Ok(())
        }

        async fn create_table(
            &self,
            _runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
            info: sail_common_datafusion::datasource::CreateTableInfo,
        ) -> datafusion_common::Result<()> {
            self.create_table_calls.fetch_add(1, Ordering::SeqCst);
            self.operations
                .lock()
                .map_err(|e| datafusion_common::DataFusionError::Execution(e.to_string()))?
                .push(info.operation);
            match &self.create_error {
                Some(message) => Err(datafusion_common::DataFusionError::Plan(message.clone())),
                None => Ok(()),
            }
        }
    }

    fn test_session_context() -> SessionContext {
        test_session_context_with_registered_format().0
    }

    fn test_session_context_with_registered_format() -> (SessionContext, Arc<AtomicUsize>) {
        test_session_context_with_format_error(None)
    }

    fn test_session_context_with_format_error(
        create_error: Option<&str>,
    ) -> (SessionContext, Arc<AtomicUsize>) {
        let (ctx, create_table_calls, _) = test_session_context_with_format_recorder(create_error);
        (ctx, create_table_calls)
    }

    fn test_session_context_with_format_recorder(create_error: Option<&str>) -> TestFormatRecorder {
        let create_table_calls = Arc::new(AtomicUsize::new(0));
        let operations = Arc::new(Mutex::new(Vec::new()));
        let registry = Arc::new(TableFormatRegistry::new());
        let register_result = registry.register(Arc::new(TestTableFormat {
            create_table_calls: create_table_calls.clone(),
            create_error: create_error.map(ToString::to_string),
            operations: operations.clone(),
        }));
        assert!(
            register_result.is_ok(),
            "failed to register test table format: {register_result:?}"
        );
        (
            test_session_context_with_registry(Some(registry)),
            create_table_calls,
            operations,
        )
    }

    fn test_session_context_with_empty_registry() -> SessionContext {
        test_session_context_with_registry(Some(Arc::new(TableFormatRegistry::new())))
    }

    fn test_session_context_without_registry() -> SessionContext {
        test_session_context_with_registry(None)
    }

    fn test_session_context_with_registry(
        registry: Option<Arc<TableFormatRegistry>>,
    ) -> SessionContext {
        let plan_service = Arc::new(PlanService::new(
            Box::new(DefaultCatalogDisplay::<TestCatalogObjectDisplay>::default()),
            Box::new(TestPlanFormatter),
        ));
        let mut config = SessionConfig::new().with_extension(plan_service);
        if let Some(registry) = registry {
            config = config.with_extension(registry);
        }
        SessionContext::new_with_config(config)
    }

    fn test_manager(alter_error: Option<&str>) -> CatalogManager {
        test_manager_with_options(true, None, alter_error, Arc::new(AtomicUsize::new(0)))
    }

    fn test_manager_with_options(
        table_exists: bool,
        create_error: Option<&str>,
        alter_error: Option<&str>,
        create_table_calls: Arc<AtomicUsize>,
    ) -> CatalogManager {
        test_manager_with_materialization_policy(
            table_exists,
            create_error,
            alter_error,
            create_table_calls,
            Arc::new(AtomicUsize::new(0)),
            true,
        )
    }

    fn test_manager_with_drop_calls(
        table_exists: bool,
        create_error: Option<&str>,
        alter_error: Option<&str>,
        create_table_calls: Arc<AtomicUsize>,
        drop_table_calls: Arc<AtomicUsize>,
    ) -> CatalogManager {
        test_manager_with_materialization_policy(
            table_exists,
            create_error,
            alter_error,
            create_table_calls,
            drop_table_calls,
            true,
        )
    }

    fn test_manager_with_materialization_policy(
        table_exists: bool,
        create_error: Option<&str>,
        alter_error: Option<&str>,
        create_table_calls: Arc<AtomicUsize>,
        drop_table_calls: Arc<AtomicUsize>,
        supports_materialization: bool,
    ) -> CatalogManager {
        let table_status = test_table_status();
        let manager = CatalogManager::try_new(crate::manager::CatalogManagerOptions {
            catalogs: std::iter::once((
                "test".to_string(),
                Arc::new(TestProvider {
                    table_status,
                    table_exists,
                    create_error: create_error.map(ToString::to_string),
                    create_table_calls,
                    drop_table_calls,
                    alter_error: alter_error.map(ToString::to_string),
                    supports_materialization,
                }) as Arc<dyn CatalogProvider>,
            ))
            .collect(),
            default_catalog: "test".to_string(),
            default_database: vec!["default".to_string()],
            global_temporary_database: vec!["global_temp".to_string()],
        });
        assert!(
            manager.is_ok(),
            "failed to construct test catalog manager: {}",
            manager
                .as_ref()
                .err()
                .map(ToString::to_string)
                .unwrap_or_default()
        );
        let Ok(manager) = manager else {
            unreachable!();
        };
        manager
    }

    fn test_table_status() -> TableStatus {
        TableStatus {
            catalog: Some("test".to_string()),
            database: vec!["default".to_string()],
            name: "items".to_string(),
            kind: TableKind::Table {
                columns: vec![TableColumnStatus {
                    name: "id".to_string(),
                    data_type: datafusion::arrow::datatypes::DataType::Int32,
                    nullable: false,
                    comment: None,
                    default: None,
                    generated_always_as: None,
                    is_partition: false,
                    is_bucket: false,
                    is_cluster: false,
                }],
                comment: None,
                constraints: vec![],
                location: Some("s3://bucket/items".to_string()),
                format: "delta".to_string(),
                partition_by: vec![],
                sort_by: vec![],
                bucket_by: None,
                properties: vec![],
                is_external: true,
            },
        }
    }

    fn test_create_table_options() -> CreateTableOptions {
        CreateTableOptions {
            columns: vec![crate::provider::CreateTableColumnOptions {
                name: "id".to_string(),
                data_type: datafusion::arrow::datatypes::DataType::Int32,
                nullable: false,
                comment: None,
                default: None,
                metadata: vec![],
                generated_always_as: None,
            }],
            comment: None,
            constraints: vec![],
            location: Some("s3://bucket/items".to_string()),
            format: "delta".to_string(),
            partition_by: vec![],
            sort_by: vec![],
            bucket_by: None,
            if_not_exists: false,
            replace: false,
            properties: vec![],
            defer_materialize: false,
            is_external: true,
        }
    }

    fn recorded_operations(
        operations: &Arc<Mutex<Vec<CreateTableOperation>>>,
    ) -> Result<Vec<CreateTableOperation>, String> {
        operations
            .lock()
            .map(|operations| operations.clone())
            .map_err(|e| e.to_string())
    }

    fn variant_physical_shape() -> datafusion::arrow::datatypes::DataType {
        use datafusion::arrow::datatypes::{DataType, Field, Fields};

        DataType::Struct(Fields::from(vec![
            Field::new("metadata", DataType::Binary, false),
            Field::new("value", DataType::Binary, false),
        ]))
    }

    #[test]
    fn create_table_schema_uses_explicit_field_metadata_for_variant() {
        let schema =
            create_table_schema_from_columns(&[crate::provider::CreateTableColumnOptions {
                name: "payload".to_string(),
                data_type: variant_physical_shape(),
                nullable: true,
                comment: None,
                default: None,
                metadata: vec![],
                generated_always_as: None,
            }]);
        assert!(
            !schema
                .field(0)
                .metadata()
                .contains_key(sail_common::spec::EXTENSION_TYPE_NAME_KEY),
            "plain structs with the same physical shape as Variant must not be marked as Variant"
        );

        let schema =
            create_table_schema_from_columns(&[crate::provider::CreateTableColumnOptions {
                name: "payload".to_string(),
                data_type: variant_physical_shape(),
                nullable: true,
                comment: None,
                default: None,
                metadata: vec![(
                    sail_common::spec::EXTENSION_TYPE_NAME_KEY.to_string(),
                    sail_common::spec::VARIANT_EXTENSION_NAME.to_string(),
                )],
                generated_always_as: None,
            }]);
        assert_eq!(
            schema
                .field(0)
                .metadata()
                .get(sail_common::spec::EXTENSION_TYPE_NAME_KEY),
            Some(&sail_common::spec::VARIANT_EXTENSION_NAME.to_string())
        );
    }

    #[test]
    fn create_table_physical_properties_exclude_catalog_encoded_options() {
        let properties = create_table_physical_properties(&[
            ("option.metadataAsDataRead".to_string(), "true".to_string()),
            ("custom.key".to_string(), "custom-value".to_string()),
            ("delta.appendOnly".to_string(), "true".to_string()),
        ]);

        assert!(!properties.contains_key("option.metadataAsDataRead"));
        assert_eq!(
            properties.get("custom.key").map(String::as_str),
            Some("custom-value")
        );
        assert_eq!(
            properties.get("delta.appendOnly").map(String::as_str),
            Some("true")
        );
    }

    #[tokio::test]
    async fn create_table_checks_catalog_before_materializing_existing_table() {
        let (ctx, materialize_calls) = test_session_context_with_registered_format();
        let create_table_calls = Arc::new(AtomicUsize::new(0));
        let manager = test_manager_with_options(true, None, None, create_table_calls);
        let command = CatalogCommand::CreateTable {
            table: vec!["items".to_string()],
            options: test_create_table_options(),
        };

        let result = command.execute(&ctx, &manager).await;
        assert!(
            result.is_err(),
            "expected existing catalog table to fail before materialization, got success: {result:?}"
        );
        let Err(error) = result else {
            unreachable!();
        };

        assert!(matches!(error, CatalogError::AlreadyExists(_, _)));
        assert_eq!(materialize_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn create_table_if_not_exists_skips_materialization_for_existing_table() {
        let (ctx, materialize_calls) = test_session_context_with_registered_format();
        let create_table_calls = Arc::new(AtomicUsize::new(0));
        let manager = test_manager_with_options(true, None, None, create_table_calls.clone());
        let mut options = test_create_table_options();
        options.if_not_exists = true;
        let command = CatalogCommand::CreateTable {
            table: vec!["items".to_string()],
            options,
        };

        let result = command.execute(&ctx, &manager).await;
        assert!(
            result.is_ok(),
            "expected CREATE TABLE IF NOT EXISTS to succeed: {result:?}"
        );
        assert_eq!(materialize_calls.load(Ordering::SeqCst), 0);
        assert_eq!(create_table_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn create_table_materializes_new_lakehouse_table() {
        let (ctx, materialize_calls) = test_session_context_with_registered_format();
        let create_table_calls = Arc::new(AtomicUsize::new(0));
        let manager = test_manager_with_options(false, None, None, create_table_calls.clone());
        let command = CatalogCommand::CreateTable {
            table: vec!["items".to_string()],
            options: test_create_table_options(),
        };

        let result = command.execute(&ctx, &manager).await;
        assert!(
            result.is_ok(),
            "expected new lakehouse table creation to succeed: {result:?}"
        );
        assert_eq!(create_table_calls.load(Ordering::SeqCst), 1);
        assert_eq!(materialize_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn create_table_does_not_materialize_when_catalog_create_fails() {
        let (ctx, materialize_calls) = test_session_context_with_registered_format();
        let create_table_calls = Arc::new(AtomicUsize::new(0));
        let manager = test_manager_with_options(
            false,
            Some("catalog create failed"),
            None,
            create_table_calls.clone(),
        );
        let command = CatalogCommand::CreateTable {
            table: vec!["items".to_string()],
            options: test_create_table_options(),
        };

        let result = command.execute(&ctx, &manager).await;
        assert!(
            result.is_err(),
            "expected catalog create failure: {result:?}"
        );
        let Err(error) = result else {
            unreachable!();
        };

        assert!(
            matches!(error, CatalogError::External(message) if message.contains("catalog create failed"))
        );
        assert_eq!(materialize_calls.load(Ordering::SeqCst), 0);
        assert_eq!(create_table_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn create_table_rolls_back_catalog_when_materialization_fails() {
        let (ctx, materialize_calls) =
            test_session_context_with_format_error(Some("storage create failed"));
        let create_table_calls = Arc::new(AtomicUsize::new(0));
        let drop_table_calls = Arc::new(AtomicUsize::new(0));
        let manager = test_manager_with_drop_calls(
            false,
            None,
            None,
            create_table_calls.clone(),
            drop_table_calls.clone(),
        );
        let command = CatalogCommand::CreateTable {
            table: vec!["items".to_string()],
            options: test_create_table_options(),
        };

        let result = command.execute(&ctx, &manager).await;
        assert!(
            result.is_err(),
            "expected storage materialization failure: {result:?}"
        );
        let Err(error) = result else {
            unreachable!();
        };

        assert!(
            matches!(error, CatalogError::External(message) if message.contains("storage create failed"))
        );
        assert_eq!(create_table_calls.load(Ordering::SeqCst), 1);
        assert_eq!(materialize_calls.load(Ordering::SeqCst), 1);
        assert_eq!(drop_table_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn create_or_replace_without_catalog_entry_uses_physical_create() {
        let (ctx, materialize_calls, operations) = test_session_context_with_format_recorder(None);
        let create_table_calls = Arc::new(AtomicUsize::new(0));
        let manager = test_manager_with_options(false, None, None, create_table_calls.clone());
        let mut options = test_create_table_options();
        options.replace = true;
        let command = CatalogCommand::CreateTable {
            table: vec!["items".to_string()],
            options,
        };

        let result = command.execute(&ctx, &manager).await;
        assert!(
            result.is_ok(),
            "expected CREATE OR REPLACE without catalog entry to create/adopt physical table: {result:?}"
        );
        assert_eq!(create_table_calls.load(Ordering::SeqCst), 1);
        assert_eq!(materialize_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            recorded_operations(&operations),
            Ok(vec![CreateTableOperation::CreateOrReplace {
                storage_action: CreateTableStorageAction::CreateOrAdopt
            }])
        );
    }

    #[tokio::test]
    async fn create_or_replace_table_reports_catalog_failure_after_materialization() {
        let (ctx, materialize_calls, operations) = test_session_context_with_format_recorder(None);
        let create_table_calls = Arc::new(AtomicUsize::new(0));
        let manager = test_manager_with_options(
            true,
            Some("catalog replace failed"),
            None,
            create_table_calls.clone(),
        );
        let mut options = test_create_table_options();
        options.replace = true;
        let command = CatalogCommand::CreateTable {
            table: vec!["items".to_string()],
            options,
        };

        let result = command.execute(&ctx, &manager).await;
        assert!(
            result.is_err(),
            "expected catalog replace failure: {result:?}"
        );
        let Err(error) = result else {
            unreachable!();
        };

        assert!(matches!(error, CatalogError::External(message)
                if message.contains("catalog replace failed")
                    && message.contains("physical table data or metadata may have been created or overwritten")
                    && message.contains("storage and catalog out of sync")));
        assert_eq!(create_table_calls.load(Ordering::SeqCst), 1);
        assert_eq!(materialize_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            recorded_operations(&operations),
            Ok(vec![CreateTableOperation::CreateOrReplace {
                storage_action: CreateTableStorageAction::Replace
            }])
        );
    }

    #[tokio::test]
    async fn create_table_requires_registry_for_lakehouse_materialization() {
        let ctx = test_session_context_without_registry();
        let create_table_calls = Arc::new(AtomicUsize::new(0));
        let manager = test_manager_with_options(false, None, None, create_table_calls.clone());
        let command = CatalogCommand::CreateTable {
            table: vec!["items".to_string()],
            options: test_create_table_options(),
        };

        let result = command.execute(&ctx, &manager).await;
        assert!(
            result.is_err(),
            "expected missing table format registry to fail: {result:?}"
        );
        let Err(error) = result else {
            unreachable!();
        };

        assert!(
            matches!(error, CatalogError::External(message) if message.contains("missing TableFormatRegistry"))
        );
        assert_eq!(create_table_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn create_table_requires_registered_lakehouse_format() {
        let ctx = test_session_context_with_empty_registry();
        let create_table_calls = Arc::new(AtomicUsize::new(0));
        let manager = test_manager_with_options(false, None, None, create_table_calls.clone());
        let command = CatalogCommand::CreateTable {
            table: vec!["items".to_string()],
            options: test_create_table_options(),
        };

        let result = command.execute(&ctx, &manager).await;
        assert!(
            result.is_err(),
            "expected unknown lakehouse format to fail: {result:?}"
        );
        let Err(error) = result else {
            unreachable!();
        };

        assert!(
            matches!(error, CatalogError::External(message) if message.contains("unknown table format 'delta'"))
        );
        assert_eq!(create_table_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn create_table_skips_materialization_when_catalog_opts_out_for_format() {
        // A catalog that creates a specific format through its own API (e.g.,
        // Iceberg REST/Glue) must be able to opt out of generic materialization.
        let (ctx, materialize_calls) = test_session_context_with_registered_format();
        let create_table_calls = Arc::new(AtomicUsize::new(0));
        let manager = test_manager_with_materialization_policy(
            false,
            None,
            None,
            create_table_calls.clone(),
            Arc::new(AtomicUsize::new(0)),
            false,
        );
        let command = CatalogCommand::CreateTable {
            table: vec!["items".to_string()],
            options: test_create_table_options(),
        };

        let result = command.execute(&ctx, &manager).await;
        assert!(
            result.is_ok(),
            "expected catalog opt-out CREATE TABLE to succeed: {result:?}"
        );
        assert_eq!(create_table_calls.load(Ordering::SeqCst), 1);
        assert_eq!(materialize_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn alter_table_returns_error_when_catalog_sync_fails() {
        let ctx = test_session_context();
        let manager = test_manager(Some("catalog sync failed"));
        let command = CatalogCommand::AlterTable {
            table: vec!["items".to_string()],
            if_exists: false,
            options: AlterTableOptions::SetTableProperties {
                properties: vec![("owner".to_string(), "alice".to_string())],
            },
        };

        let result = command.execute(&ctx, &manager).await;
        assert!(
            result.is_err(),
            "expected catalog sync failure, got success: {result:?}"
        );
        let Err(error) = result else {
            unreachable!();
        };

        assert!(
            matches!(error, CatalogError::External(message) if message.contains("catalog sync failed"))
        );
    }
}
