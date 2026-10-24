use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::TaskContext;
use sail_common_datafusion::array::serde::ArrowSerializer;
use sail_common_datafusion::catalog::{FunctionStatus, LakehouseOperation, TableKind};
use sail_common_datafusion::datasource::{DataSourceRegistry, is_lakehouse_format};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakesource::{
    LakeSourceCreateTableColumn, LakeSourceCreateTableInfo, LakeSourceCreateTableResult,
};
use sail_common_datafusion::session::plan::PlanService;
use serde::{Deserialize, Serialize};

use crate::error::{CatalogError, CatalogObject, CatalogResult};
use crate::lakehouse::{
    LakehouseCreateMaterialization, LakehouseCreatePlan, LakehouseCreateRequest,
};
use crate::manager::CatalogManager;
use crate::manager::tracker::{CatalogFunctionId, CatalogLogicalPlanId};
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
    ShowFunctions {
        database: Vec<String>,
        pattern: Option<String>,
        system_functions: Vec<FunctionStatus>,
        show_user_functions: bool,
        show_system_functions: bool,
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
    RenameTable {
        table: Vec<String>,
        destination: Vec<String>,
        if_exists: bool,
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
        system_functions: Vec<FunctionStatus>,
    },
    GetFunction {
        function: Vec<String>,
    },
    ListFunctions {
        database: Vec<String>,
        pattern: Option<String>,
        system_functions: Vec<FunctionStatus>,
    },
    DescribeFunction {
        function: Vec<String>,
        extended: bool,
        system_functions: Vec<FunctionStatus>,
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
    TruncateTable {
        table: Vec<String>,
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
            CatalogCommand::ShowFunctions { .. } => "ShowFunctions",
            CatalogCommand::ListTables { .. } => "ListTables",
            CatalogCommand::ListViews { .. } => "ListViews",
            CatalogCommand::DropTable { .. } => "DropTable",
            CatalogCommand::AlterTable { .. } => "AlterTable",
            CatalogCommand::RenameTable { .. } => "RenameTable",
            CatalogCommand::ListColumns { .. } => "ListColumns",
            CatalogCommand::FunctionExists { .. } => "FunctionExists",
            CatalogCommand::GetFunction { .. } => "GetFunction",
            CatalogCommand::ListFunctions { .. } => "ListFunctions",
            CatalogCommand::DescribeFunction { .. } => "DescribeFunction",
            CatalogCommand::RegisterFunction { .. } => "RegisterFunction",
            CatalogCommand::RegisterTableFunction { .. } => "RegisterTableFunction",
            CatalogCommand::DropFunction { .. } => "DropFunction",
            CatalogCommand::DropTemporaryView { .. } => "DropTemporaryView",
            CatalogCommand::DropView { .. } => "DropView",
            CatalogCommand::CreateTemporaryView { .. } => "CreateTemporaryView",
            CatalogCommand::CreateView { .. } => "CreateView",
            CatalogCommand::DescribeTable { .. } => "DescribeTable",
            CatalogCommand::TruncateTable { .. } => "TruncateTable",
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
            CatalogCommand::ShowFunctions { .. } => {
                ArrowSerializer::default().schema::<ShowFunctionsRow>()?
            }
            CatalogCommand::ListColumns { .. } => display.table_columns().schema()?,
            CatalogCommand::GetFunction { .. } | CatalogCommand::ListFunctions { .. } => {
                display.functions().schema()?
            }
            CatalogCommand::DescribeFunction { .. } => {
                ArrowSerializer::default().schema::<DescribeFunctionRow>()?
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
            | CatalogCommand::RenameTable { .. }
            | CatalogCommand::DropFunction { .. }
            | CatalogCommand::DropTemporaryView { .. }
            | CatalogCommand::DropView { .. }
            | CatalogCommand::TruncateTable { .. } => display.bools().schema()?,
        };
        Ok(schema)
    }

    pub async fn execute(
        self,
        ctx: &TaskContext,
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
                let existed_before = if options.mode.ignore_if_exists() {
                    match manager.get_table_or_view(&table).await {
                        Ok(_) => true,
                        Err(CatalogError::NotFound(_, _)) => false,
                        Err(e) => return Err(e),
                    }
                } else {
                    false
                };
                let Some((options, create_plan)) =
                    prepare_create_table_storage_metadata(ctx, manager, &table, options).await?
                else {
                    return Ok(display.bools().to_record_batch(vec![true])?);
                };
                let status = manager.create_table(&table, options).await?;
                if !existed_before {
                    prepare_created_table_storage_metadata(
                        ctx,
                        manager,
                        &table,
                        &status,
                        &create_plan,
                    )
                    .await?;
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
            CatalogCommand::ShowFunctions {
                database,
                pattern,
                system_functions,
                show_user_functions,
                show_system_functions,
            } => {
                let rows = manager
                    .list_functions(
                        &database,
                        pattern.as_deref(),
                        &system_functions,
                        show_user_functions,
                        show_system_functions,
                    )
                    .await?
                    .into_iter()
                    .map(|status| ShowFunctionsRow {
                        function: status.name,
                    })
                    .collect::<Vec<_>>();
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
            CatalogCommand::RenameTable {
                table,
                destination,
                if_exists,
            } => {
                match manager.rename_table(&table, &destination).await {
                    Err(CatalogError::NotFound(CatalogObject::Table, _)) if if_exists => {}
                    result => result?,
                };
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

                manager.validate_alter_table(&table, &options)?;
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
                if matches!(options, AlterTableOptions::AlterSnapshotReference { .. })
                    && (location.is_none()
                        || !format
                            .as_deref()
                            .is_some_and(|format| format.eq_ignore_ascii_case("iceberg")))
                {
                    return Err(CatalogError::NotSupported(
                        "Snapshot references require an Iceberg table with a location".into(),
                    ));
                }
                if format
                    .as_deref()
                    .is_some_and(|format| !is_lakehouse_format(format))
                    && let AlterTableOptions::AlterColumns { changes, .. } = &options
                    && changes.iter().any(|change| {
                        matches!(change, sail_common::spec::ColumnChange::Rename { .. })
                    })
                {
                    return Err(CatalogError::NotSupported(
                        "RENAME COLUMN requires a format that preserves column identities".into(),
                    ));
                }
                if format
                    .as_deref()
                    .is_some_and(|format| format.eq_ignore_ascii_case("csv"))
                    && match &options {
                        AlterTableOptions::ReplaceColumns { .. } => true,
                        AlterTableOptions::AlterColumns { changes, .. } => {
                            changes.iter().any(|change| {
                                matches!(
                                    change,
                                    sail_common::spec::ColumnChange::Add { .. }
                                        | sail_common::spec::ColumnChange::Drop { .. }
                                        | sail_common::spec::ColumnChange::Position { .. }
                                )
                            })
                        }
                        _ => false,
                    }
                {
                    return Err(CatalogError::NotSupported(
                        "CSV column additions, drops, reordering and replacement require rewriting existing files".into(),
                    ));
                }

                // Lake sources commit authoritative format metadata, including catalog
                // metadata when the provider supports an atomic update.
                if let (Some(location), Some(format)) = (location, format) {
                    // Non-lakehouse formats (e.g., plain Parquet/Hive tables) have no
                    // storage-layer metadata — the catalog is the sole source of truth.
                    // Skip straight to catalog-only update.
                    // This mirrors Spark+Delta where DeltaCatalog intercepts ALTER TABLE
                    // for Delta tables but plain tables fall through to SessionCatalog.
                    if matches!(
                        options,
                        AlterTableOptions::AlterIdentifierFields { .. }
                            | AlterTableOptions::SetWriteOrder { .. }
                            | AlterTableOptions::AlterPartitionSpec { .. }
                            | AlterTableOptions::SetPartitioning { .. }
                    ) && !format.eq_ignore_ascii_case("iceberg")
                    {
                        return Err(CatalogError::NotSupported(
                            "Identifier, partition and write order evolution require an Iceberg table".into(),
                        ));
                    }
                    if !is_lakehouse_format(&format) {
                        manager.alter_table(&table, options).await?;
                        return Ok(display.bools().to_record_batch(vec![true])?);
                    }
                    let registry = ctx.extension::<DataSourceRegistry>().map_err(|e| {
                        CatalogError::External(format!(
                            "missing DataSourceRegistry for storage-backed ALTER TABLE on format '{format}': {e}"
                        ))
                    })?;
                    let lake_source = registry.get_lake_source(&format).map_err(|e| {
                        CatalogError::External(format!(
                            "unknown lake source '{format}' for storage-backed ALTER TABLE: {e}"
                        ))
                    })?;
                    let storage_operation = (&options).into();
                    let lakehouse_table = manager
                        .resolve_lakehouse_table_status(
                            &table,
                            &table_status,
                            LakehouseOperation::Alter,
                        )
                        .await?
                        .execution;
                    if !matches!(options, AlterTableOptions::AlterSnapshotReference { .. })
                        && matches!(lakehouse_table.commit,
                        sail_common_datafusion::catalog::CommitAuthority::IcebergRestCommit
                        | sail_common_datafusion::catalog::CommitAuthority::VersionedCatalogCommit)
                    {
                        manager.alter_table(&table, options).await?;
                        return Ok(display.bools().to_record_batch(vec![true])?);
                    }
                    let result = lake_source
                        .alter_table(ctx, &location, storage_operation, Some(lakehouse_table))
                        .await
                        .map_err(|e| CatalogError::External(e.to_string()))?;

                    // Storage is the source of truth for lakehouse ALTER TABLE
                    // operations, but metadata reads still flow through the
                    // catalog. Surface catalog sync failures so callers do not
                    // observe successful storage mutation followed by stale
                    // DESCRIBE/SHOW metadata.
                    if !result.catalog_updated {
                        let mut catalog_options = catalog_sync_alter_options(&format, &options)?;
                        if let Some(schema) = result.schema {
                            if let AlterTableOptions::ReplaceColumns { fields, .. } =
                                &mut catalog_options
                            {
                                *fields = schema.fields().clone();
                            }
                            manager
                                .alter_table_with_schema(&table, catalog_options, schema)
                                .await?;
                        } else {
                            manager.alter_table(&table, catalog_options).await?;
                        }
                        if let Some(partition_by) = result.partition_by {
                            manager
                                .alter_table(
                                    &table,
                                    AlterTableOptions::SetPartitioning { partition_by },
                                )
                                .await?;
                        }
                        if !result.properties.is_empty() {
                            manager
                                .alter_table(
                                    &table,
                                    AlterTableOptions::SetTableProperties {
                                        properties: result.properties,
                                    },
                                )
                                .await?;
                        }
                        if !result.removed_properties.is_empty() {
                            manager
                                .alter_table(
                                    &table,
                                    AlterTableOptions::UnsetTableProperties {
                                        keys: result.removed_properties,
                                        if_exists: true,
                                    },
                                )
                                .await?;
                        }
                    }
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
            CatalogCommand::FunctionExists {
                function,
                system_functions,
            } => {
                let value = match manager
                    .get_function_status(&function, &system_functions)
                    .await
                {
                    Ok(_) => true,
                    Err(CatalogError::NotFound(_, _)) => false,
                    Err(e) => return Err(e),
                };
                display.bools().to_record_batch(vec![value])?
            }
            CatalogCommand::GetFunction { .. } => {
                return Err(CatalogError::NotSupported("get function".to_string()));
            }
            CatalogCommand::ListFunctions {
                database,
                pattern,
                system_functions,
            } => {
                let rows = manager
                    .list_functions(&database, pattern.as_deref(), &system_functions, true, true)
                    .await?;
                display.functions().to_record_batch(rows)?
            }
            CatalogCommand::DescribeFunction {
                function,
                extended,
                system_functions,
            } => {
                let status = manager
                    .get_function_status(&function, &system_functions)
                    .await?;
                let mut rows = vec![DescribeFunctionRow {
                    function_desc: format!("Function: {}", status.name),
                }];
                if !status.class_name.is_empty() {
                    rows.push(DescribeFunctionRow {
                        function_desc: format!("Class: {}", status.class_name),
                    });
                }
                rows.push(DescribeFunctionRow {
                    function_desc: format!("Usage: {}", status.usage()),
                });
                if extended {
                    rows.push(DescribeFunctionRow {
                        function_desc: format!("Extended Usage:{}", status.extended_usage()),
                    });
                }
                ArrowSerializer::default().build_record_batch(&rows)?
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
                    source: options.source,
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
            CatalogCommand::TruncateTable { table } => {
                // Resolve views first, as Spark does, so that a temporary view cannot
                // redirect the command to a table of the same name.
                let status = manager.get_table_or_view(&table).await?;
                let TableKind::Table {
                    location,
                    format,
                    partition_by,
                    is_external,
                    ..
                } = status.kind
                else {
                    return Err(CatalogError::NotSupported(format!(
                        "[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'TRUNCATE TABLE' expects a table but `{}` is a view.",
                        status.name
                    )));
                };
                // Like Spark's `TruncateTableCommand` (`CANNOT_TRUNCATE_EXTERNAL_TABLE`), the data
                // of an external table, which includes every table with a `LOCATION`, is never
                // deleted.
                if is_external {
                    return Err(CatalogError::NotSupported(format!(
                        "[CANNOT_TRUNCATE_EXTERNAL_TABLE] Operation not allowed: TRUNCATE TABLE on external tables: `{}`.",
                        status.name
                    )));
                }
                if is_lakehouse_format(&format) {
                    return Err(CatalogError::NotSupported(
                        "TRUNCATE TABLE is not supported for tables with \
                         transaction logs (Delta Lake, Iceberg). \
                         Use format-specific overwrite operations instead."
                            .to_string(),
                    ));
                }
                let partition_columns = partition_by
                    .iter()
                    .map(|field| field.column.clone())
                    .collect::<Vec<_>>();
                if let Some(ref loc) = location {
                    let Some(dir) = local_table_dir(loc)? else {
                        return Err(CatalogError::NotSupported(format!(
                            "TRUNCATE TABLE for non-local table location: {loc}"
                        )));
                    };
                    if dir.parent().is_none() {
                        return Err(CatalogError::External(format!(
                            "TRUNCATE TABLE on filesystem root is not allowed: {}",
                            dir.display()
                        )));
                    }
                    tokio::task::spawn_blocking(move || -> CatalogResult<()> {
                        // Like Spark, recreate the directory if it is gone.
                        if !dir.exists() {
                            return std::fs::create_dir_all(&dir).map_err(|e| {
                                CatalogError::External(format!(
                                    "failed to create table directory at {}: {e}",
                                    dir.display()
                                ))
                            });
                        }
                        let has_delta = dir.join("_delta_log").exists();
                        // Iceberg tables have a `metadata/version-hint.text` file
                        // (or `.iceberg` marker). A plain `metadata/` subdirectory
                        // is not sufficient — it could be user data.
                        let has_iceberg_meta =
                            dir.join("metadata").join("version-hint.text").exists();
                        let has_iceberg_dot = dir.join(".iceberg").exists();
                        if has_delta || has_iceberg_meta || has_iceberg_dot {
                            return Err(CatalogError::NotSupported(
                                "TRUNCATE TABLE is not supported for tables with \
                                 transaction logs (Delta Lake, Iceberg). \
                                 Use format-specific overwrite operations instead."
                                    .to_string(),
                            ));
                        }
                        // Like Spark's `TruncateTableCommand`, a partitioned table only
                        // loses its partition directories; otherwise the table
                        // directory is replaced by an empty one with the same
                        // permissions.
                        if partition_columns.is_empty() {
                            replace_with_empty_dir(&dir)
                        } else {
                            clear_partition_dirs(&dir, &partition_columns)
                        }
                        .map_err(|e| {
                            CatalogError::External(format!(
                                "failed to truncate table directory at {}: {e}",
                                dir.display()
                            ))
                        })
                    })
                    .await
                    .map_err(|e| {
                        CatalogError::External(format!("TRUNCATE TABLE task failed: {e}"))
                    })??;
                }
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

async fn prepare_create_table_storage_metadata<C: SessionExtensionAccessor>(
    ctx: &C,
    manager: &CatalogManager,
    table: &[String],
    mut options: CreateTableOptions,
) -> CatalogResult<Option<(CreateTableOptions, LakehouseCreatePlan)>> {
    match manager.get_table_or_view(table).await {
        Ok(_) if options.mode.ignore_if_exists() => return Ok(None),
        Ok(_) if !options.mode.is_replace() => {
            return Err(CatalogError::AlreadyExists(
                CatalogObject::Table,
                table.join("."),
            ));
        }
        Ok(_) => {}
        Err(CatalogError::NotFound(_, _)) if options.mode.replace_requires_existing() => {
            return Err(CatalogError::NotFound(
                CatalogObject::Table,
                table.join("."),
            ));
        }
        Err(CatalogError::NotFound(_, _)) => {}
        Err(err) => return Err(err),
    }

    let create_plan = manager
        .plan_lakehouse_create(
            table,
            LakehouseCreateRequest {
                catalog_table: table.to_vec(),
                options: options.clone(),
            },
        )
        .await?;

    let LakehouseCreateMaterialization::BeforeCatalogLakeSource { context, .. } =
        &create_plan.materialization
    else {
        return Ok(Some((options, create_plan)));
    };

    let location = options.location.clone().ok_or_else(|| {
        CatalogError::InvalidArgument(format!(
            "Location is required to create storage metadata for {} tables",
            options.format
        ))
    })?;
    let metadata = materialize_lake_source_create_metadata(
        ctx,
        &options.format,
        location,
        &options.columns,
        options.comment.clone(),
        options.partition_by.clone(),
        options.properties.clone(),
        options.mode.is_replace(),
        context.as_deref().cloned(),
    )
    .await?;
    if options.columns.is_empty() {
        if let Some(schema) = metadata.schema {
            options.columns = schema
                .fields()
                .iter()
                .map(|field| {
                    let features =
                        sail_common_datafusion::column_features::ColumnFeatures::from_field(field);
                    crate::provider::CreateTableColumnOptions {
                        name: field.name().clone(),
                        data_type: field.data_type().clone(),
                        nullable: field.is_nullable(),
                        comment: field
                            .metadata()
                            .get("comment")
                            .or_else(|| field.metadata().get("doc"))
                            .cloned(),
                        default: features.current_default(),
                        generated_always_as: features.generation_expression(),
                        identity: features.identity(),
                    }
                })
                .collect();
        }
        if let Some(partition_by) = metadata.partition_by {
            options.partition_by = partition_by;
        }
    }
    options.properties.extend(metadata.properties);
    Ok(Some((options, create_plan)))
}

async fn prepare_created_table_storage_metadata<C: SessionExtensionAccessor>(
    ctx: &C,
    manager: &CatalogManager,
    table: &[String],
    status: &sail_common_datafusion::catalog::TableStatus,
    create_plan: &LakehouseCreatePlan,
) -> CatalogResult<()> {
    let LakehouseCreateMaterialization::AfterCatalogLakeSource { mode, .. } =
        &create_plan.materialization
    else {
        return Ok(());
    };
    let sail_common_datafusion::catalog::TableKind::Table {
        columns,
        comment,
        location: Some(location),
        format,
        partition_by,
        properties,
        is_external,
        ..
    } = &status.kind
    else {
        return Ok(());
    };

    if matches!(
        *mode,
        crate::provider::LakeSourceCreateMetadataMode::PathManaged
    ) && *is_external
    {
        return Ok(());
    }
    let lakehouse_table = match *mode {
        crate::provider::LakeSourceCreateMetadataMode::PathManaged => None,
        crate::provider::LakeSourceCreateMetadataMode::CatalogCoordinated => Some(
            manager
                .resolve_lakehouse_table_status(
                    table,
                    status,
                    sail_common_datafusion::catalog::LakehouseOperation::Create,
                )
                .await?
                .execution,
        ),
    };

    materialize_lake_source_create_metadata(
        ctx,
        format,
        location.clone(),
        columns,
        comment.clone(),
        partition_by.clone(),
        properties.clone(),
        false,
        lakehouse_table,
    )
    .await
    .map(|_| ())
}

#[expect(clippy::too_many_arguments)]
async fn materialize_lake_source_create_metadata<C: SessionExtensionAccessor>(
    ctx: &C,
    format: &str,
    location: String,
    columns: &[impl CreateTableColumnView],
    comment: Option<String>,
    partition_by: Vec<sail_common_datafusion::catalog::CatalogPartitionField>,
    properties: Vec<(String, String)>,
    replace: bool,
    lakehouse_table: Option<sail_common_datafusion::catalog::LakehouseExecutionContext>,
) -> CatalogResult<LakeSourceCreateTableResult> {
    let registry = ctx.extension::<DataSourceRegistry>().map_err(|e| {
        CatalogError::External(format!(
            "missing DataSourceRegistry for CREATE TABLE on format '{format}': {e}"
        ))
    })?;
    let lake_source = registry.get_lake_source(format).map_err(|e| {
        CatalogError::External(format!(
            "unknown lake source '{format}' for CREATE TABLE: {e}"
        ))
    })?;
    lake_source
        .create_table_metadata(
            ctx.runtime_env(),
            LakeSourceCreateTableInfo {
                path: location,
                columns: columns
                    .iter()
                    .map(|column| LakeSourceCreateTableColumn {
                        name: column.name().to_string(),
                        data_type: column.data_type().clone(),
                        nullable: column.nullable(),
                        comment: column.comment().cloned(),
                        default: column.default().cloned(),
                        generated_always_as: column.generated_always_as().cloned(),
                        identity: column.identity().cloned(),
                    })
                    .collect(),
                comment,
                partition_by,
                properties,
                replace,
                lakehouse_table,
            },
        )
        .await
        .map_err(CatalogError::DataFusionError)
}

trait CreateTableColumnView {
    fn name(&self) -> &str;
    fn data_type(&self) -> &datafusion::arrow::datatypes::DataType;
    fn nullable(&self) -> bool;
    fn comment(&self) -> Option<&String>;
    fn default(&self) -> Option<&String>;
    fn generated_always_as(&self) -> Option<&String>;
    fn identity(&self) -> Option<&sail_common_datafusion::catalog::CatalogTableColumnIdentity>;
}

impl CreateTableColumnView for crate::provider::CreateTableColumnOptions {
    fn name(&self) -> &str {
        &self.name
    }

    fn data_type(&self) -> &datafusion::arrow::datatypes::DataType {
        &self.data_type
    }

    fn nullable(&self) -> bool {
        self.nullable
    }

    fn comment(&self) -> Option<&String> {
        self.comment.as_ref()
    }

    fn default(&self) -> Option<&String> {
        self.default.as_ref()
    }

    fn generated_always_as(&self) -> Option<&String> {
        self.generated_always_as.as_ref()
    }

    fn identity(&self) -> Option<&sail_common_datafusion::catalog::CatalogTableColumnIdentity> {
        self.identity.as_ref()
    }
}

impl CreateTableColumnView for sail_common_datafusion::catalog::TableColumnStatus {
    fn name(&self) -> &str {
        &self.name
    }

    fn data_type(&self) -> &datafusion::arrow::datatypes::DataType {
        &self.data_type
    }

    fn nullable(&self) -> bool {
        self.nullable
    }

    fn comment(&self) -> Option<&String> {
        self.comment.as_ref()
    }

    fn default(&self) -> Option<&String> {
        self.default.as_ref()
    }

    fn generated_always_as(&self) -> Option<&String> {
        self.generated_always_as.as_ref()
    }

    fn identity(&self) -> Option<&sail_common_datafusion::catalog::CatalogTableColumnIdentity> {
        self.identity.as_ref()
    }
}

/// Resolves a table location to the local directory that Sail's own table I/O reads and writes:
/// a `file:` URI is converted to a path (percent-decoded, `localhost` authority accepted), a bare
/// path is taken literally, and a relative path is resolved against the current directory.
/// A location with any other scheme is not local.
fn local_table_dir(location: &str) -> CatalogResult<Option<std::path::PathBuf>> {
    if location
        .get(..5)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("file:"))
    {
        let path = url::Url::parse(location)
            .ok()
            .and_then(|url| url.to_file_path().ok());
        return match path {
            Some(path) => Ok(Some(path)),
            None => Err(CatalogError::External(format!(
                "TRUNCATE TABLE cannot resolve the table location {location} to a local path"
            ))),
        };
    }
    if location.split_once(':').is_some_and(|(scheme, _)| {
        scheme.len() > 1
            && scheme.starts_with(|c: char| c.is_ascii_alphabetic())
            && scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
    }) {
        return Ok(None);
    }
    if location.is_empty() {
        return Err(CatalogError::External(
            "TRUNCATE TABLE requires a non-empty table location".to_string(),
        ));
    }
    let path = std::path::PathBuf::from(location);
    if path.is_absolute() {
        return Ok(Some(path));
    }
    let cwd = std::env::current_dir().map_err(|e| {
        CatalogError::External(format!("TRUNCATE TABLE cannot resolve table location: {e}"))
    })?;
    Ok(Some(cwd.join(path)))
}

/// Empties the partition directories of a Hive-partitioned table (`col=value/...`, one level per
/// partition column) and leaves everything else under `dir` in place, as Spark's
/// `TruncateTableCommand` does with the partition locations it gets from the catalog. Each
/// partition directory is replaced by an empty one, so a symlinked partition becomes a plain
/// directory and its target is left alone.
fn clear_partition_dirs(dir: &std::path::Path, columns: &[String]) -> std::io::Result<()> {
    let Some((column, rest)) = columns.split_first() else {
        return replace_with_empty_dir(dir);
    };
    let prefix = format!("{column}=");
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        // `Path::is_dir` follows symlinks, as the table reads do.
        let is_partition = path.is_dir()
            && entry
                .file_name()
                .to_str()
                .and_then(|n| n.get(..prefix.len()))
                .is_some_and(|head| head.eq_ignore_ascii_case(&prefix));
        if is_partition {
            clear_partition_dirs(&path, rest)?;
        }
    }
    Ok(())
}

/// Replaces `dir` with an empty directory that has the same permissions. The old contents are
/// only deleted once the new directory is in place, so a failure leaves the data where it was.
fn replace_with_empty_dir(dir: &std::path::Path) -> std::io::Result<()> {
    let permissions = std::fs::metadata(dir)?.permissions();
    let backup = {
        let name = dir.file_name().unwrap_or_default().to_string_lossy();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        dir.with_file_name(format!(".{name}.truncate-{}-{nanos}", std::process::id()))
    };
    std::fs::rename(dir, &backup)?;
    let created =
        std::fs::create_dir(dir).and_then(|()| std::fs::set_permissions(dir, permissions));
    if let Err(e) = created {
        let _ = std::fs::remove_dir(dir);
        std::fs::rename(&backup, dir)?;
        return Err(e);
    }
    std::fs::remove_dir_all(&backup)
}

fn catalog_sync_alter_options(
    format: &str,
    options: &AlterTableOptions,
) -> CatalogResult<AlterTableOptions> {
    match options {
        AlterTableOptions::UnsetTableProperties { keys, .. } => {
            // Existence was checked against the format metadata before committing.
            Ok(AlterTableOptions::UnsetTableProperties {
                keys: keys.clone(),
                if_exists: true,
            })
        }
        AlterTableOptions::DropCheckConstraint { name, .. } => {
            Ok(AlterTableOptions::UnsetTableProperties {
                keys: vec![format!("delta.constraints.{}", name.to_lowercase())],
                if_exists: true,
            })
        }
        AlterTableOptions::AddCheckConstraint { name, expression } => {
            if !format.eq_ignore_ascii_case("delta") {
                return Err(CatalogError::NotSupported(format!(
                    "CHECK constraints are not supported for {format} tables"
                )));
            }
            Ok(AlterTableOptions::SetTableProperties {
                properties: vec![(format!("delta.constraints.{name}"), expression.clone())],
            })
        }
        _ => Ok(options.clone()),
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

#[derive(Serialize, Deserialize)]
struct ShowFunctionsRow {
    function: String,
}

#[derive(Serialize, Deserialize)]
struct DescribeFunctionRow {
    function_desc: String,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion::catalog::Session;
    use datafusion::execution::context::SessionConfig;
    use datafusion::prelude::SessionContext;
    use datafusion_common::not_impl_err;
    use datafusion_expr::{LogicalPlan, TableSource};
    use sail_common_datafusion::catalog::display::{CatalogObjectDisplay, DefaultCatalogDisplay};
    use sail_common_datafusion::catalog::{
        DatabaseStatus, FunctionStatus, TableColumnStatus, TableKind, TableStatus,
    };
    use sail_common_datafusion::datasource::{DataSource, SinkInfo, SourceInfo};
    use sail_common_datafusion::lakesource::{LakeSource, LakeSourceAlterTableOperation};
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

        fn function(status: FunctionStatus) -> Self::Function {
            TestCatalogOutput { value: status.name }
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
        alter_error: Option<String>,
        table_exists: bool,
        view_lookup_supported: bool,
    }

    #[async_trait]
    impl CatalogProvider for TestProvider {
        fn get_name(&self) -> &str {
            "test"
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
            _database: &crate::provider::Namespace,
        ) -> CatalogResult<sail_common_datafusion::catalog::DatabaseStatus> {
            unreachable!()
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
            unreachable!()
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
                    CatalogObject::Table,
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
            unreachable!()
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
            view: &str,
        ) -> CatalogResult<TableStatus> {
            if self.view_lookup_supported {
                Err(CatalogError::NotFound(
                    CatalogObject::View,
                    view.to_string(),
                ))
            } else {
                Err(CatalogError::NotSupported(
                    "persistent views are not supported".to_string(),
                ))
            }
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

    struct TestLakeSource;

    #[async_trait]
    impl DataSource for TestLakeSource {
        fn name(&self) -> &str {
            "delta"
        }

        fn as_lake_source(self: Arc<Self>) -> Option<Arc<dyn LakeSource>> {
            Some(self)
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
        ) -> datafusion_common::Result<LogicalPlan> {
            not_impl_err!("unused in test")
        }
    }

    #[async_trait]
    impl LakeSource for TestLakeSource {
        async fn alter_table(
            &self,
            _ctx: &datafusion::execution::TaskContext,
            _path: &str,
            _operation: LakeSourceAlterTableOperation,
            _lakehouse_table: Option<sail_common_datafusion::catalog::LakehouseExecutionContext>,
        ) -> datafusion_common::Result<sail_common_datafusion::lakesource::LakeSourceAlterTableResult>
        {
            Ok(Default::default())
        }
    }

    fn test_session_context() -> SessionContext {
        let registry = Arc::new(DataSourceRegistry::new());
        let register_result = registry.register_data_source(Arc::new(TestLakeSource));
        assert!(
            register_result.is_ok(),
            "failed to register test lake source: {register_result:?}"
        );
        let plan_service = Arc::new(PlanService::new(
            Box::new(DefaultCatalogDisplay::<TestCatalogObjectDisplay>::default()),
            Box::new(TestPlanFormatter),
        ));
        let config = SessionConfig::new()
            .with_extension(registry)
            .with_extension(plan_service);
        SessionContext::new_with_config(config)
    }

    fn test_manager(alter_error: Option<&str>) -> CatalogManager {
        test_manager_with_catalog_behavior(alter_error, true, false)
    }

    fn test_manager_with_catalog_behavior(
        alter_error: Option<&str>,
        table_exists: bool,
        view_lookup_supported: bool,
    ) -> CatalogManager {
        let table_status = TableStatus {
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
                    identity: None,
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
        };
        let manager = CatalogManager::try_new(crate::manager::CatalogManagerOptions {
            catalogs: std::iter::once((
                "test".to_string(),
                Arc::new(TestProvider {
                    table_status,
                    alter_error: alter_error.map(ToString::to_string),
                    table_exists,
                    view_lookup_supported,
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

    #[tokio::test]
    async fn table_or_view_lookup_treats_unsupported_persistent_views_as_missing() {
        let manager = test_manager_with_catalog_behavior(None, false, false);

        let result = manager.get_table_or_view(&["missing"]).await;
        assert!(
            result.is_err(),
            "expected table-or-view lookup to report missing relation, got success: {result:?}"
        );
        let Err(error) = result else {
            unreachable!();
        };

        assert!(
            matches!(
                error,
                CatalogError::NotFound(CatalogObject::Table, ref message)
                    if message.contains("[TABLE_OR_VIEW_NOT_FOUND]")
                        && message.contains("missing")
            ),
            "unexpected table-or-view error: {error:?}"
        );

        let result = manager.get_view(&["missing"]).await;
        assert!(
            result.is_err(),
            "expected explicit view lookup to stay unsupported, got success: {result:?}"
        );
        let Err(error) = result else {
            unreachable!();
        };
        assert!(
            matches!(error, CatalogError::NotSupported(ref message) if message.contains("persistent views")),
            "unexpected explicit view error: {error:?}"
        );
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

        let result = command.execute(ctx.task_ctx().as_ref(), &manager).await;
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
    #[test]
    fn test_local_table_dir_converts_file_uris_only() {
        let dir = |location: &str| local_table_dir(location).ok().flatten();
        let spaced = Some(std::path::PathBuf::from("/tmp/a b"));
        assert_eq!(dir("file:///tmp/a%20b"), spaced);
        assert_eq!(dir("FILE:///tmp/a%20b"), spaced);
        assert_eq!(dir("file:/tmp/a%20b"), spaced);
        // `localhost` is an empty authority, as in the table I/O
        assert_eq!(dir("file://localhost/tmp/a%20b"), spaced);
        // a bare path is taken literally, so a real `%` is preserved
        assert_eq!(
            dir("/tmp/a%20b"),
            Some(std::path::PathBuf::from("/tmp/a%20b"))
        );
    }

    #[test]
    fn test_local_table_dir_resolves_relative_paths_against_cwd() {
        let cwd = std::env::current_dir().ok();
        assert!(cwd.is_some());
        let expected = cwd.map(|cwd| cwd.join("spark-warehouse/t"));
        assert_eq!(
            local_table_dir("spark-warehouse/t").ok().flatten(),
            expected
        );
    }

    #[test]
    fn test_local_table_dir_rejects_other_schemes_and_bad_locations() {
        for location in ["s3://bucket/key", "memory:///t", "hdfs://nn/path"] {
            assert!(matches!(local_table_dir(location), Ok(None)), "{location}");
        }
        for location in ["", "file://otherhost/tmp/t"] {
            assert!(local_table_dir(location).is_err(), "{location}");
        }
    }
}
