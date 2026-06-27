use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use sail_common_datafusion::array::serde::ArrowSerializer;
use sail_common_datafusion::catalog::LakehouseOperation;
use sail_common_datafusion::datasource::{
    is_lakehouse_format, TableFormatAlterTableOperation, TableFormatCreateTableColumn,
    TableFormatCreateTableInfo, TableFormatRegistry,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use serde::{Deserialize, Serialize};

use crate::error::{CatalogError, CatalogObject, CatalogResult};
use crate::lakehouse::{
    LakehouseCreateMaterialization, LakehouseCreatePlan, LakehouseCreateRequest,
};
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
    ShowFunctions {
        database: Vec<String>,
        pattern: Option<String>,
        system_functions: Vec<String>,
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
        system_functions: Vec<String>,
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
            CatalogCommand::ShowFunctions { .. } => "ShowFunctions",
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
            CatalogCommand::ShowFunctions { .. } => {
                ArrowSerializer::default().schema::<ShowFunctionsRow>()?
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
            } => {
                let rows = manager
                    .list_functions(&database, pattern.as_deref(), &system_functions)
                    .await?
                    .into_iter()
                    .map(|function| ShowFunctionsRow { function })
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
                    let storage_operation = table_format_alter_operation(&options);
                    let lakehouse_table = manager
                        .resolve_lakehouse_table_status(
                            &table,
                            &table_status,
                            LakehouseOperation::Alter,
                        )
                        .await?
                        .execution;
                    table_format
                        .alter_table(runtime, &location, storage_operation, Some(lakehouse_table))
                        .await
                        .map_err(|e| CatalogError::External(e.to_string()))?;

                    // Storage is the source of truth for lakehouse ALTER TABLE
                    // operations, but metadata reads still flow through the
                    // catalog. Surface catalog sync failures so callers do not
                    // observe successful storage mutation followed by stale
                    // DESCRIBE/SHOW metadata.
                    let catalog_options = catalog_sync_alter_options(&format, &options)?;
                    manager.alter_table(&table, catalog_options).await?;
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
            CatalogCommand::ListFunctions {
                database,
                pattern,
                system_functions,
            } => {
                let rows = manager
                    .list_functions(&database, pattern.as_deref(), &system_functions)
                    .await?;
                display.functions().to_record_batch(rows)?
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

    let LakehouseCreateMaterialization::BeforeCatalogTableFormat { context, .. } =
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
    let metadata = materialize_table_format_create_metadata(
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
    let LakehouseCreateMaterialization::AfterCatalogTableFormat { mode, .. } =
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
        crate::provider::TableFormatCreateMetadataMode::PathManaged
    ) && *is_external
    {
        return Ok(());
    }
    let lakehouse_table = match *mode {
        crate::provider::TableFormatCreateMetadataMode::PathManaged => None,
        crate::provider::TableFormatCreateMetadataMode::CatalogCoordinated => Some(
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

    materialize_table_format_create_metadata(
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
async fn materialize_table_format_create_metadata<C: SessionExtensionAccessor>(
    ctx: &C,
    format: &str,
    location: String,
    columns: &[impl CreateTableColumnView],
    comment: Option<String>,
    partition_by: Vec<sail_common_datafusion::catalog::CatalogPartitionField>,
    properties: Vec<(String, String)>,
    replace: bool,
    lakehouse_table: Option<sail_common_datafusion::catalog::LakehouseExecutionContext>,
) -> CatalogResult<sail_common_datafusion::datasource::TableFormatCreateTableResult> {
    let registry = ctx.extension::<TableFormatRegistry>().map_err(|e| {
        CatalogError::External(format!(
            "missing TableFormatRegistry for CREATE TABLE on format '{format}': {e}"
        ))
    })?;
    let table_format = registry.get(format).map_err(|e| {
        CatalogError::External(format!(
            "unknown table format '{format}' for CREATE TABLE: {e}"
        ))
    })?;
    table_format
        .create_table_metadata(
            ctx.runtime_env(),
            TableFormatCreateTableInfo {
                path: location,
                columns: columns
                    .iter()
                    .map(|column| TableFormatCreateTableColumn {
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

fn table_format_alter_operation(options: &AlterTableOptions) -> TableFormatAlterTableOperation {
    match options {
        AlterTableOptions::SetTableProperties { properties } => {
            TableFormatAlterTableOperation::SetTableProperties {
                changes: properties
                    .iter()
                    .map(|(key, value)| (key.clone(), Some(value.clone())))
                    .collect(),
                if_exists: false,
            }
        }
        AlterTableOptions::UnsetTableProperties { keys, if_exists } => {
            TableFormatAlterTableOperation::SetTableProperties {
                changes: keys.iter().map(|key| (key.clone(), None)).collect(),
                if_exists: *if_exists,
            }
        }
        AlterTableOptions::AlterColumnType { name, data_type } => {
            TableFormatAlterTableOperation::AlterColumnType {
                column_path: name.clone(),
                data_type: data_type.clone(),
            }
        }
        AlterTableOptions::AlterColumnDefault { name, default } => {
            TableFormatAlterTableOperation::AlterColumnDefault {
                column_path: name.clone(),
                default: default.clone(),
            }
        }
        AlterTableOptions::AddCheckConstraint { name, expression } => {
            TableFormatAlterTableOperation::AddCheckConstraint {
                name: name.clone(),
                expression: expression.clone(),
            }
        }
    }
}

fn catalog_sync_alter_options(
    format: &str,
    options: &AlterTableOptions,
) -> CatalogResult<AlterTableOptions> {
    match options {
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

    struct TestTableFormat;

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
        ) -> datafusion_common::Result<LogicalPlan> {
            not_impl_err!("unused in test")
        }

        async fn alter_table(
            &self,
            _runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
            _path: &str,
            _operation: TableFormatAlterTableOperation,
            _lakehouse_table: Option<sail_common_datafusion::catalog::LakehouseExecutionContext>,
        ) -> datafusion_common::Result<()> {
            Ok(())
        }
    }

    fn test_session_context() -> SessionContext {
        let registry = Arc::new(TableFormatRegistry::new());
        let register_result = registry.register(Arc::new(TestTableFormat));
        assert!(
            register_result.is_ok(),
            "failed to register test table format: {register_result:?}"
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
