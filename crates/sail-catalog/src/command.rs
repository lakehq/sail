use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::runtime_env::RuntimeEnv;
use object_store::path::Path as ObjectStorePath;
use object_store::ObjectStore;
use sail_common_datafusion::array::serde::ArrowSerializer;
use sail_common_datafusion::catalog::TableKind;
use sail_common_datafusion::datasource::{is_lakehouse_format, TableFormatRegistry};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::error::{CatalogError, CatalogResult};
use crate::manager::tracker::{CatalogFunctionId, CatalogLogicalPlanId};
use crate::manager::CatalogManager;
use crate::provider::{
    AlterTableOptions, CreateDatabaseOptions, CreatePartitionsOptions, CreateTableOptions,
    CreateTemporaryViewOptions, CreateViewOptions, DropDatabaseOptions, DropTableOptions,
    DropTemporaryViewOptions, DropViewOptions, GetPartitionsOptions, PartitionStatus,
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
    RecoverPartitions {
        table: Vec<String>,
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
            CatalogCommand::RecoverPartitions { .. } => "RecoverPartitions",
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
            | CatalogCommand::RecoverPartitions { .. }
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
                    if is_lakehouse_format(&format) {
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
                        let (changes, if_exists_flag) = match &options {
                            AlterTableOptions::SetTableProperties { properties } => (
                                properties
                                    .iter()
                                    .map(|(k, v)| (k.clone(), Some(v.clone())))
                                    .collect::<Vec<_>>(),
                                false,
                            ),
                            AlterTableOptions::UnsetTableProperties { keys, if_exists } => (
                                keys.iter().map(|k| (k.clone(), None)).collect::<Vec<_>>(),
                                *if_exists,
                            ),
                            AlterTableOptions::SetLocation { .. } => (vec![], false),
                        };
                        if !changes.is_empty() {
                            table_format
                                .alter_table_properties(runtime, &location, changes, if_exists_flag)
                                .await
                                .map_err(|e| CatalogError::External(e.to_string()))?;
                        }
                    }
                }

                manager.alter_table(&table, options).await?;
                display.bools().to_record_batch(vec![true])?
            }
            CatalogCommand::RecoverPartitions { table } => {
                recover_table_partitions(ctx, manager, &table).await?;
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

async fn recover_table_partitions<C: SessionExtensionAccessor>(
    ctx: &C,
    manager: &CatalogManager,
    table: &[String],
) -> CatalogResult<()> {
    let table_status = manager.get_table(table).await?;
    let (location, partition_columns) = match table_status.kind {
        TableKind::Table {
            location: Some(location),
            partition_by,
            ..
        } if !partition_by.is_empty() => (
            location,
            partition_by
                .into_iter()
                .map(|field| field.column)
                .collect::<Vec<_>>(),
        ),
        _ => return Ok(()),
    };
    let discovered = if let Some((store, root)) =
        object_store_root_from_table_location(ctx.runtime_env().as_ref(), &location)?
    {
        discover_object_store_partition_directories(
            store.as_ref(),
            &root,
            &location,
            &partition_columns,
        )
        .await?
    } else if let Some(root) = local_path_from_table_location(&location) {
        discover_partition_directories(&root, &location, &partition_columns)?
    } else {
        return Ok(());
    };
    if discovered.is_empty() {
        return Ok(());
    }

    let existing_partitions = match manager
        .get_partitions(table, GetPartitionsOptions::default())
        .await
    {
        Ok(partitions) => partitions,
        Err(CatalogError::NotSupported(_)) => return Ok(()),
        Err(e) => return Err(e),
    };
    let existing = existing_partitions
        .into_iter()
        .map(|partition| canonical_partition_key(&partition.spec, &partition_columns))
        .collect::<CatalogResult<HashSet<_>>>()?;
    let partitions = discovered
        .into_iter()
        .filter_map(|partition| {
            let key = canonical_partition_key(&partition.spec, &partition_columns).ok()?;
            (!existing.contains(&key)).then_some(partition)
        })
        .collect::<Vec<_>>();
    if partitions.is_empty() {
        return Ok(());
    }

    match manager
        .create_partitions(
            table,
            partitions,
            CreatePartitionsOptions {
                ignore_if_exists: true,
            },
        )
        .await
    {
        Ok(()) | Err(CatalogError::NotSupported(_)) => Ok(()),
        Err(e) => Err(e),
    }
}

fn object_store_root_from_table_location(
    runtime: &RuntimeEnv,
    location: &str,
) -> CatalogResult<Option<(Arc<dyn ObjectStore>, ObjectStorePath)>> {
    if location.starts_with("file:") || !location.contains("://") {
        return Ok(None);
    }
    let url = Url::parse(location)
        .map_err(|e| CatalogError::External(format!("Invalid table location '{location}': {e}")))?;
    let store = runtime.object_store(UrlRef(&url)).map_err(|e| {
        CatalogError::External(format!(
            "Failed to resolve object store for table location '{location}': {e}"
        ))
    })?;
    let root = ObjectStorePath::from_url_path(url.path()).map_err(|e| {
        CatalogError::External(format!(
            "Invalid object store path for table location '{location}': {e}"
        ))
    })?;
    Ok(Some((store, root)))
}

struct UrlRef<'a>(&'a Url);

impl AsRef<Url> for UrlRef<'_> {
    fn as_ref(&self) -> &Url {
        self.0
    }
}

async fn discover_object_store_partition_directories(
    store: &dyn ObjectStore,
    root: &ObjectStorePath,
    table_location: &str,
    partition_columns: &[String],
) -> CatalogResult<Vec<PartitionStatus>> {
    let mut partitions = Vec::new();
    let mut stack = vec![(root.clone(), Vec::new())];
    let table_location = table_location.trim_end_matches('/');

    while let Some((dir, spec)) = stack.pop() {
        if spec.len() == partition_columns.len() {
            partitions.push(PartitionStatus {
                spec,
                location: Some(format!(
                    "{}/{}",
                    table_location,
                    relative_object_store_partition_location(&dir, root)?
                )),
                parameters: HashMap::new(),
                create_time: None,
                last_access_time: None,
                statistics: None,
            });
            continue;
        }

        let expected_column = &partition_columns[spec.len()];
        let listing = store.list_with_delimiter(Some(&dir)).await.map_err(|e| {
            CatalogError::External(format!(
                "Failed to list partition directory '{}': {e}",
                dir.as_ref()
            ))
        })?;
        for child in listing.common_prefixes {
            let Some(segment) = child.filename() else {
                continue;
            };
            let Some((column, value)) = segment.split_once('=') else {
                continue;
            };
            if !column.eq_ignore_ascii_case(expected_column) {
                continue;
            }
            let mut spec = spec.clone();
            spec.push((
                expected_column.clone(),
                unescape_partition_path_name(value)?,
            ));
            stack.push((child, spec));
        }
    }

    Ok(partitions)
}

fn relative_object_store_partition_location(
    dir: &ObjectStorePath,
    root: &ObjectStorePath,
) -> CatalogResult<String> {
    let segments = dir
        .prefix_match(root)
        .ok_or_else(|| {
            CatalogError::External(format!(
                "Partition directory '{}' is not under table location '{}'",
                dir.as_ref(),
                root.as_ref()
            ))
        })?
        .map(|part| part.as_ref().to_string())
        .collect::<Vec<_>>();
    Ok(segments.join("/"))
}

fn discover_partition_directories(
    root: &Path,
    table_location: &str,
    partition_columns: &[String],
) -> CatalogResult<Vec<PartitionStatus>> {
    let mut partitions = Vec::new();
    discover_partition_directories_inner(
        root,
        table_location.trim_end_matches('/'),
        partition_columns,
        Vec::new(),
        &mut partitions,
    )?;
    Ok(partitions)
}

fn discover_partition_directories_inner(
    dir: &Path,
    table_location: &str,
    partition_columns: &[String],
    spec: Vec<(String, String)>,
    partitions: &mut Vec<PartitionStatus>,
) -> CatalogResult<()> {
    if spec.len() == partition_columns.len() {
        partitions.push(PartitionStatus {
            spec,
            location: Some(format!(
                "{}/{}",
                table_location,
                relative_partition_location(dir, partition_columns.len())?
            )),
            parameters: HashMap::new(),
            create_time: None,
            last_access_time: None,
            statistics: None,
        });
        return Ok(());
    }

    let expected_column = &partition_columns[spec.len()];
    let entries = std::fs::read_dir(dir).map_err(|e| {
        CatalogError::External(format!(
            "Failed to list partition directory '{}': {e}",
            dir.display()
        ))
    })?;
    for entry in entries {
        let entry = entry.map_err(|e| {
            CatalogError::External(format!(
                "Failed to read partition directory entry '{}': {e}",
                dir.display()
            ))
        })?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(segment) = path.file_name().and_then(|x| x.to_str()) else {
            continue;
        };
        let Some((column, value)) = segment.split_once('=') else {
            continue;
        };
        if !column.eq_ignore_ascii_case(expected_column) {
            continue;
        }
        let mut spec = spec.clone();
        spec.push((
            expected_column.clone(),
            unescape_partition_path_name(value)?,
        ));
        discover_partition_directories_inner(
            &path,
            table_location,
            partition_columns,
            spec,
            partitions,
        )?;
    }
    Ok(())
}

fn relative_partition_location(dir: &Path, partition_depth: usize) -> CatalogResult<String> {
    let segments = dir
        .components()
        .rev()
        .take(partition_depth)
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    if segments.len() != partition_depth {
        return Err(CatalogError::External(format!(
            "Partition directory '{}' is shallower than partition depth {partition_depth}",
            dir.display()
        )));
    }
    Ok(segments.into_iter().rev().collect::<Vec<_>>().join("/"))
}

fn canonical_partition_key(
    spec: &[(String, String)],
    partition_columns: &[String],
) -> CatalogResult<Vec<String>> {
    partition_columns
        .iter()
        .map(|column| {
            let matches = spec
                .iter()
                .filter(|(key, _)| key.eq_ignore_ascii_case(column))
                .collect::<Vec<_>>();
            match matches.as_slice() {
                [(_, value)] => Ok(value.clone()),
                [] => Err(CatalogError::External(format!(
                    "Partition spec is missing column '{column}'"
                ))),
                _ => Err(CatalogError::External(format!(
                    "Partition spec has duplicate column '{column}'"
                ))),
            }
        })
        .collect()
}

fn local_path_from_table_location(location: &str) -> Option<PathBuf> {
    let path = if let Some(path) = location.strip_prefix("file://") {
        path.strip_prefix("localhost").unwrap_or(path)
    } else if let Some(path) = location.strip_prefix("file:") {
        path
    } else if !location.contains("://") {
        location
    } else {
        return None;
    };
    Some(PathBuf::from(path))
}

fn unescape_partition_path_name(value: &str) -> CatalogResult<String> {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(CatalogError::External(format!(
                    "Invalid partition path escape in '{value}'"
                )));
            }
            let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).map_err(|e| {
                CatalogError::External(format!("Invalid partition path escape in '{value}': {e}"))
            })?;
            let byte = u8::from_str_radix(hex, 16).map_err(|e| {
                CatalogError::External(format!("Invalid partition path escape in '{value}': {e}"))
            })?;
            output.push(byte);
            index += 3;
        } else {
            output.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(output).map_err(|e| {
        CatalogError::External(format!(
            "Invalid UTF-8 partition path segment '{value}': {e}"
        ))
    })
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

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutPayload};

    use super::*;

    #[tokio::test]
    async fn discovers_partitions_from_object_store_prefixes() -> CatalogResult<()> {
        let store = InMemory::new();
        store
            .put(
                &ObjectStorePath::parse("warehouse/table/region=a%2Fb/part-0.parquet")
                    .map_err(|e| CatalogError::External(e.to_string()))?,
                PutPayload::from_static(b"one"),
            )
            .await
            .map_err(|e| CatalogError::External(e.to_string()))?;
        store
            .put(
                &ObjectStorePath::parse("warehouse/table/region=north/part-0.parquet")
                    .map_err(|e| CatalogError::External(e.to_string()))?,
                PutPayload::from_static(b"two"),
            )
            .await
            .map_err(|e| CatalogError::External(e.to_string()))?;

        let root = ObjectStorePath::parse("warehouse/table")
            .map_err(|e| CatalogError::External(e.to_string()))?;
        let mut partitions = discover_object_store_partition_directories(
            &store,
            &root,
            "s3://bucket/warehouse/table",
            &["region".to_string()],
        )
        .await?;
        partitions.sort_by(|a, b| a.spec.cmp(&b.spec));

        let actual = partitions
            .into_iter()
            .map(|partition| (partition.spec, partition.location))
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![
                (
                    vec![("region".to_string(), "a/b".to_string())],
                    Some("s3://bucket/warehouse/table/region=a%2Fb".to_string()),
                ),
                (
                    vec![("region".to_string(), "north".to_string())],
                    Some("s3://bucket/warehouse/table/region=north".to_string()),
                ),
            ]
        );
        Ok(())
    }
}
