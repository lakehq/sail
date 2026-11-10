use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::datatypes::{Schema, SchemaRef};
use dashmap::{DashMap, Entry};
use sail_catalog::error::{CatalogError, CatalogObject, CatalogResult};
use sail_catalog::provider::{
    AlterTableOptions, CatalogProvider, CreateDatabaseOptions, CreateTableColumnOptions,
    CreateTableMetadataRequirement, CreateTableMode, CreateTableOptions, CreateViewColumnOptions,
    CreateViewOptions, DropDatabaseOptions, DropTableOptions, DropViewOptions, Namespace,
    plain_lakehouse_create_table_metadata_requirement,
};
use sail_catalog::utils::quote_namespace_if_needed;
use sail_common_datafusion::catalog::{
    CatalogPartitionField, DatabaseStatus, TableColumnStatus, TableKind, TableReplacement,
    TableStatus, alter_column_default, alter_column_type,
};
use sail_common_datafusion::column_features::ColumnFeatures;
use tokio::sync::RwLock;

use crate::managed_table;

mod replacement;

static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

fn next_generation() -> u64 {
    NEXT_GENERATION.fetch_add(1, Ordering::Relaxed)
}

struct MemoryDatabase {
    generation: u64,
    table_generations: HashMap<String, u64>,
    uncertain_tables: HashSet<String>,
    status: DatabaseStatus,
    tables: HashMap<String, TableStatus>,
    views: HashMap<String, TableStatus>,
}

impl MemoryDatabase {
    fn check_known(&self, table: &str) -> CatalogResult<()> {
        if self.uncertain_tables.contains(table) {
            return Err(CatalogError::CommitStateUnknown(
                format!(
                    "replacement of {table} requires storage reconciliation before catalog access"
                )
                .into(),
            ));
        }
        Ok(())
    }

    fn changed(&mut self, table: &str) {
        self.table_generations
            .insert(table.to_string(), next_generation());
    }
}

/// An in-memory catalog provider.
pub struct MemoryCatalogProvider {
    name: String,
    default_namespace: Namespace,
    databases: Arc<DashMap<Namespace, MemoryDatabase>>,
    generation: u64,
    namespace_gates: DashMap<Namespace, Arc<RwLock<()>>>,
    table_gates: DashMap<(Namespace, String), Arc<RwLock<()>>>,
}

impl MemoryCatalogProvider {
    pub fn new(
        name: String,
        initial_database: Namespace,
        initial_database_comment: Option<String>,
    ) -> Self {
        let databases = DashMap::new();
        databases.insert(
            initial_database.clone(),
            MemoryDatabase {
                generation: next_generation(),
                table_generations: HashMap::new(),
                uncertain_tables: HashSet::new(),
                status: DatabaseStatus {
                    catalog: name.clone(),
                    database: initial_database.clone().into(),
                    comment: initial_database_comment,
                    location: None,
                    properties: vec![],
                },
                tables: HashMap::new(),
                views: HashMap::new(),
            },
        );
        Self {
            name,
            databases: Arc::new(databases),
            generation: next_generation(),
            namespace_gates: DashMap::new(),
            table_gates: DashMap::new(),
            default_namespace: initial_database,
        }
    }

    fn namespace_gate(&self, database: &Namespace) -> Arc<RwLock<()>> {
        self.namespace_gates
            .entry(database.clone())
            .or_default()
            .clone()
    }

    fn table_gate(&self, database: &Namespace, table: &str) -> Arc<RwLock<()>> {
        self.table_gates
            .entry((database.clone(), table.to_string()))
            .or_default()
            .clone()
    }

    async fn table_access(
        &self,
        database: &Namespace,
        table: &str,
    ) -> (
        tokio::sync::OwnedRwLockReadGuard<()>,
        tokio::sync::OwnedRwLockReadGuard<()>,
    ) {
        let namespace = self.namespace_gate(database).read_owned().await;
        let table = self.table_gate(database, table).read_owned().await;
        (namespace, table)
    }

    async fn ordered_table_access(
        mut gates: Vec<(String, Arc<RwLock<()>>)>,
    ) -> Vec<tokio::sync::OwnedRwLockReadGuard<()>> {
        // Queued publication writers make read lock ordering matter as well.
        gates.sort_by(|left, right| left.0.cmp(&right.0));
        gates.dedup_by(|left, right| left.0 == right.0);
        let mut access = Vec::with_capacity(gates.len());
        for (_, gate) in gates {
            access.push(gate.read_owned().await);
        }
        access
    }

    fn apply_table_alterations(
        &self,
        database: &Namespace,
        table: &str,
        options: Vec<AlterTableOptions>,
        schema: Option<&Schema>,
    ) -> CatalogResult<()> {
        let mut db = self.databases.get_mut(database).ok_or_else(|| {
            CatalogError::NotFound(CatalogObject::Database, quote_namespace_if_needed(database))
        })?;
        db.check_known(table)?;
        let status = db
            .tables
            .get_mut(table)
            .ok_or_else(|| CatalogError::NotFound(CatalogObject::Table, table.to_string()))?;
        let mut updated = status.clone();
        if let Some(schema) = schema
            && let TableKind::Table { columns, .. } = &mut updated.kind
        {
            let replace_schema = options
                .iter()
                .any(|option| matches!(option, AlterTableOptions::ReplaceColumns { .. }));
            *columns = schema
                .fields()
                .iter()
                .map(|field| {
                    let previous = columns.iter().find(|column| column.name == *field.name());
                    let inherited = previous.filter(|_| !replace_schema);
                    let features = ColumnFeatures::from_field(field);
                    TableColumnStatus {
                        name: field.name().clone(),
                        data_type: field.data_type().clone(),
                        nullable: field.is_nullable(),
                        comment: field
                            .metadata()
                            .get("comment")
                            .or_else(|| field.metadata().get("doc"))
                            .cloned(),
                        // Formats may store typed defaults instead of SQL expressions.
                        default: features
                            .current_default()
                            .or_else(|| inherited.and_then(|column| column.default.clone())),
                        generated_always_as: features.generation_expression().or_else(|| {
                            inherited.and_then(|column| column.generated_always_as.clone())
                        }),
                        identity: features
                            .identity()
                            .or_else(|| inherited.and_then(|column| column.identity.clone())),
                        is_partition: previous.is_some_and(|column| column.is_partition),
                        is_bucket: previous.is_some_and(|column| column.is_bucket),
                        is_cluster: previous.is_some_and(|column| column.is_cluster),
                    }
                })
                .collect();
        }
        for options in options {
            match &mut updated.kind {
                TableKind::Table {
                    columns,
                    comment,
                    properties,
                    partition_by,
                    location,
                    ..
                } => match options {
                    AlterTableOptions::AlterSnapshotReference { .. }
                    | AlterTableOptions::AlterIdentifierFields { .. }
                    | AlterTableOptions::AlterPartitionSpec { .. } => Ok(()),
                    AlterTableOptions::SetPartitioning {
                        partition_by: updated,
                    } => {
                        *partition_by = updated;
                        for column in columns.iter_mut() {
                            column.is_partition =
                                partition_by.iter().any(|field| field.column == column.name);
                        }
                        Ok(())
                    }
                    AlterTableOptions::SetWriteOrder { distribution, .. } => {
                        if let Some(value) = distribution {
                            properties.retain(|(key, _)| key != "write.distribution-mode");
                            properties.push(("write.distribution-mode".into(), value));
                        }
                        Ok(())
                    }
                    AlterTableOptions::SetLocation { location: updated } => {
                        *location = Some(updated);
                        Ok(())
                    }
                    AlterTableOptions::ReplaceColumns {
                        fields: replacement,
                        ..
                    } => {
                        if schema.is_none() {
                            sail_common_datafusion::catalog::replace_columns(
                                columns,
                                &arrow::datatypes::Schema::new(replacement),
                            );
                        }
                        Ok(())
                    }
                    AlterTableOptions::AlterColumns {
                        changes,
                        case_sensitive,
                    } => {
                        for change in &changes {
                            if let sail_catalog::provider::ColumnChange::Rename { path, name } =
                                change
                                && let [old] = path.as_slice()
                            {
                                for partition in partition_by.iter_mut() {
                                    if partition.column.eq_ignore_ascii_case(old) {
                                        partition.column = name.clone();
                                    }
                                }
                            }
                        }
                        if schema.is_none() {
                            sail_common_datafusion::catalog::alter_columns(
                                columns,
                                &changes,
                                case_sensitive,
                            )
                            .map_err(|e| CatalogError::InvalidArgument(e.to_string()))?;
                        }
                        for column in columns.iter_mut() {
                            column.is_partition = partition_by.iter().any(|partition| {
                                partition.column.eq_ignore_ascii_case(&column.name)
                            });
                        }
                        Ok(())
                    }
                    AlterTableOptions::SetTableProperties {
                        properties: new_props,
                    } => {
                        managed_table::validate_metadata_location_precondition(
                            table, properties, &new_props,
                        )?;
                        for (key, value) in new_props {
                            if key == "comment" {
                                *comment = Some(value.clone());
                            }
                            if let Some(existing) = properties.iter_mut().find(|(k, _)| k == &key) {
                                existing.1 = value;
                            } else {
                                properties.push((key, value));
                            }
                        }
                        Ok(())
                    }
                    AlterTableOptions::UnsetTableProperties { keys, if_exists } => {
                        for key in &keys {
                            let found = properties.iter().any(|(k, _)| k == key)
                                || (key == "comment" && comment.is_some());
                            if !found && !if_exists {
                                return Err(CatalogError::NotFound(
                                    CatalogObject::Table,
                                    format!("property '{key}' not found on table '{table}'"),
                                ));
                            }
                            if key == "comment" {
                                *comment = None;
                            }
                            properties.retain(|(k, _)| k != key);
                        }
                        Ok(())
                    }
                    AlterTableOptions::AlterColumnType {
                        name, data_type, ..
                    } => alter_column_type(columns, &name, data_type).map_err(|e| {
                        CatalogError::InvalidArgument(format!(
                            "failed to alter column type for '{}': {e}",
                            name.join(".")
                        ))
                    }),
                    AlterTableOptions::AlterColumnDefault { name, default } => {
                        alter_column_default(columns, &name, default).map_err(|e| {
                            CatalogError::InvalidArgument(format!(
                                "failed to alter column default for '{}': {e}",
                                name.join(".")
                            ))
                        })
                    }
                    AlterTableOptions::AddCheckConstraint { .. }
                    | AlterTableOptions::DropCheckConstraint { .. } => {
                        Err(CatalogError::NotSupported(
                            "CHECK constraints are handled by lake sources".to_string(),
                        ))
                    }
                },
                _ => Err(CatalogError::NotSupported(
                    "ALTER TABLE is not supported for views".to_string(),
                )),
            }?;
        }
        *status = updated;
        db.changed(table);
        Ok(())
    }
}

fn validate_create_table_options(
    format: &str,
    partition_by: &[CatalogPartitionField],
) -> CatalogResult<()> {
    if !format.eq_ignore_ascii_case("iceberg") && partition_by.iter().any(|f| f.transform.is_some())
    {
        return Err(CatalogError::NotSupported(
            "partition transforms are not supported by memory catalog".to_string(),
        ));
    }
    Ok(())
}

#[async_trait::async_trait]
impl CatalogProvider for MemoryCatalogProvider {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn default_namespace(&self) -> Namespace {
        self.default_namespace.clone()
    }

    async fn create_database(
        &self,
        database: &Namespace,
        options: CreateDatabaseOptions,
    ) -> CatalogResult<DatabaseStatus> {
        let _access = self.namespace_gate(database).write_owned().await;
        let CreateDatabaseOptions {
            if_not_exists,
            comment,
            location,
            properties,
        } = options;
        let entry = self.databases.entry(database.clone());
        match entry {
            Entry::Occupied(entry) => {
                if if_not_exists {
                    Ok(entry.get().status.clone())
                } else {
                    Err(CatalogError::AlreadyExists(
                        CatalogObject::Database,
                        quote_namespace_if_needed(database),
                    ))
                }
            }
            Entry::Vacant(entry) => {
                let status = DatabaseStatus {
                    catalog: self.name.clone(),
                    database: database.clone().into(),
                    comment,
                    location,
                    properties,
                };
                let db = MemoryDatabase {
                    generation: next_generation(),
                    table_generations: HashMap::new(),
                    uncertain_tables: HashSet::new(),
                    status: status.clone(),
                    tables: HashMap::new(),
                    views: HashMap::new(),
                };
                entry.insert(db);
                Ok(status)
            }
        }
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        match self.databases.get(database) {
            Some(db) => Ok(db.status.clone()),
            _ => Err(CatalogError::NotFound(
                CatalogObject::Database,
                quote_namespace_if_needed(database),
            )),
        }
    }

    async fn list_databases(
        &self,
        prefix: Option<&Namespace>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        Ok(self
            .databases
            .iter()
            .filter(|item| {
                if let Some(prefix) = prefix {
                    prefix.is_parent_of(item.key())
                } else {
                    item.key().tail.is_empty()
                }
            })
            .map(|item| item.value().status.clone())
            .collect())
    }

    async fn drop_database(
        &self,
        database: &Namespace,
        options: DropDatabaseOptions,
    ) -> CatalogResult<()> {
        let _access = self.namespace_gate(database).write_owned().await;
        let DropDatabaseOptions { if_exists, cascade } = options;
        match self.databases.entry(database.clone()) {
            Entry::Occupied(entry) => {
                for table in &entry.get().uncertain_tables {
                    entry.get().check_known(table)?;
                }
                if !cascade && (!entry.get().tables.is_empty() || !entry.get().views.is_empty()) {
                    return Err(CatalogError::InvalidArgument(format!(
                        "Cannot drop non-empty database '{}', use CASCADE",
                        quote_namespace_if_needed(database),
                    )));
                }
                let _ = entry.remove_entry();
                Ok(())
            }
            Entry::Vacant(_) => {
                if if_exists {
                    Ok(())
                } else {
                    Err(CatalogError::NotFound(
                        CatalogObject::Database,
                        quote_namespace_if_needed(database),
                    ))
                }
            }
        }
    }

    async fn create_table(
        &self,
        database: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<TableStatus> {
        let _access = self.table_access(database, table).await;
        let CreateTableOptions {
            columns,
            comment,
            constraints,
            location,
            format,
            partition_by,
            sort_by,
            bucket_by,
            mode,
            properties,
            is_external,
            is_write_precondition: _,
        } = options;
        validate_create_table_options(&format, &partition_by)?;
        let mut db = self.databases.get_mut(database).ok_or_else(|| {
            CatalogError::NotFound(CatalogObject::Database, quote_namespace_if_needed(database))
        })?;
        db.check_known(table)?;
        if let Some(status) = db.tables.get(table) {
            match mode {
                CreateTableMode::CreateIfNotExists => return Ok(status.clone()),
                CreateTableMode::CreateOrReplace | CreateTableMode::Replace => {
                    db.tables.remove(table);
                }
                CreateTableMode::Create => {
                    return Err(CatalogError::AlreadyExists(
                        CatalogObject::Table,
                        table.to_string(),
                    ));
                }
            }
        } else if mode.replace_requires_existing() {
            return Err(CatalogError::NotFound(
                CatalogObject::Table,
                table.to_string(),
            ));
        }
        let columns = columns
            .into_iter()
            .map(|x| {
                let CreateTableColumnOptions {
                    name,
                    data_type,
                    nullable,
                    comment,
                    default,
                    generated_always_as,
                    identity,
                } = x;
                let is_partition = partition_by
                    .iter()
                    .any(|x| x.column.eq_ignore_ascii_case(&name));
                let is_bucket = bucket_by
                    .as_ref()
                    .is_some_and(|b| b.columns.iter().any(|x| x.eq_ignore_ascii_case(&name)));
                TableColumnStatus {
                    name,
                    data_type,
                    nullable,
                    comment,
                    default,
                    generated_always_as,
                    identity,
                    is_partition,
                    is_bucket,
                    is_cluster: false,
                }
            })
            .collect();
        let status = TableStatus {
            catalog: Some(self.name.clone()),
            database: database.clone().into(),
            name: table.to_string(),
            kind: TableKind::Table {
                columns,
                comment,
                constraints,
                location,
                format,
                partition_by,
                sort_by,
                bucket_by,
                properties,
                is_external,
            },
        };
        db.tables.insert(table.to_string(), status.clone());
        db.changed(table);
        Ok(status)
    }

    async fn stage_table_replacement(
        &self,
        database: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<TableReplacement> {
        self.stage_replacement(database, table, options).await
    }

    async fn publish_table_replacement(
        &self,
        replacement: TableReplacement,
        publication: sail_catalog::provider::TablePublication,
    ) -> CatalogResult<()> {
        self.publish_replacement(replacement, publication).await
    }

    fn create_table_metadata_requirement(
        &self,
        options: &CreateTableOptions,
    ) -> CatalogResult<CreateTableMetadataRequirement> {
        validate_create_table_options(&options.format, &options.partition_by)?;
        Ok(plain_lakehouse_create_table_metadata_requirement(options))
    }

    async fn get_table(&self, database: &Namespace, table: &str) -> CatalogResult<TableStatus> {
        let _access = self.table_access(database, table).await;
        if let Some(db) = self.databases.get(database) {
            db.check_known(table)?;
            if let Some(status) = db.tables.get(table) {
                return Ok(status.clone());
            }
        }
        Err(CatalogError::NotFound(
            CatalogObject::Table,
            table.to_string(),
        ))
    }

    async fn list_tables(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        let _namespace = self.namespace_gate(database).read_owned().await;
        let gates = self
            .table_gates
            .iter()
            .filter(|entry| &entry.key().0 == database)
            .map(|entry| (entry.key().1.clone(), entry.value().clone()))
            .collect::<Vec<_>>();
        let _access = Self::ordered_table_access(gates).await;
        match self.databases.get(database) {
            Some(db) => {
                for table in &db.uncertain_tables {
                    db.check_known(table)?;
                }
                Ok(db.tables.values().cloned().collect())
            }
            _ => Err(CatalogError::NotFound(
                CatalogObject::Database,
                quote_namespace_if_needed(database),
            )),
        }
    }

    async fn drop_table(
        &self,
        database: &Namespace,
        table: &str,
        options: DropTableOptions,
    ) -> CatalogResult<()> {
        let _access = self.table_access(database, table).await;
        // In Spark, the `DROP TABLE ... PURGE` SQL statement deletes data if the table
        // is managed by the Hive metastore. `PURGE` is ignored if the table is external.
        // In Sail, all tables are external, so we ignore the `purge` option.
        let DropTableOptions {
            if_exists,
            purge: _,
        } = options;
        match self.databases.get_mut(database) {
            Some(mut db) => {
                db.check_known(table)?;
                db.changed(table);
                if db.tables.remove(table).is_some() || if_exists {
                    Ok(())
                } else {
                    Err(CatalogError::NotFound(
                        CatalogObject::Table,
                        table.to_string(),
                    ))
                }
            }
            _ => {
                if if_exists {
                    Ok(())
                } else {
                    Err(CatalogError::NotFound(
                        CatalogObject::Database,
                        quote_namespace_if_needed(database),
                    ))
                }
            }
        }
    }

    async fn rename_table(
        &self,
        database: &Namespace,
        table: &str,
        destination_database: &Namespace,
        destination_table: &str,
    ) -> CatalogResult<()> {
        let _namespace = self.namespace_gate(database).read_owned().await;
        let _access = Self::ordered_table_access(vec![
            (table.to_string(), self.table_gate(database, table)),
            (
                destination_table.to_string(),
                self.table_gate(database, destination_table),
            ),
        ])
        .await;
        if database != destination_database {
            return Err(CatalogError::NotSupported(
                "Memory catalog cannot rename across databases".into(),
            ));
        }
        let mut db = self.databases.get_mut(database).ok_or_else(|| {
            CatalogError::NotFound(CatalogObject::Database, quote_namespace_if_needed(database))
        })?;
        db.check_known(table)?;
        db.check_known(destination_table)?;
        if !db.tables.contains_key(table) {
            return Err(CatalogError::NotFound(CatalogObject::Table, table.into()));
        }
        if db.tables.contains_key(destination_table) || db.views.contains_key(destination_table) {
            return Err(CatalogError::AlreadyExists(
                CatalogObject::Table,
                destination_table.into(),
            ));
        }
        let mut status = db
            .tables
            .remove(table)
            .ok_or_else(|| CatalogError::NotFound(CatalogObject::Table, table.into()))?;
        status.name = destination_table.into();
        db.tables.insert(destination_table.into(), status);
        db.changed(table);
        db.changed(destination_table);
        Ok(())
    }

    async fn alter_table(
        &self,
        database: &Namespace,
        table: &str,
        options: AlterTableOptions,
    ) -> CatalogResult<()> {
        self.alter_table_atomically(database, table, vec![options])
            .await
    }

    async fn alter_table_with_schema(
        &self,
        database: &Namespace,
        table: &str,
        options: AlterTableOptions,
        schema: SchemaRef,
    ) -> CatalogResult<()> {
        let _access = self.table_access(database, table).await;
        self.apply_table_alterations(database, table, vec![options], Some(schema.as_ref()))
    }

    async fn alter_table_atomically(
        &self,
        database: &Namespace,
        table: &str,
        options: Vec<AlterTableOptions>,
    ) -> CatalogResult<()> {
        let _access = self.table_access(database, table).await;
        self.apply_table_alterations(database, table, options, None)
    }

    async fn create_view(
        &self,
        database: &Namespace,
        view: &str,
        options: CreateViewOptions,
    ) -> CatalogResult<TableStatus> {
        let _access = self.table_access(database, view).await;
        let CreateViewOptions {
            columns,
            definition,
            if_not_exists,
            replace,
            comment,
            properties,
        } = options;
        let mut db = self.databases.get_mut(database).ok_or_else(|| {
            CatalogError::NotFound(CatalogObject::Database, quote_namespace_if_needed(database))
        })?;
        db.check_known(view)?;
        if let Some(status) = db.views.get(view) {
            if if_not_exists {
                return Ok(status.clone());
            } else if replace {
                db.views.remove(view);
            } else {
                return Err(CatalogError::AlreadyExists(
                    CatalogObject::View,
                    view.to_string(),
                ));
            }
        }
        let columns = columns
            .into_iter()
            .map(|x| {
                let CreateViewColumnOptions {
                    name,
                    data_type,
                    nullable,
                    comment,
                } = x;
                TableColumnStatus {
                    name,
                    data_type,
                    nullable,
                    comment,
                    default: None,
                    generated_always_as: None,
                    identity: None,
                    is_partition: false,
                    is_bucket: false,
                    is_cluster: false,
                }
            })
            .collect();
        let status = TableStatus {
            catalog: Some(self.name.clone()),
            database: database.clone().into(),
            name: view.to_string(),
            kind: TableKind::View {
                columns,
                definition,
                comment,
                properties: properties.into_iter().collect(),
            },
        };
        db.views.insert(view.to_string(), status.clone());
        db.changed(view);
        Ok(status)
    }

    async fn get_view(&self, database: &Namespace, view: &str) -> CatalogResult<TableStatus> {
        if let Some(db) = self.databases.get(database)
            && let Some(status) = db.views.get(view)
        {
            return Ok(status.clone());
        }
        Err(CatalogError::NotFound(
            CatalogObject::View,
            view.to_string(),
        ))
    }

    async fn list_views(&self, database: &Namespace) -> CatalogResult<Vec<TableStatus>> {
        match self.databases.get(database) {
            Some(db) => Ok(db.views.values().cloned().collect()),
            _ => Err(CatalogError::NotFound(
                CatalogObject::Database,
                quote_namespace_if_needed(database),
            )),
        }
    }

    async fn drop_view(
        &self,
        database: &Namespace,
        view: &str,
        options: DropViewOptions,
    ) -> CatalogResult<()> {
        let _access = self.table_access(database, view).await;
        let DropViewOptions { if_exists } = options;
        match self.databases.get_mut(database) {
            Some(mut db) => {
                db.check_known(view)?;
                db.changed(view);
                if db.views.remove(view).is_some() || if_exists {
                    Ok(())
                } else {
                    Err(CatalogError::NotFound(
                        CatalogObject::View,
                        view.to_string(),
                    ))
                }
            }
            _ => {
                if if_exists {
                    Ok(())
                } else {
                    Err(CatalogError::NotFound(
                        CatalogObject::Database,
                        quote_namespace_if_needed(database),
                    ))
                }
            }
        }
    }
}
