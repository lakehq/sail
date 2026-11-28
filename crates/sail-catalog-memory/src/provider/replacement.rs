// SPDX-License-Identifier: Apache-2.0

use sail_catalog::provider::TablePublication;

use super::*;

impl MemoryCatalogProvider {
    pub(super) async fn stage_replacement(
        &self,
        database: &Namespace,
        table: &str,
        options: impl Into<StageTableOptions>,
    ) -> CatalogResult<TableReplacement> {
        let _access = self.table_access(database, table).await;
        let db = self.databases.get(database).ok_or_else(|| {
            CatalogError::NotFound(CatalogObject::Database, quote_namespace_if_needed(database))
        })?;
        db.check_known(table)?;
        if db.views.contains_key(table) {
            return Err(CatalogError::AlreadyExists(
                CatalogObject::View,
                table.into(),
            ));
        }
        let existing = db.tables.get(table);
        let (options, migration, migration_backup) = match options.into() {
            StageTableOptions::IcebergMigration {
                definition,
                source,
                backup,
            } => {
                if !existing.is_some_and(|table| table.kind == *source) {
                    return Err(CatalogError::Conflict("migration source changed".into()));
                }
                (definition, true, Some(backup))
            }
            options => (options.resolve(existing)?, false, None),
        };
        validate_create_table_options(&options.format, &options.partition_by)?;
        let is_delta = options.format.eq_ignore_ascii_case("delta");
        if !(is_delta || options.format.eq_ignore_ascii_case("iceberg"))
            || options.bucket_by.is_some()
            || !options.constraints.is_empty()
        {
            return Err(CatalogError::NotSupported(
                "staged lakehouse table publication options".into(),
            ));
        }
        if existing.is_some() && !options.mode.is_replace() {
            return Err(CatalogError::AlreadyExists(
                CatalogObject::Table,
                table.into(),
            ));
        }
        if options.mode.replace_requires_existing() && existing.is_none() {
            return Err(CatalogError::NotFound(CatalogObject::Table, table.into()));
        }
        let mut location = options.location.ok_or_else(|| {
            CatalogError::InvalidArgument("replacement requires a table location".into())
        })?;
        let mut properties = Vec::new();
        if let Some(status) = existing
            && !migration
        {
            match &status.kind {
                TableKind::Table {
                    format,
                    location: old_location,
                    properties: old_properties,
                    ..
                } if format.eq_ignore_ascii_case(&options.format) => {
                    let Some(old_location) = old_location.as_ref().filter(|old_location| {
                        sail_common_datafusion::location::same_location(old_location, &location)
                    }) else {
                        return Err(CatalogError::NotSupported(
                            "relocating a lakehouse table during replacement".into(),
                        ));
                    };
                    location = old_location.clone();
                    properties = old_properties.clone();
                }
                _ => {
                    return Err(CatalogError::NotSupported(
                        "replacement requires the same lakehouse table format".into(),
                    ));
                }
            }
        }
        // Validate the original bookkeeping too: a request must not hide a managed marker.
        let mut markers = properties.iter().chain(&options.properties);
        if is_delta {
            validate_delta_replacement_properties(markers)?;
        } else if markers.any(|(key, value)| {
            sail_common_datafusion::catalog::iceberg::is_iceberg_table_marker(
                key.trim(),
                value.trim(),
            ) || key.trim().starts_with("metadata.")
        }) {
            return Err(CatalogError::UnsupportedCapability(
                "replacement requires path-managed Iceberg publication authority".into(),
            ));
        }
        let mut catalog_table = vec![self.name.clone()];
        catalog_table.extend(Vec::<String>::from(database.clone()));
        catalog_table.push(table.to_string());
        Ok(TableReplacement {
            migration_backup,
            catalog_table,
            authority: sail_common_datafusion::catalog::ReplacementAuthority::Path {
                catalog_generation: self.generation,
                database_generation: db.generation,
                table_generation: db.table_generations.get(table).copied().unwrap_or(0),
            },
            require_existing: options.mode.replace_requires_existing(),
            existing: existing.is_some(),
            columns: options
                .columns
                .into_iter()
                .map(|column| {
                    (
                        column.name,
                        column.data_type,
                        column.nullable,
                        column.metadata,
                    )
                })
                .collect(),
            location,
            format: options.format,
            partition_by: options.partition_by,
            sort_by: options.sort_by,
            properties,
            property_updates: options
                .properties
                .into_iter()
                .chain(options.comment.map(|value| ("comment".to_string(), value)))
                .collect(),
            is_external: options.is_external,
        })
    }

    pub(super) async fn publish_replacement(
        &self,
        replacement: TableReplacement,
        publication: TablePublication,
    ) -> CatalogResult<()> {
        // Clone preparation fills in authoritative metadata after staging the target identity.
        validate_create_table_options(&replacement.format, &replacement.partition_by)?;
        if replacement.format.eq_ignore_ascii_case("delta") {
            validate_delta_replacement_properties(&replacement.properties)?;
        }
        let sail_common_datafusion::catalog::ReplacementAuthority::Path {
            catalog_generation,
            database_generation,
            table_generation,
        } = replacement.authority
        else {
            return Err(CatalogError::UnsupportedCapability(
                "memory replacement requires path authority".into(),
            ));
        };
        if catalog_generation != self.generation {
            return Err(CatalogError::Conflict(
                "replacement catalog identity changed".into(),
            ));
        }
        let [_, namespace @ .., table] = replacement.catalog_table.as_slice() else {
            return Err(CatalogError::InvalidArgument(
                "invalid replacement identity".into(),
            ));
        };
        let database = Namespace::try_from(namespace)?;
        let table = table.clone();
        let databases = self.databases.clone();
        let namespace_gate = self.namespace_gate(&database);
        let table_gate = self.table_gate(&database, &table);
        // Only this short final publication is reserved, not worker execution. Dropping the
        // caller's JoinHandle detaches this task; publication and status installation continue.
        tokio::spawn(async move {
            // Reserve the whole namespace only when the publication also installs a backup name.
            let (_shared, _exclusive) = if replacement.migration_backup.is_some() {
                (None, Some(namespace_gate.write_owned().await))
            } else {
                (Some(namespace_gate.read_owned().await), None)
            };
            let _reservation = table_gate.write_owned().await;
            let status = {
                let db = databases.get(&database).ok_or_else(|| {
                    CatalogError::Conflict("replacement database was dropped".into())
                })?;
                db.check_known(&table)?;
                if db.generation != database_generation
                    || db.table_generations.get(&table).copied().unwrap_or(0) != table_generation
                    || db.views.contains_key(&table)
                    || (replacement.require_existing && !db.tables.contains_key(&table))
                {
                    return Err(CatalogError::Conflict(
                        "replacement catalog identity changed".into(),
                    ));
                }
                if let Some(backup) = &replacement.migration_backup {
                    let backup = &backup.name;
                    db.check_known(backup)?;
                    if db.tables.contains_key(backup) || db.views.contains_key(backup) {
                        return Err(CatalogError::AlreadyExists(
                            CatalogObject::Table,
                            backup.clone(),
                        ));
                    }
                }
                prepared_status(&replacement, &database, &table)
            };
            // A publication panic is also an unknown outcome, never evidence that files are unused.
            let result = tokio::spawn(publication).await.unwrap_or_else(|error| {
                Err(CatalogError::CommitStateUnknown(error.to_string().into()))
            });
            // The namespace reservation prevents removal/recreation of this database. No DashMap
            // guard survives the I/O above and there is no await or fallible step after it.
            databases.entry(database).and_modify(|db| {
                if result.is_ok() {
                    if let Some(backup) = &replacement.migration_backup
                        && backup.retain
                        && let Some(mut original) = db.tables.remove(&table)
                    {
                        let backup = &backup.name;
                        original.name.clone_from(backup);
                        if let TableKind::Table { properties, .. } = &mut original.kind {
                            properties.push((
                                sail_common_datafusion::catalog::iceberg::MIGRATION_BACKUP_PROPERTY
                                    .into(),
                                "true".into(),
                            ));
                        }
                        db.tables.insert(backup.clone(), original);
                        db.changed(backup);
                    }
                    db.tables.insert(table.clone(), status);
                    db.changed(&table);
                } else if matches!(result, Err(CatalogError::CommitStateUnknown(_))) {
                    db.uncertain_tables.insert(table.clone());
                }
            });
            result
        })
        .await
        .map_err(|error| CatalogError::CommitStateUnknown(error.to_string().into()))?
    }
}

fn validate_delta_replacement_properties<'a>(
    properties: impl IntoIterator<Item = &'a (String, String)>,
) -> CatalogResult<()> {
    if properties
        .into_iter()
        .any(|(key, _)| sail_common_datafusion::catalog::delta::is_delta_coordinated_property(key))
    {
        return Err(CatalogError::UnsupportedCapability(
            "replacement of a catalog-coordinated Delta table".into(),
        ));
    }
    Ok(())
}

fn prepared_status(
    replacement: &TableReplacement,
    database: &Namespace,
    table: &str,
) -> TableStatus {
    TableStatus {
        catalog: replacement.catalog_table.first().cloned(),
        database: database.clone().into(),
        name: table.into(),
        kind: TableKind::Table {
            columns: replacement
                .columns
                .iter()
                .map(|(name, data_type, nullable, metadata)| TableColumnStatus {
                    metadata: metadata.clone(),
                    name: name.clone(),
                    data_type: data_type.clone(),
                    nullable: *nullable,
                    comment: None,
                    default: None,
                    generated_always_as: None,
                    identity: None,
                    is_partition: replacement
                        .partition_by
                        .iter()
                        .any(|field| field.column.eq_ignore_ascii_case(name)),
                    is_bucket: false,
                    is_cluster: false,
                })
                .collect(),
            comment: replacement
                .properties
                .iter()
                .find(|(key, _)| key == "comment")
                .map(|(_, value)| value.clone()),
            constraints: vec![],
            location: Some(replacement.location.clone()),
            format: replacement.format.clone(),
            partition_by: replacement.partition_by.clone(),
            sort_by: replacement.sort_by.clone(),
            bucket_by: None,
            properties: replacement.properties.clone(),
            is_external: replacement.is_external,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use arrow::datatypes::DataType;
    use sail_common_datafusion::uuid::{UUID_EXTENSION_NAME, UUID_METADATA_KEY, is_uuid_field};
    use tokio::sync::Barrier;

    use super::*;

    fn status_value(status: TableStatus) -> (Option<String>, Vec<String>, String, TableKind) {
        (status.catalog, status.database, status.name, status.kind)
    }

    fn options() -> CreateTableOptions {
        CreateTableOptions {
            columns: vec![CreateTableColumnOptions {
                metadata: Default::default(),
                name: "new_column".into(),
                data_type: DataType::Int64,
                nullable: false,
                comment: None,
                default: None,
                generated_always_as: None,
                identity: None,
            }],
            comment: None,
            constraints: vec![],
            location: Some("file:///replacement".into()),
            format: "iceberg".into(),
            partition_by: vec![],
            sort_by: vec![],
            bucket_by: None,
            mode: CreateTableMode::CreateOrReplace,
            properties: vec![("owner".into(), "new".into())],
            is_external: true,
            is_write_precondition: true,
        }
    }

    async fn fixture(existing: bool) -> CatalogResult<(Arc<MemoryCatalogProvider>, Namespace)> {
        let namespace = Namespace::try_from(vec!["db".to_string()])?;
        let provider = Arc::new(MemoryCatalogProvider::new(
            "memory".into(),
            namespace.clone(),
            None,
        ));
        if existing {
            let mut old = options();
            old.columns[0].name = "old_column".into();
            old.properties = vec![("owner".into(), "old".into())];
            provider.create_table(&namespace, "t", old).await?;
        }
        Ok((provider, namespace))
    }

    #[tokio::test]
    async fn missing_queries_bound_idle_gate_history() -> CatalogResult<()> {
        let (provider, namespace) = fixture(false).await?;
        for missing_namespace in [false, true] {
            for i in 0..1024 {
                let database = if missing_namespace {
                    Namespace::try_from(vec![format!("missing_{i}")])?
                } else {
                    namespace.clone()
                };
                assert!(matches!(
                    provider.get_table(&database, &format!("missing_{i}")).await,
                    Err(CatalogError::NotFound(CatalogObject::Table, _))
                ));
                assert!(provider.namespace_gates.len() <= GATE_CLEANUP_INTERVAL as usize);
                assert!(provider.table_gates.len() <= GATE_CLEANUP_INTERVAL as usize);
            }
        }
        assert_eq!(provider.databases.len(), 1);
        assert!(provider.list_tables(&namespace).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn gate_cleanup_preserves_holders_waiters_and_unpolled_acquisitions() -> CatalogResult<()>
    {
        use std::future::Future;
        use std::task::{Context, Waker};

        for namespace_gate in [false, true] {
            let (provider, namespace) = fixture(false).await?;
            let gate = || {
                if namespace_gate {
                    provider.namespace_gate(&namespace)
                } else {
                    provider.table_gate(&namespace, "t")
                }
            };
            let original = gate();
            let identity = Arc::as_ptr(&original);
            let held = original.write_owned().await;
            provider.cleanup_idle_gates();
            assert_eq!(Arc::as_ptr(&gate()), identity);
            let mut queued = Box::pin(gate().write_owned());
            let mut cx = Context::from_waker(Waker::noop());
            assert!(queued.as_mut().poll(&mut cx).is_pending());
            provider.cleanup_idle_gates();
            assert_eq!(Arc::as_ptr(&gate()), identity);
            let mut later = Box::pin(gate().read_owned());
            assert!(later.as_mut().poll(&mut cx).is_pending());
            drop(held);
            drop(later);
            provider.cleanup_idle_gates();
            assert_eq!(Arc::as_ptr(&gate()), identity);
            // Cancellation releases the queued reservation, but the unpolled
            // future must still keep the allocation registered on its own.
            let unpolled = gate().read_owned();
            drop(queued);
            provider.cleanup_idle_gates();
            assert_eq!(Arc::as_ptr(&gate()), identity);
            drop(unpolled);
            // Normal acquisitions eventually reclaim it without explicit cleanup.
            let other = Namespace::try_from(vec!["other".to_string()])?;
            for _ in 0..GATE_CLEANUP_INTERVAL {
                drop(provider.table_gate(&other, "churn"));
            }
            assert!(!provider.namespace_gates.contains_key(&namespace));
            assert!(!provider.table_gates.contains_key(&(namespace, "t".into())));
        }
        Ok(())
    }

    #[tokio::test]
    async fn ordered_reads_do_not_deadlock_with_queued_publications() -> CatalogResult<()> {
        use std::future::Future;
        use std::task::{Context, Poll, Waker};
        let a = Arc::new(RwLock::new(()));
        let b = Arc::new(RwLock::new(()));
        let mut cx = Context::from_waker(Waker::noop());
        let held_b = b.clone().write_owned().await;
        let mut rename = Box::pin(MemoryCatalogProvider::ordered_table_access(vec![
            ("a".into(), a.clone()),
            ("b".into(), b.clone()),
        ]));
        assert!(rename.as_mut().poll(&mut cx).is_pending()); // holds a, waits for b
        let mut publish_a = Box::pin(a.clone().write_owned());
        assert!(publish_a.as_mut().poll(&mut cx).is_pending()); // queues behind rename
        let mut reverse_rename = Box::pin(MemoryCatalogProvider::ordered_table_access(vec![
            ("b".into(), b.clone()),
            ("a".into(), a.clone()),
            ("a".into(), a.clone()),
        ]));
        assert!(reverse_rename.as_mut().poll(&mut cx).is_pending()); // must wait on a first
        drop(held_b);
        let Poll::Ready(rename_guards) = rename.as_mut().poll(&mut cx) else {
            return Err(CatalogError::Internal(
                "rename blocked by out-of-order reverse rename".into(),
            ));
        };
        drop(rename_guards);
        let Poll::Ready(held_a) = publish_a.as_mut().poll(&mut cx) else {
            return Err(CatalogError::Internal(
                "publication a did not acquire reservation".into(),
            ));
        };
        let held_b = b.clone().try_write_owned().map_err(|_| {
            CatalogError::Internal("reverse rename holds b while waiting for a".into())
        })?;
        drop(held_a);
        assert!(reverse_rename.as_mut().poll(&mut cx).is_pending());
        drop(held_b);
        assert!(reverse_rename.as_mut().poll(&mut cx).is_ready());
        Ok(())
    }

    #[tokio::test]
    async fn replacement_preserves_top_level_uuid_metadata() -> CatalogResult<()> {
        for existing in [false, true] {
            let (provider, namespace) = fixture(existing).await?;
            let mut request = options();
            let metadata = std::collections::BTreeMap::from([
                (UUID_METADATA_KEY.into(), UUID_EXTENSION_NAME.into()),
                ("custom".into(), "retained".into()),
            ]);
            request.columns[0].data_type = DataType::FixedSizeBinary(16);
            request.columns[0].metadata = metadata.clone();
            let staged = provider.stage_replacement(&namespace, "t", request).await?;
            assert_eq!(staged.columns[0].3, metadata);
            provider
                .publish_replacement(staged, Box::pin(async { Ok(()) }))
                .await?;
            let TableKind::Table { columns, .. } = provider.get_table(&namespace, "t").await?.kind
            else {
                return Err(CatalogError::InvalidArgument("expected table".into()));
            };
            assert_eq!(columns[0].data_type, DataType::FixedSizeBinary(16));
            assert_eq!(columns[0].metadata, metadata);
            assert!(is_uuid_field(&columns[0].field()));
        }
        Ok(())
    }

    #[tokio::test]
    async fn replacement_stage_and_definite_failure_preserve_visible_state() -> CatalogResult<()> {
        for existing in [false, true] {
            let (provider, namespace) = fixture(existing).await?;
            let before = provider
                .get_table(&namespace, "t")
                .await
                .ok()
                .map(status_value);
            let staged = provider
                .stage_replacement(&namespace, "t", options())
                .await?;
            assert_eq!(
                provider
                    .get_table(&namespace, "t")
                    .await
                    .ok()
                    .map(status_value),
                before
            );
            let result = provider
                .publish_replacement(
                    staged,
                    Box::pin(async { Err(CatalogError::Conflict("definite failure".into())) }),
                )
                .await;
            assert!(matches!(result, Err(CatalogError::Conflict(_))));
            assert_eq!(
                provider
                    .get_table(&namespace, "t")
                    .await
                    .ok()
                    .map(status_value),
                before
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn replacement_staging_keeps_explicit_properties_separate() -> CatalogResult<()> {
        let (provider, namespace) = fixture(true).await?;
        let staged = provider
            .stage_replacement(&namespace, "t", options())
            .await?;
        assert_eq!(staged.properties, vec![("owner".into(), "old".into())]);
        assert_eq!(
            staged.property_updates,
            vec![("owner".into(), "new".into())]
        );
        let mut request = options();
        request.properties.clear();
        let staged = provider.stage_replacement(&namespace, "t", request).await?;
        assert_eq!(staged.properties, vec![("owner".into(), "old".into())]);
        assert!(staged.property_updates.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn replacement_cannot_hide_original_publication_authority() -> CatalogResult<()> {
        for (key, value) in [
            ("table_type", "iceberg"),
            ("classification", "ICEBERG"),
            ("metadata.catalog", "managed"),
        ] {
            let (provider, namespace) = fixture(true).await?;
            {
                let mut db = provider.databases.get_mut(&namespace).ok_or_else(|| {
                    CatalogError::InvalidArgument("expected fixture database".into())
                })?;
                let TableKind::Table { properties, .. } = &mut db
                    .tables
                    .get_mut("t")
                    .ok_or_else(|| CatalogError::InvalidArgument("expected fixture table".into()))?
                    .kind
                else {
                    return Err(CatalogError::InvalidArgument("expected table".into()));
                };
                properties.push((key.into(), value.into()));
            }
            let mut request = options();
            request.properties.push((key.into(), "not-managed".into()));
            assert!(matches!(
                provider.stage_replacement(&namespace, "t", request).await,
                Err(CatalogError::UnsupportedCapability(_))
            ));
        }
        Ok(())
    }

    #[tokio::test]
    async fn replacement_conflicts_never_invoke_publisher() -> CatalogResult<()> {
        for mutation in [
            "catalog",
            "database",
            "generation",
            "drop",
            "rename",
            "aba",
            "absent_aba",
        ] {
            let (provider, namespace) = fixture(mutation != "absent_aba").await?;
            let mut staged = provider
                .stage_replacement(&namespace, "t", options())
                .await?;
            let sail_common_datafusion::catalog::ReplacementAuthority::Path {
                catalog_generation,
                database_generation,
                table_generation,
            } = &mut staged.authority
            else {
                unreachable!()
            };
            match mutation {
                "catalog" => *catalog_generation += 1,
                "database" => *database_generation += 1,
                "generation" => *table_generation += 1,
                "rename" => {
                    provider
                        .rename_table(&namespace, "t", &namespace, "renamed")
                        .await?
                }
                "drop" | "aba" => {
                    provider
                        .drop_table(
                            &namespace,
                            "t",
                            DropTableOptions {
                                if_exists: false,
                                purge: false,
                            },
                        )
                        .await?;
                    if mutation == "aba" {
                        provider.create_table(&namespace, "t", options()).await?;
                    }
                }
                "absent_aba" => {
                    provider.create_table(&namespace, "t", options()).await?;
                    provider
                        .drop_table(
                            &namespace,
                            "t",
                            DropTableOptions {
                                if_exists: false,
                                purge: false,
                            },
                        )
                        .await?;
                }
                _ => unreachable!(),
            }
            provider.cleanup_idle_gates();
            assert!(provider.namespace_gates.is_empty());
            assert!(provider.table_gates.is_empty());
            let called = Arc::new(AtomicBool::new(false));
            let flag = called.clone();
            let result = provider
                .publish_replacement(
                    staged,
                    Box::pin(async move {
                        flag.store(true, Ordering::SeqCst);
                        Ok(())
                    }),
                )
                .await;
            assert!(
                matches!(result, Err(CatalogError::Conflict(_))),
                "{mutation}: {result:?}"
            );
            assert!(!called.load(Ordering::SeqCst), "{mutation}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn replacement_publication_is_atomic_and_survives_caller_cancellation()
    -> CatalogResult<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            for cancel in [false, true] {
                let (provider, namespace) = fixture(true).await?;
                let staged = provider
                    .stage_replacement(&namespace, "t", options())
                    .await?;
                let expected = prepared_status(&staged, &namespace, "t");
                let entered = Arc::new(Barrier::new(2));
                let release = Arc::new(Barrier::new(2));
                let task = {
                    let provider = provider.clone();
                    let entered = entered.clone();
                    let release = release.clone();
                    tokio::spawn(async move {
                        provider
                            .publish_replacement(
                                staged,
                                Box::pin(async move {
                                    entered.wait().await;
                                    release.wait().await;
                                    Ok(())
                                }),
                            )
                            .await
                    })
                };
                entered.wait().await;
                // The reservation hides the intermediate storage/catalog boundary.
                assert!(provider.table_gate(&namespace, "t").try_read().is_err());
                provider
                    .create_table(&namespace, "unrelated", options())
                    .await?;
                provider.get_table(&namespace, "unrelated").await?;
                let independent = provider
                    .stage_replacement(&namespace, "independent", options())
                    .await?;
                provider
                    .publish_replacement(independent, Box::pin(async { Ok(()) }))
                    .await?;
                if cancel {
                    task.abort();
                }
                provider.cleanup_idle_gates();
                assert!(provider.table_gate(&namespace, "t").try_read().is_err());
                let mut listing = Box::pin(provider.list_tables(&namespace));
                {
                    use std::future::Future;
                    use std::task::{Context, Waker};
                    let mut cx = Context::from_waker(Waker::noop());
                    assert!(listing.as_mut().poll(&mut cx).is_pending());
                    // A table acquired after the listing starts also waits at the namespace gate.
                    let mut late_read = Box::pin(provider.get_table(&namespace, "late"));
                    assert!(late_read.as_mut().poll(&mut cx).is_pending());
                }
                provider.cleanup_idle_gates();
                assert!(provider.table_gate(&namespace, "t").try_read().is_err());
                release.wait().await;
                if cancel {
                    let error = task.await.err().ok_or_else(|| {
                        CatalogError::InvalidArgument("expected cancelled publication task".into())
                    })?;
                    assert!(error.is_cancelled());
                } else {
                    task.await.map_err(|error| {
                        CatalogError::InvalidArgument(format!("publication task failed: {error}"))
                    })??;
                }
                let tables = listing.await?;
                assert_eq!(tables.len(), 3);
                let listed = tables.into_iter().find(|status| status.name == "t");
                let expected = status_value(expected);
                assert_eq!(listed.map(status_value).as_ref(), Some(&expected));
                assert_eq!(
                    status_value(provider.get_table(&namespace, "t").await?),
                    expected
                );
            }
            Ok::<(), CatalogError>(())
        })
        .await
        .map_err(|error| {
            CatalogError::InvalidArgument(format!(
                "publication or catalog read deadlocked: {error}"
            ))
        })??;
        Ok(())
    }

    #[tokio::test]
    async fn replacement_unknown_and_panic_fail_closed_only_for_affected_table() -> CatalogResult<()>
    {
        tokio::time::timeout(Duration::from_secs(10), async {
            for panic in [false, true] {
                let (provider, namespace) = fixture(true).await?;
                provider
                    .create_table(&namespace, "unaffected", options())
                    .await?;
                let before = provider.get_table(&namespace, "t").await?;
                let staged = provider
                    .stage_replacement(&namespace, "t", options())
                    .await?;
                let result = provider
                    .publish_replacement(
                        staged,
                        Box::pin(async move {
                            assert!(!panic, "injected publisher panic");
                            Err(CatalogError::CommitStateUnknown(
                                "lost acknowledgement".to_string().into(),
                            ))
                        }),
                    )
                    .await;
                assert!(matches!(result, Err(CatalogError::CommitStateUnknown(_))));
                assert!(matches!(
                    provider.get_table(&namespace, "t").await,
                    Err(CatalogError::CommitStateUnknown(_))
                ));
                assert!(
                    provider
                        .stage_replacement(&namespace, "t", options())
                        .await
                        .is_err()
                );
                assert!(
                    provider
                        .drop_table(
                            &namespace,
                            "t",
                            DropTableOptions {
                                if_exists: false,
                                purge: true
                            }
                        )
                        .await
                        .is_err()
                );
                assert!(provider.get_table(&namespace, "unaffected").await.is_ok());
                assert_eq!(
                    provider
                        .databases
                        .get(&namespace)
                        .ok_or_else(|| {
                            CatalogError::InvalidArgument("expected fixture database".into())
                        })?
                        .tables
                        .get("t")
                        .cloned()
                        .map(status_value),
                    Some(status_value(before))
                );
            }
            Ok::<(), CatalogError>(())
        })
        .await
        .map_err(|error| {
            CatalogError::InvalidArgument(format!(
                "unknown outcome poisoned catalog access: {error}"
            ))
        })??;
        Ok(())
    }
}
