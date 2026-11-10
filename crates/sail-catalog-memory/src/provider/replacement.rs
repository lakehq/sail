use sail_catalog::provider::TablePublication;

use super::*;

impl MemoryCatalogProvider {
    pub(super) async fn stage_replacement(
        &self,
        database: &Namespace,
        table: &str,
        options: CreateTableOptions,
    ) -> CatalogResult<TableReplacement> {
        let _access = self.publication_gate.read().await;
        validate_create_table_options(&options.format, &options.partition_by)?;
        if !options.format.eq_ignore_ascii_case("iceberg")
            || !matches!(
                options.mode,
                CreateTableMode::Replace | CreateTableMode::CreateOrReplace
            )
            || options.bucket_by.is_some()
            || !options.constraints.is_empty()
        {
            return Err(CatalogError::NotSupported(
                "staged Iceberg table replacement options".into(),
            ));
        }
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
        if options.mode.replace_requires_existing() && existing.is_none() {
            return Err(CatalogError::NotFound(CatalogObject::Table, table.into()));
        }
        let location = options.location.ok_or_else(|| {
            CatalogError::InvalidArgument("replacement requires a table location".into())
        })?;
        let mut properties = Vec::new();
        if let Some(status) = existing {
            match &status.kind {
                TableKind::Table {
                    format,
                    location: old_location,
                    properties: old_properties,
                    ..
                } if format.eq_ignore_ascii_case("iceberg") => {
                    if old_location.as_deref() != Some(location.as_str()) {
                        return Err(CatalogError::NotSupported(
                            "relocating an Iceberg table during replacement".into(),
                        ));
                    }
                    properties = old_properties.clone();
                }
                _ => {
                    return Err(CatalogError::NotSupported(
                        "replacing a non-Iceberg table with Iceberg".into(),
                    ));
                }
            }
        }
        // Validate the original bookkeeping too: a request must not hide a managed marker.
        if properties
            .iter()
            .chain(options.properties.iter())
            .any(|(key, value)| {
                sail_common_datafusion::catalog::iceberg::is_iceberg_table_marker(
                    key.trim(),
                    value.trim(),
                ) || key.trim().starts_with("metadata.")
            })
        {
            return Err(CatalogError::UnsupportedCapability(
                "replacement requires path-managed Iceberg publication authority".into(),
            ));
        }
        let mut catalog_table = vec![self.name.clone()];
        catalog_table.extend(Vec::<String>::from(database.clone()));
        catalog_table.push(table.to_string());
        Ok(TableReplacement {
            catalog_table,
            catalog_generation: self.generation,
            database_generation: db.generation,
            table_generation: db.table_generations.get(table).copied().unwrap_or(0),
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
            partition_by: options.partition_by,
            sort_by: options.sort_by,
            properties,
            property_updates: options.properties,
            is_external: options.is_external,
        })
    }

    pub(super) async fn publish_replacement(
        &self,
        replacement: TableReplacement,
        publication: TablePublication,
    ) -> CatalogResult<()> {
        if replacement.catalog_generation != self.generation {
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
        let gate = self.publication_gate.clone();
        // Only this short final publication is reserved, not worker execution. Dropping the
        // caller's JoinHandle detaches this task; publication and status installation continue.
        tokio::spawn(async move {
            let _reservation = gate.write_owned().await;
            let status = {
                let db = databases.get(&database).ok_or_else(|| {
                    CatalogError::Conflict("replacement database was dropped".into())
                })?;
                db.check_known(&table)?;
                if db.generation != replacement.database_generation
                    || db.table_generations.get(&table).copied().unwrap_or(0)
                        != replacement.table_generation
                    || db.views.contains_key(&table)
                    || (replacement.require_existing && !db.tables.contains_key(&table))
                {
                    return Err(CatalogError::Conflict(
                        "replacement catalog identity changed".into(),
                    ));
                }
                prepared_status(&replacement, &database, &table)
            };
            // A publication panic is also an unknown outcome, never evidence that files are unused.
            let result = tokio::spawn(publication).await.unwrap_or_else(|error| {
                Err(CatalogError::CommitStateUnknown(error.to_string().into()))
            });
            // The exclusive gate prevents removal/recreation of this database. No DashMap
            // guard survives the I/O above and there is no await or fallible step after it.
            databases.entry(database).and_modify(|db| {
                if result.is_ok() {
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
            comment: None,
            constraints: vec![],
            location: Some(replacement.location.clone()),
            format: "iceberg".into(),
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
    use sail_common_datafusion::uuid::{UUID_METADATA_KEY, is_uuid_field};
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
    async fn replacement_preserves_top_level_uuid_metadata() -> CatalogResult<()> {
        for existing in [false, true] {
            let (provider, namespace) = fixture(existing).await?;
            let mut request = options();
            let metadata = std::collections::BTreeMap::from([
                (UUID_METADATA_KEY.into(), "uuid".into()),
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
            match mutation {
                "catalog" => staged.catalog_generation += 1,
                "database" => staged.database_generation += 1,
                "generation" => staged.table_generation += 1,
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
                assert!(provider.publication_gate.try_read().is_err());
                if cancel {
                    task.abort();
                }
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
                // This read waits for detached finalization, rather than polling or sleeping.
                assert_eq!(
                    status_value(provider.get_table(&namespace, "t").await?),
                    status_value(expected)
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
