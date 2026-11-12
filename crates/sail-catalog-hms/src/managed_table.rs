use std::time::{Duration, Instant};

use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::provider::AlterTableOptions;
use sail_common_datafusion::catalog::managed::{
    metadata_location_update, metadata_location_value, previous_metadata_location_update,
};
use sail_common_hms::hms::{
    CheckLockRequest, DataOperationType, LockComponent, LockLevel, LockRequest, LockState,
    LockType, Table, ThriftHiveMetastoreCheckLockException, ThriftHiveMetastoreClient,
    ThriftHiveMetastoreLockException, ThriftHiveMetastoreUnlockException, UnlockRequest,
};
use sail_thrift::MaybeException;

use crate::provider::{HmsCatalogProvider, apply_alter_table_options};

#[cfg(test)]
mod tests;

const HMS_LOCK_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
const HMS_LOCK_CHECK_INTERVAL: Duration = Duration::from_millis(200);
const HMS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
const HMS_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);
const HMS_RPC_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) fn is_metadata_location_update(options: &AlterTableOptions) -> bool {
    match options {
        AlterTableOptions::SetTableProperties { properties } => {
            metadata_location_update(properties).is_some()
        }
        _ => false,
    }
}

pub(crate) fn validate_metadata_location_precondition(
    hms_table: &Table,
    db_name: &str,
    table_name: &str,
    properties: &[(String, String)],
) -> CatalogResult<()> {
    let Some(expected) = previous_metadata_location_update(properties) else {
        return Ok(());
    };
    if metadata_location_update(properties).is_none() {
        return Ok(());
    }
    let mut aliases = hms_table
        .parameters
        .iter()
        .flatten()
        .filter(|(key, _)| {
            sail_common_datafusion::catalog::managed::is_metadata_location_key(key.trim())
        })
        .map(|(_, value)| value);
    let first = aliases.next();
    if aliases.any(|value| Some(value) != first) {
        return Err(CatalogError::CommitRejected(
            "Inconsistent Iceberg metadata pointer aliases".into(),
        ));
    }
    let current = hms_table.parameters.as_ref().and_then(|parameters| {
        metadata_location_value(
            parameters
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        )
        .map(ToString::to_string)
    });
    if current.as_deref() != Some(expected) {
        return Err(CatalogError::Conflict(format!(
            "Cannot commit catalog-managed table '{db_name}.{table_name}' because base metadata location '{expected}' does not match current HMS metadata location '{}'",
            current.unwrap_or_else(|| "<none>".to_string())
        )));
    }
    Ok(())
}

pub(crate) async fn alter_table_with_lock(
    provider: &HmsCatalogProvider,
    db_name: &str,
    table_name: &str,
    options: Vec<AlterTableOptions>,
) -> CatalogResult<()> {
    alter_under_lock(provider, db_name, table_name, |mut current| {
        validate_lock_enabled(&current)?;
        for option in options {
            let update = match &option {
                AlterTableOptions::SetTableProperties { properties } => {
                    previous_metadata_location_update(properties)
                        .zip(metadata_location_update(properties))
                        .map(|(expected, replacement)| {
                            sail_catalog::lakehouse::IcebergMetadataCasRequest::new(
                                expected.into(),
                                replacement.into(),
                            )
                        })
                        .transpose()?
                }
                _ => None,
            };
            apply_alter_table_options(&mut current, db_name, table_name, option)?;
            if let Some(update) = update {
                update_metadata_parameters(
                    current.parameters.get_or_insert_with(Default::default),
                    &update,
                );
            }
        }
        validate_lock_enabled(&current)?;
        Ok(current)
    })
    .await
}

// [CREDIT]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/hive-metastore/src/main/java/org/apache/iceberg/hive/HiveTableOperations.java#L250-L344
pub(crate) async fn compare_and_swap_iceberg_metadata(
    provider: &HmsCatalogProvider,
    database: &str,
    table: &str,
    request: sail_catalog::lakehouse::IcebergMetadataCasRequest,
) -> CatalogResult<sail_catalog::lakehouse::IcebergMetadataCasOutcome> {
    use sail_catalog::lakehouse::IcebergMetadataCasOutcome as Outcome;
    let result = alter_under_lock(provider, database, table, |mut current| {
        // HMS may omit Iceberg properties, so also validate the loaded base metadata.
        validate_lock_property(
            request
                .base_properties()
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str())),
        )?;
        validate_lock_enabled(&current)?;
        let parameters = current.parameters.get_or_insert_with(Default::default);
        if !parameters
            .get("table_type")
            .is_some_and(|value| value.eq_ignore_ascii_case("ICEBERG"))
        {
            return Err(CatalogError::CommitRejected(
                "HMS Iceberg CAS requires an Iceberg table".into(),
            ));
        }
        request
            .validate_base(
                parameters
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str())),
            )
            .map_err(outcome_error)?;
        update_metadata_parameters(parameters, &request);
        Ok(current)
    })
    .await;
    match result {
        Ok(()) => Ok(Outcome::Committed),
        Err(CatalogError::Conflict(message)) => Ok(Outcome::Conflict { message }),
        Err(CatalogError::CommitRejected(message)) => Ok(Outcome::Rejected { message }),
        Err(CatalogError::CommitStateUnknown(error)) => Ok(Outcome::Unknown {
            message: error.to_string(),
        }),
        Err(error) => Err(error),
    }
}

fn update_metadata_parameters(
    parameters: &mut std::collections::HashMap<String, String>,
    request: &sail_catalog::lakehouse::IcebergMetadataCasRequest,
) {
    use sail_common_datafusion::catalog::managed::{
        METADATA_LOCATION_UNDERSCORE_KEY, is_metadata_location_key,
    };
    parameters.retain(|key, _| {
        !key.trim()
            .eq_ignore_ascii_case("previous_metadata_location")
            && !is_metadata_location_key(key.trim())
    });
    // Official Hive clients maintain only the canonical pointer on subsequent commits.
    parameters.insert(
        METADATA_LOCATION_UNDERSCORE_KEY.into(),
        request.replacement().into(),
    );
    parameters.insert(
        "previous_metadata_location".into(),
        request.expected().into(),
    );
}

fn outcome_error(outcome: sail_catalog::lakehouse::IcebergMetadataCasOutcome) -> CatalogError {
    use sail_catalog::lakehouse::IcebergMetadataCasOutcome as Outcome;
    match outcome {
        Outcome::Conflict { message } => CatalogError::Conflict(message),
        Outcome::Rejected { message } => CatalogError::CommitRejected(message),
        _ => CatalogError::Internal("Unexpected HMS base validation outcome".into()),
    }
}

fn validate_lock_enabled(table: &Table) -> CatalogResult<()> {
    validate_lock_property(
        table
            .parameters
            .iter()
            .flatten()
            .map(|(k, v)| (k.as_str(), v.as_str())),
    )
}

pub(crate) fn validate_lock_property<'a>(
    properties: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> CatalogResult<()> {
    if properties.into_iter().any(|(key, value)| {
        key == "engine.hive.lock-enabled" && !value.eq_ignore_ascii_case("true")
    }) {
        return Err(CatalogError::CommitRejected(
            "HMS publication requires engine.hive.lock-enabled=true and cooperative locking by all writers".into(),
        ));
    }
    Ok(())
}

/// All writers must use exclusive HMS table locks. These locks are not fencing tokens:
/// a late RPC can still publish after lock loss, so uncertain outcomes must never be replayed.
async fn alter_under_lock(
    provider: &HmsCatalogProvider,
    database: &str,
    table: &str,
    prepare: impl FnOnce(Table) -> CatalogResult<Table>,
) -> CatalogResult<()> {
    let (client, heartbeat_client) = tokio::time::timeout(HMS_RPC_TIMEOUT, provider.lock_clients())
        .await
        .map_err(|_| CatalogError::External("Timed out creating HMS lock clients".into()))??;
    let lock_id = acquire_table_lock(&client, database, table).await?;
    let result = locked_alter(
        &client,
        &heartbeat_client,
        lock_id,
        database,
        table,
        prepare,
    )
    .await;
    // Unlock is not publication: failure here must not turn a confirmed commit into a retry.
    if let Err(error) = release_table_lock(&heartbeat_client, database, table, lock_id).await {
        log::warn!("Failed to release HMS table lock: {error}");
    }
    result
}

async fn locked_alter(
    client: &ThriftHiveMetastoreClient,
    heartbeat_client: &ThriftHiveMetastoreClient,
    lock_id: i64,
    database: &str,
    table: &str,
    prepare: impl FnOnce(Table) -> CatalogResult<Table>,
) -> CatalogResult<()> {
    let monitor = async {
        loop {
            heartbeat(heartbeat_client, lock_id).await?;
            tokio::time::sleep(HMS_HEARTBEAT_INTERVAL).await;
        }
    };
    tokio::pin!(monitor);
    // Poll the heartbeat first, immediately after acquisition, and throughout the reload.
    let preparation = async {
        let current = tokio::time::timeout(
            HMS_RPC_TIMEOUT,
            HmsCatalogProvider::get_table_with_client(
                client,
                database,
                table,
                "Failed to load locked HMS table",
            ),
        )
        .await
        .map_err(|_| CatalogError::External("Timed out loading locked HMS table".into()))??;
        let replacement = prepare(current)?;
        heartbeat(heartbeat_client, lock_id).await?;
        Ok::<Table, CatalogError>(replacement)
    };
    let replacement = tokio::select! {
        biased;
        result = &mut monitor => return result,
        result = preparation => result?,
    };
    let dispatched = std::sync::atomic::AtomicBool::new(false);
    let publication = async {
        dispatched.store(true, std::sync::atomic::Ordering::Relaxed);
        let result = locked_publication(client, database, table, replacement).await;
        heartbeat(heartbeat_client, lock_id)
            .await
            .map_err(|error| {
                CatalogError::CommitStateUnknown(
                    format!("HMS lock health lost after publication: {error}").into(),
                )
            })?;
        result
    };
    tokio::pin!(publication);
    tokio::select! {
        biased;
        health = &mut monitor => {
            if !dispatched.load(std::sync::atomic::Ordering::Relaxed) {
                return health;
            }
            // Poll the dispatched request to its bounded outcome, but never confirm success
            // after losing the lock. A stale read would not prove it failed either.
            let _ = publication.await;
            Err(CatalogError::CommitStateUnknown("HMS lock health lost during publication".to_string().into()))
        }
        result = &mut publication => result,
    }
}

// [CREDIT]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/hive-metastore/src/main/java/org/apache/iceberg/hive/HiveOperationsBase.java
async fn locked_publication(
    client: &ThriftHiveMetastoreClient,
    database: &str,
    table: &str,
    replacement: Table,
) -> CatalogResult<()> {
    let context = sail_common_hms::hms::EnvironmentContext {
        properties: Some(std::collections::HashMap::from([(
            "DO_NOT_UPDATE_STATS".into(),
            "true".into(),
        )])),
    };
    // There is no expected_parameter_* in lock mode, and no failover/replay of this RPC.
    match tokio::time::timeout(
        HMS_RPC_TIMEOUT,
        client.alter_table_with_environment_context(
            database.into(),
            table.into(),
            replacement,
            context,
        ),
    )
    .await
    {
        Ok(Ok(MaybeException::Ok(()))) => Ok(()),
        // Even declared exceptions can originate in listeners after commit.
        result => Err(CatalogError::CommitStateUnknown(
            format!("HMS alter may have published: {result:?}").into(),
        )),
    }
}

async fn heartbeat(client: &ThriftHiveMetastoreClient, lock_id: i64) -> CatalogResult<()> {
    match tokio::time::timeout(
        HMS_HEARTBEAT_TIMEOUT,
        client.heartbeat(sail_common_hms::hms::HeartbeatRequest {
            lockid: Some(lock_id),
            txnid: None,
        }),
    )
    .await
    {
        Ok(Ok(MaybeException::Ok(()))) => Ok(()),
        result => Err(CatalogError::External(format!(
            "HMS lock heartbeat failed: {result:?}"
        ))),
    }
}

async fn acquire_table_lock(
    client: &ThriftHiveMetastoreClient,
    db_name: &str,
    table_name: &str,
) -> CatalogResult<i64> {
    let mut response = match tokio::time::timeout(
        HMS_RPC_TIMEOUT,
        client.lock(LockRequest {
            component: vec![LockComponent {
                r#type: LockType::EXCLUSIVE,
                level: LockLevel::TABLE,
                dbname: db_name.to_string(),
                tablename: Some(table_name.to_string()),
                partitionname: None,
                operation_type: Some(DataOperationType::NO_TXN),
                is_acid: Some(false),
                is_dynamic_partition_write: Some(false),
            }],
            txnid: None,
            user: std::env::var("USER")
                .ok()
                .filter(|user| !user.is_empty())
                .unwrap_or_else(|| "sail".to_string()),
            hostname: std::env::var("HOSTNAME")
                .ok()
                .filter(|host| !host.is_empty())
                .unwrap_or_else(|| "localhost".to_string()),
            agent_info: Some("sail".into()),
        }),
    )
    .await
    .map_err(|_| CatalogError::External("Timed out acquiring HMS lock".into()))?
    {
        Ok(MaybeException::Ok(response)) => response,
        Ok(MaybeException::Exception(err)) => {
            return Err(hms_lock_error(
                "Failed to acquire HMS lock",
                db_name,
                table_name,
                err,
            ));
        }
        Err(err) => {
            return Err(HmsCatalogProvider::hms_client_error(
                &format!("Failed to acquire HMS lock for '{db_name}.{table_name}'"),
                err,
            ));
        }
    };

    let start = Instant::now();
    loop {
        match response.state {
            state if state == LockState::ACQUIRED => return Ok(response.lockid),
            state if state == LockState::WAITING => {
                if start.elapsed() >= HMS_LOCK_ACQUIRE_TIMEOUT {
                    let _ = release_table_lock(client, db_name, table_name, response.lockid).await;
                    return Err(CatalogError::Conflict(format!(
                        "Timed out while waiting for HMS lock on '{db_name}.{table_name}'"
                    )));
                }
                tokio::time::sleep(HMS_LOCK_CHECK_INTERVAL).await;
                response = match tokio::time::timeout(
                    HMS_RPC_TIMEOUT,
                    client.check_lock(CheckLockRequest {
                        lockid: response.lockid,
                        txnid: None,
                        elapsed_ms: Some(start.elapsed().as_millis() as i64),
                    }),
                )
                .await
                {
                    Err(_) => {
                        let _ =
                            release_table_lock(client, db_name, table_name, response.lockid).await;
                        return Err(CatalogError::External("Timed out checking HMS lock".into()));
                    }
                    Ok(result) => match result {
                        Ok(MaybeException::Ok(response)) => response,
                        Ok(MaybeException::Exception(err)) => {
                            let _ =
                                release_table_lock(client, db_name, table_name, response.lockid)
                                    .await;
                            return Err(hms_check_lock_error(
                                "Failed to check HMS lock",
                                db_name,
                                table_name,
                                err,
                            ));
                        }
                        Err(err) => {
                            let _ =
                                release_table_lock(client, db_name, table_name, response.lockid)
                                    .await;
                            return Err(HmsCatalogProvider::hms_client_error(
                                &format!("Failed to check HMS lock for '{db_name}.{table_name}'"),
                                err,
                            ));
                        }
                    },
                };
            }
            state if state == LockState::ABORT || state == LockState::NOT_ACQUIRED => {
                let _ = release_table_lock(client, db_name, table_name, response.lockid).await;
                return Err(CatalogError::Conflict(format!(
                    "Failed to acquire HMS lock on '{db_name}.{table_name}': {}",
                    state
                )));
            }
            state => {
                let _ = release_table_lock(client, db_name, table_name, response.lockid).await;
                return Err(CatalogError::External(format!(
                    "Failed to acquire HMS lock on '{db_name}.{table_name}': unexpected state {}",
                    state
                )));
            }
        }
    }
}

async fn release_table_lock(
    client: &ThriftHiveMetastoreClient,
    db_name: &str,
    table_name: &str,
    lock_id: i64,
) -> CatalogResult<()> {
    match tokio::time::timeout(
        HMS_HEARTBEAT_TIMEOUT,
        client.unlock(UnlockRequest { lockid: lock_id }),
    )
    .await
    .map_err(|_| CatalogError::External("Timed out releasing HMS lock".into()))?
    {
        Ok(MaybeException::Ok(())) => Ok(()),
        Ok(MaybeException::Exception(err)) => Err(hms_unlock_error(
            "Failed to release HMS lock",
            db_name,
            table_name,
            err,
        )),
        Err(err) => Err(HmsCatalogProvider::hms_client_error(
            &format!("Failed to release HMS lock for '{db_name}.{table_name}'"),
            err,
        )),
    }
}

fn hms_lock_error(
    context: &str,
    db_name: &str,
    table_name: &str,
    error: ThriftHiveMetastoreLockException,
) -> CatalogError {
    CatalogError::External(format!("{context} for '{db_name}.{table_name}': {error:?}"))
}

fn hms_check_lock_error(
    context: &str,
    db_name: &str,
    table_name: &str,
    error: ThriftHiveMetastoreCheckLockException,
) -> CatalogError {
    CatalogError::External(format!("{context} for '{db_name}.{table_name}': {error:?}"))
}

fn hms_unlock_error(
    context: &str,
    db_name: &str,
    table_name: &str,
    error: ThriftHiveMetastoreUnlockException,
) -> CatalogError {
    CatalogError::External(format!("{context} for '{db_name}.{table_name}': {error:?}"))
}
