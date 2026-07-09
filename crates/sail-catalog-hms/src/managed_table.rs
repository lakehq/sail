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
use volo_thrift::MaybeException;

use crate::provider::{HmsCatalogProvider, apply_alter_table_options};

const HMS_LOCK_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
const HMS_LOCK_CHECK_INTERVAL: Duration = Duration::from_millis(200);

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
    options: AlterTableOptions,
) -> CatalogResult<()> {
    let (_, client) = provider.current_client().await?;
    let lock_id = acquire_table_lock(&client, db_name, table_name).await?;
    // TODO: Distinguish HMS lock/heartbeat/alter failures that make commit
    // state unknown from ordinary conflicts or external errors.
    let result = async {
        let mut hms_table = HmsCatalogProvider::get_table_with_client(
            &client,
            db_name,
            table_name,
            "Failed to fetch HMS table for locked alter",
        )
        .await?;
        apply_alter_table_options(&mut hms_table, db_name, table_name, options)?;
        HmsCatalogProvider::alter_table_with_client(&client, db_name, table_name, hms_table).await
    }
    .await;
    let unlock_result = release_table_lock(&client, db_name, table_name, lock_id).await;
    match (result, unlock_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

async fn acquire_table_lock(
    client: &ThriftHiveMetastoreClient,
    db_name: &str,
    table_name: &str,
) -> CatalogResult<i64> {
    let mut response = match client
        .lock(LockRequest {
            component: vec![LockComponent {
                r#type: LockType::EXCLUSIVE,
                level: LockLevel::TABLE,
                dbname: db_name.to_string().into(),
                tablename: Some(table_name.to_string().into()),
                partitionname: None,
                operation_type: Some(DataOperationType::NO_TXN),
                is_acid: Some(false),
                is_dynamic_partition_write: Some(false),
            }],
            txnid: None,
            user: std::env::var("USER")
                .ok()
                .filter(|user| !user.is_empty())
                .unwrap_or_else(|| "sail".to_string())
                .into(),
            hostname: std::env::var("HOSTNAME")
                .ok()
                .filter(|host| !host.is_empty())
                .unwrap_or_else(|| "localhost".to_string())
                .into(),
            agent_info: Some("sail".into()),
        })
        .await
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
                response = match client
                    .check_lock(CheckLockRequest {
                        lockid: response.lockid,
                        txnid: None,
                        elapsed_ms: Some(start.elapsed().as_millis() as i64),
                    })
                    .await
                {
                    Ok(MaybeException::Ok(response)) => response,
                    Ok(MaybeException::Exception(err)) => {
                        let _ =
                            release_table_lock(client, db_name, table_name, response.lockid).await;
                        return Err(hms_check_lock_error(
                            "Failed to check HMS lock",
                            db_name,
                            table_name,
                            err,
                        ));
                    }
                    Err(err) => {
                        let _ =
                            release_table_lock(client, db_name, table_name, response.lockid).await;
                        return Err(HmsCatalogProvider::hms_client_error(
                            &format!("Failed to check HMS lock for '{db_name}.{table_name}'"),
                            err,
                        ));
                    }
                };
            }
            state if state == LockState::ABORT || state == LockState::NOT_ACQUIRED => {
                let _ = release_table_lock(client, db_name, table_name, response.lockid).await;
                return Err(CatalogError::Conflict(format!(
                    "Failed to acquire HMS lock on '{db_name}.{table_name}': {}",
                    state.to_string()
                )));
            }
            state => {
                let _ = release_table_lock(client, db_name, table_name, response.lockid).await;
                return Err(CatalogError::External(format!(
                    "Failed to acquire HMS lock on '{db_name}.{table_name}': unexpected state {}",
                    state.to_string()
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
    match client.unlock(UnlockRequest { lockid: lock_id }).await {
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
