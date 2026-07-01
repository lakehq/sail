use std::time::{Duration, Instant};

use sail_catalog::error::{CatalogError, CatalogResult};
use sail_catalog::provider::AlterTableOptions;
use sail_common_datafusion::catalog::managed::{
    metadata_location_update, previous_metadata_location_update,
};
use sail_common_hms::hms::{
    CheckLockRequest, DataOperationType, LockComponent, LockLevel, LockRequest, LockState,
    LockType, Table, ThriftHiveMetastoreCheckLockException, ThriftHiveMetastoreClient,
    ThriftHiveMetastoreLockException, ThriftHiveMetastoreUnlockException, UnlockRequest,
};
use volo_thrift::MaybeException;

use crate::provider::{apply_alter_table_options, hms_metadata_location, HmsCatalogProvider};

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
    let current = hms_metadata_location(hms_table);
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

/// Pre-validates a replacement `Table` before the drop, so client-detectable defects fail
/// while the original is still present. Rejects duplicate column names (case-insensitive, per
/// HMS/Spark), which HMS would otherwise reject server-side only after the drop.
fn validate_replacement_table(
    replacement: &Table,
    db_name: &str,
    table_name: &str,
) -> CatalogResult<()> {
    let mut seen = std::collections::HashSet::new();
    let columns = replacement
        .sd
        .as_ref()
        .and_then(|sd| sd.cols.as_deref())
        .into_iter()
        .flatten();
    let partitions = replacement.partition_keys.as_deref().into_iter().flatten();
    for field in columns.chain(partitions) {
        if let Some(name) = field.name.as_deref() {
            if !seen.insert(name.to_ascii_lowercase()) {
                return Err(CatalogError::InvalidArgument(format!(
                    "Cannot replace table '{db_name}.{table_name}': duplicate column name '{name}'"
                )));
            }
        }
    }
    Ok(())
}

/// Replaces an HMS table by drop-then-create under an exclusive lock: HMS has no atomic
/// swap (`alter_table` can't change `partition_keys`, rename/create over an existing name
/// throw, and DDL isn't transactional), so the result is a fresh, unrelated table.
///
/// Re-reads under the lock and re-validates `expected_metadata_location` (OCC) so a commit
/// that landed since the caller's read is rejected, not clobbered. On create failure the
/// original is restored from the pre-drop snapshot; a failed restore yields a
/// `CatalogError::External` making the "table left dropped" state explicit.
///
/// Residual non-atomic window: between drop and create the table is absent to any reader
/// bypassing the advisory lock. Drop is metadata-only (`delete_data = false`); Sail creates
/// only EXTERNAL tables. See `create_table` for the same-location stale-data note.
pub(crate) async fn replace_table_with_lock(
    provider: &HmsCatalogProvider,
    db_name: &str,
    table_name: &str,
    mut replacement: Table,
    expected_metadata_location: Option<String>,
) -> CatalogResult<()> {
    validate_replacement_table(&replacement, db_name, table_name)?;
    let (_, client) = provider.current_client().await?;
    let lock_id = acquire_table_lock(&client, db_name, table_name).await?;
    let result = async {
        let current = HmsCatalogProvider::get_table_with_client(
            &client,
            db_name,
            table_name,
            "Failed to fetch HMS table for locked replace",
        )
        .await?;
        crate::provider::assert_replace_metadata_location_unchanged(
            expected_metadata_location.as_deref(),
            &current,
            db_name,
            table_name,
        )?;
        // Carry the owner forward so REPLACE doesn't reset it (only `owner` is exposed;
        // grants are not carried).
        if let Some(owner) = current.owner.clone() {
            replacement.owner = Some(owner);
        }
        HmsCatalogProvider::drop_table_with_client(&client, db_name, table_name, false, false)
            .await?;
        match HmsCatalogProvider::create_table_with_client(
            &client,
            db_name,
            table_name,
            replacement,
        )
        .await
        {
            Ok(()) => Ok(()),
            Err(create_error) => {
                // Compensating restore: the drop already committed, so re-create the
                // original from the pre-drop snapshot; combine errors if that also fails.
                match HmsCatalogProvider::create_table_with_client(
                    &client, db_name, table_name, current,
                )
                .await
                {
                    Ok(()) => Err(create_error),
                    Err(restore_error) => Err(CatalogError::External(format!(
                        "Failed to replace table '{db_name}.{table_name}': create failed ({create_error}) \
                         and the compensating restore of the original also failed ({restore_error}); \
                         the table has been left dropped in HMS and must be recovered manually"
                    ))),
                }
            }
        }
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

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use sail_common_hms::hms::{FieldSchema, StorageDescriptor, Table};

    use super::validate_replacement_table;

    fn field(name: &str) -> FieldSchema {
        FieldSchema {
            name: Some(name.to_string().into()),
            r#type: Some("string".into()),
            ..Default::default()
        }
    }

    fn table_with(cols: Vec<FieldSchema>, partitions: Vec<FieldSchema>) -> Table {
        Table {
            sd: Some(StorageDescriptor {
                cols: Some(cols),
                ..Default::default()
            }),
            partition_keys: (!partitions.is_empty()).then_some(partitions),
            ..Default::default()
        }
    }

    #[test]
    fn accepts_distinct_columns() {
        let table = table_with(vec![field("id"), field("value")], vec![field("day")]);
        assert!(validate_replacement_table(&table, "db", "t").is_ok());
    }

    #[test]
    fn rejects_duplicate_regular_columns() {
        let table = table_with(vec![field("id"), field("id")], vec![]);
        let error = validate_replacement_table(&table, "db", "t").unwrap_err();
        assert!(matches!(
            error,
            sail_catalog::error::CatalogError::InvalidArgument(_)
        ));
        assert!(error.to_string().contains("duplicate column name 'id'"));
    }

    #[test]
    fn rejects_duplicate_across_regular_and_partition() {
        // HMS stores partition keys in the same COLUMNS namespace; a name shared
        // between a regular column and a partition key would also collide.
        let table = table_with(vec![field("id"), field("day")], vec![field("day")]);
        let error = validate_replacement_table(&table, "db", "t").unwrap_err();
        assert!(matches!(
            error,
            sail_catalog::error::CatalogError::InvalidArgument(_)
        ));
    }

    #[test]
    fn duplicate_detection_is_case_insensitive() {
        // HMS/Spark treat column identity case-insensitively.
        let table = table_with(vec![field("ID"), field("id")], vec![]);
        assert!(validate_replacement_table(&table, "db", "t").is_err());
    }
}
