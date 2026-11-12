//! Semantic ports of lock lifecycle and uncertain-publication cases, using real Thrift pools.
// [CREDIT]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/hive-metastore/src/test/java/org/apache/iceberg/hive/TestHiveCommitLocks.java
// [CREDIT]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/hive-metastore/src/test/java/org/apache/iceberg/hive/TestHiveCommits.java

#![expect(clippy::panic, reason = "unexpected RPCs must fail the test server")]

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use sail_catalog::lakehouse::{
    IcebergMetadataCasOutcome as Outcome, IcebergMetadataCasRequest as Request,
};
use sail_catalog::provider::{
    CatalogProvider, Namespace, RenameTableMetadataRequirement, RenameTablePrecondition,
};
use sail_common_hms::hms::{
    EnvironmentContext, GetTableRequest, GetTableResult, InvalidOperationException, LockResponse,
    MetaException, NoSuchLockException, NoSuchObjectException,
};
use sail_thrift::Transport;
use sail_thrift::transport::{read_message, write_message};
use thrift::protocol::{
    TBinaryInputProtocol, TBinaryOutputProtocol, TFieldIdentifier, TInputProtocol,
    TMessageIdentifier, TMessageType, TOutputProtocol, TSerializable, TStructIdentifier, TType,
};

use super::*;

#[derive(Clone, Copy, Default)]
enum Fault {
    #[default]
    None,
    LostAck,
    MetaException,
    StaleMessage,
}

#[derive(Default)]
struct Behavior {
    read_until_heartbeats: usize,
    alter_until_heartbeats: usize,
    heartbeat_fail_at: usize,
    heartbeat_gate_at: usize,
    read_gate_at: usize,
    occupied_destination: bool,
    failover_endpoint: bool,
    unlock_failure: bool,
    hold_alters: bool,
    lock_unavailable: bool,
    fault: Fault,
}

struct State {
    behavior: Behavior,
    table: Mutex<Table>,
    reads: AtomicUsize,
    alters: AtomicUsize,
    heartbeats: AtomicUsize,
    unlocks: AtomicUsize,
    locks: AtomicUsize,
    waiting_locks: AtomicUsize,
    // Lock IDs are ordered; only the first outstanding request for a table may acquire it.
    lock_requests: Mutex<std::collections::BTreeMap<i64, (String, String)>>,
    contexts: Mutex<Vec<EnvironmentContext>>,
    release_alters: tokio::sync::Semaphore,
    release_heartbeat: tokio::sync::Semaphore,
    release_read: tokio::sync::Semaphore,
    applied_alters: AtomicUsize,
}

struct Harness {
    provider: Arc<HmsCatalogProvider>,
    state: Arc<State>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Harness {
    async fn new(behavior: Behavior) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let uri = listener.local_addr().expect("address").to_string();
        let uris = if behavior.failover_endpoint {
            vec![uri.clone(), uri]
        } else {
            vec![uri]
        };
        let state = Arc::new(State {
            behavior,
            table: Mutex::new(Table {
                db_name: Some("db".into()),
                table_name: Some("items".into()),
                parameters: Some(HashMap::from([
                    ("table_type".into(), "ICEBERG".into()),
                    ("metadata_location".into(), "old".into()),
                ])),
                ..Default::default()
            }),
            reads: AtomicUsize::new(0),
            alters: AtomicUsize::new(0),
            heartbeats: AtomicUsize::new(0),
            unlocks: AtomicUsize::new(0),
            locks: AtomicUsize::new(0),
            waiting_locks: AtomicUsize::new(0),
            lock_requests: Mutex::new(Default::default()),
            contexts: Mutex::new(vec![]),
            release_alters: tokio::sync::Semaphore::new(0),
            release_heartbeat: tokio::sync::Semaphore::new(0),
            release_read: tokio::sync::Semaphore::new(0),
            applied_alters: AtomicUsize::new(0),
        });
        let handle = tokio::runtime::Handle::current();
        let provider = Arc::new(
            HmsCatalogProvider::try_new(
                "hms".into(),
                crate::provider::HmsCatalogConfig {
                    uris,
                    ..Default::default()
                },
                sail_common::runtime::RuntimeHandle::new(handle.clone(), handle),
            )
            .expect("provider"),
        );
        let peer_state = state.clone();
        let server = tokio::spawn(async move {
            let mut peers = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (socket, _) = accepted.expect("accept");
                        peers.spawn(serve(socket, peer_state.clone()));
                    }
                    result = peers.join_next(), if !peers.is_empty() => {
                        result.expect("peer").expect("server task");
                    }
                }
            }
        });
        Self {
            provider,
            state,
            server,
        }
    }

    async fn publish(&self, typed: bool, expected: &str) -> CatalogResult<Outcome> {
        let namespace = Namespace::try_from(vec!["db".to_string()]).expect("namespace");
        if typed {
            self.provider
                .compare_and_swap_iceberg_metadata(
                    &namespace,
                    "items",
                    Request::new(expected.into(), "new".into()).expect("request"),
                )
                .await
        } else {
            let result = self
                .provider
                .alter_table(
                    &namespace,
                    "items",
                    AlterTableOptions::SetTableProperties {
                        properties: vec![
                            ("metadata_location".into(), "new".into()),
                            ("previous_metadata_location".into(), expected.into()),
                        ],
                    },
                )
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
    }

    async fn rename(&self, destination_database: &str) -> CatalogResult<()> {
        self.provider
            .rename_table_with_precondition(
                &Namespace::try_from(vec!["db".to_string()]).expect("source namespace"),
                "items",
                &Namespace::try_from(vec![destination_database.to_string()])
                    .expect("destination namespace"),
                "renamed",
                RenameTablePrecondition::Iceberg {
                    metadata_location: "old".into(),
                },
            )
            .await
    }

    async fn rename_unguarded(&self, destination_database: &str) -> CatalogResult<()> {
        self.provider
            .rename_table(
                &Namespace::try_from(vec!["db".to_string()]).expect("source namespace"),
                "items",
                &Namespace::try_from(vec![destination_database.to_string()])
                    .expect("destination namespace"),
                "renamed",
            )
            .await
    }

    fn pointer(&self) -> String {
        self.state
            .table
            .lock()
            .expect("table")
            .parameters
            .as_ref()
            .expect("parameters")["metadata_location"]
            .clone()
    }
}

async fn wait_count(counter: &AtomicUsize, target: usize) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while counter.load(Ordering::SeqCst) < target {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("event reached");
}

trait ReplyValue {
    fn write(&self, protocol: &mut dyn TOutputProtocol) -> thrift::Result<()>;
}

impl<T: TSerializable> ReplyValue for T {
    fn write(&self, protocol: &mut dyn TOutputProtocol) -> thrift::Result<()> {
        self.write_to_out_protocol(protocol)
    }
}

fn reply(header: &TMessageIdentifier, field: Option<(i16, &dyn ReplyValue)>) -> Vec<u8> {
    let mut bytes = vec![];
    let mut p = TBinaryOutputProtocol::new(&mut bytes, true);
    p.write_message_begin(&TMessageIdentifier::new(
        &header.name,
        TMessageType::Reply,
        header.sequence_number,
    ))
    .expect("header");
    p.write_struct_begin(&TStructIdentifier::new("result"))
        .expect("result");
    if let Some((id, value)) = field {
        p.write_field_begin(&TFieldIdentifier::new("result", TType::Struct, id))
            .expect("field");
        value.write(&mut p).expect("value");
        p.write_field_end().expect("end field");
    }
    p.write_field_stop().expect("stop");
    p.write_struct_end().expect("end struct");
    p.write_message_end().expect("end message");
    bytes
}

async fn serve(socket: tokio::net::TcpStream, state: Arc<State>) {
    let (reader, mut writer) = socket.into_split();
    let mut reader = tokio::io::BufReader::new(reader);
    while let Ok(bytes) = read_message(&mut reader, Transport::Buffered).await {
        let (header, replacement, context, get, lock, lock_id) = {
            let mut p = TBinaryInputProtocol::new(Cursor::new(&bytes), true);
            let header = p.read_message_begin().expect("request header");
            let mut replacement = None;
            let mut context = None;
            let mut get = None;
            let mut lock = None;
            let mut lock_id = None;
            p.read_struct_begin().expect("args");
            loop {
                let field = p.read_field_begin().expect("arg");
                if field.field_type == TType::Stop {
                    break;
                }
                if matches!(
                    header.name.as_str(),
                    "alter_table_with_environment_context" | "alter_table"
                ) && field.id == Some(3)
                {
                    replacement = Some(Table::read_from_in_protocol(&mut p).expect("table"));
                } else if header.name == "alter_table_with_environment_context"
                    && field.id == Some(4)
                {
                    context =
                        Some(EnvironmentContext::read_from_in_protocol(&mut p).expect("context"));
                } else if header.name == "get_table_req" && field.id == Some(1) {
                    get =
                        Some(GetTableRequest::read_from_in_protocol(&mut p).expect("get request"));
                } else if header.name == "lock" && field.id == Some(1) {
                    lock = Some(LockRequest::read_from_in_protocol(&mut p).expect("lock request"));
                } else if header.name == "check_lock" && field.id == Some(1) {
                    lock_id = Some(
                        CheckLockRequest::read_from_in_protocol(&mut p)
                            .expect("check request")
                            .lockid,
                    );
                } else if header.name == "unlock" && field.id == Some(1) {
                    lock_id = Some(
                        UnlockRequest::read_from_in_protocol(&mut p)
                            .expect("unlock request")
                            .lockid,
                    );
                } else {
                    p.skip(field.field_type).expect("skip arg");
                }
                p.read_field_end().expect("arg end");
            }
            (header, replacement, context, get, lock, lock_id)
        };
        let bytes = match header.name.as_str() {
            "lock" | "check_lock" => {
                let mut requests = state.lock_requests.lock().expect("locks");
                let id = if let Some(request) = lock {
                    let id = state.locks.fetch_add(1, Ordering::SeqCst) as i64 + 1;
                    assert_eq!(request.component.len(), 1, "source lock only");
                    let component = &request.component[0];
                    assert_eq!(component.r#type, LockType::EXCLUSIVE);
                    requests.insert(
                        id,
                        (
                            component.dbname.clone(),
                            component.tablename.clone().expect("table lock"),
                        ),
                    );
                    id
                } else {
                    lock_id.expect("check lock id")
                };
                let key = requests.get(&id).expect("requested lock");
                let acquired = requests
                    .iter()
                    .find(|(_, value)| *value == key)
                    .map(|(id, _)| *id)
                    == Some(id);
                let lock_state = if state.behavior.lock_unavailable {
                    requests.remove(&id);
                    LockState::NOT_ACQUIRED
                } else if acquired {
                    LockState::ACQUIRED
                } else {
                    state.waiting_locks.fetch_add(1, Ordering::SeqCst);
                    LockState::WAITING
                };
                reply(
                    &header,
                    Some((
                        0,
                        &LockResponse {
                            lockid: id,
                            state: lock_state,
                        },
                    )),
                )
            }
            "get_table_req" => {
                let count = state.reads.fetch_add(1, Ordering::SeqCst) + 1;
                wait_count(&state.heartbeats, state.behavior.read_until_heartbeats).await;
                let table = state.table.lock().expect("table").clone();
                if count == state.behavior.read_gate_at {
                    state
                        .release_read
                        .acquire()
                        .await
                        .expect("release read")
                        .forget();
                }
                let request = get.expect("get request");
                if table.db_name.as_deref() == Some(request.db_name.as_str())
                    && table.table_name.as_deref() == Some(request.tbl_name.as_str())
                {
                    reply(&header, Some((0, &GetTableResult { table })))
                } else {
                    reply(
                        &header,
                        Some((
                            2,
                            &NoSuchObjectException {
                                message: Some("table not found".into()),
                            },
                        )),
                    )
                }
            }
            "heartbeat" => {
                let count = state.heartbeats.fetch_add(1, Ordering::SeqCst) + 1;
                if count == state.behavior.heartbeat_gate_at {
                    state
                        .release_heartbeat
                        .acquire()
                        .await
                        .expect("release heartbeat")
                        .forget();
                }
                if state.behavior.heartbeat_fail_at > 0 && count >= state.behavior.heartbeat_fail_at
                {
                    reply(
                        &header,
                        Some((
                            1,
                            &NoSuchLockException {
                                message: Some("lock lost".into()),
                            },
                        )),
                    )
                } else {
                    reply(&header, None)
                }
            }
            "alter_table_with_environment_context" | "alter_table" => {
                if let Some(context) = context {
                    state.contexts.lock().expect("contexts").push(context);
                }
                state.alters.fetch_add(1, Ordering::SeqCst);
                wait_count(&state.heartbeats, state.behavior.alter_until_heartbeats).await;
                if state.behavior.hold_alters {
                    state
                        .release_alters
                        .acquire()
                        .await
                        .expect("release")
                        .forget();
                }
                let replacement = replacement.expect("replacement");
                let is_rename = {
                    let current = state.table.lock().expect("table");
                    current.db_name != replacement.db_name
                        || current.table_name != replacement.table_name
                };
                if is_rename && state.behavior.occupied_destination {
                    let bytes = reply(
                        &header,
                        Some((
                            1,
                            &InvalidOperationException {
                                message: Some("destination exists".into()),
                            },
                        )),
                    );
                    if write_message(&mut writer, Transport::Buffered, &bytes)
                        .await
                        .is_err()
                    {
                        return;
                    }
                    continue;
                }
                *state.table.lock().expect("table") = replacement;
                state.applied_alters.fetch_add(1, Ordering::SeqCst);
                match state.behavior.fault {
                    Fault::None => reply(&header, None),
                    Fault::LostAck => return,
                    Fault::MetaException | Fault::StaleMessage => reply(&header, Some((2, &MetaException {
                        message: Some(match state.behavior.fault {
                            Fault::StaleMessage => "The table has been modified. A listener failed after commit",
                            _ => "onAlterTable failed after commit",
                        }.into()),
                    }))),
                }
            }
            "unlock" => {
                state.unlocks.fetch_add(1, Ordering::SeqCst);
                state
                    .lock_requests
                    .lock()
                    .expect("locks")
                    .remove(&lock_id.expect("unlock id"));
                if state.behavior.unlock_failure {
                    reply(
                        &header,
                        Some((
                            1,
                            &NoSuchLockException {
                                message: Some("unlock failed".into()),
                            },
                        )),
                    )
                } else {
                    reply(&header, None)
                }
            }
            method => panic!("unexpected RPC: {method}"),
        };
        if write_message(&mut writer, Transport::Buffered, &bytes)
            .await
            .is_err()
        {
            return;
        }
    }
}

#[tokio::test]
async fn heartbeat_covers_delayed_locked_reads_and_stats_are_suppressed() {
    for typed in [false, true] {
        let h = Harness::new(Behavior {
            read_until_heartbeats: 3,
            ..Default::default()
        })
        .await;
        assert_eq!(
            h.publish(typed, "old").await.expect("publish"),
            Outcome::Committed
        );
        assert!(h.state.heartbeats.load(Ordering::SeqCst) >= 5);
        assert_eq!(h.state.alters.load(Ordering::SeqCst), 1);
        assert_eq!(h.state.unlocks.load(Ordering::SeqCst), 1);
        let contexts = h.state.contexts.lock().expect("contexts");
        assert_eq!(
            contexts[0].properties,
            Some(HashMap::from([(
                "DO_NOT_UPDATE_STATS".into(),
                "true".into()
            )]))
        );
        assert_eq!(h.pointer(), "new");
    }
}

#[tokio::test]
async fn heartbeat_failure_before_publication_never_alters() {
    for typed in [false, true] {
        let h = Harness::new(Behavior {
            read_until_heartbeats: 3,
            heartbeat_fail_at: 2,
            ..Default::default()
        })
        .await;
        assert!(h.publish(typed, "old").await.is_err());
        assert_eq!(h.state.reads.load(Ordering::SeqCst), 1);
        assert_eq!(h.state.alters.load(Ordering::SeqCst), 0);
        assert_eq!(h.state.unlocks.load(Ordering::SeqCst), 1);
        assert_eq!(h.pointer(), "old");
    }
}

#[tokio::test]
async fn heartbeat_loss_during_publication_is_unknown() {
    for typed in [false, true] {
        let h = Harness::new(Behavior {
            alter_until_heartbeats: 3,
            heartbeat_fail_at: 3,
            ..Default::default()
        })
        .await;
        assert!(matches!(
            h.publish(typed, "old").await.expect("outcome"),
            Outcome::Unknown { .. }
        ));
        assert_eq!(h.state.alters.load(Ordering::SeqCst), 1);
        // The server may finish a request already received even though the client cancelled it.
        wait_count(&h.state.applied_alters, 1).await;
        assert_eq!(h.pointer(), "new");
    }
}

#[tokio::test]
async fn lost_ack_and_postcommit_exceptions_are_unknown_without_replay() {
    for typed in [false, true] {
        for fault in [Fault::LostAck, Fault::MetaException, Fault::StaleMessage] {
            let h = Harness::new(Behavior {
                fault,
                ..Default::default()
            })
            .await;
            assert!(matches!(
                h.publish(typed, "old").await.expect("outcome"),
                Outcome::Unknown { .. }
            ));
            assert_eq!(h.state.alters.load(Ordering::SeqCst), 1);
            assert_eq!(h.pointer(), "new");
        }
    }
}

#[tokio::test]
async fn successful_publication_survives_unlock_failure() {
    for typed in [false, true] {
        let h = Harness::new(Behavior {
            unlock_failure: true,
            ..Default::default()
        })
        .await;
        assert_eq!(
            h.publish(typed, "old").await.expect("outcome"),
            Outcome::Committed
        );
        assert_eq!(h.state.unlocks.load(Ordering::SeqCst), 1);
        assert_eq!(h.pointer(), "new");
    }
}

#[tokio::test]
async fn eight_blocked_alters_cannot_starve_heartbeat_pool() {
    let h = Harness::new(Behavior {
        hold_alters: true,
        ..Default::default()
    })
    .await;
    let (client, control) = h.provider.lock_clients().await.expect("clients");
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let client = client.clone();
        let table = h.state.table.lock().expect("table").clone();
        tasks.spawn(async move { locked_publication(&client, "db", "items", table).await });
    }
    wait_count(&h.state.alters, 8).await;
    tokio::time::timeout(Duration::from_secs(2), heartbeat(&control, 1))
        .await
        .expect("independent capacity")
        .expect("heartbeat");
    assert_eq!(h.state.heartbeats.load(Ordering::SeqCst), 1);
    assert_eq!(h.state.alters.load(Ordering::SeqCst), 8);
    h.state.release_alters.add_permits(8);
    while let Some(result) = tasks.join_next().await {
        result.expect("task").expect("publish");
    }
}

#[tokio::test]
async fn queued_publication_is_cancelled_before_capacity_returns_on_lock_loss() {
    for typed in [false, true] {
        let h = Harness::new(Behavior {
            heartbeat_gate_at: 2,
            heartbeat_fail_at: 3,
            hold_alters: true,
            ..Default::default()
        })
        .await;
        let publication = h.publish(typed, "old");
        tokio::pin!(publication);
        // Gate the final preparation heartbeat, after the locked reload released its permit.
        tokio::select! {
            result = &mut publication => panic!("publication completed before gate: {result:?}"),
            () = wait_count(&h.state.heartbeats, 2) => {}
        }
        assert_eq!(h.state.reads.load(Ordering::SeqCst), 1);
        let (client, _) = h.provider.lock_clients().await.expect("clients");
        let mut blockers = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let client = client.clone();
            let table = h.state.table.lock().expect("table").clone();
            blockers.spawn(async move { locked_publication(&client, "db", "items", table).await });
        }
        wait_count(&h.state.alters, 8).await;
        h.state.release_heartbeat.add_permits(1);
        // Every ordinary permit remains occupied. Unknown proves the publication was polled;
        // the failed monitor must finish it without waiting for or sending an alter.
        let result = tokio::time::timeout(Duration::from_secs(8), &mut publication)
            .await
            .expect("cancellation must not wait for capacity")
            .expect("outcome");
        assert!(matches!(result, Outcome::Unknown { .. }));
        assert!(h.state.heartbeats.load(Ordering::SeqCst) >= 3);
        h.state.release_alters.add_permits(8);
        while let Some(result) = blockers.join_next().await {
            result.expect("blocker task").expect("blocker publication");
        }
        assert_eq!(
            h.state.alters.load(Ordering::SeqCst),
            8,
            "target alter must never arrive"
        );
        assert_eq!(h.pointer(), "old");
    }
}

#[tokio::test]
async fn rename_rejects_pointer_advanced_while_waiting_for_lock() {
    for (destination, datasource_only) in [
        ("db", false),
        ("other_db", false),
        ("db", true),
        ("other_db", true),
    ] {
        let h = Harness::new(Behavior {
            hold_alters: true,
            ..Default::default()
        })
        .await;
        if datasource_only {
            let mut table = h.state.table.lock().expect("table");
            let parameters = table.parameters.as_mut().expect("parameters");
            parameters.remove("table_type");
            parameters.insert("spark.sql.sources.provider".into(), "iceberg".into());
            table.sd = Some(sail_common_hms::hms::StorageDescriptor {
                serde_info: Some(sail_common_hms::hms::SerDeInfo {
                    parameters: Some(HashMap::from([(
                        "path".into(),
                        "file:///warehouse/items".into(),
                    )])),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
        let commit = h.publish(!datasource_only, "old");
        tokio::pin!(commit);
        tokio::select! {
            result = &mut commit => panic!("commit completed before gate: {result:?}"),
            () = wait_count(&h.state.alters, 1) => {}
        }
        let rename = h.rename(destination);
        tokio::pin!(rename);
        tokio::select! {
            result = &mut rename => panic!("rename bypassed source lock: {result:?}"),
            () = wait_count(&h.state.waiting_locks, 1) => {}
        }
        assert_eq!(
            h.state.reads.load(Ordering::SeqCst),
            2,
            "rename classification only"
        );
        assert_eq!(h.state.alters.load(Ordering::SeqCst), 1);
        h.state.release_alters.add_permits(1);
        assert_eq!(commit.await.expect("commit"), Outcome::Committed);
        assert!(matches!(rename.await, Err(CatalogError::Conflict(_))));
        assert_eq!(
            h.state.reads.load(Ordering::SeqCst),
            3,
            "rename reloads under lock"
        );
        assert_eq!(
            h.state.alters.load(Ordering::SeqCst),
            1,
            "only the competing commit publishes"
        );
        assert_eq!(h.pointer(), "new");
        let table = h.state.table.lock().expect("table");
        assert_eq!(table.db_name.as_deref(), Some("db"));
        assert_eq!(table.table_name.as_deref(), Some("items"));
        assert_eq!(
            table
                .parameters
                .as_ref()
                .expect("parameters")
                .get("previous_metadata_location")
                .map(String::as_str),
            Some("old")
        );
    }
}

#[tokio::test]
async fn unguarded_iceberg_rename_never_publishes() {
    for datasource_only in [false, true] {
        let h = Harness::new(Behavior::default()).await;
        if datasource_only {
            h.state
                .table
                .lock()
                .expect("table")
                .parameters
                .as_mut()
                .expect("parameters")
                .insert("spark.sql.sources.provider".into(), "iceberg".into());
            h.state
                .table
                .lock()
                .expect("table")
                .parameters
                .as_mut()
                .expect("parameters")
                .remove("table_type");
        }
        let before = h.state.table.lock().expect("table").clone();
        assert!(matches!(
            h.rename_unguarded("other_db").await,
            Err(CatalogError::CommitRejected(_))
        ));
        assert_eq!(h.state.locks.load(Ordering::SeqCst), 0);
        assert_eq!(h.state.alters.load(Ordering::SeqCst), 0);
        assert_eq!(*h.state.table.lock().expect("table"), before);
    }
}

#[tokio::test]
async fn rename_rejects_removed_pointer_or_inconsistent_aliases_under_lock() {
    for inconsistent in [false, true] {
        let h = Harness::new(Behavior {
            read_gate_at: 1,
            ..Default::default()
        })
        .await;
        let rename = h.rename("db");
        tokio::pin!(rename);
        tokio::select! {
            result = &mut rename => panic!("rename completed before classification: {result:?}"),
            () = wait_count(&h.state.reads, 1) => {}
        }
        {
            let mut table = h.state.table.lock().expect("table");
            let parameters = table.parameters.as_mut().expect("parameters");
            if inconsistent {
                parameters.insert("metadata-location".into(), "different".into());
            } else {
                parameters.remove("metadata_location");
            }
        }
        let before = h.state.table.lock().expect("table").clone();
        h.state.release_read.add_permits(1);
        let result = rename.await;
        if inconsistent {
            assert!(matches!(result, Err(CatalogError::CommitRejected(_))));
        } else {
            assert!(matches!(result, Err(CatalogError::Conflict(_))));
        }
        assert_eq!(h.state.alters.load(Ordering::SeqCst), 0);
        assert_eq!(*h.state.table.lock().expect("table"), before);
    }
}

#[tokio::test]
async fn rename_preserves_current_parameters_when_pointer_is_unchanged() {
    let h = Harness::new(Behavior {
        read_gate_at: 1,
        ..Default::default()
    })
    .await;
    let rename = h.rename("db");
    tokio::pin!(rename);
    tokio::select! {
        result = &mut rename => panic!("rename completed before classification: {result:?}"),
        () = wait_count(&h.state.reads, 1) => {}
    }
    h.state
        .table
        .lock()
        .expect("table")
        .parameters
        .as_mut()
        .expect("parameters")
        .insert("concurrent_property".into(), "preserved".into());
    h.state.release_read.add_permits(1);
    rename.await.expect("rename");
    let table = h.state.table.lock().expect("table");
    assert_eq!(table.table_name.as_deref(), Some("renamed"));
    assert_eq!(
        table
            .parameters
            .as_ref()
            .expect("parameters")
            .get("concurrent_property")
            .map(String::as_str),
        Some("preserved")
    );
    assert_eq!(h.state.alters.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn rename_preflight_uses_raw_hms_metadata() {
    for datasource_only in [false, true] {
        let h = Harness::new(Behavior::default()).await;
        {
            let mut table = h.state.table.lock().expect("table");
            let location = "file:///warehouse/items";
            table.sd = Some(sail_common_hms::hms::StorageDescriptor {
                location: Some(
                    if datasource_only {
                        "file:///unused-hms-location"
                    } else {
                        location
                    }
                    .into(),
                ),
                serde_info: Some(sail_common_hms::hms::SerDeInfo {
                    parameters: Some(HashMap::from([("path".into(), location.into())])),
                    ..Default::default()
                }),
                ..Default::default()
            });
            if datasource_only {
                let parameters = table.parameters.as_mut().expect("parameters");
                parameters.remove("table_type");
                parameters.insert("spark.sql.sources.provider".into(), "iceberg".into());
            }
        }
        let namespace = Namespace::try_from(vec!["db".to_string()]).expect("namespace");
        assert_eq!(
            h.provider
                .rename_table_metadata_requirement(&namespace, "items")
                .await
                .expect("requirement"),
            RenameTableMetadataRequirement::Iceberg {
                table_location: "file:///warehouse/items".into(),
                metadata_location: "old".into(),
            }
        );
        assert_eq!(h.state.locks.load(Ordering::SeqCst), 0);
        assert_eq!(h.state.alters.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn rename_winner_prevents_commit_to_old_name() {
    let h = Harness::new(Behavior {
        hold_alters: true,
        ..Default::default()
    })
    .await;
    let rename = h.rename("other_db");
    tokio::pin!(rename);
    tokio::select! {
        result = &mut rename => panic!("rename completed before gate: {result:?}"),
        () = wait_count(&h.state.alters, 1) => {}
    }
    let commit = h.publish(true, "old");
    tokio::pin!(commit);
    tokio::select! {
        result = &mut commit => panic!("commit bypassed source lock: {result:?}"),
        () = wait_count(&h.state.waiting_locks, 1) => {}
    }
    h.state.release_alters.add_permits(1);
    rename.await.expect("rename");
    assert!(matches!(commit.await, Err(CatalogError::NotFound(..))));
    assert_eq!(h.state.alters.load(Ordering::SeqCst), 1);
    assert_eq!(h.pointer(), "old");
    h.state.release_alters.add_permits(1);
    assert_eq!(
        h.provider
            .compare_and_swap_iceberg_metadata(
                &Namespace::try_from(vec!["other_db".to_string()]).expect("namespace"),
                "renamed",
                Request::new("old".into(), "new".into()).expect("request"),
            )
            .await
            .expect("commit at renamed table"),
        Outcome::Committed
    );
    assert_eq!(h.pointer(), "new");
}

#[tokio::test]
async fn rename_rejects_changed_type_or_identity_after_classification() {
    for change in ["type", "creation", "location", "uuid"] {
        let h = Harness::new(Behavior {
            read_gate_at: 1,
            ..Default::default()
        })
        .await;
        let rename = h.rename("db");
        tokio::pin!(rename);
        tokio::select! {
            result = &mut rename => panic!("rename completed before classification: {result:?}"),
            () = wait_count(&h.state.reads, 1) => {}
        }
        {
            let mut table = h.state.table.lock().expect("table");
            match change {
                "type" => {
                    table
                        .parameters
                        .as_mut()
                        .expect("parameters")
                        .remove("table_type");
                }
                "creation" => {
                    table.create_time = Some(123);
                }
                "location" => {
                    table.sd = Some(sail_common_hms::hms::StorageDescriptor {
                        location: Some("file:///replacement".into()),
                        ..Default::default()
                    });
                }
                "uuid" => {
                    table
                        .parameters
                        .as_mut()
                        .expect("parameters")
                        .insert("uuid".into(), "replacement".into());
                }
                _ => unreachable!(),
            }
        }
        h.state.release_read.add_permits(1);
        assert!(matches!(rename.await, Err(CatalogError::Conflict(_))));
        assert_eq!(h.state.alters.load(Ordering::SeqCst), 0);
        assert_eq!(h.state.unlocks.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn rename_uncertain_publication_is_not_replayed() {
    for fault in [Fault::LostAck, Fault::MetaException, Fault::StaleMessage] {
        let h = Harness::new(Behavior {
            fault,
            failover_endpoint: true,
            ..Default::default()
        })
        .await;
        assert!(matches!(
            h.rename("other_db").await,
            Err(CatalogError::CommitStateUnknown(_))
        ));
        assert_eq!(h.state.alters.load(Ordering::SeqCst), 1);
        assert_eq!(h.state.locks.load(Ordering::SeqCst), 1);
        assert_eq!(
            h.state.table.lock().expect("table").table_name.as_deref(),
            Some("renamed")
        );
    }
    let h = Harness::new(Behavior {
        alter_until_heartbeats: 3,
        heartbeat_fail_at: 3,
        ..Default::default()
    })
    .await;
    assert!(matches!(
        h.rename("db").await,
        Err(CatalogError::CommitStateUnknown(_))
    ));
    assert_eq!(h.state.alters.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn rename_rejects_disabled_locks_and_occupied_destination() {
    let h = Harness::new(Behavior::default()).await;
    h.state
        .table
        .lock()
        .expect("table")
        .parameters
        .as_mut()
        .expect("parameters")
        .insert("engine.hive.lock-enabled".into(), "false".into());
    assert!(matches!(
        h.rename("db").await,
        Err(CatalogError::CommitRejected(_))
    ));
    assert_eq!(h.state.alters.load(Ordering::SeqCst), 0);
    let h = Harness::new(Behavior {
        occupied_destination: true,
        ..Default::default()
    })
    .await;
    // Declared alter exceptions can originate after commit, so the client remains conservative.
    assert!(matches!(
        h.rename("db").await,
        Err(CatalogError::CommitStateUnknown(_))
    ));
    assert_eq!(h.state.alters.load(Ordering::SeqCst), 1);
    assert_eq!(h.state.applied_alters.load(Ordering::SeqCst), 0);
    assert_eq!(
        h.state.table.lock().expect("table").table_name.as_deref(),
        Some("items")
    );
    assert_eq!(h.pointer(), "old");
}

#[tokio::test]
async fn non_iceberg_rename_keeps_unlocked_path() {
    let h = Harness::new(Behavior::default()).await;
    h.state.table.lock().expect("table").parameters = None;
    h.rename_unguarded("other_db").await.expect("rename");
    assert_eq!(h.state.locks.load(Ordering::SeqCst), 0);
    assert_eq!(h.state.reads.load(Ordering::SeqCst), 1);
    assert_eq!(h.state.alters.load(Ordering::SeqCst), 1);
    assert_eq!(
        h.state.table.lock().expect("table").db_name.as_deref(),
        Some("other_db")
    );
}

#[tokio::test]
async fn stale_base_and_inconsistent_aliases_never_publish() {
    for typed in [false, true] {
        let h = Harness::new(Behavior::default()).await;
        assert!(matches!(
            h.publish(typed, "stale").await.expect("outcome"),
            Outcome::Conflict { .. }
        ));
        h.state
            .table
            .lock()
            .expect("table")
            .parameters
            .as_mut()
            .expect("parameters")
            .insert("MeTaDaTa-LoCaTiOn".into(), "different".into());
        assert!(matches!(
            h.publish(typed, "old").await.expect("outcome"),
            Outcome::Rejected { .. }
        ));
        assert_eq!(h.state.alters.load(Ordering::SeqCst), 0);
        assert_eq!(h.pointer(), "old");
    }
}

#[tokio::test]
async fn disabled_locks_and_unavailable_locks_never_fall_back_to_unlocked_alter() {
    for typed in [false, true] {
        let h = Harness::new(Behavior::default()).await;
        h.state
            .table
            .lock()
            .expect("table")
            .parameters
            .as_mut()
            .expect("parameters")
            .insert("engine.hive.lock-enabled".into(), "false".into());
        assert!(matches!(
            h.publish(typed, "old").await.expect("outcome"),
            Outcome::Rejected { .. }
        ));
        assert_eq!(h.state.alters.load(Ordering::SeqCst), 0);
        let h = Harness::new(Behavior {
            lock_unavailable: true,
            ..Default::default()
        })
        .await;
        let result = h.publish(typed, "old").await;
        assert!(matches!(
            result,
            Err(CatalogError::Conflict(_)) | Ok(Outcome::Conflict { .. })
        ));
        assert_eq!(h.state.alters.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn disabled_lock_in_base_metadata_is_rejected_even_when_hms_omits_it() {
    let h = Harness::new(Behavior::default()).await;
    let request = Request::new("old".into(), "new".into())
        .expect("request")
        .with_base_properties(vec![("engine.hive.lock-enabled".into(), "false".into())]);
    let outcome = h
        .provider
        .compare_and_swap_iceberg_metadata(
            &Namespace::try_from(vec!["db".to_string()]).expect("namespace"),
            "items",
            request,
        )
        .await
        .expect("outcome");
    assert!(matches!(outcome, Outcome::Rejected { .. }));
    assert_eq!(h.state.alters.load(Ordering::SeqCst), 0);
    assert_eq!(h.pointer(), "old");
}
