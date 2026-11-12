//! Semantic ports of lock lifecycle and uncertain-publication cases, using real Thrift pools.
// [CREDIT]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/hive-metastore/src/test/java/org/apache/iceberg/hive/TestHiveCommitLocks.java
// [CREDIT]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/hive-metastore/src/test/java/org/apache/iceberg/hive/TestHiveCommits.java

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use sail_catalog::lakehouse::{
    IcebergMetadataCasOutcome as Outcome, IcebergMetadataCasRequest as Request,
};
use sail_catalog::provider::{CatalogProvider, Namespace};
use sail_common_hms::hms::{
    EnvironmentContext, GetTableResult, LockResponse, MetaException, NoSuchLockException,
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
    contexts: Mutex<Vec<EnvironmentContext>>,
    release_alters: tokio::sync::Semaphore,
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
        let state = Arc::new(State {
            behavior,
            table: Mutex::new(Table {
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
            contexts: Mutex::new(vec![]),
            release_alters: tokio::sync::Semaphore::new(0),
        });
        let handle = tokio::runtime::Handle::current();
        let provider = Arc::new(
            HmsCatalogProvider::try_new(
                "hms".into(),
                crate::provider::HmsCatalogConfig {
                    uris: vec![uri],
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
        let (header, replacement, context) = {
            let mut p = TBinaryInputProtocol::new(Cursor::new(&bytes), true);
            let header = p.read_message_begin().expect("request header");
            let mut replacement = None;
            let mut context = None;
            p.read_struct_begin().expect("args");
            loop {
                let field = p.read_field_begin().expect("arg");
                if field.field_type == TType::Stop {
                    break;
                }
                if header.name == "alter_table_with_environment_context" && field.id == Some(3) {
                    replacement = Some(Table::read_from_in_protocol(&mut p).expect("table"));
                } else if header.name == "alter_table_with_environment_context"
                    && field.id == Some(4)
                {
                    context =
                        Some(EnvironmentContext::read_from_in_protocol(&mut p).expect("context"));
                } else {
                    p.skip(field.field_type).expect("skip arg");
                }
                p.read_field_end().expect("arg end");
            }
            (header, replacement, context)
        };
        let bytes = match header.name.as_str() {
            "lock" => reply(
                &header,
                Some((
                    0,
                    &LockResponse {
                        lockid: 1,
                        state: if state.behavior.lock_unavailable {
                            LockState::NOT_ACQUIRED
                        } else {
                            LockState::ACQUIRED
                        },
                    },
                )),
            ),
            "get_table_req" => {
                state.reads.fetch_add(1, Ordering::SeqCst);
                wait_count(&state.heartbeats, state.behavior.read_until_heartbeats).await;
                let table = state.table.lock().expect("table").clone();
                reply(&header, Some((0, &GetTableResult { table })))
            }
            "heartbeat" => {
                let count = state.heartbeats.fetch_add(1, Ordering::SeqCst) + 1;
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
            "alter_table_with_environment_context" => {
                state
                    .contexts
                    .lock()
                    .expect("contexts")
                    .push(context.expect("environment"));
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
                *state.table.lock().expect("table") = replacement.expect("replacement");
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
