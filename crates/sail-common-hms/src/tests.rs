//! Wire/transport utility tests. All peers use Apache's codecs rather than Sail's generator.
#![expect(clippy::unwrap_used)]

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use sail_thrift::transport::{read_message, write_message};
use sail_thrift::{Client, Connector, MaybeException, ReadHalf, Transport, WriteHalf};
use thrift::protocol::{
    TBinaryInputProtocol, TBinaryOutputProtocol, TFieldIdentifier, TInputProtocol, TListIdentifier,
    TMessageIdentifier, TMessageType, TOutputProtocol, TSerializable, TStructIdentifier, TType,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, DuplexStream};
use tokio::sync::{mpsc, oneshot};

use crate::hms::{
    FieldSchema, GetTableRequest, LockState, SkewedInfo, StorageDescriptor, Table,
    ThriftHiveMetastoreClient, ThriftHiveMetastoreDropTableException,
};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn encode_reply(
    name: &str,
    sequence: i32,
    body: impl FnOnce(&mut dyn TOutputProtocol) -> thrift::Result<()>,
) -> thrift::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut p = TBinaryOutputProtocol::new(&mut bytes, true);
    p.write_message_begin(&TMessageIdentifier::new(
        name,
        TMessageType::Reply,
        sequence,
    ))?;
    p.write_struct_begin(&TStructIdentifier::new("result"))?;
    body(&mut p)?;
    p.write_field_stop()?;
    p.write_struct_end()?;
    p.write_message_end()?;
    Ok(bytes)
}

fn databases_reply(sequence: i32) -> thrift::Result<Vec<u8>> {
    encode_reply("get_all_databases", sequence, |p| {
        p.write_field_begin(&TFieldIdentifier::new("success", TType::List, 0))?;
        p.write_list_begin(&TListIdentifier::new(TType::String, 2))?;
        p.write_string("default")?;
        p.write_string("analytics")?;
        p.write_list_end()?;
        p.write_field_end()
    })
}

fn request_header(bytes: &[u8]) -> thrift::Result<TMessageIdentifier> {
    let mut p = TBinaryInputProtocol::new(Cursor::new(bytes), true);
    let header = p.read_message_begin()?;
    p.skip(TType::Struct)?;
    p.read_message_end()?;
    Ok(header)
}

struct TestConnector {
    peers: mpsc::UnboundedSender<DuplexStream>,
    count: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Connector for TestConnector {
    async fn connect(&self) -> std::io::Result<(ReadHalf, WriteHalf)> {
        self.count.fetch_add(1, Ordering::SeqCst);
        let (client, peer) = tokio::io::duplex(4096);
        self.peers
            .send(peer)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let (reader, writer) = tokio::io::split(client);
        Ok((Box::new(reader), Box::new(writer)))
    }
}

fn client(
    transport: Transport,
) -> (
    ThriftHiveMetastoreClient,
    mpsc::UnboundedReceiver<DuplexStream>,
    Arc<AtomicUsize>,
) {
    let (peers, receiver) = mpsc::unbounded_channel();
    let count = Arc::new(AtomicUsize::new(0));
    (
        ThriftHiveMetastoreClient(Client::new(
            TestConnector {
                peers,
                count: count.clone(),
            },
            transport,
        )),
        receiver,
        count,
    )
}

#[tokio::test]
async fn fragmented_responses_and_connection_reuse() -> TestResult {
    for transport in [Transport::Buffered, Transport::Framed] {
        let (client, mut peers, count) = client(transport);
        let server = tokio::spawn(async move {
            let peer = peers.recv().await.unwrap();
            let (reader, mut writer) = tokio::io::split(peer);
            let mut reader = BufReader::new(reader);
            for sequence in [1, 2] {
                let request = read_message(&mut reader, transport).await.unwrap();
                let header = request_header(&request).unwrap();
                assert_eq!(header.name, "get_all_databases");
                assert_eq!(header.sequence_number, sequence);
                let reply = databases_reply(sequence).unwrap();
                let mut wire = Vec::new();
                if matches!(transport, Transport::Framed) {
                    wire.extend_from_slice(&(reply.len() as i32).to_be_bytes());
                }
                wire.extend_from_slice(&reply);
                for byte in wire {
                    writer.write_all(&[byte]).await.unwrap();
                    tokio::task::yield_now().await;
                }
            }
        });
        for _ in 0..2 {
            assert_eq!(
                client.get_all_databases().await?,
                MaybeException::Ok(vec!["default".into(), "analytics".into()])
            );
        }
        server.await?;
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    Ok(())
}

#[tokio::test]
async fn closed_idle_connections_are_replaced() -> TestResult {
    for transport in [Transport::Buffered, Transport::Framed] {
        let (client, mut peers, count) = client(transport);
        let (close, should_close) = oneshot::channel();
        let (closed, was_closed) = oneshot::channel();
        let server = tokio::spawn(async move {
            let peer = peers.recv().await.unwrap();
            let (reader, mut writer) = tokio::io::split(peer);
            let mut reader = BufReader::new(reader);
            read_message(&mut reader, transport).await.unwrap();
            write_message(&mut writer, transport, &databases_reply(1).unwrap())
                .await
                .unwrap();
            should_close.await.unwrap();
            drop(reader);
            drop(writer);
            closed.send(()).unwrap();

            let peer = peers.recv().await.unwrap();
            let (reader, mut writer) = tokio::io::split(peer);
            let request = read_message(&mut BufReader::new(reader), transport)
                .await
                .unwrap();
            assert_eq!(request_header(&request).unwrap().sequence_number, 1);
            write_message(&mut writer, transport, &databases_reply(1).unwrap())
                .await
                .unwrap();
        });
        client.get_all_databases().await?;
        close.send(()).unwrap();
        was_closed.await?;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), client.get_all_databases()).await??,
            MaybeException::Ok(vec!["default".into(), "analytics".into()])
        );
        server.await?;
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    Ok(())
}

#[tokio::test(start_paused = true)]
async fn expired_idle_connections_are_replaced() -> TestResult {
    for transport in [Transport::Buffered, Transport::Framed] {
        let (client, mut peers, count) = client(transport);
        let server = tokio::spawn(async move {
            let peer = peers.recv().await.unwrap();
            let (reader, mut writer) = tokio::io::split(peer);
            let mut reader = BufReader::new(reader);
            read_message(&mut reader, transport).await.unwrap();
            write_message(&mut writer, transport, &databases_reply(1).unwrap())
                .await
                .unwrap();
            let mut extra = Vec::new();
            reader.read_to_end(&mut extra).await.unwrap();
            assert!(
                extra.is_empty(),
                "expired connection received another request"
            );

            let peer = peers.recv().await.unwrap();
            let (reader, mut writer) = tokio::io::split(peer);
            let request = read_message(&mut BufReader::new(reader), transport)
                .await
                .unwrap();
            assert_eq!(request_header(&request).unwrap().sequence_number, 1);
            write_message(&mut writer, transport, &databases_reply(1).unwrap())
                .await
                .unwrap();
        });
        client.get_all_databases().await?;
        tokio::time::advance(Duration::from_secs(16)).await;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), client.get_all_databases()).await??,
            MaybeException::Ok(vec!["default".into(), "analytics".into()])
        );
        server.await?;
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    Ok(())
}

#[tokio::test]
async fn cancellation_discards_inflight_connection() -> TestResult {
    let (client, mut peers, count) = client(Transport::Buffered);
    let first = client.clone();
    let task = tokio::spawn(async move { first.get_all_databases().await });
    let peer = peers.recv().await.unwrap();
    let mut first_peer = BufReader::new(peer);
    read_message(&mut first_peer, Transport::Buffered).await?;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let server = tokio::spawn(async move {
        let peer = peers.recv().await.unwrap();
        let (reader, mut writer) = tokio::io::split(peer);
        let request = read_message(&mut BufReader::new(reader), Transport::Buffered)
            .await
            .unwrap();
        let header = request_header(&request).unwrap();
        write_message(
            &mut writer,
            Transport::Buffered,
            &databases_reply(header.sequence_number).unwrap(),
        )
        .await
        .unwrap();
    });
    assert!(matches!(
        client.get_all_databases().await?,
        MaybeException::Ok(_)
    ));
    server.await?;
    assert_eq!(count.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn validates_reply_envelopes_and_missing_results() -> TestResult {
    for kind in 0..5 {
        let (client, mut peers, _) = client(Transport::Framed);
        let server = tokio::spawn(async move {
            let peer = peers.recv().await.unwrap();
            let (reader, mut writer) = tokio::io::split(peer);
            read_message(&mut BufReader::new(reader), Transport::Framed)
                .await
                .unwrap();
            let mut reply = match kind {
                0 => databases_reply(999).unwrap(),
                1 => encode_reply("wrong_method", 1, |_| Ok(())).unwrap(),
                2 => encode_reply("get_all_databases", 1, |_| Ok(())).unwrap(),
                3 => {
                    let mut bytes = databases_reply(1).unwrap();
                    bytes[3] = TMessageType::Call as u8;
                    bytes
                }
                _ => databases_reply(1).unwrap(),
            };

            if kind == 4 {
                reply.push(0);
            }
            write_message(&mut writer, Transport::Framed, &reply)
                .await
                .unwrap();
        });
        assert!(client.get_all_databases().await.is_err(), "case {kind}");
        server.await?;
    }

    Ok(())
}

#[tokio::test]
async fn declared_exception_keeps_original_field_id() -> TestResult {
    let (client, mut peers, _) = client(Transport::Buffered);
    let server = tokio::spawn(async move {
        let peer = peers.recv().await.unwrap();
        let (reader, mut writer) = tokio::io::split(peer);
        let request = read_message(&mut BufReader::new(reader), Transport::Buffered)
            .await
            .unwrap();
        assert_eq!(request_header(&request).unwrap().name, "drop_table");
        let reply = encode_reply("drop_table", 1, |p| {
            p.write_field_begin(&TFieldIdentifier::new("o3", TType::Struct, 2))?;
            p.write_struct_begin(&TStructIdentifier::new("MetaException"))?;
            p.write_field_begin(&TFieldIdentifier::new("message", TType::String, 1))?;
            p.write_string("metadata failure")?;
            p.write_field_end()?;
            p.write_field_stop()?;
            p.write_struct_end()?;
            p.write_field_end()
        })
        .unwrap();
        write_message(&mut writer, Transport::Buffered, &reply)
            .await
            .unwrap();
    });
    let result = client
        .drop_table("db".into(), "table".into(), false)
        .await?;
    assert!(
        matches!(result,MaybeException::Exception(ThriftHiveMetastoreDropTableException::O3(e)) if e.message.as_deref() == Some("metadata failure"))
    );
    server.await?;
    Ok(())
}

#[test]
fn domain_codecs_preserve_nested_metadata_and_defaults() -> TestResult {
    let table = Table {
        table_name: Some("items".into()),
        sd: Some(StorageDescriptor {
            cols: Some(vec![FieldSchema {
                name: Some("id".into()),
                r#type: Some("bigint".into()),
                ..Default::default()
            }]),
            skewed_info: Some(SkewedInfo {
                skewed_col_value_location_maps: Some(HashMap::from([(
                    vec!["a".into(), "b".into()],
                    "s3://bucket/path".into(),
                )])),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    let mut bytes = Vec::new();
    table.write_to_out_protocol(&mut TBinaryOutputProtocol::new(&mut bytes, true))?;
    let decoded =
        Table::read_from_in_protocol(&mut TBinaryInputProtocol::new(Cursor::new(&bytes), true))?;
    assert_eq!(decoded, table);
    assert_eq!(decoded.temporary, Some(false));
    assert_eq!(LockState(1234).to_string(), "1234");
    let mut empty = Vec::new();
    let mut p = TBinaryOutputProtocol::new(&mut empty, true);
    p.write_struct_begin(&TStructIdentifier::new("request"))?;
    p.write_field_stop()?;
    p.write_struct_end()?;
    assert!(
        GetTableRequest::read_from_in_protocol(&mut TBinaryInputProtocol::new(
            Cursor::new(empty),
            true
        ))
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn scanner_handles_nested_values_and_concatenated_messages() -> TestResult {
    let mut message = encode_reply("test", 1, |p| {
        p.write_field_begin(&TFieldIdentifier::new("table", TType::Struct, 10))?;
        Table {
            sd: Some(StorageDescriptor {
                skewed_info: Some(SkewedInfo {
                    skewed_col_value_location_maps: Some(HashMap::from([(
                        vec!["a".into()],
                        "location".into(),
                    )])),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .write_to_out_protocol(p)?;
        p.write_field_end()
    })?;
    let first = message.clone();
    message.extend_from_slice(&databases_reply(2)?);
    let mut reader = BufReader::new(message.as_slice());
    assert_eq!(read_message(&mut reader, Transport::Buffered).await?, first);
    assert_eq!(
        read_message(&mut reader, Transport::Buffered).await?,
        databases_reply(2)?
    );
    Ok(())
}

#[tokio::test]
async fn rejects_invalid_lengths_and_truncation() -> TestResult {
    for length in [-1_i32, 0, i32::MAX] {
        let bytes = length.to_be_bytes();
        assert!(
            read_message(&mut BufReader::new(bytes.as_slice()), Transport::Framed)
                .await
                .is_err()
        );
    }

    let bytes = databases_reply(1)?;
    for end in 0..bytes.len() {
        assert!(
            read_message(&mut BufReader::new(&bytes[..end]), Transport::Buffered)
                .await
                .is_err()
        );
    }

    Ok(())
}

#[tokio::test]
async fn application_exceptions_are_classified_by_kind() -> TestResult {
    let (client, mut peers, _) = client(Transport::Buffered);
    let server = tokio::spawn(async move {
        let peer = peers.recv().await.unwrap();
        let (reader, mut writer) = tokio::io::split(peer);
        read_message(&mut BufReader::new(reader), Transport::Buffered)
            .await
            .unwrap();
        let mut response = Vec::new();
        let mut p = TBinaryOutputProtocol::new(&mut response, true);
        p.write_message_begin(&TMessageIdentifier::new(
            "get_table_req",
            TMessageType::Exception,
            1,
        ))
        .unwrap();
        thrift::Error::write_application_error_to_out_protocol(
            &thrift::ApplicationError::new(
                thrift::ApplicationErrorKind::UnknownMethod,
                "legacy peer",
            ),
            &mut p,
        )
        .unwrap();
        p.write_message_end().unwrap();
        write_message(&mut writer, Transport::Buffered, &response)
            .await
            .unwrap();
    });
    let error = client
        .get_table_req(GetTableRequest {
            db_name: "db".into(),
            tbl_name: "items".into(),
            capabilities: None,
        })
        .await
        .unwrap_err();
    assert!(error.is_unknown_method());
    server.await?;
    Ok(())
}

#[tokio::test]
async fn concurrent_calls_use_independent_connections() -> TestResult {
    let (client, mut peers, count) = client(Transport::Buffered);
    let server = tokio::spawn(async move {
        // Both calls must acquire independent connections before either receives a response.
        let first = peers.recv().await.unwrap();
        let second = peers.recv().await.unwrap();
        for peer in [first, second] {
            let (reader, mut writer) = tokio::io::split(peer);
            let request = read_message(&mut BufReader::new(reader), Transport::Buffered)
                .await
                .unwrap();
            let header = request_header(&request).unwrap();
            write_message(
                &mut writer,
                Transport::Buffered,
                &databases_reply(header.sequence_number).unwrap(),
            )
            .await
            .unwrap();
        }
    });
    let (first, second) = tokio::join!(client.get_all_databases(), client.get_all_databases());
    assert!(matches!(first?, MaybeException::Ok(_)));
    assert!(matches!(second?, MaybeException::Ok(_)));
    server.await?;
    assert_eq!(count.load(Ordering::SeqCst), 2);
    Ok(())
}

#[test]
fn unknown_fields_are_skipped_and_unknown_enums_are_retained() -> TestResult {
    let mut bytes = Vec::new();
    let mut p = TBinaryOutputProtocol::new(&mut bytes, true);
    p.write_struct_begin(&TStructIdentifier::new("Table"))?;
    p.write_field_begin(&TFieldIdentifier::new("future_field", TType::List, 999))?;
    p.write_list_begin(&TListIdentifier::new(TType::I32, 2))?;
    p.write_i32(10)?;
    p.write_i32(20)?;
    p.write_list_end()?;
    p.write_field_end()?;
    p.write_field_begin(&TFieldIdentifier::new("tableName", TType::String, 1))?;
    p.write_string("items")?;
    p.write_field_end()?;
    p.write_field_stop()?;
    p.write_struct_end()?;
    let table =
        Table::read_from_in_protocol(&mut TBinaryInputProtocol::new(bytes.as_slice(), true))?;
    assert_eq!(table.table_name.as_deref(), Some("items"));
    assert_eq!(table.temporary, Some(false));
    let bytes = 1234_i32.to_be_bytes();
    assert_eq!(
        LockState::read_from_in_protocol(&mut TBinaryInputProtocol::new(bytes.as_slice(), true))?,
        LockState(1234)
    );
    Ok(())
}

#[tokio::test]
async fn scanner_accepts_legacy_binary_headers() -> TestResult {
    let mut bytes = Vec::new();
    let mut p = TBinaryOutputProtocol::new(&mut bytes, false);
    p.write_message_begin(&TMessageIdentifier::new("legacy", TMessageType::Reply, 1))?;
    p.write_struct_begin(&TStructIdentifier::new("result"))?;
    p.write_field_stop()?;
    p.write_struct_end()?;
    p.write_message_end()?;
    assert_eq!(
        read_message(&mut BufReader::new(bytes.as_slice()), Transport::Buffered).await?,
        bytes
    );
    Ok(())
}

#[tokio::test]
async fn scanner_accepts_all_binary_value_kinds_in_unknown_fields() -> TestResult {
    let message = encode_reply("future_fields", 1, |p| {
        for (id, ty) in [
            (1, TType::Bool),
            (2, TType::I08),
            (3, TType::I16),
            (4, TType::I64),
            (5, TType::Double),
            (6, TType::Uuid),
        ] {
            p.write_field_begin(&TFieldIdentifier::new("future", ty, id))?;
            match ty {
                TType::Bool => p.write_bool(true)?,
                TType::I08 => p.write_i8(-1)?,
                TType::I16 => p.write_i16(-123)?,
                TType::I64 => p.write_i64(i64::MAX)?,
                TType::Double => p.write_double(1.5)?,
                TType::Uuid => p.write_uuid(&Default::default())?,
                _ => unreachable!(),
            }

            p.write_field_end()?;
        }

        p.write_field_begin(&TFieldIdentifier::new("future_set", TType::Set, 7))?;
        p.write_set_begin(&thrift::protocol::TSetIdentifier::new(TType::I32, 2))?;
        p.write_i32(1)?;
        p.write_i32(2)?;
        p.write_set_end()?;
        p.write_field_end()
    })?;
    assert_eq!(
        read_message(&mut BufReader::new(message.as_slice()), Transport::Buffered).await?,
        message
    );
    // The official runtime must agree with the scanner on the complete boundary.
    request_header(&message)?;
    Ok(())
}
