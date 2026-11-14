use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::stream::{self, BoxStream};
use prost::Message;

use crate::common::{
    CompressionCodec, PartitionLocation, PartitionSplitMode, UserIdentifier, WorkerIdentity,
};
use crate::endpoint::EndpointResolver;
use crate::error::{CelebornError, CelebornResult};
use crate::protocol::StatusCode;
use crate::protocol::proto::{
    MessageType, PbCommitFiles, PbCommitFilesResponse, PbOpenStream, PbReserveSlots,
    PbReserveSlotsResponse, PbStreamHandler,
};
use crate::protocol::transport::{TransportConnection, TransportMessage};

const WORKER_ENDPOINT_NAME: &str = "WorkerEndpoint";

/// Metrics reported by a worker after committing shuffle files.
#[derive(Debug, Clone, Copy, Default)]
pub struct CommitMetrics {
    pub total_written: i64,
    pub file_count: i64,
}

/// A small async client for a Celeborn worker endpoint.
#[derive(Clone)]
pub struct WorkerClient {
    location: PartitionLocation,
    rpc: TransportConnection,
    push: TransportConnection,
    fetch: TransportConnection,
}

/// Reuses transport connections for partition locations on the same Celeborn worker.
///
/// A worker can own many partitions, but each worker needs only one RPC, push, and fetch
/// connection. Returned clients retain their individual partition location for wire metadata.
#[derive(Clone, Default)]
pub struct WorkerClientPool {
    clients: Arc<Mutex<HashMap<WorkerIdentity, WorkerClient>>>,
}

impl WorkerClientPool {
    pub fn client(&self, options: WorkerClientOptions) -> WorkerClient {
        let worker_identity = options.location.worker_identity();
        let mut clients = self
            .clients
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        match clients.entry(worker_identity) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                entry.get().with_location(options.location)
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let client = WorkerClient::new(options);
                entry.insert(client.clone());
                client
            }
        }
    }
}

/// Options for a [`WorkerClient`].
#[derive(Debug, Clone)]
pub struct WorkerClientOptions {
    pub location: PartitionLocation,
    pub endpoint_resolver: Option<Arc<dyn EndpointResolver>>,
    pub timeout: Duration,
}

impl WorkerClientOptions {
    pub fn new(location: PartitionLocation) -> Self {
        Self {
            location,
            endpoint_resolver: None,
            timeout: Duration::from_secs(30),
        }
    }

    pub fn with_endpoint_resolver(
        mut self,
        endpoint_resolver: Option<Arc<dyn EndpointResolver>>,
    ) -> Self {
        self.endpoint_resolver = endpoint_resolver;
        self
    }
}

impl WorkerClient {
    pub fn new(options: WorkerClientOptions) -> Self {
        let (rpc_host, rpc_port) = endpoint(
            &options.location,
            options.endpoint_resolver.as_ref(),
            options.location.rpc_port,
        );
        let (push_host, push_port) = endpoint(
            &options.location,
            options.endpoint_resolver.as_ref(),
            options.location.push_port,
        );
        let (fetch_host, fetch_port) = endpoint(
            &options.location,
            options.endpoint_resolver.as_ref(),
            options.location.fetch_port,
        );
        Self {
            location: options.location,
            rpc: TransportConnection::new(rpc_host, rpc_port, options.timeout),
            push: TransportConnection::new(push_host, push_port, options.timeout),
            fetch: TransportConnection::new(fetch_host, fetch_port, options.timeout),
        }
    }

    fn with_location(&self, location: PartitionLocation) -> Self {
        Self {
            location,
            rpc: self.rpc.clone(),
            push: self.push.clone(),
            fetch: self.fetch.clone(),
        }
    }

    /// Reserve a worker-side file writer for this partition location.
    pub async fn reserve_slots(
        &self,
        application_id: String,
        shuffle_id: i32,
        primary_locations: Vec<PartitionLocation>,
        replica_locations: Vec<PartitionLocation>,
        user_identifier: UserIdentifier,
        split_threshold: i64,
        split_mode: PartitionSplitMode,
    ) -> CelebornResult<()> {
        let request = PbReserveSlots {
            application_id,
            shuffle_id,
            primary_locations: primary_locations.into_iter().map(Into::into).collect(),
            replica_locations: replica_locations.into_iter().map(Into::into).collect(),
            split_threshold,
            split_mode: i32::from(split_mode),
            partition_type: 0,
            range_read_filter: false,
            user_identifier: Some(user_identifier.into()),
            push_data_timeout: 30_000,
            partition_split_enabled: true,
            available_storage_types: 0,
            partition_locations_pair: None,
            is_segment_granularity_visible: false,
        };
        let response = self
            .request_worker(MessageType::ReserveSlots, request.encode_to_vec())
            .await?;
        if response.message_type != MessageType::ReserveSlotsResponse {
            return Err(CelebornError::Protocol(
                "invalid reserve slots response".to_string(),
            ));
        }
        let response = PbReserveSlotsResponse::decode(response.payload.as_slice())?;
        if response.status != i32::from(StatusCode::Success as u8) {
            return Err(CelebornError::Master {
                status: response.status,
            });
        }
        Ok(())
    }

    /// Commit a mapper's output on this worker so it can be read by downstream tasks.
    pub async fn commit_files(
        &self,
        application_id: String,
        shuffle_id: i32,
        primary_locations: Vec<PartitionLocation>,
        replica_locations: Vec<PartitionLocation>,
        map_attempts: Vec<i32>,
    ) -> CelebornResult<CommitMetrics> {
        let request = PbCommitFiles {
            application_id,
            shuffle_id,
            primary_ids: primary_locations
                .iter()
                .map(PartitionLocation::unique_id)
                .collect(),
            replica_ids: replica_locations
                .iter()
                .map(PartitionLocation::unique_id)
                .collect(),
            map_attempts,
            epoch: 0,
            mock_failure: false,
        };
        let response = self
            .request_worker(MessageType::CommitFiles, request.encode_to_vec())
            .await?;
        if response.message_type != MessageType::CommitFilesResponse {
            return Err(CelebornError::Protocol(
                "invalid commit files response".to_string(),
            ));
        }
        let response = PbCommitFilesResponse::decode(response.payload.as_slice())?;
        if response.status != i32::from(StatusCode::Success as u8) {
            return Err(CelebornError::Master {
                status: response.status,
            });
        }
        Ok(CommitMetrics {
            total_written: response.total_written,
            file_count: i64::from(response.file_count),
        })
    }

    /// Open a lazy stream of committed batches for this primary partition.
    pub async fn read_partition_stream(
        &self,
        shuffle_key: &str,
        compression: CompressionCodec,
    ) -> BoxStream<'static, CelebornResult<Bytes>> {
        let request = PbOpenStream {
            shuffle_key: shuffle_key.to_string(),
            file_name: format!("{}-{}-0", self.location.id, self.location.epoch),
            start_index: 0,
            end_index: i32::MAX,
            initial_credit: 0,
            read_local_shuffle: false,
        };
        let response = match self
            .request_fetch(MessageType::OpenStream, request.encode_to_vec())
            .await
        {
            Ok(response) => response,
            Err(error) => return Box::pin(stream::once(async move { Err(error) })),
        };
        if response.message_type != MessageType::StreamHandler {
            return Box::pin(stream::once(async {
                Err(CelebornError::Protocol(
                    "invalid open stream response".to_string(),
                ))
            }));
        }
        let handler = match PbStreamHandler::decode(response.payload.as_slice()) {
            Ok(handler) => handler,
            Err(error) => return Box::pin(stream::once(async move { Err(error.into()) })),
        };
        let client = self.clone();
        Box::pin(stream::try_unfold(
            (
                client,
                handler.stream_id,
                0,
                handler.num_chunks,
                BytesMut::new(),
                compression,
            ),
            |(client, stream_id, mut chunk_index, num_chunks, mut buffered, compression)| async move {
                loop {
                    if let Some(batch) = take_batch(&mut buffered)? {
                        let batch = decompress(batch, compression)?;
                        return Ok(Some((
                            batch,
                            (
                                client,
                                stream_id,
                                chunk_index,
                                num_chunks,
                                buffered,
                                compression,
                            ),
                        )));
                    }
                    if chunk_index == num_chunks {
                        return if buffered.is_empty() {
                            Ok(None)
                        } else {
                            Err(CelebornError::Protocol("truncated batch data".to_string()))
                        };
                    }
                    buffered.extend_from_slice(
                        &client
                            .fetch
                            .fetch_chunk(stream_id, chunk_index, 0, i32::MAX)
                            .await?,
                    );
                    chunk_index += 1;
                }
            },
        ))
    }

    async fn request_fetch(
        &self,
        message_type: MessageType,
        payload: Vec<u8>,
    ) -> CelebornResult<TransportMessage> {
        let response = self
            .fetch
            .send_rpc(TransportMessage::new(message_type, payload).encode()?)
            .await?;
        TransportMessage::decode(&response)
    }

    /// Push one Celeborn map-data batch to this primary partition location.
    pub async fn push_data(
        &self,
        shuffle_key: &str,
        map_id: i32,
        attempt_id: i32,
        batch_id: i32,
        data: Bytes,
        compression: CompressionCodec,
    ) -> CelebornResult<usize> {
        let uncompressed_len = data.len();
        let data = compress(data, compression)?;
        let location_id = self.location.unique_id();
        self.push
            .push_data(
                shuffle_key,
                &location_id,
                map_id,
                attempt_id,
                batch_id,
                &data,
            )
            .await?;
        // The public acknowledgement represents accepted input bytes, independent of the codec.
        Ok(uncompressed_len + 16)
    }

    async fn request_worker(
        &self,
        message_type: MessageType,
        payload: Vec<u8>,
    ) -> CelebornResult<TransportMessage> {
        let payload = TransportMessage::new(message_type, payload)
            .into_rpc_envelope(
                &self.location.host,
                &self.location.host,
                self.location.rpc_port,
                WORKER_ENDPOINT_NAME,
            )
            .encode()?;
        let response = self.rpc.send_rpc(payload).await?;
        TransportMessage::decode_java(&response)
    }
}

fn compress(data: Bytes, compression: CompressionCodec) -> CelebornResult<Bytes> {
    match compression {
        CompressionCodec::None => Ok(data),
        CompressionCodec::Lz4 => {
            const MAGIC: &[u8; 8] = b"LZ4Block";
            const HEADER_LENGTH: usize = 21;
            const RAW: u8 = 0x10;
            const COMPRESSED: u8 = 0x20;
            const SEED: u32 = 0x9747_b28c;

            let compressed = lz4_flex::block::compress(&data);
            let (method, payload) = if compressed.len() >= data.len() {
                (RAW, data.as_ref())
            } else {
                (COMPRESSED, compressed.as_slice())
            };
            let mut checksum = twox_hash::XxHash32::with_seed(SEED);
            checksum.write(&data);
            let checksum = checksum.finish() as u32;
            let payload_length = u32::try_from(payload.len()).map_err(|_| {
                CelebornError::Protocol("compressed push data is too large".to_string())
            })?;
            let data_length = u32::try_from(data.len())
                .map_err(|_| CelebornError::Protocol("push data is too large".to_string()))?;
            let mut output = Vec::with_capacity(HEADER_LENGTH + payload.len());
            output.extend_from_slice(MAGIC);
            output.push(method);
            output.extend_from_slice(&payload_length.to_le_bytes());
            output.extend_from_slice(&data_length.to_le_bytes());
            output.extend_from_slice(&checksum.to_le_bytes());
            output.extend_from_slice(payload);
            Ok(Bytes::from(output))
        }
        CompressionCodec::Zstd { level } => {
            const MAGIC: &[u8; 9] = b"ZSTDBlock";
            const HEADER_LENGTH: usize = 22;
            const RAW: u8 = 0x10;
            const COMPRESSED: u8 = 0x30;

            let compressed = zstd::bulk::compress(&data, i32::from(level))?;
            let (method, payload) = if compressed.len() >= data.len() {
                (RAW, data.as_ref())
            } else {
                (COMPRESSED, compressed.as_slice())
            };
            let payload_length = u32::try_from(payload.len()).map_err(|_| {
                CelebornError::Protocol("compressed push data is too large".to_string())
            })?;
            let data_length = u32::try_from(data.len())
                .map_err(|_| CelebornError::Protocol("push data is too large".to_string()))?;
            let mut output = Vec::with_capacity(HEADER_LENGTH + payload.len());
            output.extend_from_slice(MAGIC);
            output.push(method);
            output.extend_from_slice(&payload_length.to_le_bytes());
            output.extend_from_slice(&data_length.to_le_bytes());
            output.extend_from_slice(&crc32fast::hash(&data).to_le_bytes());
            output.extend_from_slice(payload);
            Ok(Bytes::from(output))
        }
    }
}

fn decompress(data: Bytes, compression: CompressionCodec) -> CelebornResult<Bytes> {
    match compression {
        CompressionCodec::None => Ok(data),
        CompressionCodec::Lz4 => decompress_block(
            data,
            b"LZ4Block",
            0x10,
            0x20,
            |data, len| {
                lz4_flex::block::decompress(data, len)
                    .map_err(|error| CelebornError::Protocol(format!("invalid LZ4 block: {error}")))
            },
            |data| {
                const SEED: u32 = 0x9747_b28c;
                let mut checksum = twox_hash::XxHash32::with_seed(SEED);
                checksum.write(data);
                checksum.finish() as u32
            },
        ),
        CompressionCodec::Zstd { .. } => decompress_block(
            data,
            b"ZSTDBlock",
            0x10,
            0x30,
            |data, len| {
                zstd::bulk::decompress(data, len).map_err(|error| {
                    CelebornError::Protocol(format!("invalid Zstd block: {error}"))
                })
            },
            crc32fast::hash,
        ),
    }
}

fn decompress_block(
    data: Bytes,
    magic: &[u8],
    raw_method: u8,
    compressed_method: u8,
    decompress: impl FnOnce(&[u8], usize) -> CelebornResult<Vec<u8>>,
    checksum: impl FnOnce(&[u8]) -> u32,
) -> CelebornResult<Bytes> {
    let magic_len = magic.len();
    let header_length = magic_len + 13;
    if data.len() < header_length || &data[..magic_len] != magic {
        return Err(CelebornError::Protocol(
            "invalid compressed block header".to_string(),
        ));
    }
    let method = data[magic_len];
    let compressed_len = usize::try_from(u32::from_le_bytes(
        data[magic_len + 1..magic_len + 5]
            .try_into()
            .map_err(|_| CelebornError::Protocol("invalid compressed block length".to_string()))?,
    ))
    .map_err(|_| CelebornError::Protocol("compressed block is too large".to_string()))?;
    let original_len = usize::try_from(u32::from_le_bytes(
        data[magic_len + 5..magic_len + 9].try_into().map_err(|_| {
            CelebornError::Protocol("invalid decompressed block length".to_string())
        })?,
    ))
    .map_err(|_| CelebornError::Protocol("decompressed block is too large".to_string()))?;
    let expected_checksum =
        u32::from_le_bytes(data[magic_len + 9..header_length].try_into().map_err(|_| {
            CelebornError::Protocol("invalid compressed block checksum".to_string())
        })?);
    let payload_end = header_length
        .checked_add(compressed_len)
        .ok_or_else(|| CelebornError::Protocol("compressed block is too large".to_string()))?;
    if payload_end != data.len() {
        return Err(CelebornError::Protocol(
            "truncated compressed block".to_string(),
        ));
    }
    let output = match method {
        method if method == raw_method => {
            if compressed_len != original_len {
                return Err(CelebornError::Protocol(
                    "invalid raw compressed block".to_string(),
                ));
            }
            data.slice(header_length..)
        }
        method if method == compressed_method => {
            Bytes::from(decompress(&data[header_length..], original_len)?)
        }
        _ => {
            return Err(CelebornError::Protocol(format!(
                "unsupported compressed block method: {method}"
            )));
        }
    };
    if output.len() != original_len || checksum(&output) != expected_checksum {
        return Err(CelebornError::Protocol(
            "compressed block checksum mismatch".to_string(),
        ));
    }
    Ok(output)
}

fn take_batch(buffered: &mut BytesMut) -> CelebornResult<Option<Bytes>> {
    const HEADER_LENGTH: usize = 16;
    if buffered.len() < HEADER_LENGTH {
        return Ok(None);
    }
    let length = i32::from_be_bytes(
        buffered[12..HEADER_LENGTH]
            .try_into()
            .map_err(|_| CelebornError::Protocol("invalid batch header".to_string()))?,
    );
    let length = usize::try_from(length)
        .map_err(|_| CelebornError::Protocol("invalid batch length".to_string()))?;
    let end = HEADER_LENGTH
        .checked_add(length)
        .ok_or_else(|| CelebornError::Protocol("invalid batch length".to_string()))?;
    if buffered.len() < end {
        return Ok(None);
    }
    let mut batch = buffered.split_to(end);
    Ok(Some(batch.split_off(HEADER_LENGTH).freeze()))
}

fn endpoint(
    location: &PartitionLocation,
    endpoint_resolver: Option<&Arc<dyn EndpointResolver>>,
    port: u16,
) -> (String, u16) {
    endpoint_resolver
        .map(|resolver| resolver.resolve(&location.host, port))
        .unwrap_or_else(|| (location.host.clone(), port))
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use bytes::Bytes;

    use super::{WorkerClientOptions, WorkerClientPool, compress, decompress};
    use crate::common::{CompressionCodec, PartitionLocation};

    fn location(id: i32) -> PartitionLocation {
        PartitionLocation {
            mode: 0,
            id,
            epoch: 0,
            host: "worker".to_string(),
            rpc_port: 12000,
            push_port: 12001,
            fetch_port: 12002,
            replicate_port: 12003,
            peer: None,
        }
    }

    #[test]
    fn worker_client_pool_reuses_connections_for_partitions_on_one_worker() {
        let pool = WorkerClientPool::default();
        let first = pool.client(WorkerClientOptions::new(location(0)));
        let second = pool.client(WorkerClientOptions::new(location(1)));

        assert_eq!(first.location.id, 0);
        assert_eq!(second.location.id, 1);
        assert!(first.rpc.shares_stream_with(&second.rpc));
        assert!(first.push.shares_stream_with(&second.push));
        assert!(first.fetch.shares_stream_with(&second.fetch));
    }

    #[test]
    fn compression_uses_celeborn_block_framing() {
        let data = Bytes::from(vec![42; 1_024]);
        let lz4 = compress(data.clone(), CompressionCodec::Lz4).unwrap();
        let zstd = compress(data.clone(), CompressionCodec::Zstd { level: 1 }).unwrap();

        assert_eq!(&lz4[..8], b"LZ4Block");
        assert_eq!(&zstd[..9], b"ZSTDBlock");
        assert_eq!(
            u32::from_le_bytes(lz4[13..17].try_into().unwrap()),
            data.len() as u32
        );
        assert_eq!(
            u32::from_le_bytes(zstd[14..18].try_into().unwrap()),
            data.len() as u32
        );
        assert_eq!(decompress(lz4, CompressionCodec::Lz4).unwrap(), data);
        assert_eq!(
            decompress(zstd, CompressionCodec::Zstd { level: 1 }).unwrap(),
            data
        );
    }

    #[test]
    fn raw_compressed_blocks_are_sliced_without_copying() {
        let mut state = 1_u32;
        let data = Bytes::from(
            (0..1_024)
                .map(|_| {
                    state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                    (state >> 24) as u8
                })
                .collect::<Vec<_>>(),
        );
        let encoded = compress(data.clone(), CompressionCodec::Lz4).unwrap();
        assert_eq!(encoded[8], 0x10);

        let expected_data = encoded.slice(21..);
        let decoded = decompress(encoded, CompressionCodec::Lz4).unwrap();
        assert_eq!(decoded, data);
        assert_eq!(decoded.as_ptr(), expected_data.as_ptr());
    }
}
