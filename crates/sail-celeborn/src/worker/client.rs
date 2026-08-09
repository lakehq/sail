use std::sync::Arc;
use std::time::Duration;

use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::endpoint::EndpointResolver;
use crate::error::{CelebornError, CelebornResult};
use crate::master::{MasterClient, MasterClientOptions, PartitionLocation, UserIdentifier};
use crate::protocol::MessageType;
use crate::protocol::proto::{
    PbCommitFiles, PbCommitFilesResponse, PbOpenStream, PbReserveSlots, PbReserveSlotsResponse,
    PbStreamHandler,
};
use crate::protocol::transport::TransportResponse;

const PUSH_DATA: u8 = 11;
const RPC_REQUEST: u8 = 3;
const RPC_RESPONSE: u8 = 4;
const CHUNK_FETCH_REQUEST: u8 = 0;
const CHUNK_FETCH_SUCCESS: u8 = 1;
const MAP_ENDED: u8 = 15;
const PUSH_DATA_SUCCESS_PRIMARY_CONGESTED: u8 = 30;
const PUSH_DATA_SUCCESS_REPLICA_CONGESTED: u8 = 31;
const WORKER_ENDPOINT_NAME: &str = "WorkerEndpoint";

/// A small async client for a Celeborn worker endpoint.
#[derive(Clone)]
pub struct WorkerClient {
    location: PartitionLocation,
    endpoint_resolver: Option<Arc<dyn EndpointResolver>>,
    timeout: Duration,
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
        Self {
            location: options.location,
            endpoint_resolver: options.endpoint_resolver,
            timeout: options.timeout,
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
    ) -> CelebornResult<()> {
        let (host, port) = self.endpoint(self.location.rpc_port);
        let client = MasterClient::new(MasterClientOptions::new(host, port));
        let request = PbReserveSlots {
            application_id,
            shuffle_id,
            primary_locations: primary_locations.into_iter().map(Into::into).collect(),
            replica_locations: replica_locations.into_iter().map(Into::into).collect(),
            split_threshold: 1_i64 << 30,
            split_mode: 0,
            partition_type: 0,
            range_read_filter: false,
            user_identifier: Some(user_identifier.into()),
            push_data_timeout: 30_000,
            partition_split_enabled: true,
            available_storage_types: 0,
            partition_locations_pair: None,
            is_segment_granularity_visible: false,
        };
        let response = client
            .request_endpoint(
                WORKER_ENDPOINT_NAME,
                MessageType::RESERVE_SLOTS,
                request.encode_to_vec(),
            )
            .await?;
        if response.message_type != MessageType::RESERVE_SLOTS_RESPONSE {
            return Err(CelebornError::Protocol(
                "invalid reserve slots response".to_string(),
            ));
        }
        let response = PbReserveSlotsResponse::decode(response.payload.as_slice())?;
        if response.status != 0 {
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
    ) -> CelebornResult<()> {
        let (host, port) = self.endpoint(self.location.rpc_port);
        let client = MasterClient::new(MasterClientOptions::new(host, port));
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
        let response = client
            .request_endpoint(
                WORKER_ENDPOINT_NAME,
                MessageType::COMMIT_FILES,
                request.encode_to_vec(),
            )
            .await?;
        if response.message_type != MessageType::COMMIT_FILES_RESPONSE {
            return Err(CelebornError::Protocol(
                "invalid commit files response".to_string(),
            ));
        }
        let response = PbCommitFilesResponse::decode(response.payload.as_slice())?;
        if response.status != 0 {
            return Err(CelebornError::Master {
                status: response.status,
            });
        }
        Ok(())
    }

    /// Fetch all committed batches for this primary partition.
    pub async fn read_partition(&self, shuffle_key: &str) -> CelebornResult<Vec<u8>> {
        let request = PbOpenStream {
            shuffle_key: shuffle_key.to_string(),
            file_name: format!("{}-{}-0", self.location.id, self.location.epoch),
            start_index: 0,
            end_index: i32::MAX,
            initial_credit: 0,
            read_local_shuffle: false,
        };
        let response = self
            .request_fetch(MessageType::OPEN_STREAM, request.encode_to_vec())
            .await?;
        if response.message_type != MessageType::STREAM_HANDLER {
            return Err(CelebornError::Protocol(
                "invalid open stream response".to_string(),
            ));
        }
        let handler = PbStreamHandler::decode(response.payload.as_slice())?;
        let mut batches = Vec::new();
        for chunk_index in 0..handler.num_chunks {
            batches.extend(self.fetch_chunk(handler.stream_id, chunk_index).await?);
        }
        Self::decode_batches(&batches)
    }

    async fn fetch_chunk(&self, stream_id: i64, chunk_index: i32) -> CelebornResult<Vec<u8>> {
        tokio::time::timeout(self.timeout, async {
            let (host, port) = self.endpoint(self.location.fetch_port);
            let mut stream = TcpStream::connect((&*host, port)).await?;
            stream.write_i32(20).await?;
            stream.write_u8(CHUNK_FETCH_REQUEST).await?;
            stream.write_i32(0).await?;
            stream.write_i64(stream_id).await?;
            stream.write_i32(chunk_index).await?;
            stream.write_i32(0).await?;
            stream.write_i32(i32::MAX).await?;
            stream.flush().await?;
            let encoded_length = stream.read_i32().await?;
            let message_type = stream.read_u8().await?;
            let body_length = stream.read_i32().await?;
            if encoded_length != 20 || message_type != CHUNK_FETCH_SUCCESS || body_length < 0 {
                return Err(CelebornError::Protocol(
                    "invalid chunk fetch response".to_string(),
                ));
            }
            if stream.read_i64().await? != stream_id || stream.read_i32().await? != chunk_index {
                return Err(CelebornError::Protocol(
                    "chunk response does not match request".to_string(),
                ));
            }
            if stream.read_i32().await? != 0 || stream.read_i32().await? < 0 {
                return Err(CelebornError::Protocol(
                    "invalid chunk response slice".to_string(),
                ));
            }
            let mut body = vec![
                0;
                usize::try_from(body_length).map_err(
                    |_| CelebornError::Protocol("invalid chunk response length".to_string())
                )?
            ];
            stream.read_exact(&mut body).await?;
            Ok(body)
        })
        .await
        .map_err(|_| CelebornError::Timeout)?
    }

    async fn request_fetch(
        &self,
        message_type: i32,
        payload: Vec<u8>,
    ) -> CelebornResult<TransportResponse> {
        tokio::time::timeout(self.timeout, async {
            let (host, port) = self.endpoint(self.location.fetch_port);
            let mut stream = TcpStream::connect((&*host, port)).await?;
            let transport = encode_transport_message(message_type, payload)?;
            let body_length = i32::try_from(transport.len())
                .map_err(|_| CelebornError::Protocol("fetch request is too large".to_string()))?;

            stream.write_i32(12).await?;
            stream.write_u8(RPC_REQUEST).await?;
            stream.write_i32(body_length).await?;
            stream.write_i64(0).await?;
            stream.write_i32(body_length).await?;
            stream.write_all(&transport).await?;
            stream.flush().await?;

            let encoded_length = stream.read_i32().await?;
            let response_type = stream.read_u8().await?;
            let response_body_length = stream.read_i32().await?;
            if encoded_length != 12 || response_type != RPC_RESPONSE || response_body_length < 0 {
                return Err(CelebornError::Protocol(
                    "invalid open stream response".to_string(),
                ));
            }
            if stream.read_i64().await? != 0 || stream.read_i32().await? != response_body_length {
                return Err(CelebornError::Protocol(
                    "open stream response does not match request".to_string(),
                ));
            }
            let mut response = vec![
                0;
                usize::try_from(response_body_length).map_err(|_| {
                    CelebornError::Protocol("invalid open stream response length".to_string())
                })?
            ];
            stream.read_exact(&mut response).await?;
            decode_transport_message(&response)
        })
        .await
        .map_err(|_| CelebornError::Timeout)?
    }

    fn decode_batches(bytes: &[u8]) -> CelebornResult<Vec<u8>> {
        let mut position = 0;
        let mut data = Vec::new();
        while position < bytes.len() {
            let header_end = position
                .checked_add(16)
                .ok_or_else(|| CelebornError::Protocol("invalid batch header".to_string()))?;
            if header_end > bytes.len() {
                return Err(CelebornError::Protocol(
                    "truncated batch header".to_string(),
                ));
            }
            let length = i32::from_be_bytes(
                bytes[position + 12..header_end]
                    .try_into()
                    .map_err(|_| CelebornError::Protocol("invalid batch header".to_string()))?,
            );
            let length = usize::try_from(length)
                .map_err(|_| CelebornError::Protocol("invalid batch length".to_string()))?;
            position = header_end;
            let end = position
                .checked_add(length)
                .ok_or_else(|| CelebornError::Protocol("invalid batch length".to_string()))?;
            if end > bytes.len() {
                return Err(CelebornError::Protocol("truncated batch data".to_string()));
            }
            data.extend_from_slice(&bytes[position..end]);
            position = end;
        }
        Ok(data)
    }

    /// Push one Celeborn map-data batch to this primary partition location.
    pub async fn push_data(
        &self,
        shuffle_key: &str,
        map_id: i32,
        attempt_id: i32,
        batch_id: i32,
        data: &[u8],
    ) -> CelebornResult<usize> {
        tokio::time::timeout(self.timeout, async {
            let (host, port) = self.endpoint(self.location.push_port);
            let mut stream = TcpStream::connect((&*host, port)).await?;
            let body_length = 16 + data.len();
            let shuffle_key = shuffle_key.as_bytes();
            let location_id = self.location.unique_id();
            let location_id = location_id.as_bytes();
            let encoded_length = 8 + 1 + 4 + shuffle_key.len() + 4 + location_id.len();
            let encoded_length = i32::try_from(encoded_length)
                .map_err(|_| CelebornError::Protocol("push header is too large".to_string()))?;
            let body_length = i32::try_from(body_length)
                .map_err(|_| CelebornError::Protocol("push body is too large".to_string()))?;

            stream.write_i32(encoded_length).await?;
            stream.write_u8(PUSH_DATA).await?;
            stream.write_i32(body_length).await?;
            stream.write_i64(0).await?;
            stream.write_u8(0).await?;
            write_bytes(&mut stream, shuffle_key).await?;
            write_bytes(&mut stream, location_id).await?;
            stream.write_i32(map_id).await?;
            stream.write_i32(attempt_id).await?;
            stream.write_i32(batch_id).await?;
            stream
                .write_i32(
                    i32::try_from(data.len()).map_err(|_| {
                        CelebornError::Protocol("push data is too large".to_string())
                    })?,
                )
                .await?;
            stream.write_all(data).await?;
            stream.flush().await?;

            let response_length = stream.read_i32().await?;
            let response_type = stream.read_u8().await?;
            let response_body_length = stream.read_i32().await?;
            if response_length != 12 || response_type != RPC_RESPONSE || response_body_length < 0 {
                return Err(CelebornError::Protocol("invalid push response".to_string()));
            }
            if stream.read_i64().await? != 0 {
                return Err(CelebornError::Protocol(
                    "push response ID does not match request".to_string(),
                ));
            }
            let declared_body_length = stream.read_i32().await?;
            if declared_body_length != response_body_length {
                return Err(CelebornError::Protocol(
                    "invalid push response length".to_string(),
                ));
            }
            let mut response = vec![
                0;
                usize::try_from(response_body_length).map_err(|_| {
                    CelebornError::Protocol("invalid push response length".to_string())
                })?
            ];
            stream.read_exact(&mut response).await?;
            match response.first().copied() {
                // Celeborn normally returns an empty response on success. It can also return a
                // success status, MAP_ENDED, or a congestion notification after accepting the
                // batch.
                None
                | Some(
                    0
                    | MAP_ENDED
                    | PUSH_DATA_SUCCESS_PRIMARY_CONGESTED
                    | PUSH_DATA_SUCCESS_REPLICA_CONGESTED,
                ) => {}
                Some(status) => {
                    return Err(CelebornError::Application(format!(
                        "worker requested push recovery with status {status}"
                    )));
                }
            }
            usize::try_from(body_length)
                .map_err(|_| CelebornError::Protocol("invalid push body length".to_string()))
        })
        .await
        .map_err(|_| CelebornError::Timeout)?
    }

    fn endpoint(&self, port: u16) -> (String, u16) {
        self.endpoint_resolver
            .as_ref()
            .map(|resolver| resolver.resolve(&self.location.host, port))
            .unwrap_or_else(|| (self.location.host.clone(), port))
    }
}

async fn write_bytes(stream: &mut TcpStream, bytes: &[u8]) -> CelebornResult<()> {
    stream
        .write_i32(
            i32::try_from(bytes.len())
                .map_err(|_| CelebornError::Protocol("string is too large".to_string()))?,
        )
        .await?;
    stream.write_all(bytes).await?;
    Ok(())
}

fn encode_transport_message(message_type: i32, payload: Vec<u8>) -> CelebornResult<Vec<u8>> {
    let payload_length = i32::try_from(payload.len())
        .map_err(|_| CelebornError::Protocol("transport payload is too large".to_string()))?;
    let mut message = Vec::with_capacity(payload.len() + 8);
    message.extend_from_slice(&message_type.to_be_bytes());
    message.extend_from_slice(&payload_length.to_be_bytes());
    message.extend_from_slice(&payload);
    Ok(message)
}

fn decode_transport_message(bytes: &[u8]) -> CelebornResult<TransportResponse> {
    if bytes.len() < 8 {
        return Err(CelebornError::Protocol(
            "truncated transport response".to_string(),
        ));
    }
    let message_type = i32::from_be_bytes(bytes[..4].try_into().map_err(|_| {
        CelebornError::Protocol("invalid transport response message type".to_string())
    })?);
    let payload_length = i32::from_be_bytes(bytes[4..8].try_into().map_err(|_| {
        CelebornError::Protocol("invalid transport response payload length".to_string())
    })?);
    let payload_length = usize::try_from(payload_length).map_err(|_| {
        CelebornError::Protocol("invalid transport response payload length".to_string())
    })?;
    if bytes.len() != payload_length + 8 {
        return Err(CelebornError::Protocol(
            "invalid transport response payload length".to_string(),
        ));
    }
    Ok(TransportResponse {
        message_type,
        payload: bytes[8..].to_vec(),
    })
}
