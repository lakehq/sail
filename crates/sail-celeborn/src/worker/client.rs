use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, BoxStream};
use prost::Message;

use crate::endpoint::EndpointResolver;
use crate::error::{CelebornError, CelebornResult};
use crate::master::{PartitionLocation, UserIdentifier};
use crate::protocol::StatusCode;
use crate::protocol::proto::{
    MessageType, PbCommitFiles, PbCommitFilesResponse, PbOpenStream, PbReserveSlots,
    PbReserveSlotsResponse, PbStreamHandler,
};
use crate::protocol::transport::{TransportConnection, TransportMessage};

const WORKER_ENDPOINT_NAME: &str = "WorkerEndpoint";

/// A small async client for a Celeborn worker endpoint.
#[derive(Clone)]
pub struct WorkerClient {
    location: PartitionLocation,
    rpc: TransportConnection,
    push: TransportConnection,
    fetch: TransportConnection,
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

    /// Reserve a worker-side file writer for this partition location.
    pub async fn reserve_slots(
        &self,
        application_id: String,
        shuffle_id: i32,
        primary_locations: Vec<PartitionLocation>,
        replica_locations: Vec<PartitionLocation>,
        user_identifier: UserIdentifier,
        split_threshold: i64,
        split_mode: i32,
    ) -> CelebornResult<()> {
        let request = PbReserveSlots {
            application_id,
            shuffle_id,
            primary_locations: primary_locations.into_iter().map(Into::into).collect(),
            replica_locations: replica_locations.into_iter().map(Into::into).collect(),
            split_threshold,
            split_mode,
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
    ) -> CelebornResult<()> {
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
        Ok(())
    }

    /// Open a lazy stream of committed batches for this primary partition.
    pub async fn read_partition_stream(
        &self,
        shuffle_key: &str,
    ) -> BoxStream<'static, CelebornResult<Vec<u8>>> {
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
            (client, handler.stream_id, 0, handler.num_chunks, Vec::new()),
            |(client, stream_id, mut chunk_index, num_chunks, mut buffered)| async move {
                loop {
                    if let Some(batch) = take_batch(&mut buffered)? {
                        return Ok(Some((
                            batch,
                            (client, stream_id, chunk_index, num_chunks, buffered),
                        )));
                    }
                    if chunk_index == num_chunks {
                        return if buffered.is_empty() {
                            Ok(None)
                        } else {
                            Err(CelebornError::Protocol("truncated batch data".to_string()))
                        };
                    }
                    buffered.extend(
                        client
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
        data: &[u8],
    ) -> CelebornResult<usize> {
        let location_id = self.location.unique_id();
        self.push
            .push_data(
                shuffle_key,
                &location_id,
                map_id,
                attempt_id,
                batch_id,
                data,
            )
            .await?;
        Ok(data.len() + 16)
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

fn take_batch(buffered: &mut Vec<u8>) -> CelebornResult<Option<Vec<u8>>> {
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
    let mut batch = buffered.drain(..end).collect::<Vec<_>>();
    Ok(Some(batch.split_off(HEADER_LENGTH)))
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
