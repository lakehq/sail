use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use prost::Message;
use uuid::Uuid;

use crate::common::{
    PartitionLocation, SlotReservation, UserIdentifier, WorkerIdentity, WorkerSlotLocations,
};
use crate::endpoint::parse_endpoint;
use crate::error::{CelebornError, CelebornResult};
use crate::protocol::StatusCode;
use crate::protocol::proto::{
    MessageType, PbHeartbeatFromApplication, PbHeartbeatFromApplicationResponse,
    PbRegisterApplicationInfo, PbRequestSlots, PbRequestSlotsResponse, PbUnregisterShuffle,
    PbUnregisterShuffleResponse, PbWorkerInfo,
};
use crate::protocol::transport::{TransportConnection, TransportMessage};

const MASTER_ENDPOINT_NAME: &str = "MasterEndpoint";

/// The master endpoints and timeout used by a [`MasterClient`].
#[derive(Debug, Clone)]
pub struct MasterClientOptions {
    pub endpoints: Vec<String>,
    pub timeout: Duration,
}

impl MasterClientOptions {
    pub fn new(endpoints: Vec<String>) -> Self {
        Self {
            endpoints,
            timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone)]
struct MasterConnection {
    host: String,
    port: u16,
    connection: TransportConnection,
}

impl MasterConnection {
    fn try_new(endpoint: String, timeout: Duration) -> Option<Self> {
        let (host, port) = parse_endpoint(&endpoint)?;
        Some(Self {
            host: host.clone(),
            port,
            connection: TransportConnection::new(host, port, timeout),
        })
    }
}

#[derive(Debug, Clone)]
enum MasterConnections {
    Available {
        valid: Vec<MasterConnection>,
        current: Arc<AtomicUsize>,
        invalid: Vec<String>,
    },
    Unavailable {
        invalid: Vec<String>,
    },
}

impl MasterConnections {
    fn new(options: MasterClientOptions) -> Self {
        let mut valid = Vec::new();
        let mut invalid = Vec::new();
        for endpoint in options.endpoints {
            if let Some(connection) = MasterConnection::try_new(endpoint.clone(), options.timeout) {
                valid.push(connection);
            } else {
                invalid.push(endpoint);
            }
        }
        if valid.is_empty() {
            Self::Unavailable { invalid }
        } else {
            Self::Available {
                valid,
                current: Arc::new(AtomicUsize::new(0)),
                invalid,
            }
        }
    }

    fn fallback_error(&self) -> CelebornError {
        let invalid = match self {
            Self::Available { invalid, .. } | Self::Unavailable { invalid } => invalid,
        };
        if invalid.is_empty() {
            CelebornError::InvalidArgument(
                "at least one Celeborn master endpoint must be configured".to_string(),
            )
        } else {
            CelebornError::InvalidArgument(format!(
                "invalid Celeborn master endpoints: {}",
                invalid.join(", ")
            ))
        }
    }

    fn advance_after_failure(current: &AtomicUsize, index: usize, count: usize) {
        let next = (index + 1) % count;
        let _ = current.compare_exchange(index, next, Ordering::Relaxed, Ordering::Relaxed);
    }
}

/// A small async client for Celeborn's Netty master RPC protocol.
///
/// Clones share a serialized TCP connection, matching Celeborn's Netty client channel lifecycle.
#[derive(Debug, Clone)]
pub struct MasterClient {
    connections: MasterConnections,
}

impl MasterClient {
    pub fn new(options: MasterClientOptions) -> Self {
        Self {
            connections: MasterConnections::new(options),
        }
    }

    pub async fn register_application(
        &self,
        application_id: String,
        user_identifier: UserIdentifier,
    ) -> CelebornResult<()> {
        let request = PbRegisterApplicationInfo {
            app_id: application_id,
            user_identifier: Some(user_identifier.into()),
            extra_info: HashMap::new(),
            request_id: request_id(),
        };
        let response = self
            .request(
                MessageType::RegisterApplicationInfo,
                request.encode_to_vec(),
            )
            .await?;
        if response.message_type != MessageType::OneWayMessageResponse {
            return Err(CelebornError::Protocol(format!(
                "invalid registration acknowledgement message type: expected {} but got {}",
                MessageType::OneWayMessageResponse as i32,
                response.message_type as i32
            )));
        }
        Ok(())
    }

    /// Keep an application alive on the master while its shuffle client is running.
    pub async fn heartbeat_application(
        &self,
        application_id: String,
        metrics: crate::common::ApplicationMetrics,
    ) -> CelebornResult<()> {
        let request = PbHeartbeatFromApplication {
            app_id: application_id,
            total_written: metrics.total_written,
            file_count: metrics.file_count,
            request_id: request_id(),
            need_checked_worker_list: Vec::new(),
            should_response: true,
            shuffle_count: metrics.shuffle_count,
            shuffle_fallback_counts: metrics.shuffle_fallback_counts,
            application_count: metrics.application_count,
            application_fallback_counts: metrics.application_fallback_counts,
        };
        let response = self
            .request(
                MessageType::HeartbeatFromApplication,
                request.encode_to_vec(),
            )
            .await?;
        if response.message_type != MessageType::HeartbeatFromApplicationResponse {
            return Err(CelebornError::Protocol(format!(
                "invalid application heartbeat response message type: expected {} but got {}",
                MessageType::HeartbeatFromApplicationResponse as i32,
                response.message_type as i32
            )));
        }
        let response = PbHeartbeatFromApplicationResponse::decode(response.payload.as_slice())?;
        if response.status != i32::from(StatusCode::Success as u8) {
            return Err(CelebornError::Master {
                status: response.status,
            });
        }
        Ok(())
    }

    #[expect(clippy::too_many_arguments)]
    pub async fn request_slots(
        &self,
        application_id: String,
        shuffle_id: i32,
        partition_ids: Vec<i32>,
        hostname: String,
        should_replicate: bool,
        max_workers: i32,
        user_identifier: UserIdentifier,
        excluded_workers: Vec<PartitionLocation>,
    ) -> CelebornResult<SlotReservation> {
        let request = PbRequestSlots {
            application_id,
            shuffle_id,
            partition_id_list: partition_ids,
            hostname,
            should_replicate,
            request_id: request_id(),
            storage_type: 0,
            user_identifier: Some(user_identifier.into()),
            should_rack_aware: false,
            max_workers,
            available_storage_types: 0,
            excluded_worker_set: excluded_workers
                .into_iter()
                .map(|worker| PbWorkerInfo {
                    host: worker.host,
                    rpc_port: i32::from(worker.rpc_port),
                    push_port: i32::from(worker.push_port),
                    fetch_port: i32::from(worker.fetch_port),
                    replicate_port: i32::from(worker.replicate_port),
                    disks: Vec::new(),
                    user_resource_consumption: HashMap::new(),
                    internal_port: 0,
                    network_location: String::new(),
                })
                .collect(),
            // Keep locations unpacked so the native client can address the selected worker.
            packed: false,
            tags_expr: String::new(),
        };
        let response = self
            .request(MessageType::RequestSlots, request.encode_to_vec())
            .await?;
        if response.message_type != MessageType::RequestSlotsResponse {
            return Err(CelebornError::Protocol(format!(
                "invalid slot response message type: expected {} but got {}",
                MessageType::RequestSlotsResponse as i32,
                response.message_type as i32
            )));
        }
        let response = PbRequestSlotsResponse::decode(response.payload.as_slice())?;
        if response.status != i32::from(StatusCode::Success as u8) {
            return Err(CelebornError::Master {
                status: response.status,
            });
        }
        let mut primary_locations = HashMap::new();
        let mut worker_locations = HashMap::new();
        let mut worker_ids = Vec::with_capacity(response.worker_resource.len());
        for (worker_id, resource) in response.worker_resource {
            let worker_identity = worker_id.parse::<WorkerIdentity>()?;
            worker_ids.push(worker_identity.clone());
            let primary = resource
                .primary_partitions
                .into_iter()
                .map(PartitionLocation::try_from)
                .collect::<CelebornResult<Vec<_>>>()?;
            let replica = resource
                .replica_partitions
                .into_iter()
                .map(PartitionLocation::try_from)
                .collect::<CelebornResult<Vec<_>>>()?;
            // Celeborn can include resource entries for workers that did not receive any
            // partitions in this reservation. Keep the identity, but defer client creation until
            // a partition location selects that worker.
            if primary.is_empty() && replica.is_empty() {
                continue;
            }
            primary_locations.extend(
                primary
                    .iter()
                    .cloned()
                    .map(|location| (location.id, location)),
            );
            worker_locations.insert(
                worker_identity,
                WorkerSlotLocations {
                    primary_locations: primary,
                    replica_locations: replica,
                },
            );
        }
        worker_ids.sort();
        worker_ids.dedup();
        Ok(SlotReservation {
            worker_ids,
            primary_locations,
            worker_locations,
        })
    }

    pub async fn unregister_shuffle(
        &self,
        application_id: String,
        shuffle_id: i32,
    ) -> CelebornResult<()> {
        let request = PbUnregisterShuffle {
            app_id: application_id,
            shuffle_id,
            request_id: request_id(),
        };
        let response = self
            .request(MessageType::UnregisterShuffle, request.encode_to_vec())
            .await?;
        if response.message_type != MessageType::UnregisterShuffleResponse {
            return Err(CelebornError::Protocol(format!(
                "invalid unregister response message type: expected {} but got {}",
                MessageType::UnregisterShuffleResponse as i32,
                response.message_type as i32
            )));
        }
        let response = PbUnregisterShuffleResponse::decode(response.payload.as_slice())?;
        if response.status != i32::from(StatusCode::Success as u8) {
            return Err(CelebornError::Master {
                status: response.status,
            });
        }
        Ok(())
    }

    async fn request(
        &self,
        message_type: MessageType,
        payload: Vec<u8>,
    ) -> CelebornResult<TransportMessage> {
        let mut last_error = None;
        if let MasterConnections::Available { valid, current, .. } = &self.connections {
            let mut index = current.load(Ordering::Relaxed) % valid.len();
            for _ in 0..valid.len() {
                let connection = &valid[index];
                let request = TransportMessage::new(message_type, payload.clone())
                    .into_rpc_envelope(
                        &connection.host,
                        &connection.host,
                        connection.port,
                        MASTER_ENDPOINT_NAME,
                    )
                    .encode()?;
                match connection.connection.send_rpc(request).await {
                    Ok(response) => match TransportMessage::decode_java(&response) {
                        Ok(response) => return Ok(response),
                        Err(error) => {
                            last_error = Some(error);
                            MasterConnections::advance_after_failure(current, index, valid.len());
                            index = (index + 1) % valid.len();
                        }
                    },
                    Err(error) => {
                        last_error = Some(error);
                        MasterConnections::advance_after_failure(current, index, valid.len());
                        index = (index + 1) % valid.len();
                    }
                }
            }
        }
        Err(last_error.unwrap_or_else(|| self.connections.fallback_error()))
    }
}

fn request_id() -> String {
    format!("{}#0", Uuid::new_v4())
}
