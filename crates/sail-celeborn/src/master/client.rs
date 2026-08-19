use std::collections::HashMap;
use std::time::Duration;

use prost::Message;
use uuid::Uuid;

use crate::common::{PartitionLocation, SlotReservation, UserIdentifier, WorkerSlotLocations};
use crate::error::{CelebornError, CelebornResult};
use crate::protocol::StatusCode;
use crate::protocol::proto::{
    MessageType, PbRegisterApplicationInfo, PbRequestSlots, PbRequestSlotsResponse,
    PbUnregisterShuffle, PbUnregisterShuffleResponse, PbWorkerInfo,
};
use crate::protocol::transport::{TransportConnection, TransportMessage};

const MASTER_ENDPOINT_NAME: &str = "MasterEndpoint";

/// The master endpoint and timeout used by a [`MasterClient`].
#[derive(Debug, Clone)]
pub struct MasterClientOptions {
    pub host: String,
    pub port: u16,
    pub timeout: Duration,
}

impl MasterClientOptions {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            timeout: Duration::from_secs(30),
        }
    }
}

/// A small async client for Celeborn's Netty master RPC protocol.
///
/// Clones share a serialized TCP connection, matching Celeborn's Netty client channel lifecycle.
#[derive(Debug, Clone)]
pub struct MasterClient {
    options: MasterClientOptions,
    connection: TransportConnection,
}

impl MasterClient {
    pub fn new(options: MasterClientOptions) -> Self {
        let connection = TransportConnection::new(&options.host, options.port, options.timeout);
        Self {
            options,
            connection,
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
        let mut worker_ids = response.worker_resource.keys().cloned().collect::<Vec<_>>();
        worker_ids.sort();
        let mut primary_locations = HashMap::new();
        let mut worker_locations = HashMap::new();
        for (worker_id, resource) in response.worker_resource {
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
            primary_locations.extend(
                primary
                    .iter()
                    .cloned()
                    .map(|location| (location.id, location)),
            );
            worker_locations.insert(
                worker_id,
                WorkerSlotLocations {
                    primary_locations: primary,
                    replica_locations: replica,
                },
            );
        }
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
        let payload = TransportMessage::new(message_type, payload)
            .into_rpc_envelope(
                &self.options.host,
                &self.options.host,
                self.options.port,
                MASTER_ENDPOINT_NAME,
            )
            .encode()?;
        let response = self.connection.send_rpc(payload).await?;
        TransportMessage::decode_java(&response)
    }
}

fn request_id() -> String {
    format!("{}#0", Uuid::new_v4())
}
