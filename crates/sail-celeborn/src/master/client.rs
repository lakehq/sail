use std::collections::HashMap;
use std::time::Duration;

use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use uuid::Uuid;

use crate::error::{CelebornError, CelebornResult};
use crate::protocol::MessageType;
use crate::protocol::proto::{
    PbRegisterApplicationInfo, PbRequestSlots, PbRequestSlotsResponse, PbUnregisterShuffle,
    PbUnregisterShuffleResponse,
};
use crate::protocol::transport::{
    NATIVE_TRANSPORT_MARKER, RPC_FAILURE, RPC_HEADER_LENGTH, RPC_REQUEST, RPC_RESPONSE,
    TransportResponse, decode_java_transport_message,
};

const MASTER_ENDPOINT_NAME: &str = "MasterEndpoint";

use super::{PartitionLocation, SlotReservation, UserIdentifier, WorkerSlotLocations};

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
/// Each request uses an independent TCP connection. This keeps the client stateless and makes it
/// suitable for dispatch from the lifecycle actor without synchronization.
#[derive(Debug, Clone)]
pub struct MasterClient {
    options: MasterClientOptions,
}

impl MasterClient {
    pub fn new(options: MasterClientOptions) -> Self {
        Self { options }
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
                MessageType::REGISTER_APPLICATION_INFO,
                request.encode_to_vec(),
            )
            .await?;
        if response.message_type != MessageType::ONE_WAY_MESSAGE_RESPONSE {
            return Err(CelebornError::Protocol(format!(
                "invalid registration acknowledgement message type: expected {} but got {}",
                MessageType::ONE_WAY_MESSAGE_RESPONSE,
                response.message_type
            )));
        }
        Ok(())
    }

    pub async fn request_slots(
        &self,
        application_id: String,
        shuffle_id: i32,
        partition_ids: Vec<i32>,
        hostname: String,
        should_replicate: bool,
        max_workers: i32,
        user_identifier: UserIdentifier,
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
            excluded_worker_set: Vec::new(),
            // Keep locations unpacked so the native client can address the selected worker.
            packed: false,
            tags_expr: String::new(),
        };
        let response = self
            .request(MessageType::REQUEST_SLOTS, request.encode_to_vec())
            .await?;
        if response.message_type != MessageType::REQUEST_SLOTS_RESPONSE {
            return Err(CelebornError::Protocol(format!(
                "invalid slot response message type: expected {} but got {}",
                MessageType::REQUEST_SLOTS_RESPONSE,
                response.message_type
            )));
        }
        let response = PbRequestSlotsResponse::decode(response.payload.as_slice())?;
        if response.status != 0 {
            return Err(CelebornError::Master {
                status: response.status,
            });
        }
        let mut worker_ids = response.worker_resource.keys().cloned().collect::<Vec<_>>();
        worker_ids.sort();
        let mut primary_locations = std::collections::HashMap::new();
        let mut worker_locations = std::collections::HashMap::new();
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
            .request(MessageType::UNREGISTER_SHUFFLE, request.encode_to_vec())
            .await?;
        if response.message_type != MessageType::UNREGISTER_SHUFFLE_RESPONSE {
            return Err(CelebornError::Protocol(format!(
                "invalid unregister response message type: expected {} but got {}",
                MessageType::UNREGISTER_SHUFFLE_RESPONSE,
                response.message_type
            )));
        }
        let response = PbUnregisterShuffleResponse::decode(response.payload.as_slice())?;
        if response.status != 0 {
            return Err(CelebornError::Master {
                status: response.status,
            });
        }
        Ok(())
    }

    async fn request(
        &self,
        message_type: i32,
        payload: Vec<u8>,
    ) -> CelebornResult<TransportResponse> {
        self.request_endpoint(MASTER_ENDPOINT_NAME, message_type, payload)
            .await
    }

    pub(crate) async fn request_endpoint(
        &self,
        endpoint: &str,
        message_type: i32,
        payload: Vec<u8>,
    ) -> CelebornResult<TransportResponse> {
        tokio::time::timeout(self.options.timeout, async {
            let mut stream = TcpStream::connect((&*self.options.host, self.options.port)).await?;
            let request_id = 0_i64;
            let payload = self.rpc_envelope(endpoint, message_type, payload)?;
            let body_length = i32::try_from(payload.len())
                .map_err(|_| CelebornError::Protocol("request body is too large".to_string()))?;

            // Celeborn's Netty decoder treats this as the encoded RPC header size, then reads
            // `transport_length` bytes as its body.
            stream.write_i32(RPC_HEADER_LENGTH).await?;
            stream.write_u8(RPC_REQUEST).await?;
            stream.write_i32(body_length).await?;
            stream.write_i64(request_id).await?;
            stream.write_i32(body_length).await?;
            stream.write_all(&payload).await?;
            stream.flush().await?;

            let encoded_length = stream.read_i32().await?;
            let response_type = stream.read_u8().await?;
            let body_length = stream.read_i32().await?;
            if response_type == RPC_FAILURE {
                if encoded_length < 12 || body_length != 0 {
                    return Err(CelebornError::Protocol(
                        "invalid RPC failure frame header".to_string(),
                    ));
                }
                let response_id = stream.read_i64().await?;
                if response_id != request_id {
                    return Err(CelebornError::Protocol(
                        "RPC failure ID does not match request".to_string(),
                    ));
                }
                let error_length = stream.read_i32().await?;
                let error_length = usize::try_from(error_length).map_err(|_| {
                    CelebornError::Protocol("invalid RPC failure length".to_string())
                })?;
                let mut error = vec![0; error_length];
                stream.read_exact(&mut error).await?;
                return Err(CelebornError::Protocol(
                    String::from_utf8_lossy(&error).into_owned(),
                ));
            }
            if encoded_length != RPC_HEADER_LENGTH || body_length < 0 {
                return Err(CelebornError::Protocol(format!(
                    "invalid RPC response frame header: encoded length {encoded_length}, body length {body_length}"
                )));
            }
            if response_type != RPC_RESPONSE {
                return Err(CelebornError::Protocol(format!(
                    "invalid RPC response frame message type: expected {RPC_RESPONSE} but got {response_type}"
                )));
            }
            let response_id = stream.read_i64().await?;
            if response_id != request_id {
                return Err(CelebornError::Protocol(
                    "RPC response ID does not match request".to_string(),
                ));
            }
            let declared_body_length = stream.read_i32().await?;
            if declared_body_length != body_length || body_length < 8 {
                return Err(CelebornError::Protocol(
                    "invalid RPC response body length".to_string(),
                ));
            }
            let response_length = usize::try_from(body_length).map_err(|_| {
                CelebornError::Protocol("invalid RPC response body length".to_string())
            })?;
            let mut response = vec![0; response_length];
            stream.read_exact(&mut response).await?;
            decode_java_transport_message(&response)
        })
        .await
        .map_err(|_| CelebornError::Timeout)?
    }

    fn rpc_envelope(
        &self,
        endpoint: &str,
        message_type: i32,
        payload: Vec<u8>,
    ) -> CelebornResult<Vec<u8>> {
        let host = self.options.host.as_bytes();
        let endpoint = endpoint.as_bytes();
        let host_length = u16::try_from(host.len())
            .map_err(|_| CelebornError::Protocol("master host is too long".to_string()))?;
        let endpoint_length = u16::try_from(endpoint.len())
            .map_err(|_| CelebornError::Protocol("master endpoint name is too long".to_string()))?;
        let payload_length = i32::try_from(payload.len())
            .map_err(|_| CelebornError::Protocol("request payload is too large".to_string()))?;
        let mut bytes = Vec::with_capacity(host.len() + endpoint.len() + payload.len() + 24);
        // Sender RpcAddress: a lightweight client address is sufficient because the master only
        // uses it for endpoint bookkeeping.
        bytes.push(1);
        bytes.extend_from_slice(&host_length.to_be_bytes());
        bytes.extend_from_slice(host);
        bytes.extend_from_slice(&0_i32.to_be_bytes());
        // Receiver RpcAddress and endpoint name.
        bytes.push(1);
        bytes.extend_from_slice(&host_length.to_be_bytes());
        bytes.extend_from_slice(host);
        bytes.extend_from_slice(&i32::from(self.options.port).to_be_bytes());
        bytes.extend_from_slice(&endpoint_length.to_be_bytes());
        bytes.extend_from_slice(endpoint);
        bytes.push(NATIVE_TRANSPORT_MARKER);
        bytes.extend_from_slice(&message_type.to_be_bytes());
        bytes.extend_from_slice(&payload_length.to_be_bytes());
        bytes.extend_from_slice(&payload);
        Ok(bytes)
    }
}

fn request_id() -> String {
    format!("{}#0", Uuid::new_v4())
}
