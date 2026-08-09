use std::collections::HashMap;

use crate::error::CelebornError;
use crate::protocol::proto::{PbPartitionLocation, PbUserIdentifier};

/// Slots reserved by the Celeborn master for a shuffle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotReservation {
    /// Celeborn worker unique IDs that received slots.
    pub worker_ids: Vec<String>,
    /// Primary partition locations keyed by reduce partition ID.
    pub primary_locations: HashMap<i32, PartitionLocation>,
    /// Slots to reserve, grouped by the worker that owns them.
    pub worker_locations: HashMap<String, WorkerSlotLocations>,
}

/// The primary and replica slots that must be reserved on one worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerSlotLocations {
    pub primary_locations: Vec<PartitionLocation>,
    pub replica_locations: Vec<PartitionLocation>,
}

/// The Celeborn tenant and user that own an application.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserIdentifier {
    pub tenant_id: String,
    pub name: String,
}

impl From<UserIdentifier> for PbUserIdentifier {
    fn from(user_identifier: UserIdentifier) -> Self {
        Self {
            tenant_id: user_identifier.tenant_id,
            name: user_identifier.name,
        }
    }
}

/// A worker endpoint selected for a shuffle partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionLocation {
    pub mode: i32,
    pub id: i32,
    pub epoch: i32,
    pub host: String,
    pub rpc_port: u16,
    pub push_port: u16,
    pub fetch_port: u16,
    pub replicate_port: u16,
    pub peer: Option<Box<PartitionLocation>>,
}

impl PartitionLocation {
    pub fn unique_id(&self) -> String {
        format!("{}-{}", self.id, self.epoch)
    }
}

impl From<PartitionLocation> for PbPartitionLocation {
    fn from(location: PartitionLocation) -> Self {
        Self {
            mode: location.mode,
            id: location.id,
            epoch: location.epoch,
            host: location.host,
            rpc_port: i32::from(location.rpc_port),
            push_port: i32::from(location.push_port),
            fetch_port: i32::from(location.fetch_port),
            replicate_port: i32::from(location.replicate_port),
            peer: location.peer.map(|peer| Box::new((*peer).into())),
            storage_info: None,
            map_id_bitmap: Vec::new(),
        }
    }
}

impl TryFrom<PbPartitionLocation> for PartitionLocation {
    type Error = CelebornError;

    fn try_from(location: PbPartitionLocation) -> Result<Self, Self::Error> {
        Ok(Self {
            mode: location.mode,
            id: location.id,
            epoch: location.epoch,
            host: location.host,
            rpc_port: u16::try_from(location.rpc_port)
                .map_err(|_| CelebornError::Protocol("invalid worker RPC port".to_string()))?,
            push_port: u16::try_from(location.push_port)
                .map_err(|_| CelebornError::Protocol("invalid worker push port".to_string()))?,
            fetch_port: u16::try_from(location.fetch_port)
                .map_err(|_| CelebornError::Protocol("invalid worker fetch port".to_string()))?,
            replicate_port: u16::try_from(location.replicate_port).map_err(|_| {
                CelebornError::Protocol("invalid worker replication port".to_string())
            })?,
            peer: location
                .peer
                .map(|peer| Self::try_from(*peer).map(Box::new))
                .transpose()?,
        })
    }
}
