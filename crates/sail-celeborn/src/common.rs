use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::error::CelebornError;
use crate::protocol::proto::{PbPartitionLocation, PbUserIdentifier};

/// The wire-level behavior a Celeborn worker uses when a partition exceeds its split threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
#[repr(i32)]
pub enum PartitionSplitMode {
    Soft = 0,
    Hard = 1,
}

impl Display for PartitionSplitMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Soft => f.write_str("soft"),
            Self::Hard => f.write_str("hard"),
        }
    }
}

impl FromStr for PartitionSplitMode {
    type Err = CelebornError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "soft" => Ok(Self::Soft),
            "hard" => Ok(Self::Hard),
            _ => Err(CelebornError::InvalidArgument(format!(
                "partition split mode must be `soft` or `hard` but got {value}"
            ))),
        }
    }
}

/// Slots reserved by the Celeborn master for a shuffle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotReservation {
    /// Celeborn worker unique IDs that received slots.
    pub worker_ids: Vec<WorkerIdentity>,
    /// Primary partition locations keyed by reduce partition ID.
    pub primary_locations: HashMap<i32, PartitionLocation>,
    /// Slots to reserve, grouped by the worker that owns them.
    pub worker_locations: HashMap<WorkerIdentity, WorkerSlotLocations>,
}

/// The stable network identity of a Celeborn worker.
///
/// Partition locations include a partition ID and epoch. Those values are not part of the worker
/// identity and must not determine which transport connections are reused.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorkerIdentity {
    pub host: String,
    pub rpc_port: u16,
    pub push_port: u16,
    pub fetch_port: u16,
    pub replicate_port: u16,
}

impl Display for WorkerIdentity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}:{}:{}",
            self.host, self.rpc_port, self.push_port, self.fetch_port, self.replicate_port
        )
    }
}

impl FromStr for WorkerIdentity {
    type Err = CelebornError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut parts = value.rsplitn(5, ':');
        let replicate_port = parse_worker_port(value, "replicate", parts.next())?;
        let fetch_port = parse_worker_port(value, "fetch", parts.next())?;
        let push_port = parse_worker_port(value, "push", parts.next())?;
        let rpc_port = parse_worker_port(value, "rpc", parts.next())?;
        let host = parts
            .next()
            .filter(|host| !host.is_empty())
            .ok_or_else(|| {
                CelebornError::InvalidArgument(format!("invalid Celeborn worker identity: {value}"))
            })?;
        Ok(Self {
            host: host.to_string(),
            rpc_port,
            push_port,
            fetch_port,
            replicate_port,
        })
    }
}

fn parse_worker_port(
    worker_id: &str,
    name: &str,
    value: Option<&str>,
) -> Result<u16, CelebornError> {
    value
        .ok_or_else(|| {
            CelebornError::InvalidArgument(format!("invalid Celeborn worker identity: {worker_id}"))
        })?
        .parse::<u16>()
        .map_err(|_| {
            CelebornError::InvalidArgument(format!(
                "invalid {name} port in Celeborn worker identity: {worker_id}"
            ))
        })
}

/// Compression applied to individual push-data batches.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CompressionCodec {
    None,
    #[default]
    Lz4,
    Zstd {
        level: i8,
    },
}

/// Metrics sent with an application heartbeat.
#[derive(Debug, Clone, Default)]
pub struct ApplicationMetrics {
    pub total_written: i64,
    pub file_count: i64,
    pub shuffle_count: i64,
    pub application_count: i64,
    pub shuffle_fallback_counts: HashMap<String, i64>,
    pub application_fallback_counts: HashMap<String, i64>,
}

impl ApplicationMetrics {
    pub fn add_assign(&mut self, other: Self) {
        self.total_written = self.total_written.saturating_add(other.total_written);
        self.file_count = self.file_count.saturating_add(other.file_count);
        self.shuffle_count = self.shuffle_count.saturating_add(other.shuffle_count);
        self.application_count = self
            .application_count
            .saturating_add(other.application_count);
        for (key, value) in other.shuffle_fallback_counts {
            let count = self.shuffle_fallback_counts.entry(key).or_default();
            *count = count.saturating_add(value);
        }
        for (key, value) in other.application_fallback_counts {
            let count = self.application_fallback_counts.entry(key).or_default();
            *count = count.saturating_add(value);
        }
    }
}

impl Display for CompressionCodec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => f.write_str("none"),
            Self::Lz4 => f.write_str("lz4"),
            Self::Zstd { level } => write!(f, "zstd({level})"),
        }
    }
}

impl FromStr for CompressionCodec {
    type Err = CelebornError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "none" => Ok(Self::None),
            "lz4" => Ok(Self::Lz4),
            value => value
                .strip_prefix("zstd(")
                .and_then(|value| value.strip_suffix(')'))
                .ok_or_else(|| {
                    CelebornError::InvalidArgument(format!(
                        "invalid Celeborn compression codec: {value}"
                    ))
                })?
                .parse::<i8>()
                .map_err(|_| {
                    CelebornError::InvalidArgument(format!(
                        "invalid Celeborn zstd compression level: {value}"
                    ))
                })
                .and_then(|level| {
                    (-5..=22)
                        .contains(&level)
                        .then_some(Self::Zstd { level })
                        .ok_or_else(|| {
                            CelebornError::InvalidArgument(format!(
                                "invalid Celeborn zstd compression level: {value}"
                            ))
                        })
                }),
        }
    }
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

    pub fn worker_identity(&self) -> WorkerIdentity {
        WorkerIdentity {
            host: self.host.clone(),
            rpc_port: self.rpc_port,
            push_port: self.push_port,
            fetch_port: self.fetch_port,
            replicate_port: self.replicate_port,
        }
    }

    pub fn set_epoch(&mut self, epoch: i32) {
        self.epoch = epoch;
        if let Some(peer) = &mut self.peer {
            peer.set_epoch(epoch);
        }
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

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::{CompressionCodec, WorkerIdentity};

    #[test]
    fn parses_compression_codecs() {
        assert!(matches!("none".parse(), Ok(CompressionCodec::None)));
        assert!(matches!("lz4".parse(), Ok(CompressionCodec::Lz4)));
        assert!(matches!(
            "zstd(1)".parse(),
            Ok(CompressionCodec::Zstd { level: 1 })
        ));
        assert!("zstd".parse::<CompressionCodec>().is_err());
        assert!(matches!(
            "zstd(-5)".parse(),
            Ok(CompressionCodec::Zstd { level: -5 })
        ));
        assert!("zstd(-6)".parse::<CompressionCodec>().is_err());
        assert!("zstd(127)".parse::<CompressionCodec>().is_err());
    }

    #[test]
    fn parses_worker_identity() {
        assert_eq!(
            "worker:12000:12001:12002:12003"
                .parse::<WorkerIdentity>()
                .unwrap(),
            WorkerIdentity {
                host: "worker".to_string(),
                rpc_port: 12000,
                push_port: 12001,
                fetch_port: 12002,
                replicate_port: 12003,
            }
        );
    }
}
