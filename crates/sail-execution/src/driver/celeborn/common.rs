use sail_celeborn::common::PartitionLocation;
use sail_celeborn::error::CelebornError;

use crate::driver::r#gen::CelebornPartitionLocation;

impl From<PartitionLocation> for CelebornPartitionLocation {
    fn from(location: PartitionLocation) -> Self {
        Self {
            id: location.id,
            epoch: location.epoch,
            host: location.host,
            rpc_port: u32::from(location.rpc_port),
            push_port: u32::from(location.push_port),
            fetch_port: u32::from(location.fetch_port),
            replicate_port: u32::from(location.replicate_port),
            peer: location.peer.map(|peer| Box::new((*peer).into())),
            mode: location.mode,
        }
    }
}

impl TryFrom<CelebornPartitionLocation> for PartitionLocation {
    type Error = CelebornError;

    fn try_from(location: CelebornPartitionLocation) -> Result<Self, Self::Error> {
        Ok(Self {
            id: location.id,
            epoch: location.epoch,
            host: location.host,
            rpc_port: u16::try_from(location.rpc_port)
                .map_err(|_| CelebornError::Protocol("invalid Celeborn RPC port".to_string()))?,
            push_port: u16::try_from(location.push_port)
                .map_err(|_| CelebornError::Protocol("invalid Celeborn push port".to_string()))?,
            fetch_port: u16::try_from(location.fetch_port)
                .map_err(|_| CelebornError::Protocol("invalid Celeborn fetch port".to_string()))?,
            replicate_port: u16::try_from(location.replicate_port).map_err(|_| {
                CelebornError::Protocol("invalid Celeborn replication port".to_string())
            })?,
            peer: location
                .peer
                .map(|peer| Self::try_from(*peer).map(Box::new))
                .transpose()?,
            mode: location.mode,
        })
    }
}
