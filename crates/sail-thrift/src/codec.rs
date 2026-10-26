//! Wire validation shared by generated client codecs.

use thrift::protocol::TType;
use thrift::{ProtocolError, ProtocolErrorKind};

pub fn invalid_data(message: &str) -> thrift::Error {
    ProtocolError::new(ProtocolErrorKind::InvalidData, message).into()
}

pub fn wire_len(size: usize) -> thrift::Result<i32> {
    i32::try_from(size).map_err(|_| invalid_data("Thrift container exceeds wire size"))
}

pub fn check_container(size: i32, actual: TType, expected: TType) -> thrift::Result<()> {
    // Every supported element occupies at least one byte. Bound allocations and
    // loops even when a framed peer supplies malicious container metadata.
    if size < 0 || size as usize > crate::transport::MAX_MESSAGE_SIZE || actual != expected {
        return Err(invalid_data(
            "invalid Thrift container size or element type",
        ));
    }

    Ok(())
}

pub fn check_map(
    header: &thrift::protocol::TMapIdentifier,
    key: TType,
    value: TType,
) -> thrift::Result<()> {
    if header.size < 0
        || header.size as usize > crate::transport::MAX_MESSAGE_SIZE
        || (header.size > 0 && (header.key_type != Some(key) || header.value_type != Some(value)))
    {
        return Err(invalid_data("invalid Thrift map size or element type"));
    }

    Ok(())
}
