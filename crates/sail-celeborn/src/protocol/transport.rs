use crate::error::{CelebornError, CelebornResult};

pub(crate) const RPC_REQUEST: u8 = 3;
pub(crate) const RPC_RESPONSE: u8 = 4;
pub(crate) const RPC_FAILURE: u8 = 5;
pub(crate) const RPC_HEADER_LENGTH: i32 = 12;
pub(crate) const NATIVE_TRANSPORT_MARKER: u8 = 0xff;

pub(crate) struct TransportResponse {
    pub(crate) message_type: i32,
    pub(crate) payload: Vec<u8>,
}

/// Decode Java serialization's stable `TransportMessage` layout used by Celeborn master replies.
/// The V1 wrapper contains the message type followed by a serialized `byte[]` payload.
pub(crate) fn decode_java_transport_message(bytes: &[u8]) -> CelebornResult<TransportResponse> {
    let mut reader = JavaReader::new(bytes);
    if reader.read_exact(4)? != [0xac, 0xed, 0x00, 0x05] || reader.read_u8()? != 0x73 {
        return Err(CelebornError::Protocol(
            "expected Java-serialized Celeborn transport response".to_string(),
        ));
    }
    reader.read_class_description()?;
    let message_type = reader.read_i32()?;
    let payload = reader.read_byte_array()?;
    Ok(TransportResponse {
        message_type,
        payload,
    })
}

struct JavaReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> JavaReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn read_exact(&mut self, length: usize) -> CelebornResult<&'a [u8]> {
        let end = self.position.checked_add(length).ok_or_else(|| {
            CelebornError::Protocol("invalid Java serialization length".to_string())
        })?;
        let bytes = self.bytes.get(self.position..end).ok_or_else(|| {
            CelebornError::Protocol("truncated Java-serialized response".to_string())
        })?;
        self.position = end;
        Ok(bytes)
    }

    fn read_u8(&mut self) -> CelebornResult<u8> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> CelebornResult<u16> {
        let bytes = self.read_exact(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_i32(&mut self) -> CelebornResult<i32> {
        let bytes = self.read_exact(4)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_class_description(&mut self) -> CelebornResult<()> {
        if self.read_u8()? != 0x72 {
            return Err(CelebornError::Protocol(
                "expected Java class description".to_string(),
            ));
        }
        let class_name_length = usize::from(self.read_u16()?);
        let class_name = self.read_exact(class_name_length)?;
        if class_name != b"org.apache.celeborn.common.network.protocol.TransportMessage" {
            return Err(CelebornError::Protocol(
                "unexpected Java response class".to_string(),
            ));
        }
        self.read_exact(8)?; // serialVersionUID
        self.read_exact(1)?; // class flags
        let field_count = usize::from(self.read_u16()?);
        for _ in 0..field_count {
            let field_type = self.read_u8()?;
            let field_name_length = usize::from(self.read_u16()?);
            self.read_exact(field_name_length)?;
            if field_type == b'L' || field_type == b'[' {
                self.read_string()?;
            }
        }
        if self.read_u8()? != 0x78 || self.read_u8()? != 0x70 {
            return Err(CelebornError::Protocol(
                "unsupported Java response class hierarchy".to_string(),
            ));
        }
        Ok(())
    }

    fn read_string(&mut self) -> CelebornResult<()> {
        match self.read_u8()? {
            0x74 => {
                let length = usize::from(self.read_u16()?);
                self.read_exact(length)?;
                Ok(())
            }
            0x71 => {
                self.read_exact(4)?;
                Ok(())
            }
            _ => Err(CelebornError::Protocol(
                "unsupported Java class field descriptor".to_string(),
            )),
        }
    }

    fn read_byte_array(&mut self) -> CelebornResult<Vec<u8>> {
        let token = self.read_u8()?;
        if token == 0x70 {
            return Ok(Vec::new());
        }
        if token != 0x75 {
            return Err(CelebornError::Protocol(format!(
                "expected Java byte array payload, got token {token:#x}"
            )));
        }
        self.read_array_description()?;
        let length = usize::try_from(self.read_i32()?)
            .map_err(|_| CelebornError::Protocol("invalid Java byte array length".to_string()))?;
        Ok(self.read_exact(length)?.to_vec())
    }

    fn read_array_description(&mut self) -> CelebornResult<()> {
        if self.read_u8()? != 0x72 {
            return Err(CelebornError::Protocol(
                "expected Java byte array class description".to_string(),
            ));
        }
        let name_length = usize::from(self.read_u16()?);
        if self.read_exact(name_length)? != b"[B" {
            return Err(CelebornError::Protocol(
                "expected Java byte array class".to_string(),
            ));
        }
        self.read_exact(8)?;
        self.read_exact(1)?;
        if self.read_u16()? != 0 || self.read_u8()? != 0x78 || self.read_u8()? != 0x70 {
            return Err(CelebornError::Protocol(
                "unsupported Java byte array hierarchy".to_string(),
            ));
        }
        Ok(())
    }
}
