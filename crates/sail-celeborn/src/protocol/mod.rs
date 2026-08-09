//! Common Celeborn wire protocol types and transport-message decoding.

#[expect(clippy::enum_variant_names)]
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/celeborn.rs"));
}

pub(crate) mod transport;

/// Celeborn transport-message type identifiers.
pub struct MessageType;

impl MessageType {
    pub const REQUEST_SLOTS: i32 = 6;
    pub const REQUEST_SLOTS_RESPONSE: i32 = 9;
    pub const UNREGISTER_SHUFFLE: i32 = 16;
    pub const UNREGISTER_SHUFFLE_RESPONSE: i32 = 17;
    pub const RESERVE_SLOTS: i32 = 28;
    pub const RESERVE_SLOTS_RESPONSE: i32 = 29;
    pub const COMMIT_FILES: i32 = 30;
    pub const COMMIT_FILES_RESPONSE: i32 = 31;
    pub const ONE_WAY_MESSAGE_RESPONSE: i32 = 40;
    pub const OPEN_STREAM: i32 = 51;
    pub const STREAM_HANDLER: i32 = 52;
    pub const REGISTER_APPLICATION_INFO: i32 = 96;
}
