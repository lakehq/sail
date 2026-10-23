//! Session action messages from Arrow's format/Flight.proto. Arrow Rust does not
//! expose these experimental messages yet. Flight actions use their raw protobuf
//! encoding (unlike Flight SQL actions, which wrap messages in Any).
use std::collections::HashMap;

#[derive(Clone, PartialEq, prost::Message)]
pub struct SessionOptionValue {
    #[prost(oneof = "session_option_value::Value", tags = "1, 2, 3, 4, 5")]
    pub value: Option<session_option_value::Value>,
}

pub mod session_option_value {
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct StringList {
        #[prost(string, repeated, tag = "1")]
        pub values: Vec<String>,
    }
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Value {
        #[prost(string, tag = "1")]
        String(String),
        #[prost(bool, tag = "2")]
        Bool(bool),
        #[prost(sfixed64, tag = "3")]
        Int(i64),
        #[prost(double, tag = "4")]
        Double(f64),
        #[prost(message, tag = "5")]
        Strings(StringList),
    }
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct SessionOptions {
    #[prost(map = "string, message", tag = "1")]
    pub session_options: HashMap<String, SessionOptionValue>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct SetSessionOptionsResult {
    #[prost(map = "string, message", tag = "1")]
    pub errors: HashMap<String, SessionOptionError>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct SessionOptionError {
    // Flight.proto: INVALID_NAME=1, INVALID_VALUE=2, ERROR=3.
    #[prost(int32, tag = "1")]
    pub value: i32,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct CloseSessionResult {
    // Flight.proto: CLOSED=1.
    #[prost(int32, tag = "1")]
    pub status: i32,
}
