//! Native async Thrift clients backed by Apache Thrift's in-memory binary codecs.

mod client;
pub mod codec;
pub mod transport;

pub use client::{Client, ClientError, Connector, MaybeException, ReadHalf, WriteHalf};
pub use transport::Transport;
