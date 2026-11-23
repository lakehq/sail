// SPDX-License-Identifier: Apache-2.0

mod builder;
mod connection;

pub use builder::{ServerBuilder, ServerBuilderOptions};

/// Time given to active RPCs to finish before their connections are closed.
pub const CONNECTION_GRACE_PERIOD: std::time::Duration = std::time::Duration::from_secs(5);
