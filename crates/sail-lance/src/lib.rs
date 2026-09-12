// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance table format for [Sail](https://github.com/lakehq/sail).
//!
//! Sail is a Rust implementation of the Spark Connect server built on
//! DataFusion. It reaches its formats through a `TableFormat` trait, which this
//! crate implements for Lance: registering [`LanceTableFormat`] makes
//! `spark.read.format("lance")`, `df.write.format("lance")` and
//! `CREATE TABLE ... USING lance` work in a Sail session without any JVM.
//!
//! ```text
//!   Spark client ──Spark Connect──▶ Sail ──TableFormat──▶ sail-lance ──▶ lance::Dataset
//! ```
//!
//! Sail and Lance are built against the same DataFusion and Arrow releases, so
//! record batches and filter expressions pass between them unchanged.
//!
//! * [`LanceTableFormat`] plans reads and writes for Sail.
//! * [`LanceTableProvider`] is the DataFusion table provider it hands back,
//!   with projection, filters, limit and vector search pushed into the Lance
//!   scanner.
//! * [`LancePhysicalPlanner`] turns the write node into a physical write, and
//!   belongs in a Sail session's extension planners.

mod exec;
mod filter;
mod format;
mod options;
mod provider;
mod sink;
mod uri;
mod write;

/// Returns the message of a call that must fail.
#[cfg(test)]
pub(crate) fn error_message<T>(result: datafusion_common::Result<T>) -> String {
    match result {
        Ok(_) => "the call unexpectedly succeeded".to_string(),
        Err(error) => error.to_string(),
    }
}

pub use format::LanceTableFormat;
pub use options::{
    DatasetRef, LanceReadOptions, LanceWriteMode, LanceWriteOptions, NearestOptions,
};
pub use provider::LanceTableProvider;
pub use write::{LancePhysicalPlanner, LanceWriteNode};
