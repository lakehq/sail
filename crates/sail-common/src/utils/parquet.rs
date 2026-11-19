// SPDX-License-Identifier: Apache-2.0

/// The "created by" property written to the metadata of Parquet files written by Sail.
pub const SAIL_PARQUET_CREATED_BY: &str = concat!("sail version ", env!("CARGO_PKG_VERSION"));
