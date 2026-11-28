// SPDX-License-Identifier: Apache-2.0

use datafusion_common::{DataFusionError, Result};

pub(crate) const PUFFIN_MAGIC: &[u8; 4] = b"PFA1";

pub(crate) fn append_puffin_footer(
    bytes: &mut Vec<u8>,
    blobs: Vec<serde_json::Value>,
) -> Result<i64> {
    let footer = serde_json::to_vec(&serde_json::json!({ "blobs": blobs }))
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let footer_size = i32::try_from(footer.len()).map_err(|_| {
        datafusion_common::exec_datafusion_err!("Iceberg Puffin footer is too large")
    })?;
    bytes.extend_from_slice(PUFFIN_MAGIC);
    bytes.extend_from_slice(&footer);
    bytes.extend_from_slice(&footer_size.to_le_bytes());
    bytes.extend_from_slice(&[0; 4]);
    bytes.extend_from_slice(PUFFIN_MAGIC);
    Ok(i64::from(footer_size) + 16)
}
