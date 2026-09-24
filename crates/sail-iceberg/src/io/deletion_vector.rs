use std::io::{Cursor, Read};

use bytes::Bytes;
use datafusion_common::{DataFusionError, Result, exec_err};
use object_store::ObjectStoreExt;
use roaring::{RoaringBitmap, RoaringTreemap};
use url::Url;

use super::StoreContext;
use crate::spec::{DataContentType, DataFile, DataFileFormat};

const PUFFIN_MAGIC: &[u8; 4] = b"PFA1";
const VECTOR_MAGIC: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];

fn encode_blob(positions: &RoaringTreemap) -> Result<Vec<u8>> {
    if positions
        .max()
        .is_some_and(|position| position > i64::MAX as u64)
    {
        return exec_err!("Iceberg deletion vector positions must fit in a non-negative long");
    }
    let length = u32::try_from(4 + positions.serialized_size()).map_err(|_| {
        datafusion_common::exec_datafusion_err!("Iceberg deletion vector is too large")
    })?;
    let mut blob = Vec::with_capacity(length as usize + 8);
    blob.extend_from_slice(&length.to_be_bytes());
    blob.extend_from_slice(&VECTOR_MAGIC);
    positions.serialize_into(&mut blob)?;
    blob.extend_from_slice(&crc32fast::hash(&blob[4..]).to_be_bytes());
    Ok(blob)
}

fn decode_blob(blob: &[u8], cardinality: u64) -> Result<RoaringTreemap> {
    if blob.len() < 20 {
        return exec_err!("Truncated Iceberg deletion vector");
    }
    let mut word = [0; 4];
    word.copy_from_slice(&blob[..4]);
    if u32::from_be_bytes(word) as usize != blob.len() - 8 || blob[4..8] != VECTOR_MAGIC {
        return exec_err!("Invalid Iceberg deletion vector length or magic");
    }
    let checksum_start = blob.len() - 4;
    word.copy_from_slice(&blob[checksum_start..]);
    if u32::from_be_bytes(word) != crc32fast::hash(&blob[4..checksum_start]) {
        return exec_err!("Invalid Iceberg deletion vector checksum");
    }
    let mut cursor = Cursor::new(&blob[8..checksum_start]);
    let mut count = [0; 8];
    cursor.read_exact(&mut count)?;
    let count = u64::from_le_bytes(count);
    if count > (blob.len() / 12) as u64 {
        return exec_err!("Invalid Iceberg deletion vector bitmap count");
    }
    let mut bitmaps = Vec::new();
    let mut previous = None;
    for _ in 0..count {
        cursor.read_exact(&mut word)?;
        let key = u32::from_le_bytes(word);
        if key > i32::MAX as u32 || previous.is_some_and(|value| key <= value) {
            return exec_err!("Invalid Iceberg deletion vector bitmap key");
        }
        previous = Some(key);
        bitmaps.push((key, RoaringBitmap::deserialize_from(&mut cursor)?));
    }
    if cursor.position() != (checksum_start - 8) as u64 {
        return exec_err!("Trailing bytes in Iceberg deletion vector");
    }
    let positions = RoaringTreemap::from_bitmaps(bitmaps);
    if positions.len() != cardinality {
        return exec_err!("Iceberg deletion vector cardinality does not match manifest");
    }
    Ok(positions)
}

pub(crate) async fn read_deletion_vector(
    store_ctx: &StoreContext,
    file: &DataFile,
) -> Result<RoaringTreemap> {
    file.validate_deletion_vector()
        .map_err(DataFusionError::Execution)?;
    let (Some(offset), Some(size)) = (file.content_offset, file.content_size_in_bytes) else {
        return exec_err!("Iceberg deletion vector is missing its content range");
    };
    let (store, path) = store_ctx.resolve(&file.file_path)?;
    let blob = store
        .get_range(&path, offset as u64..(offset + size) as u64)
        .await?;
    decode_blob(&blob, file.record_count)
}

/// One blob per file keeps publication and cleanup independent across writers.
pub(crate) async fn write_deletion_vector(
    store_ctx: &StoreContext,
    data_url: &Url,
    target: &DataFile,
    positions: &RoaringTreemap,
) -> Result<DataFile> {
    let blob = encode_blob(positions)?;
    let size = i64::try_from(blob.len()).map_err(|_| {
        datafusion_common::exec_datafusion_err!("Iceberg deletion vector is too large")
    })?;
    let footer = serde_json::to_vec(&serde_json::json!({
        "blobs": [{
            "type": "deletion-vector-v1",
            "fields": [2147483645],
            "snapshot-id": -1,
            "sequence-number": -1,
            "offset": 4,
            "length": size,
            "properties": {
                "referenced-data-file": target.file_path,
                "cardinality": positions.len().to_string(),
            },
        }],
    }))
    .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let footer_size = i32::try_from(footer.len()).map_err(|_| {
        datafusion_common::exec_datafusion_err!("Iceberg Puffin footer is too large")
    })?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(PUFFIN_MAGIC);
    bytes.extend_from_slice(&blob);
    bytes.extend_from_slice(PUFFIN_MAGIC);
    bytes.extend_from_slice(&footer);
    bytes.extend_from_slice(&footer_size.to_le_bytes());
    bytes.extend_from_slice(&[0; 4]);
    bytes.extend_from_slice(PUFFIN_MAGIC);
    let file_size_in_bytes = bytes.len() as u64;
    let name = format!("deletion-vector-{}.puffin", uuid::Uuid::new_v4());
    store_ctx
        .prefixed
        .put(
            &object_store::path::Path::from(name.as_str()),
            Bytes::from(bytes).into(),
        )
        .await?;
    let file_path = data_url
        .join(&name)
        .map_err(|error| DataFusionError::External(Box::new(error)))?
        .to_string();
    Ok(DataFile {
        content: DataContentType::PositionDeletes,
        file_path,
        file_format: DataFileFormat::Puffin,
        partition: target.partition.clone(),
        partition_spec_id: target.partition_spec_id,
        record_count: positions.len(),
        file_size_in_bytes,
        referenced_data_file: Some(target.file_path.clone()),
        content_offset: Some(4),
        content_size_in_bytes: Some(size),
        column_sizes: Default::default(),
        value_counts: Default::default(),
        null_value_counts: Default::default(),
        nan_value_counts: Default::default(),
        lower_bounds: Default::default(),
        upper_bounds: Default::default(),
        block_size_in_bytes: None,
        key_metadata: None,
        split_offsets: vec![],
        equality_ids: vec![],
        sort_order_id: None,
        first_row_id: None,
    })
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn portable_vector_round_trip_and_corruption() {
        let positions =
            RoaringTreemap::from_iter([0, 1, 65536, u32::MAX as u64, 1 << 32, i64::MAX as u64]);
        let blob = encode_blob(&positions).unwrap();
        assert_eq!(decode_blob(&blob, positions.len()).unwrap(), positions);
        assert!(decode_blob(&blob, positions.len() + 1).is_err());
        for length in 0..blob.len() {
            assert!(decode_blob(&blob[..length], positions.len()).is_err());
        }
        for offset in [0, 4, 8, 16, blob.len() - 1] {
            let mut corrupt = blob.clone();
            corrupt[offset] ^= 1;
            assert!(decode_blob(&corrupt, positions.len()).is_err());
        }
        assert!(encode_blob(&RoaringTreemap::from_iter([1 << 63])).is_err());
    }

    #[test]
    fn rejects_invalid_keys_and_trailing_bytes_even_with_valid_checksum() {
        let positions = RoaringTreemap::from_iter([1, 1 << 32]);
        let blob = encode_blob(&positions).unwrap();
        let mut negative = blob.clone();
        negative[19] = 0x80;
        let mut duplicate = blob.clone();
        let second_key = 20 + RoaringBitmap::from_iter([1]).serialized_size();
        duplicate[second_key..second_key + 4].copy_from_slice(&0u32.to_le_bytes());
        let mut trailing = blob.clone();
        trailing.insert(trailing.len() - 4, 0);
        let length = trailing.len() as u32 - 8;
        trailing[..4].copy_from_slice(&length.to_be_bytes());
        for mut corrupt in [negative, duplicate, trailing] {
            let checksum = corrupt.len() - 4;
            let crc = crc32fast::hash(&corrupt[4..checksum]);
            corrupt[checksum..].copy_from_slice(&crc.to_be_bytes());
            assert!(decode_blob(&corrupt, positions.len()).is_err());
        }
    }
}
