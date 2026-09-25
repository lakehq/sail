use std::io::{Cursor, Read};

use bytes::Bytes;
use datafusion_common::{DataFusionError, Result, exec_err};
use object_store::ObjectStoreExt;
use roaring::{RoaringBitmap, RoaringTreemap};
use url::Url;

use super::StoreContext;
use crate::spec::{DataContentType, DataFile, DataFileFormat, Literal};

const PUFFIN_MAGIC: &[u8; 4] = b"PFA1";
const VECTOR_MAGIC: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];

fn encode_blob(positions: &mut RoaringTreemap) -> Result<Vec<u8>> {
    if positions
        .max()
        .is_some_and(|position| position > i64::MAX as u64)
    {
        return exec_err!("Iceberg deletion vector positions must fit in a non-negative long");
    }
    positions.optimize();
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
    file_path: &str,
    range: std::ops::Range<u64>,
    cardinality: u64,
) -> Result<RoaringTreemap> {
    if range.start < 4 || range.start >= range.end || range.end > i64::MAX as u64 {
        return exec_err!("Invalid Iceberg deletion vector range");
    }
    let (store, path) = store_ctx.resolve(file_path)?;
    let blob = store.get_range(&path, range).await?;
    decode_blob(&blob, cardinality)
}

pub(crate) struct DeletionVector {
    pub referenced_data_file: String,
    pub partition_spec_id: i32,
    pub partition: Vec<Option<Literal>>,
    pub positions: RoaringTreemap,
}

pub(crate) const TARGET_PUFFIN_SIZE: usize = 64 * 1024 * 1024;

pub(crate) async fn write_deletion_vectors(
    store_ctx: &StoreContext,
    data_url: &Url,
    vectors: Vec<DeletionVector>,
    target_size: usize,
) -> Result<Vec<DataFile>> {
    let mut bytes = PUFFIN_MAGIC.to_vec();
    let mut blobs = Vec::new();
    let mut entries = Vec::new();
    let mut files = Vec::new();
    let mut footer_size = 0;
    for mut vector in vectors {
        let blob = encode_blob(&mut vector.positions)?;
        let size = blob.len() as i64;
        let mut descriptor = serde_json::json!({
            "type": "deletion-vector-v1",
            "fields": [2147483645],
            "snapshot-id": -1,
            "sequence-number": -1,
            "offset": bytes.len(),
            "length": size,
            "properties": {
                "referenced-data-file": vector.referenced_data_file,
                "cardinality": vector.positions.len().to_string(),
            },
        });
        let descriptor_size = serde_json::to_vec(&descriptor)
            .map_err(|error| DataFusionError::External(Box::new(error)))?
            .len()
            + 1;
        if !entries.is_empty()
            && bytes.len() + blob.len() + footer_size + descriptor_size + 32 > target_size
        {
            files.extend(publish_puffin(store_ctx, data_url, bytes, blobs, entries).await?);
            bytes = PUFFIN_MAGIC.to_vec();
            blobs = Vec::new();
            entries = Vec::new();
            footer_size = 0;
            descriptor["offset"] = serde_json::json!(bytes.len());
        }
        let offset = bytes.len() as i64;
        bytes.extend_from_slice(&blob);
        blobs.push(descriptor);
        footer_size += descriptor_size;
        entries.push(DataFile {
            content: DataContentType::PositionDeletes,
            file_path: String::new(),
            file_format: DataFileFormat::Puffin,
            partition: vector.partition,
            partition_spec_id: vector.partition_spec_id,
            record_count: vector.positions.len(),
            file_size_in_bytes: 0,
            referenced_data_file: Some(vector.referenced_data_file),
            content_offset: Some(offset),
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
        });
    }
    if !entries.is_empty() {
        files.extend(publish_puffin(store_ctx, data_url, bytes, blobs, entries).await?);
    }
    Ok(files)
}

async fn publish_puffin(
    store_ctx: &StoreContext,
    data_url: &Url,
    mut bytes: Vec<u8>,
    blobs: Vec<serde_json::Value>,
    mut entries: Vec<DataFile>,
) -> Result<Vec<DataFile>> {
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
    let file_size_in_bytes = bytes.len() as u64;
    let name = format!("deletion-vector-{}.puffin", uuid::Uuid::new_v4());
    let file_path = data_url
        .join(&name)
        .map_err(|error| DataFusionError::External(Box::new(error)))?
        .to_string();
    store_ctx
        .prefixed
        .put(
            &object_store::path::Path::from(name.as_str()),
            Bytes::from(bytes).into(),
        )
        .await?;
    for entry in &mut entries {
        entry.file_path = file_path.clone();
        entry.file_size_in_bytes = file_size_in_bytes;
    }
    Ok(entries)
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::spec::PrimitiveLiteral;

    #[test]
    fn shared_puffin_preserves_each_blob_and_can_roll_between_blobs() -> Result<()> {
        futures::executor::block_on(async {
            let url = Url::parse("file:///table/data/").unwrap();
            let store = StoreContext::new(
                std::sync::Arc::new(object_store::memory::InMemory::new()),
                &url,
            )?;
            for target_size in [TARGET_PUFFIN_SIZE, 1] {
                let vectors = (0..2)
                    .map(|id| DeletionVector {
                        referenced_data_file: format!("file:///table/data/source-{id}.parquet"),
                        partition_spec_id: id,
                        partition: vec![Some(Literal::Primitive(PrimitiveLiteral::Int(id)))],
                        positions: RoaringTreemap::from_iter([id as u64, 65536 + id as u64]),
                    })
                    .collect();
                let files = write_deletion_vectors(&store, &url, vectors, target_size).await?;
                assert_eq!(files.len(), 2);
                assert_eq!(files[0].file_path == files[1].file_path, target_size != 1);
                for (id, file) in files.iter().enumerate() {
                    assert_eq!(file.partition_spec_id, id as i32);
                    assert_eq!(
                        file.partition,
                        vec![Some(Literal::Primitive(PrimitiveLiteral::Int(id as i32)))]
                    );
                    let descriptor = crate::spec::delete_index::PositionDeleteFile::try_from(file)?;
                    let crate::spec::delete_index::PositionDeleteFile::DeletionVector {
                        range,
                        cardinality,
                        ..
                    } = descriptor
                    else {
                        return exec_err!("Expected a deletion vector descriptor");
                    };
                    let positions =
                        read_deletion_vector(&store, &file.file_path, range.clone(), cardinality)
                            .await?;
                    assert_eq!(
                        positions,
                        RoaringTreemap::from_iter([id as u64, 65536 + id as u64])
                    );
                    let (object_store, path) = store.resolve(&file.file_path)?;
                    let bytes = object_store.get(&path).await?.bytes().await?;
                    assert_eq!(bytes.len() as u64, file.file_size_in_bytes);
                    let footer_size = u32::from_le_bytes(
                        bytes[bytes.len() - 12..bytes.len() - 8].try_into().unwrap(),
                    ) as usize;
                    let footer: serde_json::Value = serde_json::from_slice(
                        &bytes[bytes.len() - 12 - footer_size..bytes.len() - 12],
                    )
                    .unwrap();
                    let blobs = footer["blobs"].as_array().unwrap();
                    let blob = blobs
                        .iter()
                        .find(|blob| blob["offset"].as_u64() == Some(range.start))
                        .unwrap();
                    assert_eq!(
                        blob["properties"]["referenced-data-file"].as_str(),
                        file.referenced_data_file.as_deref()
                    );
                    assert_eq!(blob["length"].as_u64(), Some(range.end - range.start));
                    assert_eq!(blobs.len(), if target_size == 1 { 1 } else { 2 });
                }
            }
            assert!(
                write_deletion_vectors(&store, &url, vec![], TARGET_PUFFIN_SIZE)
                    .await?
                    .is_empty()
            );
            Ok(())
        })
    }

    #[test]
    fn portable_vector_round_trip_and_corruption() {
        let mut positions =
            RoaringTreemap::from_iter([0, 1, 65536, u32::MAX as u64, 1 << 32, i64::MAX as u64]);
        let blob = encode_blob(&mut positions).unwrap();
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
        assert!(encode_blob(&mut RoaringTreemap::from_iter([1 << 63])).is_err());
    }

    #[test]
    fn portable_run_container_encoding() {
        let mut positions = RoaringTreemap::from_iter(4000..14000);
        let blob = encode_blob(&mut positions).unwrap();
        assert_eq!(
            blob,
            [
                0x00, 0x00, 0x00, 0x1f, 0xd1, 0xd3, 0x39, 0x64, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3b, 0x30, 0x00, 0x00, 0x01, 0x00, 0x00, 0x0f,
                0x27, 0x01, 0x00, 0xa0, 0x0f, 0x0f, 0x27, 0x2f, 0x17, 0xa8, 0x37,
            ]
        );
        assert_eq!(decode_blob(&blob, 10000).unwrap(), positions);
    }

    #[test]
    fn rejects_invalid_keys_and_trailing_bytes_even_with_valid_checksum() {
        let mut positions = RoaringTreemap::from_iter([1, 1 << 32]);
        let blob = encode_blob(&mut positions).unwrap();
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
