//! Deletion Vector file storage: read and write DV files.
//!
//! DV file format:
//! ```text
//! [version: 1 byte]   // always 1
//! For each DV in the file:
//!   [dataSize: 4 bytes BE]   // size of bitmap data (without checksum)
//!   [bitmapData: dataSize bytes]  // serialized RoaringBitmapArray (portable format)
//!                               //   internally: [magic: 4 bytes LE][roaring data]
//!   [checksum: 4 bytes BE]  // CRC-32 of bitmapData
//! ```

use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use url::Url;

use super::bitmap::DeletionVectorBitmap;
use super::resolve::{new_uuid_dv_path, resolve_dv_absolute_path};
use crate::spec::{DeletionVectorDescriptor, DeltaError, DeltaResult, StorageType};

/// Version byte for the DV file format.
const DV_FILE_FORMAT_VERSION: u8 = 1;

/// Read a deletion vector from storage according to a [`DeletionVectorDescriptor`].
///
/// For UUID and absolute path storage types, reads the file, validates the checksum,
/// and deserializes the bitmap. For inline storage, decodes the Z85-encoded data
/// from the descriptor.
pub async fn read_deletion_vector(
    store: &dyn ObjectStore,
    table_root: &Url,
    dv: &DeletionVectorDescriptor,
) -> DeltaResult<DeletionVectorBitmap> {
    match dv.storage_type {
        StorageType::Inline => {
            // Inline DVs are Z85-encoded bitmap data
            let decoded = super::z85::z85_decode(&dv.path_or_inline_dv)?;
            DeletionVectorBitmap::deserialize(&decoded)
        }
        StorageType::UuidRelativePath | StorageType::AbsolutePath => {
            let dv_url = resolve_dv_absolute_path(table_root, dv)?
                .ok_or_else(|| DeltaError::generic("expected file-based DV but got None"))?;

            // Use the full path from the URL so the object store can find the file.
            let path = Path::from(dv_url.path());

            let data = store
                .get(&path)
                .await
                .map_err(|e| DeltaError::generic(format!("failed to read DV file: {e}")))?
                .bytes()
                .await
                .map_err(|e| DeltaError::generic(format!("failed to read DV bytes: {e}")))?;

            read_dv_from_file_bytes(&data, dv.offset, dv.size_in_bytes)
        }
    }
}

/// Read a single DV entry from a DV file's raw bytes.
fn read_dv_from_file_bytes(
    data: &[u8],
    offset: Option<i32>,
    size_in_bytes: i32,
) -> DeltaResult<DeletionVectorBitmap> {
    if data.is_empty() {
        return Err(DeltaError::generic("DV file is empty"));
    }
    // First byte is format version
    if data[0] != DV_FILE_FORMAT_VERSION {
        return Err(DeltaError::generic(format!(
            "unsupported DV file format version: {}, expected {DV_FILE_FORMAT_VERSION}",
            data[0]
        )));
    }

    if size_in_bytes < 0 {
        return Err(DeltaError::generic(format!(
            "invalid DV descriptor: size_in_bytes must be non-negative, got {size_in_bytes}"
        )));
    }
    let size = size_in_bytes as usize;

    let raw_offset = offset.unwrap_or(1);
    if raw_offset < 0 {
        return Err(DeltaError::generic(format!(
            "invalid DV descriptor: offset must be non-negative, got {raw_offset}"
        )));
    }
    let start = raw_offset as usize;

    // Each entry: [dataSize(4)] [bitmapData(dataSize)] [checksum(4)]
    let header_end = start
        .checked_add(4)
        .ok_or_else(|| DeltaError::generic("DV descriptor offset overflows address space"))?;
    if header_end > data.len() {
        return Err(DeltaError::generic(
            "DV file too short to read data size header",
        ));
    }
    // DataSize and checksum in the DV file are big-endian.
    let data_size = u32::from_be_bytes([
        data[start],
        data[start + 1],
        data[start + 2],
        data[start + 3],
    ]) as usize;

    let bitmap_start = header_end;
    let bitmap_end = bitmap_start
        .checked_add(data_size)
        .ok_or_else(|| DeltaError::generic("DV file data_size overflows address space"))?;
    let checksum_end = bitmap_end
        .checked_add(4)
        .ok_or_else(|| DeltaError::generic("DV file checksum range overflows address space"))?;
    if checksum_end > data.len() {
        return Err(DeltaError::generic(
            "DV file too short to contain bitmap and checksum",
        ));
    }

    let bitmap_data = &data[bitmap_start..bitmap_end];

    // Validate checksum (big-endian per Delta protocol spec)
    let stored_checksum = u32::from_be_bytes([
        data[bitmap_end],
        data[bitmap_end + 1],
        data[bitmap_end + 2],
        data[bitmap_end + 3],
    ]);
    let computed_checksum = crc32fast::hash(bitmap_data);
    if stored_checksum != computed_checksum {
        return Err(DeltaError::generic(format!(
            "DV file checksum mismatch: stored={stored_checksum:#x}, computed={computed_checksum:#x}"
        )));
    }

    // The size_in_bytes field in the descriptor refers to the raw bitmap data size
    if data_size != size {
        log::warn!(
            "DV data_size ({data_size}) differs from descriptor size_in_bytes ({size}); using file header"
        );
    }

    DeletionVectorBitmap::deserialize(bitmap_data)
}

/// Writer for deletion vector files.
pub struct DeletionVectorWriter {
    store: Arc<dyn ObjectStore>,
    table_root: Url,
}

impl DeletionVectorWriter {
    pub fn new(store: Arc<dyn ObjectStore>, table_root: Url) -> Self {
        Self { store, table_root }
    }

    /// Write a deletion vector bitmap to a new file and return the descriptor.
    ///
    /// Uses UUID-based relative path storage.
    pub async fn write(
        &self,
        bitmap: &DeletionVectorBitmap,
    ) -> DeltaResult<DeletionVectorDescriptor> {
        let bitmap_data = bitmap.serialize()?;
        let size_in_bytes = bitmap_data.len() as i32;

        // Build the DV file content
        let checksum = crc32fast::hash(&bitmap_data);
        let data_size = bitmap_data.len() as u32;

        // DV file wrapper uses big-endian for dataSize and checksum per Delta protocol spec.
        let mut file_content = Vec::with_capacity(1 + 4 + bitmap_data.len() + 4);
        file_content.push(DV_FILE_FORMAT_VERSION);
        file_content.extend_from_slice(&data_size.to_be_bytes());
        file_content.extend_from_slice(&bitmap_data);
        file_content.extend_from_slice(&checksum.to_be_bytes());

        let (encoded_uuid, relative_path, _uuid) = new_uuid_dv_path()?;
        // The relative_path is relative to the table root (e.g., "70/deletion_vector_<uuid>.bin").
        // We must prefix it with the table root path for the object store.
        let table_root_path = Path::from(self.table_root.path());
        let full_path = Path::from(format!(
            "{}{}{}",
            table_root_path,
            object_store::path::DELIMITER,
            relative_path
        ));
        let payload = PutPayload::from(Bytes::from(file_content));
        self.store
            .put(&full_path, payload)
            .await
            .map_err(|e| DeltaError::generic(format!("failed to write DV file: {e}")))?;

        Ok(DeletionVectorDescriptor {
            storage_type: StorageType::UuidRelativePath,
            path_or_inline_dv: encoded_uuid,
            offset: Some(1), // after the version byte
            size_in_bytes,
            cardinality: bitmap.len() as i64,
        })
    }
}

pub async fn write_deletion_vector(
    store: Arc<dyn ObjectStore>,
    table_root: &Url,
    bitmap: &DeletionVectorBitmap,
) -> DeltaResult<DeletionVectorDescriptor> {
    let writer = DeletionVectorWriter::new(store, table_root.clone());
    writer.write(bitmap).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_dv_file_roundtrip() {
        let mut bm = DeletionVectorBitmap::new();
        bm.insert(3);
        bm.insert(7);
        bm.insert(42);

        let bitmap_data = bm.serialize().unwrap();
        let checksum = crc32fast::hash(&bitmap_data);
        let data_size = bitmap_data.len() as u32;

        let mut file_content = Vec::new();
        file_content.push(DV_FILE_FORMAT_VERSION);
        file_content.extend_from_slice(&data_size.to_be_bytes());
        file_content.extend_from_slice(&bitmap_data);
        file_content.extend_from_slice(&checksum.to_be_bytes());

        let bm2 = read_dv_from_file_bytes(&file_content, None, bitmap_data.len() as i32).unwrap();
        assert_eq!(bm2.len(), 3);
        assert!(bm2.contains(3));
        assert!(bm2.contains(7));
        assert!(bm2.contains(42));
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_dv_file_with_offset() {
        let mut bm = DeletionVectorBitmap::new();
        bm.insert(10);
        bm.insert(20);

        let bitmap_data = bm.serialize().unwrap();
        let checksum = crc32fast::hash(&bitmap_data);
        let data_size = bitmap_data.len() as u32;

        // Build file with version byte and one entry
        let mut file_content = Vec::new();
        file_content.push(DV_FILE_FORMAT_VERSION);
        let entry_offset = file_content.len();
        file_content.extend_from_slice(&data_size.to_be_bytes());
        file_content.extend_from_slice(&bitmap_data);
        file_content.extend_from_slice(&checksum.to_be_bytes());

        let bm2 = read_dv_from_file_bytes(
            &file_content,
            Some(entry_offset as i32),
            bitmap_data.len() as i32,
        )
        .unwrap();
        assert_eq!(bm2.len(), 2);
        assert!(bm2.contains(10));
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_checksum_mismatch() {
        let mut bm = DeletionVectorBitmap::new();
        bm.insert(1);
        let bitmap_data = bm.serialize().unwrap();
        let data_size = bitmap_data.len() as u32;

        let mut file_content = Vec::new();
        file_content.push(DV_FILE_FORMAT_VERSION);
        file_content.extend_from_slice(&data_size.to_be_bytes());
        file_content.extend_from_slice(&bitmap_data);
        file_content.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]); // wrong checksum (BE)

        let result = read_dv_from_file_bytes(&file_content, None, bitmap_data.len() as i32);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("checksum"));
    }
}
