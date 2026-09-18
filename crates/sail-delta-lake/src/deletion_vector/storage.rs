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

            let offset = u64::try_from(dv.offset.unwrap_or(1))
                .map_err(|_| DeltaError::generic("DV offset must be non-negative"))?;
            let size = u64::try_from(dv.size_in_bytes)
                .map_err(|_| DeltaError::generic("DV size must be non-negative"))?;
            let (version, entry) = if offset == 1 {
                let bytes = read_dv_range(store, &path, offset, size, true).await?;
                (bytes.slice(..1), bytes.slice(1..))
            } else {
                futures::try_join!(
                    async { Ok::<_, DeltaError>(store.get_range(&path, 0..1).await?) },
                    read_dv_range(store, &path, offset, size, false),
                )?
            };
            validate_dv_version(&version)?;
            read_dv_entry(&entry, dv.size_in_bytes)
        }
    }
}

async fn read_dv_range(
    store: &dyn ObjectStore,
    path: &Path,
    offset: u64,
    descriptor_size: u64,
    include_version: bool,
) -> DeltaResult<Bytes> {
    let start = if include_version { 0 } else { offset };
    let header = (offset - start) as usize;
    let bytes = match store
        .get_range(path, start..offset + descriptor_size + 8)
        .await
    {
        Ok(bytes) => bytes,
        Err(error) => {
            // An oversized descriptor can extend beyond EOF. The existing reader
            // accepts a different size in the entry header, so retry that range.
            let prefix = store.get_range(path, start..offset + 4).await?;
            let actual = dv_entry_size(prefix.get(header..).unwrap_or_default())?;
            if actual == descriptor_size {
                return Err(error.into());
            }
            store.get_range(path, start..offset + actual + 8).await?
        }
    };
    let actual = dv_entry_size(bytes.get(header..).unwrap_or_default())?;
    if actual > descriptor_size {
        Ok(store.get_range(path, start..offset + actual + 8).await?)
    } else {
        Ok(bytes)
    }
}

fn dv_entry_size(entry: &[u8]) -> DeltaResult<u64> {
    let header: [u8; 4] = entry
        .get(..4)
        .ok_or_else(|| DeltaError::generic("DV file too short to read data size header"))?
        .try_into()
        .map_err(|_| DeltaError::generic("invalid DV size header"))?;
    Ok(u32::from_be_bytes(header) as u64)
}

fn validate_dv_version(data: &[u8]) -> DeltaResult<()> {
    match data.first() {
        Some(&DV_FILE_FORMAT_VERSION) => Ok(()),
        Some(version) => Err(DeltaError::generic(format!(
            "unsupported DV file format version: {version}"
        ))),
        None => Err(DeltaError::generic("DV file is empty")),
    }
}

#[cfg(test)]
fn read_dv_from_file_bytes(
    data: &[u8],
    offset: Option<i32>,
    size_in_bytes: i32,
) -> DeltaResult<DeletionVectorBitmap> {
    validate_dv_version(data)?;
    let offset = usize::try_from(offset.unwrap_or(1))
        .map_err(|_| DeltaError::generic("DV offset must be non-negative"))?;
    let entry = data
        .get(offset..)
        .ok_or_else(|| DeltaError::generic("DV offset exceeds file length"))?;
    read_dv_entry(entry, size_in_bytes)
}

fn read_dv_entry(data: &[u8], size_in_bytes: i32) -> DeltaResult<DeletionVectorBitmap> {
    let size = usize::try_from(size_in_bytes)
        .map_err(|_| DeltaError::generic("DV size must be non-negative"))?;
    let start = 0;
    let header_end = 4;
    if data.len() < header_end {
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
    use std::fmt;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::{
        CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutResult,
    };

    use super::*;

    #[derive(Debug, Default)]
    struct RangeStore {
        data: InMemory,
        reads: Mutex<Vec<std::ops::Range<u64>>>,
    }

    impl fmt::Display for RangeStore {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "RangeStore")
        }
    }

    #[async_trait]
    #[expect(clippy::unwrap_used)]
    impl ObjectStore for RangeStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.data.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.data.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if let Some(GetRange::Bounded(range)) = &options.range {
                self.reads.lock().unwrap().push(range.clone());
            } else {
                return Err(object_store::Error::NotSupported {
                    source: "DV read must request a bounded range".into(),
                });
            }
            self.data.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.data.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.data.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.data.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.data.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    #[expect(clippy::unwrap_used)]
    async fn dv_reads_only_the_requested_segment_and_checks_crc() {
        let store = RangeStore::default();
        let bitmap = DeletionVectorBitmap::from_row_indices([3, 8, 4096]);
        let payload = bitmap.serialize().unwrap();
        let offset = 2 * 1024 * 1024;
        let mut bytes = vec![0; offset];
        bytes[0] = DV_FILE_FORMAT_VERSION;
        bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&payload);
        bytes.extend_from_slice(&crc32fast::hash(&payload).to_be_bytes());
        let entry_end = bytes.len();
        bytes.resize(entry_end + 2 * 1024 * 1024, 0);
        let path = Path::from("table/vectors.bin");
        store.put(&path, bytes.clone().into()).await.unwrap();
        let root = Url::parse("memory:///table/").unwrap();
        let descriptor = DeletionVectorDescriptor {
            storage_type: StorageType::AbsolutePath,
            path_or_inline_dv: "memory:///table/vectors.bin".into(),
            offset: Some(offset as i32),
            size_in_bytes: payload.len() as i32,
            cardinality: 3,
        };
        let result = read_deletion_vector(&store, &root, &descriptor)
            .await
            .unwrap();
        assert_eq!(result.inner(), bitmap.inner());
        let mut ranges = store.reads.lock().unwrap().clone();
        ranges.sort_by_key(|range| range.start);
        assert_eq!(ranges, vec![0..1, offset as u64..entry_end as u64]);
        bytes[entry_end - 1] ^= 1;
        store.put(&path, bytes.into()).await.unwrap();
        assert!(
            read_deletion_vector(&store, &root, &descriptor)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    #[expect(clippy::unwrap_used)]
    async fn range_reads_preserve_header_size_tolerance() {
        let store = Arc::new(RangeStore::default());
        let root = Url::parse("memory:///table/").unwrap();
        let bitmap = DeletionVectorBitmap::from_row_indices([1, 5]);
        let writer = DeletionVectorWriter::new(store.clone(), root.clone());
        let descriptor = writer.write(&bitmap).await.unwrap();
        for size in [
            descriptor.size_in_bytes - 4,
            descriptor.size_in_bytes + 4096,
        ] {
            let descriptor = DeletionVectorDescriptor {
                size_in_bytes: size,
                ..descriptor.clone()
            };
            let result = read_deletion_vector(store.as_ref(), &root, &descriptor)
                .await
                .unwrap();
            assert_eq!(result.inner(), bitmap.inner());
        }
    }

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
