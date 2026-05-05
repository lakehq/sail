//! Roaring bitmap wrapper for Deletion Vectors.

use roaring::RoaringTreemap;

use crate::spec::{DeltaError, DeltaResult};

/// Magic number for the portable RoaringBitmapArray format used by Delta.
const PORTABLE_ROARING_BITMAP_MAGIC: u32 = 1681511377;

/// Wrapper around a 64-bit roaring bitmap (treemap) for deletion vectors.
#[derive(Debug, Clone)]
pub struct DeletionVectorBitmap {
    inner: RoaringTreemap,
}

impl DeletionVectorBitmap {
    /// Create an empty bitmap.
    pub fn new() -> Self {
        Self {
            inner: RoaringTreemap::new(),
        }
    }

    /// Create from an existing treemap.
    pub fn from_treemap(treemap: RoaringTreemap) -> Self {
        Self { inner: treemap }
    }

    /// Create from an iterator of row indices.
    pub fn from_row_indices(iter: impl IntoIterator<Item = u64>) -> Self {
        let mut inner = RoaringTreemap::new();
        for idx in iter {
            inner.insert(idx);
        }
        Self { inner }
    }

    /// Add a row index to the bitmap.
    pub fn insert(&mut self, row_idx: u64) -> bool {
        self.inner.insert(row_idx)
    }

    /// Check if a row index is present.
    pub fn contains(&self, row_idx: u64) -> bool {
        self.inner.contains(row_idx)
    }

    /// Number of set bits (deleted rows).
    pub fn len(&self) -> u64 {
        self.inner.len()
    }

    /// Whether the bitmap is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Get the underlying treemap reference.
    pub fn inner(&self) -> &RoaringTreemap {
        &self.inner
    }

    /// Serialize to the Delta portable format.
    ///
    /// Format: `[magic(4 bytes LE)] [portable roaring treemap serialization]`
    pub fn serialize(&self) -> DeltaResult<Vec<u8>> {
        let serialized_size = self.inner.serialized_size();
        let mut buf = Vec::with_capacity(4 + serialized_size);
        buf.extend_from_slice(&PORTABLE_ROARING_BITMAP_MAGIC.to_le_bytes());
        self.inner.serialize_into(&mut buf).map_err(|e| {
            DeltaError::generic(format!("failed to serialize deletion vector bitmap: {e}"))
        })?;
        Ok(buf)
    }

    /// Deserialize from the Delta portable format.
    ///
    /// Expects: `[magic(4 bytes LE)] [portable roaring treemap serialization]`
    pub fn deserialize(data: &[u8]) -> DeltaResult<Self> {
        if data.len() < 4 {
            return Err(DeltaError::generic(
                "deletion vector bitmap data too short for magic number",
            ));
        }
        let magic = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        if magic != PORTABLE_ROARING_BITMAP_MAGIC {
            return Err(DeltaError::generic(format!(
                "unexpected deletion vector bitmap magic number: {magic}, expected {PORTABLE_ROARING_BITMAP_MAGIC}"
            )));
        }
        let inner = RoaringTreemap::deserialize_from(&data[4..]).map_err(|e| {
            DeltaError::generic(format!("failed to deserialize deletion vector bitmap: {e}"))
        })?;
        Ok(Self { inner })
    }

    /// Merge another bitmap into this one (union).
    pub fn union_with(&mut self, other: &DeletionVectorBitmap) {
        self.inner |= &other.inner;
    }
}

impl Default for DeletionVectorBitmap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_empty_bitmap_roundtrip() {
        let bm = DeletionVectorBitmap::new();
        let data = bm.serialize().unwrap();
        let bm2 = DeletionVectorBitmap::deserialize(&data).unwrap();
        assert!(bm2.is_empty());
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_bitmap_roundtrip_with_values() {
        let mut bm = DeletionVectorBitmap::new();
        bm.insert(3);
        bm.insert(4);
        bm.insert(7);
        bm.insert(11);
        bm.insert(18);
        bm.insert(29);
        assert_eq!(bm.len(), 6);

        let data = bm.serialize().unwrap();
        let bm2 = DeletionVectorBitmap::deserialize(&data).unwrap();
        assert_eq!(bm2.len(), 6);
        assert!(bm2.contains(3));
        assert!(bm2.contains(29));
        assert!(!bm2.contains(5));
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_bitmap_large_indices() {
        let mut bm = DeletionVectorBitmap::new();
        bm.insert(0);
        bm.insert(u32::MAX as u64);
        bm.insert(u32::MAX as u64 + 1);
        bm.insert(u64::MAX - 1);
        assert_eq!(bm.len(), 4);

        let data = bm.serialize().unwrap();
        let bm2 = DeletionVectorBitmap::deserialize(&data).unwrap();
        assert_eq!(bm2.len(), 4);
        assert!(bm2.contains(u64::MAX - 1));
    }

    #[test]
    fn test_invalid_magic() {
        let data = [0u8; 10];
        let result = DeletionVectorBitmap::deserialize(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_union() {
        let mut bm1 = DeletionVectorBitmap::from_row_indices([1, 3, 5]);
        let bm2 = DeletionVectorBitmap::from_row_indices([2, 3, 6]);
        bm1.union_with(&bm2);
        assert_eq!(bm1.len(), 5); // 1, 2, 3, 5, 6
        assert!(bm1.contains(2));
        assert!(bm1.contains(6));
    }
}
