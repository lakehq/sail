//! Path resolution for Deletion Vector descriptors.

use url::Url;
use uuid::Uuid;

use super::z85;
use crate::spec::{DeletionVectorDescriptor, DeltaError, DeltaResult, StorageType};

/// Resolve a [`DeletionVectorDescriptor`] to an absolute path relative to the given table root.
///
/// For UUID-based storage (`u`), `pathOrInlineDv` has the form:
///   `<random prefix (optional)><base85-encoded UUID (exactly 20 chars)>`
/// The file path is then:
///   `<table_root>/<random prefix>/deletion_vector_<uuid>.bin`
/// where the prefix is empty, or a short string (e.g. `"d2"`).
///
/// For absolute paths (`p`), the path is returned as-is.
///
/// For inline DVs (`i`), this function returns `None` since the data is embedded
/// in the descriptor itself.
pub fn resolve_dv_absolute_path(
    table_root: &Url,
    dv: &DeletionVectorDescriptor,
) -> DeltaResult<Option<Url>> {
    match dv.storage_type {
        StorageType::UuidRelativePath => {
            // Per the Delta protocol, pathOrInlineDv is <prefix><20-char Z85 UUID>.
            // The UUID is always exactly 20 base85 characters (16 bytes * 5/4).
            // Everything before those 20 chars is the random directory prefix.
            let path_field = &dv.path_or_inline_dv;
            if path_field.len() < 20 {
                return Err(DeltaError::generic(format!(
                    "UuidRelativePath has too few characters: expected ≥20, got {}",
                    path_field.len()
                )));
            }
            let prefix = &path_field[..path_field.len() - 20];
            let uuid_encoded = &path_field[path_field.len() - 20..];
            let decoded = decode_z85_uuid(uuid_encoded)?;
            let uuid = Uuid::from_bytes(decoded);
            let relative_path = if prefix.is_empty() {
                format!("deletion_vector_{uuid}.bin")
            } else {
                format!("{prefix}/deletion_vector_{uuid}.bin")
            };
            let path = table_root
                .join(&relative_path)
                .map_err(|e| DeltaError::generic(format!("failed to resolve DV path: {e}")))?;
            Ok(Some(path))
        }
        StorageType::AbsolutePath => {
            let path = Url::parse(&dv.path_or_inline_dv)
                .map_err(|e| DeltaError::generic(format!("invalid absolute DV path: {e}")))?;
            Ok(Some(path))
        }
        StorageType::Inline => Ok(None),
    }
}

/// Compute the storage fields for a new UUID-based deletion vector file.
///
/// Returns `(path_or_inline_dv, relative_path, uuid)` where:
/// - `path_or_inline_dv` is `<2-char prefix><Z85-encoded UUID>` (22 chars) to store in the
///   descriptor; the prefix is the first 2 hex characters of the UUID string representation.
/// - `relative_path` is `<prefix>/deletion_vector_<uuid>.bin`, the path relative to the
///   table root where the DV file will be written.
pub fn new_uuid_dv_path() -> DeltaResult<(String, String, Uuid)> {
    let uuid = Uuid::new_v4();
    let encoded = encode_z85_uuid(&uuid)?;
    let uuid_str = uuid.to_string();
    // Use the first 2 hex chars of the UUID as the random directory prefix.
    // This embeds the prefix directly in path_or_inline_dv per the Delta protocol spec.
    let prefix = &uuid_str[..2];
    let path_or_inline_dv = format!("{prefix}{encoded}");
    let relative_path = format!("{prefix}/deletion_vector_{uuid_str}.bin");
    Ok((path_or_inline_dv, relative_path, uuid))
}

// ── Z85 (base-85) encoding for UUIDs ─────────────────────────────────────────

fn encode_z85_uuid(uuid: &Uuid) -> DeltaResult<String> {
    let bytes = uuid.as_bytes();
    z85::z85_encode(bytes)
}

fn decode_z85_uuid(encoded: &str) -> DeltaResult<[u8; 16]> {
    let bytes = z85::z85_decode(encoded)?;
    if bytes.len() != 16 {
        return Err(DeltaError::generic(format!(
            "Z85-decoded UUID has wrong length: expected 16, got {}",
            bytes.len()
        )));
    }
    let mut result = [0u8; 16];
    result.copy_from_slice(&bytes);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_z85_roundtrip() {
        let uuid = Uuid::new_v4();
        let encoded = encode_z85_uuid(&uuid).unwrap();
        let decoded = decode_z85_uuid(&encoded).unwrap();
        assert_eq!(uuid.as_bytes(), &decoded);
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_z85_known_value() {
        // Spec example: pathOrInlineDv = "ab^-aqEH.-t@S}K{vb[*k^"
        // This is <2-char prefix "ab"><20-char Z85 UUID>.
        let path_or_inline_dv = "ab^-aqEH.-t@S}K{vb[*k^";
        // Total is 22 chars: 2-char prefix + 20-char UUID
        assert_eq!(path_or_inline_dv.len(), 22);
        // The UUID part is the last 20 chars
        let uuid_encoded = &path_or_inline_dv[path_or_inline_dv.len() - 20..];
        assert_eq!(uuid_encoded.len(), 20);
        let decoded = z85::z85_decode(uuid_encoded);
        assert!(decoded.is_ok());
        let bytes = decoded.unwrap();
        assert_eq!(bytes.len(), 16);
        // Prefix is everything before the last 20 chars
        let prefix = &path_or_inline_dv[..path_or_inline_dv.len() - 20];
        assert_eq!(prefix, "ab");
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_resolve_uuid_path() {
        let table_root = Url::parse("s3://mybucket/mytable/").unwrap();
        let uuid = Uuid::parse_str("d2c639aa-8816-431a-aaf6-d3fe2512ff61").unwrap();
        // path_or_inline_dv embeds the 2-char prefix before the 20-char Z85 UUID
        let prefix = &uuid.to_string()[..2]; // "d2"
        let path_or_inline_dv = format!("{}{}", prefix, encode_z85_uuid(&uuid).unwrap());
        let dv = DeletionVectorDescriptor {
            storage_type: StorageType::UuidRelativePath,
            path_or_inline_dv,
            offset: Some(4),
            size_in_bytes: 40,
            cardinality: 6,
        };
        let path = resolve_dv_absolute_path(&table_root, &dv).unwrap().unwrap();
        assert!(path.as_str().contains("deletion_vector_d2c639aa"));
        assert!(path.as_str().contains("/d2/"));
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_resolve_inline_returns_none() {
        let table_root = Url::parse("s3://mybucket/mytable/").unwrap();
        let dv = DeletionVectorDescriptor {
            storage_type: StorageType::Inline,
            path_or_inline_dv: "some_inline_data".to_string(),
            offset: None,
            size_in_bytes: 10,
            cardinality: 2,
        };
        let result = resolve_dv_absolute_path(&table_root, &dv).unwrap();
        assert!(result.is_none());
    }
}
