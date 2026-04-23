use std::collections::HashMap;

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use crate::spec::manifest::{DataContentType, DataFile, DataFileFormat};
use crate::spec::types::values::{Literal, PrimitiveLiteral};

/// A single delete file, augmented with the sequence number inherited from its
/// parent manifest entry and the partition-spec-unpartitioned flag resolved from
/// table metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteFileRef {
    /// The underlying delete file (content is `PositionDeletes` or `EqualityDeletes`).
    pub data_file: DataFile,
    /// Data sequence number. Per spec v2, manifest entries inherit this from the
    /// owning `manifest_file.sequence_number` when the entry-level value is null.
    pub data_sequence_number: i64,
    /// The partition spec id that wrote this delete file.
    pub partition_spec_id: i32,
    /// Whether the owning partition spec is unpartitioned (used for global equality
    /// delete routing).
    pub is_unpartitioned_spec: bool,
}

impl DeleteFileRef {
    /// Whether this ref describes a v3 deletion vector (Puffin blob).
    pub fn is_deletion_vector(&self) -> bool {
        self.data_file.content == DataContentType::PositionDeletes
            && self.data_file.file_format == DataFileFormat::Puffin
            && self.data_file.content_offset.is_some()
            && self.data_file.content_size_in_bytes.is_some()
    }
}

/// Canonical binary encoding of a partition tuple for equality-keyed lookups.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionKey {
    pub spec_id: i32,
    pub bytes: Vec<u8>,
}

impl PartitionKey {
    /// Build a [`PartitionKey`] for a delete file or data file's partition values.
    pub fn new(spec_id: i32, values: &[Option<Literal>]) -> Self {
        let mut bytes: Vec<u8> = Vec::with_capacity(values.len() * 8);
        for v in values {
            match v {
                None => bytes.push(0),
                Some(lit) => {
                    bytes.push(1);
                    encode_literal(lit, &mut bytes);
                }
            }
        }
        Self { spec_id, bytes }
    }
}

fn encode_literal(lit: &Literal, out: &mut Vec<u8>) {
    match lit {
        Literal::Primitive(p) => encode_primitive(p, out),
        Literal::Struct(fields) => {
            out.extend_from_slice(&(fields.len() as u32).to_le_bytes());
            for (name, inner) in fields {
                out.extend_from_slice(&(name.len() as u32).to_le_bytes());
                out.extend_from_slice(name.as_bytes());
                match inner {
                    None => out.push(0),
                    Some(l) => {
                        out.push(1);
                        encode_literal(l, out);
                    }
                }
            }
        }
        Literal::List(items) => {
            out.extend_from_slice(&(items.len() as u32).to_le_bytes());
            for item in items {
                match item {
                    None => out.push(0),
                    Some(l) => {
                        out.push(1);
                        encode_literal(l, out);
                    }
                }
            }
        }
        Literal::Map(entries) => {
            out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
            for (k, v) in entries {
                encode_literal(k, out);
                match v {
                    None => out.push(0),
                    Some(l) => {
                        out.push(1);
                        encode_literal(l, out);
                    }
                }
            }
        }
    }
}

fn encode_primitive(p: &PrimitiveLiteral, out: &mut Vec<u8>) {
    match p {
        PrimitiveLiteral::Boolean(b) => {
            out.push(0x01);
            out.push(u8::from(*b));
        }
        PrimitiveLiteral::Int(i) => {
            out.push(0x02);
            out.extend_from_slice(&i.to_le_bytes());
        }
        PrimitiveLiteral::Long(l) => {
            out.push(0x03);
            out.extend_from_slice(&l.to_le_bytes());
        }
        PrimitiveLiteral::Float(OrderedFloat(f)) => {
            out.push(0x04);
            // Normalize +0.0/-0.0 via bit-pattern per Iceberg Scan Planning note 3.
            out.extend_from_slice(&f.to_bits().to_le_bytes());
        }
        PrimitiveLiteral::Double(OrderedFloat(f)) => {
            out.push(0x05);
            out.extend_from_slice(&f.to_bits().to_le_bytes());
        }
        PrimitiveLiteral::Int128(i) => {
            out.push(0x06);
            out.extend_from_slice(&i.to_le_bytes());
        }
        PrimitiveLiteral::UInt128(i) => {
            out.push(0x07);
            out.extend_from_slice(&i.to_le_bytes());
        }
        PrimitiveLiteral::String(s) => {
            out.push(0x08);
            out.extend_from_slice(&(s.len() as u32).to_le_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        PrimitiveLiteral::Binary(b) => {
            out.push(0x09);
            out.extend_from_slice(&(b.len() as u32).to_le_bytes());
            out.extend_from_slice(b);
        }
    }
}

/// A scoped index of delete files, keyed to support the lookup rules
#[derive(Debug, Default)]
pub struct DeleteFileIndex {
    /// Unpartitioned equality delete files apply to every data file.
    global_eq: Vec<DeleteFileRef>,
    /// Partitioned equality delete files keyed by the writer's partition tuple.
    eq_by_partition: HashMap<PartitionKey, Vec<DeleteFileRef>>,
    /// Partition-scoped position delete files (applied to every data file in the same
    /// partition whose sequence number is ≤ the delete's).
    pos_by_partition: HashMap<PartitionKey, Vec<DeleteFileRef>>,
    /// Position delete files targeting a specific data file path (`referenced_data_file`).
    pos_by_path: HashMap<String, Vec<DeleteFileRef>>,
}

/// Result of looking up applicable deletes for a data file.
#[derive(Debug, Default, Clone)]
pub struct MatchedDeletes {
    pub positional: Vec<DeleteFileRef>,
    pub equality: Vec<DeleteFileRef>,
}

impl MatchedDeletes {
    pub fn is_empty(&self) -> bool {
        self.positional.is_empty() && self.equality.is_empty()
    }
}

impl DeleteFileIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether the index contains no delete files.
    pub fn is_empty(&self) -> bool {
        self.global_eq.is_empty()
            && self.eq_by_partition.is_empty()
            && self.pos_by_partition.is_empty()
            && self.pos_by_path.is_empty()
    }

    /// Register a delete file reference into the index. Returns `Err` if the ref is a
    /// deletion vector (v3) — callers handle DV support separately.
    pub fn insert(&mut self, file_ref: DeleteFileRef) -> Result<(), DeleteIndexError> {
        if file_ref.is_deletion_vector() {
            return Err(DeleteIndexError::DeletionVectorUnsupported(
                file_ref.data_file.file_path.clone(),
            ));
        }
        match file_ref.data_file.content {
            DataContentType::Data => Err(DeleteIndexError::NotADeleteFile(
                file_ref.data_file.file_path.clone(),
            )),
            DataContentType::EqualityDeletes => {
                if file_ref.is_unpartitioned_spec {
                    self.global_eq.push(file_ref);
                } else {
                    let key = PartitionKey::new(
                        file_ref.partition_spec_id,
                        &file_ref.data_file.partition,
                    );
                    self.eq_by_partition.entry(key).or_default().push(file_ref);
                }
                Ok(())
            }
            DataContentType::PositionDeletes => {
                if let Some(ref path) = file_ref.data_file.referenced_data_file {
                    self.pos_by_path
                        .entry(path.clone())
                        .or_default()
                        .push(file_ref);
                } else {
                    let key = PartitionKey::new(
                        file_ref.partition_spec_id,
                        &file_ref.data_file.partition,
                    );
                    self.pos_by_partition.entry(key).or_default().push(file_ref);
                }
                Ok(())
            }
        }
    }

    /// Return all delete files applicable to `data_file` at `data_sequence_number`.
    pub fn for_data_file(&self, data_file: &DataFile, data_sequence_number: i64) -> MatchedDeletes {
        let mut matched = MatchedDeletes::default();
        let key = PartitionKey::new(data_file.partition_spec_id, &data_file.partition);

        // Positional deletes that reference this path.
        if let Some(refs) = self.pos_by_path.get(&data_file.file_path) {
            for r in refs {
                if data_sequence_number <= r.data_sequence_number {
                    matched.positional.push(r.clone());
                }
            }
        }

        // Positional deletes scoped to the same partition.
        if let Some(refs) = self.pos_by_partition.get(&key) {
            for r in refs {
                if data_sequence_number <= r.data_sequence_number {
                    matched.positional.push(r.clone());
                }
            }
        }

        // Equality deletes scoped to the same partition.
        if let Some(refs) = self.eq_by_partition.get(&key) {
            for r in refs {
                if data_sequence_number < r.data_sequence_number {
                    matched.equality.push(r.clone());
                }
            }
        }

        // Global (unpartitioned) equality deletes.
        for r in &self.global_eq {
            if data_sequence_number < r.data_sequence_number {
                matched.equality.push(r.clone());
            }
        }

        matched
    }
}

/// Errors surfaced when building a [`DeleteFileIndex`].
#[derive(Debug)]
pub enum DeleteIndexError {
    DeletionVectorUnsupported(String),
    NotADeleteFile(String),
}

impl std::fmt::Display for DeleteIndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DeletionVectorUnsupported(path) => write!(
                f,
                "v3 deletion vectors are not yet supported (file: {path})"
            ),
            Self::NotADeleteFile(path) => {
                write!(f, "attempted to index a non-delete file: {path}")
            }
        }
    }
}

impl std::error::Error for DeleteIndexError {}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]
    use super::*;
    use crate::spec::manifest::DataFileFormat;

    fn unpartitioned_data(path: &str, seq: i64) -> (DataFile, i64) {
        (
            DataFile {
                content: DataContentType::Data,
                file_path: path.to_string(),
                file_format: DataFileFormat::Parquet,
                partition: vec![],
                record_count: 100,
                file_size_in_bytes: 1024,
                column_sizes: HashMap::new(),
                value_counts: HashMap::new(),
                null_value_counts: HashMap::new(),
                nan_value_counts: HashMap::new(),
                lower_bounds: HashMap::new(),
                upper_bounds: HashMap::new(),
                block_size_in_bytes: None,
                key_metadata: None,
                split_offsets: vec![],
                equality_ids: vec![],
                sort_order_id: None,
                first_row_id: None,
                partition_spec_id: 0,
                referenced_data_file: None,
                content_offset: None,
                content_size_in_bytes: None,
            },
            seq,
        )
    }

    fn make_delete(
        content: DataContentType,
        path: &str,
        partition: Vec<Option<Literal>>,
        partition_spec_id: i32,
        referenced_data_file: Option<String>,
        seq: i64,
        is_unpartitioned_spec: bool,
    ) -> DeleteFileRef {
        DeleteFileRef {
            data_file: DataFile {
                content,
                file_path: path.to_string(),
                file_format: DataFileFormat::Parquet,
                partition,
                record_count: 10,
                file_size_in_bytes: 256,
                column_sizes: HashMap::new(),
                value_counts: HashMap::new(),
                null_value_counts: HashMap::new(),
                nan_value_counts: HashMap::new(),
                lower_bounds: HashMap::new(),
                upper_bounds: HashMap::new(),
                block_size_in_bytes: None,
                key_metadata: None,
                split_offsets: vec![],
                equality_ids: if matches!(content, DataContentType::EqualityDeletes) {
                    vec![1]
                } else {
                    vec![]
                },
                sort_order_id: None,
                first_row_id: None,
                partition_spec_id,
                referenced_data_file,
                content_offset: None,
                content_size_in_bytes: None,
            },
            data_sequence_number: seq,
            partition_spec_id,
            is_unpartitioned_spec,
        }
    }

    #[test]
    fn position_delete_scoped_to_path_applies_when_seq_le() {
        let mut idx = DeleteFileIndex::new();
        idx.insert(make_delete(
            DataContentType::PositionDeletes,
            "s3://t/pd1.parquet",
            vec![],
            0,
            Some("s3://t/data1.parquet".to_string()),
            5,
            true,
        ))
        .unwrap();

        let (mut df, _) = unpartitioned_data("s3://t/data1.parquet", 5);
        df.partition_spec_id = 0;

        // data_seq <= delete_seq → applies
        let m = idx.for_data_file(&df, 5);
        assert_eq!(m.positional.len(), 1);
        assert!(m.equality.is_empty());

        // data_seq > delete_seq → does not apply
        let m = idx.for_data_file(&df, 6);
        assert!(m.positional.is_empty());
    }

    #[test]
    fn position_delete_scoped_to_partition_requires_matching_partition() {
        let mut idx = DeleteFileIndex::new();
        idx.insert(make_delete(
            DataContentType::PositionDeletes,
            "s3://t/pd1.parquet",
            vec![Some(Literal::Primitive(PrimitiveLiteral::Int(10)))],
            1,
            None,
            7,
            false,
        ))
        .unwrap();

        // Matching partition, data_seq <= delete_seq → applies
        let (mut df, _) = unpartitioned_data("s3://t/data2.parquet", 3);
        df.partition = vec![Some(Literal::Primitive(PrimitiveLiteral::Int(10)))];
        df.partition_spec_id = 1;
        let m = idx.for_data_file(&df, 3);
        assert_eq!(m.positional.len(), 1);

        // Different partition value → does not apply
        df.partition = vec![Some(Literal::Primitive(PrimitiveLiteral::Int(20)))];
        let m = idx.for_data_file(&df, 3);
        assert!(m.positional.is_empty());
    }

    #[test]
    fn equality_delete_requires_strict_less_than() {
        let mut idx = DeleteFileIndex::new();
        idx.insert(make_delete(
            DataContentType::EqualityDeletes,
            "s3://t/eq1.parquet",
            vec![Some(Literal::Primitive(PrimitiveLiteral::Int(42)))],
            1,
            None,
            10,
            false,
        ))
        .unwrap();

        let (mut df, _) = unpartitioned_data("s3://t/d.parquet", 0);
        df.partition = vec![Some(Literal::Primitive(PrimitiveLiteral::Int(42)))];
        df.partition_spec_id = 1;

        // data_seq < delete_seq → applies
        let m = idx.for_data_file(&df, 9);
        assert_eq!(m.equality.len(), 1);

        // data_seq == delete_seq → does NOT apply (spec: strictly less than)
        let m = idx.for_data_file(&df, 10);
        assert!(m.equality.is_empty());

        // data_seq > delete_seq → does not apply
        let m = idx.for_data_file(&df, 11);
        assert!(m.equality.is_empty());
    }

    #[test]
    fn global_equality_applies_across_partitions() {
        let mut idx = DeleteFileIndex::new();
        idx.insert(make_delete(
            DataContentType::EqualityDeletes,
            "s3://t/eq-global.parquet",
            vec![],
            0,
            None,
            20,
            true, // unpartitioned spec → global
        ))
        .unwrap();

        let (mut df, _) = unpartitioned_data("s3://t/d.parquet", 5);
        df.partition = vec![Some(Literal::Primitive(PrimitiveLiteral::Int(99)))];
        df.partition_spec_id = 2;
        let m = idx.for_data_file(&df, 5);
        assert_eq!(m.equality.len(), 1);

        // Different partition, still applies (global)
        df.partition = vec![Some(Literal::Primitive(PrimitiveLiteral::Int(777)))];
        df.partition_spec_id = 3;
        let m = idx.for_data_file(&df, 5);
        assert_eq!(m.equality.len(), 1);
    }

    #[test]
    fn float_partition_key_uses_bit_pattern() {
        let pos_zero = vec![Some(Literal::Primitive(PrimitiveLiteral::Float(
            OrderedFloat(0.0f32),
        )))];
        let neg_zero = vec![Some(Literal::Primitive(PrimitiveLiteral::Float(
            OrderedFloat(-0.0f32),
        )))];
        let k_pos = PartitionKey::new(0, &pos_zero);
        let k_neg = PartitionKey::new(0, &neg_zero);
        // Per spec: +0.0 and -0.0 have distinct bit patterns → distinct keys.
        assert_ne!(k_pos, k_neg);

        // Same float re-encoded produces identical keys.
        let pos_zero_dup = vec![Some(Literal::Primitive(PrimitiveLiteral::Float(
            OrderedFloat(0.0f32),
        )))];
        let k_dup = PartitionKey::new(0, &pos_zero_dup);
        assert_eq!(k_pos, k_dup);
    }

    #[test]
    fn deletion_vector_rejected_at_insert() {
        let mut dv = make_delete(
            DataContentType::PositionDeletes,
            "s3://t/dv.puffin",
            vec![],
            0,
            Some("s3://t/d.parquet".to_string()),
            5,
            true,
        );
        dv.data_file.file_format = DataFileFormat::Puffin;
        dv.data_file.content_offset = Some(64);
        dv.data_file.content_size_in_bytes = Some(128);
        assert!(dv.is_deletion_vector());

        let mut idx = DeleteFileIndex::new();
        let err = idx.insert(dv).unwrap_err();
        assert!(matches!(
            err,
            DeleteIndexError::DeletionVectorUnsupported(_)
        ));
    }

    #[test]
    fn parquet_pos_delete_with_content_offset_is_not_dv() {
        let mut pos = make_delete(
            DataContentType::PositionDeletes,
            "s3://t/pos-deletes.parquet",
            vec![],
            0,
            Some("s3://t/d.parquet".to_string()),
            5,
            true,
        );
        assert_eq!(pos.data_file.file_format, DataFileFormat::Parquet);
        pos.data_file.content_offset = Some(64);
        pos.data_file.content_size_in_bytes = Some(128);
        assert!(!pos.is_deletion_vector());

        let mut idx = DeleteFileIndex::new();
        assert!(idx.insert(pos).is_ok());
    }

    #[test]
    fn non_delete_file_rejected_at_insert() {
        let mut data_like = make_delete(
            DataContentType::PositionDeletes,
            "s3://t/x.parquet",
            vec![],
            0,
            None,
            5,
            true,
        );
        data_like.data_file.content = DataContentType::Data;
        let mut idx = DeleteFileIndex::new();
        let err = idx.insert(data_like).unwrap_err();
        assert!(matches!(err, DeleteIndexError::NotADeleteFile(_)));
    }
}
