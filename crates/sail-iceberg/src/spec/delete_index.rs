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
        self.data_file.is_deletion_vector()
    }
}

/// The read descriptor after snapshot, sequence, and partition matching.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PositionDeleteFile {
    Parquet {
        path: String,
        size: u64,
    },
    DeletionVector {
        path: String,
        range: std::ops::Range<u64>,
        cardinality: u64,
    },
}

impl TryFrom<&DataFile> for PositionDeleteFile {
    type Error = datafusion_common::DataFusionError;

    fn try_from(file: &DataFile) -> Result<Self, Self::Error> {
        if file.is_deletion_vector() {
            file.validate_deletion_vector()
                .map_err(Self::Error::Execution)?;
            let (Some(offset), Some(size)) = (file.content_offset, file.content_size_in_bytes)
            else {
                return datafusion_common::exec_err!("Missing Iceberg deletion vector range");
            };
            Ok(Self::DeletionVector {
                path: file.file_path.clone(),
                range: offset as u64..(offset + size) as u64,
                cardinality: file.record_count,
            })
        } else if file.content == DataContentType::PositionDeletes
            && file.file_format == DataFileFormat::Parquet
        {
            Ok(Self::Parquet {
                path: file.file_path.clone(),
                size: file.file_size_in_bytes,
            })
        } else {
            datafusion_common::exec_err!(
                "Unsupported Iceberg position delete file: {}",
                file.file_path
            )
        }
    }
}

/// Canonical binary encoding of a partition tuple for equality-keyed lookups.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionKey {
    pub spec_id: i32,
    pub bytes: Vec<u8>,
}

impl PartitionKey {
    /// Build a key with the same encoding before and after numeric type promotion.
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
        Literal::Null => out.push(0),
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
            encode_primitive(&PrimitiveLiteral::Long(i64::from(*i)), out);
        }
        PrimitiveLiteral::Long(l) => {
            out.push(0x03);
            out.extend_from_slice(&l.to_le_bytes());
        }
        PrimitiveLiteral::Float(OrderedFloat(f)) => {
            encode_primitive(&PrimitiveLiteral::Double(OrderedFloat(f64::from(*f))), out);
        }
        PrimitiveLiteral::Double(OrderedFloat(f)) => {
            out.push(0x05);
            let bits = if f.is_nan() {
                f64::NAN.to_bits()
            } else {
                f.to_bits()
            };
            out.extend_from_slice(&bits.to_le_bytes());
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

    /// Register a delete file, rejecting malformed or duplicate deletion vectors.
    pub fn insert(&mut self, file_ref: DeleteFileRef) -> Result<(), DeleteIndexError> {
        if file_ref.is_deletion_vector() {
            file_ref
                .data_file
                .validate_deletion_vector()
                .map_err(DeleteIndexError::InvalidDeletionVector)?;
            if let Some(path) = &file_ref.data_file.referenced_data_file
                && self
                    .pos_by_path
                    .get(path)
                    .is_some_and(|files| files.iter().any(DeleteFileRef::is_deletion_vector))
            {
                return Err(DeleteIndexError::InvalidDeletionVector(format!(
                    "Multiple Iceberg deletion vectors reference {path}"
                )));
            }
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
                let path = referenced_path(&file_ref.data_file);
                if let Some(path) = path {
                    self.pos_by_path.entry(path).or_default().push(file_ref);
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
                if data_sequence_number <= r.data_sequence_number
                    && key == PartitionKey::new(r.partition_spec_id, &r.data_file.partition)
                {
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

        if matched
            .positional
            .iter()
            .any(DeleteFileRef::is_deletion_vector)
        {
            matched.positional.retain(DeleteFileRef::is_deletion_vector);
        }

        // Equality deletes scoped to the same partition.
        if let Some(refs) = self.eq_by_partition.get(&key) {
            for r in refs {
                if data_sequence_number < r.data_sequence_number
                    && equality_may_match(data_file, &r.data_file)
                {
                    matched.equality.push(r.clone());
                }
            }
        }

        // Global (unpartitioned) equality deletes.
        for r in &self.global_eq {
            if data_sequence_number < r.data_sequence_number
                && equality_may_match(data_file, &r.data_file)
            {
                matched.equality.push(r.clone());
            }
        }

        matched
    }
}

/// Errors surfaced when building a [`DeleteFileIndex`].
#[derive(Debug)]
pub enum DeleteIndexError {
    InvalidDeletionVector(String),
    NotADeleteFile(String),
}

impl std::fmt::Display for DeleteIndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidDeletionVector(message) => write!(f, "{message}"),
            Self::NotADeleteFile(path) => {
                write!(f, "attempted to index a non-delete file: {path}")
            }
        }
    }
}

impl std::error::Error for DeleteIndexError {}

fn referenced_path(file: &DataFile) -> Option<String> {
    const PATH_FIELD_ID: i32 = 2_147_483_546;
    if let Some(path) = &file.referenced_data_file {
        return Some(path.clone());
    }
    let lower = file.lower_bounds.get(&PATH_FIELD_ID)?;
    let upper = file.upper_bounds.get(&PATH_FIELD_ID)?;
    if lower != upper {
        return None;
    }
    if let PrimitiveLiteral::String(path) = &lower.literal {
        Some(path.clone())
    } else {
        None
    }
}

fn equality_may_match(data: &DataFile, delete: &DataFile) -> bool {
    if data.record_count == 0 || delete.record_count == 0 {
        return false;
    }
    delete.equality_ids.iter().all(|id| {
        let data_nulls = data.null_value_counts.get(id).copied();
        let delete_nulls = delete.null_value_counts.get(id).copied();
        if (data_nulls == Some(data.record_count) && delete_nulls == Some(0))
            || (delete_nulls == Some(delete.record_count) && data_nulls == Some(0))
        {
            return false;
        }
        // Equality deletes match NULL and NaN keys as well as ordinary values.
        if data_nulls != Some(0) && delete_nulls != Some(0) {
            return true;
        }
        let bounds = [
            data.lower_bounds.get(id),
            data.upper_bounds.get(id),
            delete.lower_bounds.get(id),
            delete.upper_bounds.get(id),
        ];
        if bounds.iter().flatten().any(|bound| {
            matches!(
                bound.literal,
                PrimitiveLiteral::Float(_) | PrimitiveLiteral::Double(_)
            )
        }) && data.nan_value_counts.get(id) != Some(&0)
            && delete.nan_value_counts.get(id) != Some(&0)
        {
            return true;
        }
        let less = |left: Option<&crate::spec::Datum>, right: Option<&crate::spec::Datum>| {
            left.zip(right).is_some_and(|(left, right)| {
                crate::datasource::predicate::compare(&left.literal, &right.literal)
                    .is_some_and(|order| order.is_lt())
            })
        };
        !less(bounds[1], bounds[2]) && !less(bounds[3], bounds[0])
    })
}

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
    fn equality_bounds_keep_possible_null_and_nan_matches() {
        use crate::spec::{Datum, PrimitiveType};
        for (nulls, nans, expected) in [
            (Some(0), Some(0), false),
            (Some(1), Some(0), true),
            (None, Some(0), true),
            (Some(0), Some(1), true),
            (Some(0), None, true),
        ] {
            let (mut data, _) = unpartitioned_data("data", 1);
            let mut delete = make_delete(
                DataContentType::EqualityDeletes,
                "delete",
                vec![],
                0,
                None,
                2,
                true,
            );
            for (file, value) in [(&mut data, 1.0), (&mut delete.data_file, 2.0)] {
                file.lower_bounds.insert(
                    1,
                    Datum::new(
                        PrimitiveType::Double,
                        PrimitiveLiteral::Double(value.into()),
                    ),
                );
                file.upper_bounds = file.lower_bounds.clone();
                file.null_value_counts = nulls.map(|count| (1, count)).into_iter().collect();
                file.nan_value_counts = nans.map(|count| (1, count)).into_iter().collect();
            }
            let mut index = DeleteFileIndex::new();
            index.insert(delete).unwrap();
            assert_eq!(!index.for_data_file(&data, 1).equality.is_empty(), expected);
            assert!(index.for_data_file(&data, 2).equality.is_empty());
        }
    }

    #[test]
    fn position_path_bounds_only_infer_a_single_target() {
        use crate::spec::{Datum, PrimitiveType};
        for upper in ["target", "z-other"] {
            let mut delete = make_delete(
                DataContentType::PositionDeletes,
                "delete",
                vec![],
                0,
                None,
                2,
                true,
            );
            delete.data_file.lower_bounds.insert(
                2_147_483_546,
                Datum::new(
                    PrimitiveType::String,
                    PrimitiveLiteral::String("target".into()),
                ),
            );
            delete.data_file.upper_bounds.insert(
                2_147_483_546,
                Datum::new(
                    PrimitiveType::String,
                    PrimitiveLiteral::String(upper.into()),
                ),
            );
            let mut index = DeleteFileIndex::new();
            index.insert(delete).unwrap();
            let (target, _) = unpartitioned_data("target", 2);
            let (other, _) = unpartitioned_data("other", 2);
            assert_eq!(index.for_data_file(&target, 2).positional.len(), 1);
            assert_eq!(
                index.for_data_file(&other, 2).positional.len(),
                usize::from(upper != "target")
            );
            assert!(index.for_data_file(&target, 3).positional.is_empty());
        }
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
    fn partition_keys_preserve_numeric_promotions() {
        let key = |value| PartitionKey::new(0, &[Some(Literal::Primitive(value))]);
        for value in [i32::MIN, -1, 0, 1, i32::MAX] {
            assert_eq!(
                key(PrimitiveLiteral::Int(value)),
                key(PrimitiveLiteral::Long(i64::from(value)))
            );
        }
        for value in [
            f32::NEG_INFINITY,
            -0.0,
            0.0,
            0.1,
            f32::MAX,
            f32::INFINITY,
            f32::NAN,
        ] {
            assert_eq!(
                key(PrimitiveLiteral::Float(OrderedFloat(value))),
                key(PrimitiveLiteral::Double(OrderedFloat(f64::from(value))))
            );
        }
        assert_ne!(
            key(PrimitiveLiteral::Int(-1)),
            key(PrimitiveLiteral::Long(i64::MAX))
        );
        assert_ne!(
            key(PrimitiveLiteral::Float(OrderedFloat(0.1))),
            key(PrimitiveLiteral::Double(OrderedFloat(0.1)))
        );
        assert_ne!(
            key(PrimitiveLiteral::Int(1)),
            key(PrimitiveLiteral::Double(OrderedFloat(1.0)))
        );
    }

    #[test]
    fn floating_point_partition_key_matches_iceberg_equality() {
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

        let float_nan = vec![Some(Literal::Primitive(PrimitiveLiteral::Float(
            OrderedFloat(f32::from_bits(0x7fc0_0001)),
        )))];
        let other_float_nan = vec![Some(Literal::Primitive(PrimitiveLiteral::Float(
            OrderedFloat(f32::from_bits(0xffc0_0042)),
        )))];
        assert_eq!(
            PartitionKey::new(0, &float_nan),
            PartitionKey::new(0, &other_float_nan)
        );

        let double_nan = vec![Some(Literal::Primitive(PrimitiveLiteral::Double(
            OrderedFloat(f64::from_bits(0x7ff8_0000_0000_0001)),
        )))];
        let other_double_nan = vec![Some(Literal::Primitive(PrimitiveLiteral::Double(
            OrderedFloat(f64::from_bits(0xfff8_0000_0000_0042)),
        )))];
        assert_eq!(
            PartitionKey::new(0, &double_nan),
            PartitionKey::new(0, &other_double_nan)
        );
    }

    #[test]
    fn deletion_vector_supersedes_position_deletes_and_rejects_duplicates() {
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

        dv.data_file.file_size_in_bytes = 256;
        let mut idx = DeleteFileIndex::new();
        idx.insert(dv.clone()).unwrap();
        let mut older = dv.clone();
        older.data_file.file_format = DataFileFormat::Parquet;
        older.data_file.content_offset = None;
        older.data_file.content_size_in_bytes = None;
        idx.insert(older).unwrap();
        let mut data = dv.data_file.clone();
        data.content = DataContentType::Data;
        data.file_path = "s3://t/d.parquet".to_string();
        assert_eq!(idx.for_data_file(&data, 5).positional, vec![dv.clone()]);
        assert!(idx.for_data_file(&data, 6).is_empty());
        data.partition_spec_id = 1;
        assert!(idx.for_data_file(&data, 5).is_empty());
        assert!(idx.insert(dv).is_err());
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
