/// Planning-only Parquet metadata attached to a `PartitionedFile`.
///
/// File repartitioning clones these extensions. The optimizer uses the offsets
/// to replace byte splits with whole row groups, then emits ordinary file ranges.
/// Workers do not need this metadata or a custom scan codec.
#[derive(Debug)]
pub struct ParquetScanMetadata {
    pub row_groups: Vec<ParquetRowGroup>,
}

#[derive(Debug)]
pub struct ParquetRowGroup {
    /// The first column's dictionary/data page offset, as used by Parquet range pruning.
    pub offset: i64,
    pub compressed_size: u64,
    pub num_rows: u64,
}
