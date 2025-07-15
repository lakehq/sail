//! Logical operators and physical executions for CDF
use std::collections::HashMap;
use std::sync::LazyLock;

/// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/delta_datafusion/cdf/mod.rs>
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use deltalake::kernel::{Add, AddCDCFile, Remove};
use deltalake::DeltaResult;

pub(crate) use self::scan_utils::*;

pub mod scan;
mod scan_utils;

/// Change type column name
#[allow(dead_code)]
pub const CHANGE_TYPE_COL: &str = "_change_type";
/// Commit version column name
#[allow(dead_code)]
pub const COMMIT_VERSION_COL: &str = "_commit_version";
/// Commit Timestamp column name
#[allow(dead_code)]
pub const COMMIT_TIMESTAMP_COL: &str = "_commit_timestamp";

#[allow(dead_code)]
pub(crate) static CDC_PARTITION_SCHEMA: LazyLock<Vec<Field>> = LazyLock::new(|| {
    vec![
        Field::new(COMMIT_VERSION_COL, DataType::Int64, true),
        Field::new(
            COMMIT_TIMESTAMP_COL,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
    ]
});

#[allow(dead_code)]
pub(crate) static ADD_PARTITION_SCHEMA: LazyLock<Vec<Field>> = LazyLock::new(|| {
    vec![
        Field::new(CHANGE_TYPE_COL, DataType::Utf8, true),
        Field::new(COMMIT_VERSION_COL, DataType::Int64, true),
        Field::new(
            COMMIT_TIMESTAMP_COL,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
    ]
});

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct CdcDataSpec<F: FileAction> {
    version: i64,
    timestamp: i64,
    actions: Vec<F>,
}

#[allow(dead_code)]
impl<F: FileAction> CdcDataSpec<F> {
    pub fn new(version: i64, timestamp: i64, actions: Vec<F>) -> Self {
        Self {
            version,
            timestamp,
            actions,
        }
    }
}

/// This trait defines a generic set of operations used by CDF Reader
#[allow(dead_code)]
pub trait FileAction {
    /// Adds partition values
    fn partition_values(&self) -> DeltaResult<&HashMap<String, Option<String>>>;
    /// Physical Path to the data
    fn path(&self) -> String;
    /// Byte size of the physical file
    fn size(&self) -> DeltaResult<usize>;
}

impl FileAction for Add {
    fn partition_values(&self) -> DeltaResult<&HashMap<String, Option<String>>> {
        Ok(&self.partition_values)
    }

    fn path(&self) -> String {
        self.path.clone()
    }

    fn size(&self) -> DeltaResult<usize> {
        Ok(self.size as usize)
    }
}

impl FileAction for AddCDCFile {
    fn partition_values(&self) -> DeltaResult<&HashMap<String, Option<String>>> {
        Ok(&self.partition_values)
    }

    fn path(&self) -> String {
        self.path.clone()
    }

    fn size(&self) -> DeltaResult<usize> {
        Ok(self.size as usize)
    }
}

impl FileAction for Remove {
    fn partition_values(&self) -> DeltaResult<&HashMap<String, Option<String>>> {
        // If extended_file_metadata is true, it should be required to have this filled in
        if self.extended_file_metadata.unwrap_or_default() {
            Ok(self
                .partition_values
                .as_ref()
                .expect("partition_values should be present when extended_file_metadata is true"))
        } else {
            match self.partition_values {
                Some(ref part_map) => Ok(part_map),
                _ => Err(deltalake::DeltaTableError::MetadataError(
                    "Remove action is missing required field: 'partition_values'".to_string(),
                )),
            }
        }
    }

    fn path(&self) -> String {
        self.path.clone()
    }

    fn size(&self) -> DeltaResult<usize> {
        // If extended_file_metadata is true, it should be required to have this filled in
        if self.extended_file_metadata.unwrap_or_default() {
            Ok(self
                .size
                .expect("size should be present when extended_file_metadata is true")
                as usize)
        } else {
            match self.size {
                Some(size) => Ok(size as usize),
                _ => Err(deltalake::DeltaTableError::MetadataError(
                    "Remove action is missing required field: 'size'".to_string(),
                )),
            }
        }
    }
}
