// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod actions;
pub mod checkpoint;
pub mod metadata;
pub mod operation;
pub mod properties;
pub mod protocol;
pub mod schema;
pub mod statistics;

pub use actions::{
    Action, Add, CommitInfo, DeletionVectorDescriptor, Remove, RemoveOptions, StorageType,
    Transaction,
};
pub use checkpoint::{
    protocol_from_checkpoint, protocol_to_checkpoint, CheckpointActionRow, CheckpointAdd,
    CheckpointDeletionVector, CheckpointProtocol, CheckpointRemove, LastCheckpointHint,
};
pub use datafusion::arrow::datatypes::SchemaRef;
pub use metadata::{Format, Metadata};
pub use operation::{DeltaOperation, MergePredicate, SaveMode};
pub use properties::{DataSkippingNumIndexedCols, IsolationLevel, TableProperties};
pub use protocol::{Protocol, TableFeature};
pub use schema::{
    ArrayType, ColumnMappingMode, ColumnMetadataKey, ColumnName, DataType, DecimalType, MapType,
    MetadataValue, PrimitiveType, Schema, StructField, StructType,
};
pub use statistics::{ColumnCountStat, ColumnValueStat, Stats};

// [Credit]: <https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/crates/core/src/kernel/models/actions.rs>
/// Checks if any field (including nested) in the provided iterator is a `timestampNtz`.
pub fn contains_timestampntz<'a>(mut fields: impl Iterator<Item = &'a StructField>) -> bool {
    fn has_timestamp(dtype: &DataType) -> bool {
        match dtype {
            &DataType::TIMESTAMP_NTZ => true,
            DataType::Array(inner) => has_timestamp(inner.element_type()),
            DataType::Struct(struct_type) => {
                struct_type.fields().any(|f| has_timestamp(f.data_type()))
            }
            _ => false,
        }
    }

    fields.any(|field| has_timestamp(field.data_type()))
}

/// Checks if any field (including nested) in an Arrow schema contains a `timestamp_ntz` type.
///
/// In Arrow, `TimestampNtz` is represented as `Timestamp(Microsecond, None)` (no timezone).
pub fn contains_timestampntz_arrow(schema: &datafusion::arrow::datatypes::Schema) -> bool {
    fn has_timestamp_ntz(dt: &datafusion::arrow::datatypes::DataType) -> bool {
        use datafusion::arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
        match dt {
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => true,
            ArrowDataType::Struct(fields) => {
                fields.iter().any(|f| has_timestamp_ntz(f.data_type()))
            }
            ArrowDataType::List(elem)
            | ArrowDataType::LargeList(elem)
            | ArrowDataType::FixedSizeList(elem, _) => has_timestamp_ntz(elem.data_type()),
            _ => false,
        }
    }
    schema
        .fields()
        .iter()
        .any(|f| has_timestamp_ntz(f.data_type()))
}
