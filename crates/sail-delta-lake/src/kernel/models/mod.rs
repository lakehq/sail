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

pub mod fields;

mod actions;
mod metadata;
mod protocol;
mod scalars;

#[allow(unused_imports)]
pub use actions::{
    Action, Add, AddCDCFile, CheckpointMetadata, ColumnCountStat, ColumnValueStat, CommitInfo,
    DeletionVectorDescriptor, DomainMetadata, IsolationLevel, Remove, Sidecar, Stats, StorageType,
    Transaction,
};
#[allow(unused_imports)]
pub use delta_kernel::actions::{Metadata, Protocol};
#[allow(unused_imports)]
pub use delta_kernel::expressions::Scalar;
#[allow(unused_imports)]
pub use delta_kernel::schema::{
    ColumnMetadataKey, DataType, MetadataValue, Schema, SchemaRef, StructField, StructType,
};
#[allow(unused_imports)]
pub use metadata::{new_metadata, MetadataExt};
#[allow(unused_imports)]
pub use protocol::ProtocolExt;
#[allow(unused_imports)]
pub use scalars::{ScalarExt, NULL_PARTITION_VALUE_DATA_PATH};

/// Checks if any field (including nested) in the provided iterator is a `timestampNtz`.
#[allow(dead_code)]
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
