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

mod actions;
mod metadata;

pub use actions::{
    Action, Add, CommitInfo, DeletionVectorDescriptor, Remove, RemoveOptions, StorageType,
    Transaction,
};
pub use delta_kernel::actions::{Metadata, Protocol};
pub use delta_kernel::schema::{DataType, Schema, StructField, StructType};
pub use metadata::MetadataExt;

pub use super::statistics::{ColumnCountStat, ColumnValueStat, Stats};
pub use crate::conversion::ScalarExt;

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
