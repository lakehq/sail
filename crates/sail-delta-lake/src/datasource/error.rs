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

use datafusion_common::DataFusionError;
use object_store::Error as ObjectStoreError;

use crate::kernel::DeltaTableError;

/// Convert DeltaTableError to DataFusionError
pub fn delta_to_datafusion_error(err: DeltaTableError) -> DataFusionError {
    match err {
        DeltaTableError::Arrow(source) => DataFusionError::ArrowError(Box::new(source), None),
        DeltaTableError::IOError(source) => DataFusionError::IoError(source),
        DeltaTableError::ObjectStore(source) => DataFusionError::ObjectStore(Box::new(source)),
        DeltaTableError::Parquet(source) => DataFusionError::ParquetError(Box::new(source)),
        DeltaTableError::ObjectStorePath(source) => {
            DataFusionError::ObjectStore(Box::new(ObjectStoreError::InvalidPath { source }))
        }
        _ => DataFusionError::External(Box::new(err)),
    }
}

/// Convert DataFusionError to DeltaTableError
pub fn datafusion_to_delta_error(err: DataFusionError) -> DeltaTableError {
    match err {
        DataFusionError::ArrowError(source, _) => DeltaTableError::Arrow(*source),
        DataFusionError::IoError(source) => DeltaTableError::IOError(source),
        DataFusionError::ObjectStore(source) => DeltaTableError::ObjectStore(*source),
        DataFusionError::ParquetError(source) => DeltaTableError::Parquet(*source),
        _ => DeltaTableError::Generic(err.to_string()),
    }
}
