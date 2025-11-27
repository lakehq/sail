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

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::schema::StructType;
use delta_kernel::table_features::ColumnMappingMode;

use super::converter::get_physical_arrow_schema;
use super::mapping::{
    annotate_new_fields_for_column_mapping, annotate_schema_for_column_mapping,
    compute_max_column_id,
};
use crate::kernel::models::{Metadata, MetadataExt};
use crate::kernel::DeltaResult;

/// Annotate a kernel schema for column mapping (assign ids + physical names).
pub fn annotate_for_column_mapping(schema: &StructType) -> StructType {
    annotate_schema_for_column_mapping(schema)
}

/// Evolve table schema and update metadata according to column mapping mode.
pub fn evolve_schema(
    existing: &StructType,
    candidate: &StructType,
    metadata: &Metadata,
    mode: ColumnMappingMode,
) -> DeltaResult<(StructType, Metadata)> {
    let updated = if matches!(mode, ColumnMappingMode::Name | ColumnMappingMode::Id) {
        let next_id = metadata
            .configuration()
            .get("delta.columnMapping.maxColumnId")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or_else(|| compute_max_column_id(existing));

        let (annotated, last_id) =
            annotate_new_fields_for_column_mapping(existing, candidate, next_id + 1);

        let meta_with_schema = metadata.clone().with_schema(&annotated)?;
        let meta_with_max = meta_with_schema.add_config_key(
            "delta.columnMapping.maxColumnId".to_string(),
            last_id.to_string(),
        )?;
        (annotated, meta_with_max)
    } else {
        let meta = metadata.clone().with_schema(candidate)?;
        (candidate.clone(), meta)
    };
    Ok(updated)
}

/// Get the Arrow physical schema for reading/writing files, enriched with PARQUET:field_id
/// when column mapping Name/Id mode is active.
pub fn get_physical_schema(logical: &StructType, mode: ColumnMappingMode) -> ArrowSchema {
    get_physical_arrow_schema(logical, mode)
}
