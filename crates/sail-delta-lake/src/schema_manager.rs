use datafusion::arrow::datatypes::Schema as ArrowSchema;
use delta_kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use delta_kernel::schema::StructType;
use delta_kernel::table_features::ColumnMappingMode;
#[allow(deprecated)]
use deltalake::kernel::MetadataExt;
use deltalake::DeltaResult;

use crate::column_mapping::{
    annotate_new_fields_for_column_mapping, annotate_schema_for_column_mapping,
    compute_max_column_id, enrich_arrow_with_parquet_field_ids,
};

/// Convert logical Arrow schema to kernel `StructType`.
pub fn logical_arrow_to_kernel(arrow: &ArrowSchema) -> DeltaResult<StructType> {
    Ok(arrow.try_into_kernel()?)
}

/// Convert kernel `StructType` to logical Arrow schema.
pub fn kernel_to_logical_arrow(schema: &StructType) -> ArrowSchema {
    // FIXME: surface error instead of defaulting to empty schema
    schema
        .try_into_arrow()
        .unwrap_or_else(|_| ArrowSchema::empty())
}

/// Annotate a kernel schema for column mapping (assign ids + physical names).
pub fn annotate_for_column_mapping(schema: &StructType) -> StructType {
    annotate_schema_for_column_mapping(schema)
}

/// Evolve table schema and update metadata according to column mapping mode.
pub fn evolve_schema(
    existing: &StructType,
    candidate: &StructType,
    metadata: &deltalake::kernel::Metadata,
    mode: ColumnMappingMode,
) -> DeltaResult<(StructType, deltalake::kernel::Metadata)> {
    let updated = if matches!(mode, ColumnMappingMode::Name | ColumnMappingMode::Id) {
        let next_id = metadata
            .configuration()
            .get("delta.columnMapping.maxColumnId")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or_else(|| compute_max_column_id(existing));

        let (annotated, last_id) =
            annotate_new_fields_for_column_mapping(existing, candidate, next_id + 1);

        #[allow(deprecated)]
        let meta_with_schema = metadata.clone().with_schema(&annotated)?;
        #[allow(deprecated)]
        let meta_with_max = meta_with_schema.add_config_key(
            "delta.columnMapping.maxColumnId".to_string(),
            last_id.to_string(),
        )?;
        (annotated, meta_with_max)
    } else {
        #[allow(deprecated)]
        let meta = metadata.clone().with_schema(candidate)?;
        (candidate.clone(), meta)
    };
    Ok(updated)
}

/// Get the Arrow physical schema used for file writes, enriched with PARQUET:field_id
/// when column mapping Name/Id mode is active.
pub fn get_physical_write_schema(logical: &StructType, mode: ColumnMappingMode) -> ArrowSchema {
    let physical_kernel = logical.make_physical(mode);
    // FIXME: surface error instead of defaulting to empty schema
    let physical_arrow: ArrowSchema = (&physical_kernel)
        .try_into_arrow()
        .unwrap_or_else(|_| ArrowSchema::empty());
    match mode {
        ColumnMappingMode::Name | ColumnMappingMode::Id => {
            enrich_arrow_with_parquet_field_ids(&physical_arrow, logical)
        }
        ColumnMappingMode::None => physical_arrow,
    }
}
