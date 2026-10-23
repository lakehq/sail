//! Query-analysis policy, shared by native resolution and kernel field validation.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field};
use datafusion_common::Result;
use datafusion_expr::ReturnFieldArgs;
use sail_common::geoarrow::extension::GeoArrowWkbType;

pub fn return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Binary)
}

pub fn return_field_from_args(_args: ReturnFieldArgs) -> Result<Arc<Field>> {
    let mut metadata = HashMap::new();
    metadata.insert(
        arrow_schema::extension::EXTENSION_TYPE_NAME_KEY.to_string(),
        GeoArrowWkbType::NAME.to_string(),
    );
    metadata.insert(
        arrow_schema::extension::EXTENSION_TYPE_METADATA_KEY.to_string(),
        r#"{"crs":"SRID:0"}"#.to_string(),
    );
    Ok(Arc::new(
        Field::new("st_geomfromwkb", DataType::Binary, true).with_metadata(metadata),
    ))
}
