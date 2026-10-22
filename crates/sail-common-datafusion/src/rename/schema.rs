use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion_common::plan_err;

pub fn rename_schema(schema: &Schema, names: &[String]) -> datafusion_common::Result<SchemaRef> {
    if schema.fields().len() != names.len() {
        return plan_err!(
            "cannot rename fields for schema with {} fields using {} names",
            schema.fields().len(),
            names.len()
        );
    }
    let fields = schema
        .fields()
        .iter()
        .zip(names.iter())
        .map(|(field, name)| field.as_ref().clone().with_name(name))
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new(fields)))
}

/// Renames the fields of the schema, and adds the metadata to each field,
/// replacing the field metadata of the same keys.
pub fn rename_schema_with_metadata(
    schema: &Schema,
    names: &[String],
    metadata: &[HashMap<String, String>],
) -> datafusion_common::Result<SchemaRef> {
    let schema = rename_schema(schema, names)?;
    if metadata.iter().all(|x| x.is_empty()) {
        return Ok(schema);
    }
    if schema.fields().len() != metadata.len() {
        return plan_err!(
            "cannot add metadata to schema with {} fields using {} metadata maps",
            schema.fields().len(),
            metadata.len()
        );
    }
    let fields = schema
        .fields()
        .iter()
        .zip(metadata.iter())
        .map(|(field, metadata)| {
            let mut field_metadata = field.metadata().clone();
            field_metadata.extend(metadata.clone());
            field.as_ref().clone().with_metadata(field_metadata)
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}
