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
