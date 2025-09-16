//! Utilities for working with the schema of encoded flow event streams.
//! A flow event stream can contain both data or markers, and the flow event stream
//! can be encoded as a stream of Arrow record batches with additional fields
//! that carry the information about the marker and data retraction.

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::{plan_err, Result};

/// The name of the marker field in encoded an [`RecordBatch`] for
/// flow events.
pub const MARKER_FIELD_NAME: &str = "_marker";

/// The name of the retraction field in encoded an [`RecordBatch`] for
/// flow events.
pub const RETRACTED_FIELD_NAME: &str = "_retracted";

fn is_marker_field(field: &Field) -> bool {
    field.name() == MARKER_FIELD_NAME
        && field.data_type() == &DataType::Binary
        && field.is_nullable()
}

fn is_retracted_field(field: &Field) -> bool {
    // We ignore nullability when validating the retracted field.
    field.name() == RETRACTED_FIELD_NAME && field.data_type() == &DataType::Boolean
}

pub fn is_flow_event_schema(schema: &Schema) -> bool {
    let mut fields = schema.fields().iter();
    fields.next().is_some_and(|x| is_marker_field(x.as_ref()))
        && fields
            .next()
            .is_some_and(|x| is_retracted_field(x.as_ref()))
}

pub fn to_flow_event_schema(schema: &Schema) -> Schema {
    let mut fields = vec![
        Field::new(MARKER_FIELD_NAME, DataType::Binary, true),
        // The retraction field is non-nullable to avoid the overhead of null buffer,
        // although the value will only be meaningful for non-marker rows.
        Field::new(RETRACTED_FIELD_NAME, DataType::Boolean, false),
    ];
    fields.extend(schema.fields().iter().map(|x| x.as_ref().clone()));
    Schema::new(fields)
}

pub fn to_flow_event_projection(projection: &[usize]) -> Vec<usize> {
    let mut output = vec![0, 1];
    output.extend(projection.iter().map(|x| x + 2));
    output
}

pub fn to_flow_event_field_names(names: &[String]) -> Vec<String> {
    let mut output = vec![
        MARKER_FIELD_NAME.to_string(),
        RETRACTED_FIELD_NAME.to_string(),
    ];
    output.extend_from_slice(names);
    output
}

pub fn try_from_flow_event_schema(schema: &Schema) -> Result<Schema> {
    let mut fields = schema.fields().iter();
    if !fields.next().is_some_and(|x| is_marker_field(x.as_ref())) {
        return plan_err!("missing marker field in the flow event schema");
    }
    if !fields
        .next()
        .is_some_and(|x| is_retracted_field(x.as_ref()))
    {
        return plan_err!("missing retracted field in the flow event schema");
    }
    Ok(Schema::new(fields.cloned().collect::<Vec<_>>()))
}
