//! Query-analysis policy, shared by native resolution and kernel field validation.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, TimeUnit};
use datafusion_common::{Result, plan_err};

pub fn return_type(arg_types: &[DataType]) -> Result<DataType> {
    let tz = match arg_types {
        [DataType::Timestamp(TimeUnit::Microsecond, tz)] => tz.clone(),
        other => {
            return plan_err!(
                "window_buckets expects a single Timestamp(Microsecond, *) argument, got {other:?}"
            );
        }
    };
    let field_type = DataType::Timestamp(TimeUnit::Microsecond, tz);
    let item = DataType::Struct(Fields::from(vec![
        Field::new("start", field_type.clone(), true),
        Field::new("end", field_type, true),
    ]));
    Ok(DataType::List(Arc::new(Field::new("item", item, true))))
}
