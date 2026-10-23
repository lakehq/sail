//! Query-analysis policy, shared by native resolution and kernel field validation.

use arrow_schema::DataType;
use datafusion_common::{Result, plan_err};

pub fn return_type(_: &[DataType]) -> Result<DataType> {
    plan_err!(
        "{} should be rewritten during logical plan analysis",
        "table_input"
    )
}
