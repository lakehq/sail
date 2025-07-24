use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{not_impl_err, Result};

use crate::extension::logical::FileWriteOptions;

pub fn create_file_write_physical_plan(
    _input: Arc<dyn ExecutionPlan>,
    _options: FileWriteOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    not_impl_err!("create file write physical plan")
}
