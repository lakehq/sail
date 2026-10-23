use std::sync::Arc;

use arrow_schema::FieldRef;
use datafusion_common::Result;
use datafusion_expr::{ReturnFieldArgs, ScalarUDFImpl};
pub fn return_field_from_args(
    inner: &dyn ScalarUDFImpl,
    args: ReturnFieldArgs,
) -> Result<FieldRef> {
    let field = inner.return_field_from_args(args)?;
    Ok(Arc::new(field.as_ref().clone().with_nullable(true)))
}
