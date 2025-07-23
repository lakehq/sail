pub mod models;
pub mod log_data;
mod arrow;

use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use std::sync::LazyLock;

pub(crate) static ARROW_HANDLER: LazyLock<ArrowEvaluationHandler> =
    LazyLock::new(|| ArrowEvaluationHandler {});
