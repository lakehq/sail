pub mod models;
pub mod snapshot;

use std::sync::LazyLock;

use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;

pub(crate) static ARROW_HANDLER: LazyLock<ArrowEvaluationHandler> =
    LazyLock::new(|| ArrowEvaluationHandler {});
