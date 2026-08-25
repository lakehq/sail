use sail_common::spec;

use crate::error::{SparkError, SparkResult};
use crate::spark::connect::analyze_plan_request::explain::ExplainMode;

impl TryFrom<ExplainMode> for spec::SparkExplainMode {
    type Error = SparkError;

    fn try_from(value: ExplainMode) -> SparkResult<spec::SparkExplainMode> {
        match value {
            ExplainMode::Unspecified => Ok(spec::SparkExplainMode::Unspecified),
            ExplainMode::Simple => Ok(spec::SparkExplainMode::Simple),
            ExplainMode::Extended => Ok(spec::SparkExplainMode::Extended),
            ExplainMode::Codegen => Ok(spec::SparkExplainMode::Codegen),
            ExplainMode::Cost => Ok(spec::SparkExplainMode::Cost),
            ExplainMode::Formatted => Ok(spec::SparkExplainMode::Formatted),
        }
    }
}
