use crate::spark::connect::DataType;
use framework_common::spec;
use framework_plan::config::DataTypeFormatter;
use framework_plan::error::{PlanError, PlanResult};
use std::hash::Hash;

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct SparkDataTypeFormatter;

impl DataTypeFormatter for SparkDataTypeFormatter {
    fn to_simple_string(&self, data_type: spec::DataType) -> PlanResult<String> {
        DataType::try_from(data_type)
            .and_then(|x| x.to_simple_string())
            .map_err(|e| PlanError::internal(e.to_string()))
    }
}
