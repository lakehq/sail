use datafusion::arrow::datatypes::DataType;
use datafusion_common::plan_err;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

pub(super) struct PythonUdf {
    pub python_version: String,
    pub eval_type: spec::PySparkUdfType,
    pub command: Vec<u8>,
    pub output_type: DataType,
    pub output_metadata: Vec<(String, String)>,
}

pub(super) struct PythonUdtf {
    pub python_version: String,
    pub eval_type: spec::PySparkUdfType,
    pub command: Vec<u8>,
    /// The return type of the UDTF. When `None`, the UDTF uses an `analyze` static method
    /// to determine the return type dynamically.
    pub return_type: Option<DataType>,
}

impl PlanResolver<'_> {
    pub(super) fn resolve_python_udf(
        &self,
        function: spec::FunctionDefinition,
        state: &mut PlanResolverState,
    ) -> PlanResult<PythonUdf> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;

        let (output_type, eval_type, command, python_version) = match function {
            spec::FunctionDefinition::PythonUdf {
                output_type,
                eval_type,
                command,
                python_version,
                additional_includes: _,
            } => (output_type, eval_type, command, python_version),
            spec::FunctionDefinition::ScalarScalaUdf { .. } => {
                return Err(PlanError::todo("Scala UDF is not supported yet"));
            }
            spec::FunctionDefinition::JavaUdf { class_name, .. } => {
                return plan_err!("Can not load class {class_name}")?;
            }
        };
        let output_metadata = user_defined_type_metadata(&output_type);
        let output_type = self.resolve_data_type(&output_type, state)?;
        Ok(PythonUdf {
            python_version,
            eval_type,
            command,
            output_type,
            output_metadata,
        })
    }

    pub(super) fn resolve_python_udtf(
        &self,
        function: spec::TableFunctionDefinition,
        state: &mut PlanResolverState,
    ) -> PlanResult<PythonUdtf> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;

        let (return_type, eval_type, command, python_version) = match function {
            spec::TableFunctionDefinition::PythonUdtf {
                return_type,
                eval_type,
                command,
                python_version,
            } => (return_type, eval_type, command, python_version),
        };
        let return_type = return_type
            .map(|rt| self.resolve_data_type(&rt, state))
            .transpose()?;
        Ok(PythonUdtf {
            python_version,
            eval_type,
            command,
            return_type,
        })
    }
}

fn user_defined_type_metadata(data_type: &spec::DataType) -> Vec<(String, String)> {
    match data_type {
        spec::DataType::UserDefined {
            jvm_class,
            python_class,
            serialized_python_class,
            ..
        } => [
            ("udt.jvm_class", jvm_class.as_ref()),
            ("udt.python_class", python_class.as_ref()),
            (
                "udt.serialized_python_class",
                serialized_python_class.as_ref(),
            ),
        ]
        .into_iter()
        .filter_map(|(key, value)| value.map(|value| (key.to_string(), value.to_string())))
        .collect(),
        _ => vec![],
    }
}
