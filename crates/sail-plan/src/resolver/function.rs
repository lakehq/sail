use std::collections::BTreeMap;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::plan_err;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

pub(super) struct PythonUdf {
    pub python_version: String,
    pub eval_type: spec::PySparkUdfType,
    pub command: Vec<u8>,
    pub output_type: DataType,
    /// The metadata of the output field, such as the user-defined type of the output.
    pub output_metadata: BTreeMap<String, String>,
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
        // A user-defined type is only resolved as a field, which has the type in its metadata.
        let output = self.resolve_field(
            &spec::Field {
                name: String::new(),
                data_type: output_type,
                nullable: true,
                metadata: vec![],
            },
            state,
        )?;
        Ok(PythonUdf {
            python_version,
            eval_type,
            command,
            output_type: output.data_type().clone(),
            output_metadata: output
                .metadata()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
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
