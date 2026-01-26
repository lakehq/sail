use std::sync::Arc;

use datafusion_common::TableReference;
use datafusion_expr::{Expr, ExprSchemable, Extension, LogicalPlan, Projection};
use sail_common::spec;
use sail_common_datafusion::udf::StreamUDF;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_logical_plan::map_partitions::MapPartitionsNode;
use sail_python_udf::cereal::pyspark_udtf::PySparkUdtfPayload;
use sail_python_udf::get_udf_name;
use sail_python_udf::udf::pyspark_udtf::{PySparkUDTF, PySparkUdtfKind};

use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::NamedExpr;
use crate::resolver::function::PythonUdtf;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::table_input::TableInputRewriter;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_common_inline_udtf(
        &self,
        udtf: spec::CommonInlineUserDefinedTableFunction,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;
        let spec::CommonInlineUserDefinedTableFunction {
            function_name,
            deterministic,
            arguments,
            function,
        } = udtf;
        let function_name: String = function_name.into();
        let function = self.resolve_python_udtf(function, state)?;
        let input = self.resolve_query_empty(true)?;
        let arguments = self
            .resolve_named_expressions(arguments, input.schema(), state)
            .await?;
        self.resolve_python_udtf_plan(
            function,
            &function_name,
            input,
            arguments,
            None,
            None,
            deterministic,
            state,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn resolve_python_udtf_plan(
        &self,
        function: PythonUdtf,
        name: &str,
        plan: LogicalPlan,
        arguments: Vec<NamedExpr>,
        function_output_names: Option<Vec<String>>,
        function_output_qualifier: Option<TableReference>,
        deterministic: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;

        let payload = PySparkUdtfPayload::build(
            &function.python_version,
            &function.command,
            function.eval_type,
            arguments.len(),
            &function.return_type,
            &self.config.pyspark_udf_config,
        )?;
        let kind = match function.eval_type {
            spec::PySparkUdfType::Table => PySparkUdtfKind::Table,
            spec::PySparkUdfType::ArrowTable => PySparkUdtfKind::ArrowTable,
            _ => {
                return Err(PlanError::invalid(format!(
                    "PySpark UDTF type: {:?}",
                    function.eval_type,
                )))
            }
        };
        // Determine the number of passthrough columns before rewriting the plan
        // for `TABLE (...)` arguments.
        let passthrough_columns = plan.schema().fields().len();
        let projections = plan
            .schema()
            .columns()
            .into_iter()
            .map(|col| {
                Ok(NamedExpr::new(
                    vec![state.get_field_info(col.name())?.name().to_string()],
                    Expr::Column(col),
                ))
            })
            .chain(arguments.into_iter().map(Ok))
            .collect::<PlanResult<Vec<_>>>()?;
        let (plan, projections) =
            self.rewrite_projection::<TableInputRewriter>(plan, projections, state)?;
        let input_names = projections
            .iter()
            .map(|e| e.name.clone().one())
            .collect::<datafusion_common::Result<_>>()?;
        let projections = self.rewrite_named_expressions(projections, state)?;
        let input_types = projections
            .iter()
            .map(|e| e.get_type(plan.schema()))
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        let udtf = PySparkUDTF::try_new(
            kind,
            get_udf_name(name, &payload),
            payload,
            input_names,
            input_types,
            passthrough_columns,
            function.return_type,
            function_output_names,
            deterministic,
            self.config.pyspark_udf_config.clone(),
        )?;
        let output_names = state.register_fields(udtf.output_schema().fields());
        let output_qualifiers = (0..output_names.len())
            .map(|i| {
                if i < passthrough_columns {
                    let (qualifier, _) = plan.schema().qualified_field(i);
                    qualifier.cloned()
                } else {
                    function_output_qualifier.clone()
                }
            })
            .collect();
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(MapPartitionsNode::try_new(
                Arc::new(LogicalPlan::Projection(Projection::try_new(
                    projections,
                    Arc::new(plan),
                )?)),
                output_names,
                output_qualifiers,
                Arc::new(udtf),
            )?),
        }))
    }
}
