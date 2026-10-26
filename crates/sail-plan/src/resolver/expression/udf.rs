use std::collections::HashSet;
use std::sync::Arc;

use arrow_schema::{DataType, Schema};
use datafusion_common::ScalarValue;
use datafusion_expr::{AggregateUDF, ScalarUDF};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use sail_python_udf::cereal::pyspark_udf::PySparkUdfPayload;
use sail_python_udf::get_udf_name;
use sail_python_udf::udf::pyspark_map_iter_udf::{PySparkMapIterKind, PySparkMapIterUDF};
use sail_python_udf::udf::pyspark_udaf::{
    PySparkAggregateMode, PySparkGroupAggKind, PySparkGroupAggregateUDF,
};
use sail_python_udf::udf::pyspark_udf::{PySparkUDF, PySparkUdfKind};
use sail_python_udf::udf::pyspark_unresolved_udf::PySparkUnresolvedUDF;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::analyzer::typing;
use crate::error::{PlanError, PlanResult};
use crate::resolver::data_type::DataTypeResolver;
use crate::resolver::{NamedExpr, PlanResolver};

pub(in crate::resolver) struct PythonFunction {
    pub python_version: String,
    pub eval_type: spec::PySparkUdfType,
    pub command: Vec<u8>,
    pub output_type: DataType,
}

impl PlanResolver<'_> {
    pub(in crate::resolver) fn python_definition(
        &self,
        definition: spec::FunctionDefinition,
    ) -> PlanResult<PythonFunction> {
        match definition {
            spec::FunctionDefinition::PythonUdf {
                output_type,
                eval_type,
                command,
                python_version,
                ..
            } => {
                let output_type = DataTypeResolver {
                    config: &self.context.config,
                    allow_large_var_types: true,
                }
                .resolve_data_type(&output_type)?;
                Ok(PythonFunction {
                    python_version,
                    eval_type,
                    command,
                    output_type,
                })
            }
            spec::FunctionDefinition::ScalarScalaUdf { .. } => {
                Err(PlanError::todo("Scala UDF is not supported yet"))
            }
            spec::FunctionDefinition::JavaUdf { class_name, .. } => Err(PlanError::analysis(
                format!("Can not load class {class_name}"),
            )),
        }
    }

    pub(in crate::resolver) fn registered_function(
        &self,
        name: &str,
    ) -> PlanResult<Option<ScalarUDF>> {
        match self
            .context
            .session
            .state_ref()
            .read()
            .config()
            .get_extension::<CatalogManager>()
        {
            Some(manager) => manager.get_function(name).map_err(Into::into),
            None => Ok(None),
        }
    }

    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn inline_python(
        &mut self,
        id: ExprId,
        request: spec::CommonInlineUserDefinedFunction,
        schema: &QuerySchema,
    ) -> PlanResult<NamedExpr> {
        let spec::CommonInlineUserDefinedFunction {
            function_name,
            deterministic,
            is_distinct,
            arguments,
            function,
        } = request;
        let definition = self.python_definition(function)?;
        let name: String = function_name.into();
        self.python_arguments(
            id,
            &name,
            definition,
            deterministic,
            is_distinct,
            arguments,
            schema,
            PySparkAggregateMode::Grouped,
        )
        .await
    }

    #[expect(clippy::too_many_arguments)]
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn python_arguments(
        &mut self,
        id: ExprId,
        name: &str,
        definition: PythonFunction,
        deterministic: bool,
        distinct: bool,
        requests: Vec<spec::Expr>,
        schema: &QuerySchema,
        mode: PySparkAggregateMode,
    ) -> PlanResult<NamedExpr> {
        let mut names = vec![];
        let mut values = vec![];
        let mut keywords = vec![];
        let mut seen = HashSet::new();
        let mut named = false;
        for request in requests {
            let (request, keyword) = match request {
                spec::Expr::NamedArgument { key, value } => {
                    if !seen.insert(key.clone()) {
                        return Err(PlanError::analysis(format!(
                            "[DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE] Duplicate named argument: '{key}' is assigned more than once."
                        )));
                    }
                    named = true;
                    (*value, Some(key))
                }
                request => {
                    if named {
                        return Err(PlanError::analysis(
                            "[UNEXPECTED_POSITIONAL_ARGUMENT] Positional argument follows a named (keyword) argument.",
                        ));
                    }
                    (request, None)
                }
            };
            let argument = self.resolve_named_expression(request, schema).await?;
            names.push(argument.name);
            values.push(argument.expression);
            keywords.push(keyword);
        }
        let display_name = self
            .context
            .session
            .extension::<PlanService>()?
            .plan_formatter()
            .function_to_string(name, names.iter().map(String::as_str).collect(), distinct)?;
        let expression = self.python_call(
            id,
            name,
            definition,
            deterministic,
            distinct,
            values,
            &names,
            &keywords,
            mode,
        )?;
        Ok(NamedExpr {
            name: display_name,
            expression,
        })
    }

    #[expect(clippy::too_many_arguments)]
    pub(in crate::resolver) fn python_call(
        &mut self,
        id: ExprId,
        name: &str,
        definition: PythonFunction,
        deterministic: bool,
        distinct: bool,
        mut arguments: Vec<Expr>,
        names: &[String],
        keywords: &[Option<String>],
        mode: PySparkAggregateMode,
    ) -> PlanResult<Expr> {
        use spec::PySparkUdfType as Kind;
        let mut input_types = typing::argument_fields(&arguments)?
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        let payload = PySparkUdfPayload::build(
            &definition.python_version,
            &definition.command,
            definition.eval_type,
            &(0..arguments.len()).collect::<Vec<_>>(),
            &input_types,
            keywords,
            &self.context.config.pyspark_udf_config,
        )?;
        let scalar_kind = match definition.eval_type {
            Kind::Batched => Some(PySparkUdfKind::Batch),
            Kind::ArrowBatched => Some(PySparkUdfKind::ArrowBatch),
            Kind::ScalarPandas => Some(PySparkUdfKind::ScalarPandas),
            Kind::ScalarPandasIter => Some(PySparkUdfKind::ScalarPandasIter),
            Kind::ScalarArrow => Some(PySparkUdfKind::ScalarArrow),
            Kind::ScalarArrowIter => Some(PySparkUdfKind::ScalarArrowIter),
            _ => None,
        };
        if let Some(kind) = scalar_kind {
            if distinct {
                return Err(PlanError::analysis(
                    "DISTINCT requires an aggregate function",
                ));
            }
            let function = PySparkUDF::new(
                kind,
                get_udf_name(name, &payload),
                payload,
                deterministic,
                input_types,
                definition.output_type,
                Arc::clone(&self.context.config.pyspark_udf_config),
            );
            return typing::resolve_scalar_call(
                id,
                Arc::new(ScalarUDF::from(function)),
                arguments,
                &mut self.state.ids,
            );
        }
        let kind = match definition.eval_type {
            Kind::GroupedAggPandas | Kind::GroupedAggPandasIter | Kind::WindowAggPandas => {
                PySparkGroupAggKind::Pandas
            }
            Kind::GroupedAggArrow | Kind::GroupedAggArrowIter | Kind::WindowAggArrow => {
                PySparkGroupAggKind::Arrow
            }
            other => {
                return Err(PlanError::invalid(format!(
                    "unsupported Python UDF type for common inline UDF: {other:?}"
                )));
            }
        };
        for argument in &arguments {
            if argument.any(&|node| matches!(node, ExprKind::AggregateFunction { .. }))? {
                return Err(PlanError::analysis(format!(
                    "The aggregate function '{name}' cannot take an argument containing another aggregate function."
                )));
            }
        }
        let actual_arg_count = arguments.len();
        if arguments.is_empty() {
            arguments.push(
                self.context
                    .function_context(&mut self.state.ids)
                    .literal(ScalarValue::Int64(Some(0))),
            );
            input_types.push(DataType::Int64);
        }
        let function = PySparkGroupAggregateUDF::new(
            kind,
            mode,
            get_udf_name(name, &payload),
            payload,
            deterministic,
            names.to_vec(),
            input_types,
            definition.output_type,
            Arc::clone(&self.context.config.pyspark_udf_config),
            actual_arg_count,
        );
        let mut expression = self
            .context
            .function_context(&mut self.state.ids)
            .aggregate_call(Arc::new(AggregateUDF::from(function)), arguments)?;
        expression.id = id;
        if let Deferred::Resolved(ExprKind::AggregateFunction {
            distinct: value, ..
        }) = &mut expression.kind
        {
            *value = distinct;
        }
        Ok(expression)
    }

    pub(in crate::resolver) fn registered_python(
        function: &ScalarUDF,
    ) -> PlanResult<Option<(PythonFunction, bool)>> {
        let Some(function) = function.inner().downcast_ref::<PySparkUnresolvedUDF>() else {
            return Ok(None);
        };
        if function.eval_type().is_table_function() {
            return Err(PlanError::analysis(
                "user-defined table function cannot be used as a scalar function",
            ));
        }
        Ok(Some((
            PythonFunction {
                python_version: function.python_version().into(),
                eval_type: function.eval_type(),
                command: function.command().to_vec(),
                output_type: function
                    .output_type()
                    .cloned()
                    .ok_or_else(|| PlanError::internal("scalar UDF has no return type"))?,
            },
            function.deterministic(),
        )))
    }

    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn map_partitions(
        &mut self,
        request: spec::QueryPlan,
        function: spec::CommonInlineUserDefinedFunction,
    ) -> PlanResult<Query> {
        let spec::CommonInlineUserDefinedFunction {
            function_name,
            is_distinct,
            arguments,
            function,
            ..
        } = function;
        if is_distinct {
            return Err(PlanError::invalid("distinct MapPartitions UDF"));
        }
        let input = self
            .resolve_query_plan(spec::QueryPlan {
                plan_id: None,
                node: spec::QueryNode::Project {
                    input: Some(Box::new(request)),
                    expressions: arguments,
                },
            })
            .await?;
        let input_names = input
            .schema
            .resolved("map partitions input")?
            .attributes
            .iter()
            .map(|a| a.name.clone())
            .collect();
        let definition = self.python_definition(function)?;
        let DataType::Struct(fields) = definition.output_type else {
            return Err(PlanError::invalid(
                "MapPartitions UDF output type must be struct",
            ));
        };
        let kind = match definition.eval_type {
            spec::PySparkUdfType::MapPandasIter => PySparkMapIterKind::Pandas,
            spec::PySparkUdfType::MapArrowIter => PySparkMapIterKind::Arrow,
            _ => {
                return Err(PlanError::invalid(
                    "only MapPandasIter/MapArrowIter UDF is supported in MapPartitions",
                ));
            }
        };
        let payload = PySparkUdfPayload::build(
            &definition.python_version,
            &definition.command,
            definition.eval_type,
            &[0],
            &[],
            &[],
            &self.context.config.pyspark_udf_config,
        )?;
        let function = PySparkMapIterUDF::new(
            kind,
            get_udf_name(function_name.as_ref(), &payload),
            payload,
            input_names,
            Arc::new(Schema::new(fields.clone())),
            Arc::clone(&self.context.config.pyspark_udf_config),
        );
        let attributes = fields
            .iter()
            .map(|field| {
                self.context
                    .function_context(&mut self.state.ids)
                    .attribute(field.name().clone(), ExprType::from_field(field))
            })
            .collect();
        Ok(Query {
            kind: Deferred::Resolved(QueryKind::MapPartitions {
                input: Box::new(input),
                function: Arc::new(function),
            }),
            schema: Deferred::Resolved(QuerySchema { attributes }),
        })
    }
}
