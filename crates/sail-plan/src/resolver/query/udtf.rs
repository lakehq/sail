use std::collections::HashSet;
use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_expr::ScalarUDF;
use sail_common::spec;
use sail_common_plan::udf::StreamUDF;
use sail_python_udf::cereal::pyspark_udtf::PySparkUdtfPayload;
use sail_python_udf::get_udf_name;
use sail_python_udf::udf::pyspark_udtf::{PySparkUDTF, PySparkUdtfKind};
use sail_python_udf::udf::pyspark_unresolved_udf::PySparkUnresolvedUDF;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::resolver::data_type::DataTypeResolver;
use crate::resolver::{NamedExpr, PlanResolver};

pub(in crate::resolver) struct PythonTableFunction {
    pub python_version: String,
    pub eval_type: spec::PySparkUdfType,
    pub command: Vec<u8>,
    pub return_type: Option<DataType>,
}

impl PlanResolver<'_> {
    pub(in crate::resolver) fn registered_python_table(
        function: &ScalarUDF,
    ) -> PlanResult<(PythonTableFunction, bool)> {
        let Some(function) = function.inner().downcast_ref::<PySparkUnresolvedUDF>() else {
            return Err(PlanError::analysis(
                "scalar function cannot be used as a table function",
            ));
        };
        if !function.eval_type().is_table_function() {
            return Err(PlanError::analysis(
                "scalar function cannot be used as a table function",
            ));
        }
        Ok((
            PythonTableFunction {
                python_version: function.python_version().into(),
                eval_type: function.eval_type(),
                command: function.command().into(),
                return_type: function.output_type().cloned(),
            },
            function.deterministic(),
        ))
    }

    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn inline_python_table(
        &mut self,
        request: spec::CommonInlineUserDefinedTableFunction,
    ) -> PlanResult<Query> {
        let spec::CommonInlineUserDefinedTableFunction {
            function_name,
            deterministic,
            arguments,
            function,
        } = request;
        let spec::TableFunctionDefinition::PythonUdtf {
            return_type,
            eval_type,
            command,
            python_version,
        } = function;
        let types = DataTypeResolver {
            config: &self.context.config,
            allow_large_var_types: true,
        };
        let function = PythonTableFunction {
            return_type: return_type
                .map(|t| types.resolve_data_type(&t))
                .transpose()?,
            eval_type,
            command,
            python_version,
        };
        self.python_table(
            Self::empty(true),
            function_name.as_ref(),
            function,
            deterministic,
            arguments,
            None,
        )
        .await
    }

    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn python_table(
        &mut self,
        mut input: Query,
        name: &str,
        function: PythonTableFunction,
        deterministic: bool,
        requests: Vec<spec::Expr>,
        aliases: Option<Vec<String>>,
    ) -> PlanResult<Query> {
        let passthrough = input.schema.resolved("UDTF input")?.attributes.clone();
        let mut arguments = vec![];
        let mut keywords = vec![];
        let mut is_tables = vec![];
        let mut literals = vec![];
        let mut seen = HashSet::new();
        let mut named = false;
        for request in requests {
            let (request, keyword) = match request {
                spec::Expr::NamedArgument { key, value } => {
                    if !seen.insert(key.clone()) {
                        return Err(PlanError::analysis(format!(
                            "[DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE] Duplicate named argument: {key}"
                        )));
                    }
                    named = true;
                    (*value, Some(key))
                }
                request => {
                    if named {
                        return Err(PlanError::analysis(
                            "[UNEXPECTED_POSITIONAL_ARGUMENT] Positional argument follows a named argument",
                        ));
                    }
                    (request, None)
                }
            };
            keywords.push(keyword);
            let argument = if let spec::Expr::Table { expr } = request {
                let query = match *expr {
                    spec::Expr::ScalarSubquery { subquery } => *subquery,
                    spec::Expr::UnresolvedAttribute {
                        name,
                        plan_id: None,
                        is_metadata_column: false,
                    } => spec::QueryPlan::new(spec::QueryNode::Read {
                        read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
                            name,
                            temporal: None,
                            sample: None,
                            options: vec![],
                        })),
                        is_streaming: false,
                    }),
                    _ => {
                        return Err(PlanError::invalid(
                            "expected a query or table reference for table input",
                        ));
                    }
                };
                let table = self.resolve_query_plan(query).await?;
                let table = self.visible_output(table)?;
                let fields = table.schema.resolved("table argument")?;
                let names = fields.attributes.iter().map(|a| a.name.clone()).collect();
                let values = fields
                    .attributes
                    .iter()
                    .map(|a| self.reference(a))
                    .collect::<PlanResult<Vec<_>>>()?;
                let expression = self.context.function_context().scalar_udf(
                    Arc::new(ScalarUDF::from(
                        sail_function::scalar::struct_function::StructFunction::new(names),
                    )),
                    values,
                )?;
                let attributes = input
                    .schema
                    .resolved("UDTF input")?
                    .attributes
                    .iter()
                    .chain(&fields.attributes)
                    .cloned()
                    .collect();
                input = Query {
                    kind: Deferred::Resolved(QueryKind::Join {
                        lateral: false,
                        left: Box::new(input),
                        right: Box::new(table),
                        kind: datafusion_common::JoinType::Inner,
                        condition: None,
                    }),
                    schema: Deferred::Resolved(QuerySchema { attributes }),
                };
                is_tables.push(true);
                literals.push(None);
                NamedExpr {
                    name: "table".into(),
                    expression,
                }
            } else {
                let argument = self
                    .resolve_named_expression(
                        request,
                        input.schema.resolved("UDTF argument scope")?,
                    )
                    .await?;
                // The checked evaluator refuses attribute references. Volatile expressions
                // must remain unevaluated when invoking a dynamic analyze method.
                let volatile = argument.expression.any(&|node| match node {
                    ExprKind::ScalarFunction { function, .. } => {
                        function.signature().volatility == datafusion_expr::Volatility::Volatile
                    }
                    _ => false,
                })?;
                literals.push(if volatile {
                    None
                } else {
                    crate::analyzer::lowering::evaluate_constant(&argument.expression).ok()
                });
                is_tables.push(false);
                argument
            };
            arguments.push(argument);
        }
        let tables = is_tables.iter().filter(|t| **t).count();
        if tables > 1 && !self.context.config.tvf_allow_multiple_table_arguments {
            return Err(PlanError::analysis(format!(
                "[TABLE_VALUED_FUNCTION_TOO_MANY_TABLE_ARGUMENTS] Expected at most one table argument, got {tables}"
            )));
        }
        let count = arguments.len();
        let mut projections = passthrough
            .iter()
            .map(|a| {
                Ok(NamedExpr {
                    name: a.name.clone(),
                    expression: self.reference(a)?,
                })
            })
            .collect::<PlanResult<Vec<_>>>()?;
        projections.extend(arguments);
        let input = self.project(input, projections)?;
        let attributes = &input.schema.resolved("UDTF projected input")?.attributes;
        let names = attributes.iter().map(|a| a.name.clone()).collect();
        let types = attributes
            .iter()
            .map(|a| {
                Ok(a.result_type
                    .resolved("UDTF argument type")?
                    .data_type
                    .clone())
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let kind = match function.eval_type {
            spec::PySparkUdfType::Table => PySparkUdtfKind::Table,
            spec::PySparkUdfType::ArrowTable | spec::PySparkUdfType::ArrowUdtf => {
                PySparkUdtfKind::ArrowTable
            }
            other => return Err(PlanError::invalid(format!("PySpark UDTF type: {other:?}"))),
        };
        let return_type = match function.return_type {
            Some(t) => t,
            None => PySparkUdtfPayload::analyze(
                &function.python_version,
                &function.command,
                function.eval_type,
                &types[passthrough.len()..],
                &literals,
                &keywords,
                &is_tables,
                self.context
                    .config
                    .pyspark_udf_config
                    .arrow_use_large_var_types,
            )?,
        };
        let payload = PySparkUdtfPayload::build(
            &function.python_version,
            &function.command,
            function.eval_type,
            count,
            &types,
            passthrough.len(),
            &keywords,
            &return_type,
            &self.context.config.pyspark_udf_config,
        )?;
        let function = PySparkUDTF::try_new(
            kind,
            get_udf_name(name, &payload),
            payload,
            names,
            types,
            passthrough.len(),
            return_type,
            aliases,
            deterministic,
            Arc::clone(&self.context.config.pyspark_udf_config),
        )?;
        let mut attributes = passthrough.clone();
        for (attribute, field) in attributes.iter_mut().zip(function.output_schema().fields()) {
            attribute.result_type = Deferred::Resolved(ExprType::from_field(field));
        }
        for field in function
            .output_schema()
            .fields()
            .iter()
            .skip(passthrough.len())
        {
            attributes.push(
                self.context
                    .function_context()
                    .attribute(field.name().clone(), ExprType::from_field(field)),
            );
        }
        Ok(Query {
            kind: Deferred::Resolved(QueryKind::MapPartitions {
                input: Box::new(input),
                function: Arc::new(function),
            }),
            schema: Deferred::Resolved(QuerySchema { attributes }),
        })
    }
}
