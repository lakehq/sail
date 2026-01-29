use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::{Column, DFSchema, JoinType};
use datafusion_expr::expr::{AggregateFunctionParams, ScalarFunction};
use datafusion_expr::{
    expr, ident, AggregateUDF, Expr, ExprSchemable, Extension, LogicalPlan, LogicalPlanBuilder,
    ScalarUDF,
};
use datafusion_functions::core::expr_ext::FieldAccessor;
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_logical_plan::map_partitions::MapPartitionsNode;
use sail_python_udf::cereal::pyspark_udf::PySparkUdfPayload;
use sail_python_udf::get_udf_name;
use sail_python_udf::udf::pyspark_batch_collector::PySparkBatchCollectorUDF;
use sail_python_udf::udf::pyspark_cogroup_map_udf::PySparkCoGroupMapUDF;
use sail_python_udf::udf::pyspark_group_map_udf::PySparkGroupMapUDF;
use sail_python_udf::udf::pyspark_map_iter_udf::{PySparkMapIterKind, PySparkMapIterUDF};

use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_map_partitions(
        &self,
        input: spec::QueryPlan,
        function: spec::CommonInlineUserDefinedFunction,
        _is_barrier: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;
        let spec::CommonInlineUserDefinedFunction {
            function_name,
            deterministic: _,
            is_distinct,
            arguments,
            function,
        } = function;
        if is_distinct {
            return Err(PlanError::invalid("distinct MapPartitions UDF"));
        }
        let function_name: String = function_name.into();
        let input = self
            .resolve_query_project(Some(input), arguments, state)
            .await?;
        let input_names = Self::get_field_names(input.schema(), state)?;
        let function = self.resolve_python_udf(function, state)?;
        let output_schema = match function.output_type {
            DataType::Struct(fields) => Arc::new(Schema::new(fields)),
            _ => {
                return Err(PlanError::invalid(
                    "MapPartitions UDF output type must be struct",
                ))
            }
        };
        let output_names = state.register_fields(output_schema.fields());
        let output_qualifiers = vec![None; output_names.len()];
        let payload = PySparkUdfPayload::build(
            &function.python_version,
            &function.command,
            function.eval_type,
            // MapPartitions UDF has the iterator as the only argument
            &[0],
            &self.config.pyspark_udf_config,
        )?;
        let kind = match function.eval_type {
            spec::PySparkUdfType::MapPandasIter => PySparkMapIterKind::Pandas,
            spec::PySparkUdfType::MapArrowIter => PySparkMapIterKind::Arrow,
            _ => {
                return Err(PlanError::invalid(
                    "only MapPandasIter UDF is supported in MapPartitions",
                ));
            }
        };
        let func = PySparkMapIterUDF::new(
            kind,
            get_udf_name(&function_name, &payload),
            payload,
            input_names,
            output_schema,
            self.config.pyspark_udf_config.clone(),
        );
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(MapPartitionsNode::try_new(
                Arc::new(input),
                output_names,
                output_qualifiers,
                Arc::new(func),
            )?),
        }))
    }

    pub(super) async fn resolve_query_group_map(
        &self,
        map: spec::GroupMap,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;
        let spec::GroupMap {
            input,
            grouping_expressions: grouping,
            function,
            sorting_expressions,
            initial_input,
            initial_grouping_expressions,
            is_map_groups_with_state,
            output_mode,
            timeout_conf,
            state_schema,
            transform_with_state_info,
        } = map;
        // The following group map fields are not used in PySpark,
        // so there is no plan to support them.
        if !sorting_expressions.is_empty() {
            return Err(PlanError::invalid(
                "sorting expressions not supported in group map",
            ));
        }
        if initial_input.is_some() {
            return Err(PlanError::invalid(
                "initial input not supported in group map",
            ));
        }
        if !initial_grouping_expressions.is_empty() {
            return Err(PlanError::invalid(
                "initial grouping expressions not supported in group map",
            ));
        }
        if is_map_groups_with_state.is_some() {
            return Err(PlanError::invalid(
                "is map groups with state not supported in group map",
            ));
        }
        if output_mode.is_some() {
            return Err(PlanError::invalid("output mode not supported in group map"));
        }
        if timeout_conf.is_some() {
            return Err(PlanError::invalid(
                "timeout configuration not supported in group map",
            ));
        }
        if state_schema.is_some() {
            return Err(PlanError::invalid(
                "state schema not supported in group map",
            ));
        }
        if transform_with_state_info.is_some() {
            return Err(PlanError::invalid(
                "transform with state info not supported in group map",
            ));
        }

        let spec::CommonInlineUserDefinedFunction {
            function_name,
            deterministic,
            is_distinct,
            arguments,
            function,
        } = function;
        let function_name: String = function_name.into();
        let function = self.resolve_python_udf(function, state)?;
        let output_fields = match function.output_type {
            DataType::Struct(fields) => fields,
            _ => {
                return Err(PlanError::invalid(
                    "GroupMap UDF output type must be struct",
                ))
            }
        };
        let udf_output_type = DataType::List(Arc::new(Field::new_list_field(
            DataType::Struct(output_fields.clone()),
            false,
        )));
        if !matches!(
            function.eval_type,
            spec::PySparkUdfType::GroupedMapPandas | spec::PySparkUdfType::GroupedMapArrow
        ) {
            return Err(PlanError::invalid(
                "only GroupedMapArrow/GroupedMapPandas UDF is supported in GroupedMap",
            ));
        }
        let is_pandas = matches!(function.eval_type, spec::PySparkUdfType::GroupedMapPandas);
        let input = self.resolve_query_plan(*input, state).await?;
        let schema = input.schema();
        let args = self
            .resolve_named_expressions(arguments, schema, state)
            .await?;
        let grouping = self
            .resolve_named_expressions(grouping, schema, state)
            .await?;
        let (args, offsets) = Self::resolve_group_map_argument_offsets(&args, &grouping)?;
        let input_names = args
            .iter()
            .map(|x| Ok(x.name.clone().one()?))
            .collect::<PlanResult<Vec<_>>>()?;
        let args = args.into_iter().map(|x| x.expr).collect::<Vec<_>>();
        let grouping = grouping.into_iter().map(|x| x.expr).collect::<Vec<_>>();
        let input_types = Self::resolve_expression_types(&args, schema)?;
        let payload = PySparkUdfPayload::build(
            &function.python_version,
            &function.command,
            function.eval_type,
            &offsets,
            &self.config.pyspark_udf_config,
        )?;
        let udaf = PySparkGroupMapUDF::new(
            get_udf_name(&function_name, &payload),
            payload,
            deterministic,
            input_names,
            input_types,
            udf_output_type,
            is_pandas,
            self.config.pyspark_udf_config.clone(),
        );
        let agg = Expr::AggregateFunction(expr::AggregateFunction {
            func: Arc::new(AggregateUDF::from(udaf)),
            params: AggregateFunctionParams {
                args,
                distinct: is_distinct,
                filter: None,
                order_by: vec![],
                null_treatment: None,
            },
        });
        let output_name = agg.name_for_alias()?;
        let output_col = Column::new_unqualified(&output_name);
        let plan = LogicalPlanBuilder::from(input)
            .aggregate(grouping, vec![agg])?
            .project(vec![Expr::Column(output_col.clone())])?
            .unnest_column(output_col.clone())?
            .project(
                output_fields
                    .iter()
                    .map(|f| {
                        let expr = Expr::Column(output_col.clone()).field(f.name());
                        let name = state.register_field(f);
                        Ok(expr.alias(name))
                    })
                    .collect::<PlanResult<Vec<_>>>()?,
            )?
            .build()?;
        Ok(plan)
    }

    pub(super) async fn resolve_query_co_group_map(
        &self,
        map: spec::CoGroupMap,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        state.config_mut().arrow_allow_large_var_types = true;
        let spec::CoGroupMap {
            input: left,
            input_grouping_expressions: left_grouping,
            other: right,
            other_grouping_expressions: right_grouping,
            function,
            input_sorting_expressions: left_sorting,
            other_sorting_expressions: right_sorting,
        } = map;
        // The following co-group map fields are not used in PySpark,
        // so there is no plan to support them.
        if !left_sorting.is_empty() || !right_sorting.is_empty() {
            return Err(PlanError::invalid(
                "sorting expressions not supported in co-group map",
            ));
        }

        // prepare the inputs aggregation and the join operation
        let left = self
            .resolve_co_group_map_data(*left, left_grouping, state)
            .await?;
        let right = self
            .resolve_co_group_map_data(*right, right_grouping, state)
            .await?;
        if left.grouping.len() != right.grouping.len() {
            return Err(PlanError::invalid(
                "child plan grouping expressions must have the same length",
            ));
        }
        let on = left
            .grouping
            .iter()
            .zip(right.grouping.iter())
            .map(|(left, right)| left.clone().eq(right.clone()))
            .collect::<Vec<_>>();
        let offsets: Vec<usize> = left
            .offsets
            .into_iter()
            .chain(right.offsets.into_iter())
            .collect();

        // prepare the output mapping UDF
        let spec::CommonInlineUserDefinedFunction {
            function_name,
            deterministic,
            is_distinct,
            arguments: _, // no arguments are passed for co-group map
            function,
        } = function;
        if is_distinct {
            return Err(PlanError::invalid("distinct CoGroupMap UDF"));
        }
        let function_name: String = function_name.into();
        let function = self.resolve_python_udf(function, state)?;
        let output_fields = match function.output_type {
            DataType::Struct(fields) => fields,
            _ => {
                return Err(PlanError::invalid(
                    "GroupMap UDF output type must be struct",
                ))
            }
        };
        let mapper_output_type = DataType::List(Arc::new(Field::new_list_field(
            DataType::Struct(output_fields.clone()),
            false,
        )));
        if !matches!(
            function.eval_type,
            spec::PySparkUdfType::CogroupedMapPandas | spec::PySparkUdfType::CogroupedMapArrow
        ) {
            return Err(PlanError::invalid(
                "only CoGroupedMapPandas/CoGroupedMapArrow UDF is supported in co-group map",
            ));
        }
        let is_pandas = matches!(function.eval_type, spec::PySparkUdfType::CogroupedMapPandas);
        let payload = PySparkUdfPayload::build(
            &function.python_version,
            &function.command,
            function.eval_type,
            &offsets,
            &self.config.pyspark_udf_config,
        )?;
        let udf = PySparkCoGroupMapUDF::try_new(
            get_udf_name(&function_name, &payload),
            payload,
            deterministic,
            left.mapper_input_types,
            left.mapper_input_names,
            right.mapper_input_types,
            right.mapper_input_names,
            mapper_output_type,
            is_pandas,
            self.config.pyspark_udf_config.clone(),
        )?;
        let mapping = Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::from(udf)),
            args: vec![left.mapper_input, right.mapper_input],
        });
        let output_name = mapping.name_for_alias()?;
        let output_col = Column::new_unqualified(&output_name);

        let builder = if on.is_empty() {
            LogicalPlanBuilder::new(left.plan).cross_join(right.plan)?
        } else {
            LogicalPlanBuilder::new(left.plan).join_on(right.plan, JoinType::Full, on)?
        };
        let plan = builder
            .project(vec![mapping])?
            .unnest_column(output_col.clone())?
            .project(
                output_fields
                    .iter()
                    .map(|f| {
                        let expr = Expr::Column(output_col.clone()).field(f.name());
                        let name = state.register_field(f);
                        Ok(expr.alias(name))
                    })
                    .collect::<PlanResult<Vec<_>>>()?,
            )?
            .build()?;
        Ok(plan)
    }

    async fn resolve_co_group_map_data(
        &self,
        plan: spec::QueryPlan,
        grouping: Vec<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<CoGroupMapData> {
        let plan = self.resolve_query_plan(plan, state).await?;
        let schema = plan.schema();
        let grouping = self
            .resolve_named_expressions(grouping, schema, state)
            .await?;
        let args: Vec<_> = schema
            .columns()
            .into_iter()
            .map(|col| {
                Ok(NamedExpr {
                    name: vec![state.get_field_info(&col.name)?.name().to_string()],
                    expr: Expr::Column(col),
                    metadata: vec![],
                })
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let (args, offsets) = Self::resolve_group_map_argument_offsets(&args, &grouping)?;
        let input_names = args
            .iter()
            .map(|x| Ok(x.name.clone().one()?))
            .collect::<PlanResult<Vec<_>>>()?;
        let args = args.into_iter().map(|x| x.expr).collect::<Vec<_>>();
        let group_exprs = grouping
            .into_iter()
            .map(|x| {
                let name = x.name.clone().one()?;
                Ok(x.expr.clone().alias(state.register_field_name(name)))
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let input_types = Self::resolve_expression_types(&args, plan.schema())?;
        let udaf = PySparkBatchCollectorUDF::new(input_types.clone(), input_names.clone());
        let agg = Expr::AggregateFunction(expr::AggregateFunction {
            func: Arc::new(AggregateUDF::from(udaf)),
            params: AggregateFunctionParams {
                args,
                distinct: false,
                filter: None,
                order_by: vec![],
                null_treatment: None,
            },
        });
        let agg_name = agg.name_for_alias()?;
        let agg_alias = state.register_field_name(&agg_name);
        let agg_col = ident(&agg_name).alias(agg_alias.clone());
        let grouping = group_exprs
            .iter()
            .map(|x| Ok(ident(x.name_for_alias()?)))
            .collect::<PlanResult<Vec<_>>>()?;
        let mut projections = grouping.clone();
        projections.push(agg_col);
        let plan = LogicalPlanBuilder::new(plan)
            .aggregate(group_exprs, vec![agg])?
            .project(projections)?
            .build()?;
        Ok(CoGroupMapData {
            plan,
            grouping,
            mapper_input: ident(agg_alias),
            mapper_input_types: input_types,
            mapper_input_names: input_names,
            offsets,
        })
    }

    pub(super) async fn resolve_query_apply_in_pandas_with_state(
        &self,
        _apply: spec::ApplyInPandasWithState,
        _state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        Err(PlanError::todo("apply in pandas with state"))
    }

    /// Resolves argument offsets for group map operations.
    /// Returns the deduplicated argument expressions and the offset array.
    /// The result offset array `offsets` has the following layout.
    ///   `offsets[0]`: the length of the offset array.
    ///   `offsets[1]`: the number of grouping (key) expressions.
    ///   `offsets[2..offsets[1]+2]`: the offsets of the grouping (key) expressions.
    ///   `offsets[offsets[1]+2..offsets[0]+1]`: the offsets of the data (value) expressions.
    /// See also:
    ///   org.apache.spark.sql.execution.python.PandasGroupUtils#resolveArgOffsets
    fn resolve_group_map_argument_offsets(
        exprs: &[NamedExpr],
        grouping_exprs: &[NamedExpr],
    ) -> PlanResult<(Vec<NamedExpr>, Vec<usize>)> {
        let mut out = exprs.to_vec();
        let mut key_offsets = vec![];
        let mut value_offsets = vec![];
        for expr in grouping_exprs {
            if let Some(pos) = exprs.iter().position(|x| x == expr) {
                key_offsets.push(pos);
            } else {
                let pos = out.len();
                out.push(expr.clone());
                key_offsets.push(pos);
            }
        }
        for i in 0..exprs.len() {
            value_offsets.push(i);
        }
        let mut offsets = Vec::with_capacity(2 + key_offsets.len() + value_offsets.len());
        offsets.push(1 + key_offsets.len() + value_offsets.len());
        offsets.push(key_offsets.len());
        offsets.extend(key_offsets);
        offsets.extend(value_offsets);
        Ok((out, offsets))
    }

    fn resolve_expression_types(exprs: &[Expr], schema: &DFSchema) -> PlanResult<Vec<DataType>> {
        exprs
            .iter()
            .map(|arg| Ok(arg.to_field(schema)?.1.data_type().clone()))
            .collect::<PlanResult<Vec<_>>>()
    }
}

struct CoGroupMapData {
    plan: LogicalPlan,
    grouping: Vec<Expr>,
    mapper_input: Expr,
    mapper_input_types: Vec<DataType>,
    mapper_input_names: Vec<String>,
    offsets: Vec<usize>,
}
