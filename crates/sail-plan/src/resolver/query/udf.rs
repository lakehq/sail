use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields};
use datafusion::functions::core;
use datafusion_common::{JoinType, ScalarValue};
use datafusion_expr::{AggregateUDF, Operator, ScalarUDF};
use sail_common::spec;
use sail_python_udf::cereal::pyspark_udf::PySparkUdfPayload;
use sail_python_udf::get_udf_name;
use sail_python_udf::udf::pyspark_batch_collector::PySparkBatchCollectorUDF;
use sail_python_udf::udf::pyspark_cogroup_map_udf::PySparkCoGroupMapUDF;
use sail_python_udf::udf::pyspark_group_map_udf::{PySparkGroupMapMode, PySparkGroupMapUDF};

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::resolver::{NamedExpr, PlanResolver};

struct GroupInput {
    plan: Query,
    keys: Vec<Attribute>,
    values: Vec<Attribute>,
    offsets: Vec<usize>,
}
struct CollectedGroup {
    plan: Query,
    keys: Vec<Attribute>,
    value: Attribute,
    types: Vec<DataType>,
    names: Vec<String>,
    offsets: Vec<usize>,
}

impl PlanResolver<'_> {
    /// Materialize each argument once. Grouping and aggregate references then
    /// share attribute IDs without sharing expression occurrence IDs.
    #[async_recursion::async_recursion]
    async fn group_input(
        &mut self,
        mut input: Query,
        requests: Vec<spec::Expr>,
        grouping: Vec<spec::Expr>,
    ) -> PlanResult<GroupInput> {
        let previous = self.expression_replacements.len();
        let mut generators = vec![];
        for request in requests.iter().chain(&grouping) {
            crate::resolver::tree::explode::nested_generators(request, &mut generators);
        }
        for request in generators {
            if self
                .expression_replacements
                .iter()
                .any(|(original, _)| original == &request)
            {
                continue;
            }
            let (next, mut values) = self.generator(input, request.clone()).await?;
            input = next;
            if values.len() != 1 {
                return Err(PlanError::analysis(
                    "grouping generator must produce one column",
                ));
            }
            let value = values
                .pop()
                .ok_or_else(|| PlanError::internal("missing generated group"))?;
            self.expression_replacements.push((request, value));
        }
        let schema = input.schema.resolved("group input")?;
        let mut arguments = vec![];
        for request in requests {
            arguments.push(self.resolve_named_expression(request, schema).await?);
        }
        let count = arguments.len();
        let mut keys = vec![];
        for request in grouping {
            let key = self.resolve_named_expression(request, schema).await?;
            let mut position = None;
            for (index, value) in arguments.iter().enumerate() {
                if value.expression.equivalent(&key.expression)? {
                    position = Some(index);
                    break;
                }
            }
            keys.push(position.unwrap_or_else(|| {
                let index = arguments.len();
                arguments.push(key);
                index
            }));
        }
        self.expression_replacements.truncate(previous);
        let plan = self.project(input, arguments)?;
        let values = plan.schema.resolved("group arguments")?.attributes.clone();
        let offsets = std::iter::once(1 + keys.len() + count)
            .chain(std::iter::once(keys.len()))
            .chain(keys.iter().copied())
            .chain(0..count)
            .collect();
        let keys = keys.into_iter().map(|i| values[i].clone()).collect();
        Ok(GroupInput {
            plan,
            keys,
            values,
            offsets,
        })
    }

    pub(in crate::resolver) fn unpack_struct_list(
        &mut self,
        input: Query,
        array: &Attribute,
        fields: &Fields,
    ) -> PlanResult<Query> {
        let (input, element) = self.unnest(input, array, false)?;
        let mut output = vec![];
        for field in fields {
            let element = self.reference(&element)?;
            let name = self
                .context
                .function_context(&mut self.state.ids)
                .literal(ScalarValue::Utf8(Some(field.name().clone())));
            let expression = self
                .context
                .function_context(&mut self.state.ids)
                .scalar_udf(core::get_field(), vec![element, name])?;
            output.push(NamedExpr {
                name: field.name().clone(),
                expression,
            });
        }
        self.project(input, output)
    }

    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn group_map(
        &mut self,
        map: spec::GroupMap,
    ) -> PlanResult<Query> {
        let spec::GroupMap {
            input,
            grouping_expressions,
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
        if !sorting_expressions.is_empty()
            || initial_input.is_some()
            || !initial_grouping_expressions.is_empty()
            || is_map_groups_with_state.is_some()
            || output_mode.is_some()
            || timeout_conf.is_some()
            || state_schema.is_some()
            || transform_with_state_info.is_some()
        {
            return Err(PlanError::invalid(
                "stateful or sorted GroupMap is not supported",
            ));
        }
        let spec::CommonInlineUserDefinedFunction {
            function_name,
            deterministic,
            is_distinct,
            arguments,
            function,
        } = function;
        let definition = self.python_definition(function)?;
        let DataType::Struct(fields) = definition.output_type else {
            return Err(PlanError::invalid(
                "GroupMap UDF output type must be struct",
            ));
        };
        use spec::PySparkUdfType as Kind;
        let (is_pandas, is_iter) = match definition.eval_type {
            Kind::GroupedMapPandas => (true, false),
            Kind::GroupedMapArrow => (false, false),
            Kind::GroupedMapPandasIter => (true, true),
            Kind::GroupedMapArrowIter => (false, true),
            _ => {
                return Err(PlanError::invalid(
                    "only grouped map Pandas/Arrow UDFs are supported in GroupMap",
                ));
            }
        };
        let input = self.resolve_query_plan(*input).await?;
        let input = self
            .group_input(input, arguments, grouping_expressions)
            .await?;
        let names = input.values.iter().map(|a| a.name.clone()).collect();
        let types = input
            .values
            .iter()
            .map(|a| Ok(a.result_type.resolved("group argument")?.data_type.clone()))
            .collect::<PlanResult<Vec<_>>>()?;
        let payload = PySparkUdfPayload::build(
            &definition.python_version,
            &definition.command,
            definition.eval_type,
            &input.offsets,
            &types,
            &[],
            &self.context.config.pyspark_udf_config,
        )?;
        let function = PySparkGroupMapUDF::new(
            get_udf_name(function_name.as_ref(), &payload),
            payload,
            deterministic,
            names,
            types,
            DataType::List(Arc::new(Field::new_list_field(
                DataType::Struct(fields.clone()),
                false,
            ))),
            PySparkGroupMapMode { is_pandas, is_iter },
            Arc::clone(&self.context.config.pyspark_udf_config),
        );
        let arguments = input
            .values
            .iter()
            .map(|a| self.reference(a))
            .collect::<PlanResult<Vec<_>>>()?;
        let mut expression = self
            .context
            .function_context(&mut self.state.ids)
            .aggregate_call(Arc::new(AggregateUDF::from(function)), arguments)?;
        if let Deferred::Resolved(ExprKind::AggregateFunction { distinct, .. }) =
            &mut expression.kind
        {
            *distinct = is_distinct;
        }
        let grouping = input
            .keys
            .iter()
            .map(|a| {
                Ok(NamedExpr {
                    name: a.name.clone(),
                    expression: self.reference(a)?,
                })
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let plan = self.aggregate(
            input.plan,
            grouping,
            vec![NamedExpr {
                name: "group_map".into(),
                expression,
            }],
            None,
        )?;
        let array = plan.schema.resolved("group result")?.attributes[0].clone();
        let input = self.unpack_struct_list(plan, &array, &fields)?;
        Ok(Query {
            schema: input.schema.clone(),
            kind: Deferred::Resolved(QueryKind::Scope {
                scope: None,
                input: Box::new(input),
            }),
        })
    }

    #[async_recursion::async_recursion]
    async fn collect_groups(
        &mut self,
        request: spec::QueryPlan,
        grouping: Vec<spec::Expr>,
    ) -> PlanResult<CollectedGroup> {
        let input = self.resolve_query_plan(request).await?;
        let input = self.visible_output(input)?;
        // Resolve by ID so duplicate user-facing names are never ambiguous.
        let mut values = input.schema.resolved("co-group input")?.attributes.clone();
        let count = values.len();
        let mut plan = input;
        let mut keys = vec![];
        for request in grouping {
            let key = self
                .resolve_named_expression(request, plan.schema.resolved("co-group input")?)
                .await?;
            let position = match key.expression.kind.resolved("co-group key")? {
                ExprKind::Attribute(id) => values.iter().position(|a| a.id == *id),
                _ => None,
            };
            let index = if let Some(position) = position {
                position
            } else {
                let (next, attribute) = self.append(plan, key.expression, key.name)?;
                plan = next;
                let index = values.len();
                values.push(attribute);
                index
            };
            keys.push(index);
        }
        let offsets = std::iter::once(1 + keys.len() + count)
            .chain(std::iter::once(keys.len()))
            .chain(keys.iter().copied())
            .chain(0..count)
            .collect();
        let names = values.iter().map(|a| a.name.clone()).collect::<Vec<_>>();
        let types = values
            .iter()
            .map(|a| {
                Ok(a.result_type
                    .resolved("co-group argument")?
                    .data_type
                    .clone())
            })
            .collect::<PlanResult<Vec<_>>>()?;
        let arguments = values
            .iter()
            .map(|a| self.reference(a))
            .collect::<PlanResult<Vec<_>>>()?;
        let expression = self
            .context
            .function_context(&mut self.state.ids)
            .aggregate_call(
                Arc::new(AggregateUDF::from(PySparkBatchCollectorUDF::new(
                    types.clone(),
                    names.clone(),
                ))),
                arguments,
            )?;
        let mut grouping = vec![];
        for index in keys {
            let source = &values[index];
            grouping.push(Projection {
                expression: self.reference(source)?,
                attribute: self
                    .context
                    .function_context(&mut self.state.ids)
                    .attribute(
                        source.name.clone(),
                        source.result_type.resolved("co-group key")?.clone(),
                    ),
            });
        }
        let value = self
            .context
            .function_context(&mut self.state.ids)
            .attribute(
                "collected_group".into(),
                expression
                    .result_type
                    .resolved("co-group collector")?
                    .clone(),
            );
        let keys = grouping
            .iter()
            .map(|p| p.attribute.clone())
            .collect::<Vec<_>>();
        let schema = QuerySchema {
            attributes: keys
                .iter()
                .cloned()
                .chain(std::iter::once(value.clone()))
                .collect(),
        };
        let plan = Query {
            kind: Deferred::Resolved(QueryKind::Aggregate {
                input: Box::new(plan),
                grouping,
                grouping_sets: None,
                grouping_id: None,
                aggregates: vec![Projection {
                    expression,
                    attribute: value.clone(),
                }],
            }),
            schema: Deferred::Resolved(schema),
        };
        Ok(CollectedGroup {
            plan,
            keys,
            value,
            names,
            types,
            offsets,
        })
    }

    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn co_group_map(
        &mut self,
        map: spec::CoGroupMap,
    ) -> PlanResult<Query> {
        let spec::CoGroupMap {
            input,
            input_grouping_expressions,
            other,
            other_grouping_expressions,
            function,
            input_sorting_expressions,
            other_sorting_expressions,
        } = map;
        if !input_sorting_expressions.is_empty() || !other_sorting_expressions.is_empty() {
            return Err(PlanError::invalid(
                "sorting expressions not supported in co-group map",
            ));
        }
        let left = self
            .collect_groups(*input, input_grouping_expressions)
            .await?;
        let right = self
            .collect_groups(*other, other_grouping_expressions)
            .await?;
        if left.keys.len() != right.keys.len() {
            return Err(PlanError::invalid(
                "child plan grouping expressions must have the same length",
            ));
        }
        let spec::CommonInlineUserDefinedFunction {
            function_name,
            deterministic,
            is_distinct,
            function,
            ..
        } = function;
        if is_distinct {
            return Err(PlanError::invalid("distinct CoGroupMap UDF"));
        }
        let definition = self.python_definition(function)?;
        let DataType::Struct(fields) = definition.output_type else {
            return Err(PlanError::invalid(
                "CoGroupMap UDF output type must be struct",
            ));
        };
        let is_pandas = match definition.eval_type {
            spec::PySparkUdfType::CogroupedMapPandas => true,
            spec::PySparkUdfType::CogroupedMapArrow => false,
            _ => {
                return Err(PlanError::invalid(
                    "only cogrouped map Pandas/Arrow UDFs are supported in CoGroupMap",
                ));
            }
        };
        let offsets = left
            .offsets
            .into_iter()
            .chain(right.offsets)
            .collect::<Vec<_>>();
        let payload = PySparkUdfPayload::build(
            &definition.python_version,
            &definition.command,
            definition.eval_type,
            &offsets,
            &[],
            &[],
            &self.context.config.pyspark_udf_config,
        )?;
        let function = PySparkCoGroupMapUDF::try_new(
            get_udf_name(function_name.as_ref(), &payload),
            payload,
            deterministic,
            left.types,
            left.names,
            right.types,
            right.names,
            DataType::List(Arc::new(Field::new_list_field(
                DataType::Struct(fields.clone()),
                false,
            ))),
            is_pandas,
            Arc::clone(&self.context.config.pyspark_udf_config),
        )?;
        let mut condition = None;
        for (left, right) in left.keys.iter().zip(&right.keys) {
            let l = self.reference(left)?;
            let r = self.reference(right)?;
            let equality =
                self.context
                    .function_context(&mut self.state.ids)
                    .binary(l, Operator::Eq, r)?;
            condition = Some(if let Some(previous) = condition {
                self.context.function_context(&mut self.state.ids).binary(
                    previous,
                    Operator::And,
                    equality,
                )?
            } else {
                equality
            });
        }
        let mut attributes = left
            .plan
            .schema
            .resolved("co-group left")?
            .attributes
            .iter()
            .chain(&right.plan.schema.resolved("co-group right")?.attributes)
            .cloned()
            .collect::<Vec<_>>();
        if condition.is_some() {
            for attribute in &mut attributes {
                let mut field = attribute
                    .result_type
                    .resolved("co-group outer field")?
                    .clone();
                field.nullable = true;
                attribute.result_type = Deferred::Resolved(field);
            }
        }
        let schema = QuerySchema { attributes };
        let args = [left.value.id, right.value.id]
            .into_iter()
            .map(|id| self.reference(schema.attribute(id)?))
            .collect::<PlanResult<Vec<_>>>()?;
        let expression = self
            .context
            .function_context(&mut self.state.ids)
            .scalar_udf(Arc::new(ScalarUDF::from(function)), args)?;
        let plan = Query {
            schema: Deferred::Resolved(schema),
            kind: Deferred::Resolved(QueryKind::Join {
                lateral: false,
                left: Box::new(left.plan),
                right: Box::new(right.plan),
                kind: if condition.is_some() {
                    JoinType::Full
                } else {
                    JoinType::Inner
                },
                condition: condition.map(Box::new),
            }),
        };
        let plan = self.project(
            plan,
            vec![NamedExpr {
                name: "co_group_map".into(),
                expression,
            }],
        )?;
        let array = plan.schema.resolved("co-group result")?.attributes[0].clone();
        let input = self.unpack_struct_list(plan, &array, &fields)?;
        Ok(Query {
            schema: input.schema.clone(),
            kind: Deferred::Resolved(QueryKind::Scope {
                scope: None,
                input: Box::new(input),
            }),
        })
    }
}
