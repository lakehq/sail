use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion_common::{Column, DFSchema, DFSchemaRef};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{ExprSchemable, ScalarUDF};
use sail_common::spec;
use sail_function::scalar::higher_order::{
    SailArrayAggregate, SailArrayExists, SailArrayFilter, SailArrayForAll, SailArraySort,
    SailArrayTransform, SailArrayZipWith, SailMapFilter, SailMapTransformKeys,
    SailMapTransformValues, SailMapZipWith,
};

use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Simplifies a logical expression (e.g. rewrites `coalesce` to `CASE`)
    /// and then compiles it to a physical expression.
    fn create_simplified_physical_expr(
        &self,
        expr: datafusion_expr::Expr,
        schema: &DFSchemaRef,
    ) -> PlanResult<Arc<dyn datafusion::physical_expr::PhysicalExpr>> {
        let simplify_context = SimplifyContext::default().with_schema(schema.clone());
        let simplifier = ExprSimplifier::new(simplify_context).with_max_cycles(10);
        let simplified = simplifier.simplify(expr)?;
        Ok(self.ctx.create_physical_expr(simplified, schema)?)
    }

    pub(super) async fn resolve_expression_lambda_function(
        &self,
        _function: spec::Expr,
        _arguments: Vec<spec::UnresolvedNamedLambdaVariable>,
        _schema: &DFSchemaRef,
        _state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        Err(PlanError::invalid(
            "lambda function cannot appear outside of a higher-order function call",
        ))
    }

    pub(super) async fn resolve_expression_named_lambda_variable(
        &self,
        variable: spec::UnresolvedNamedLambdaVariable,
        _schema: &DFSchemaRef,
        _state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let parts: Vec<String> = variable.name.into();
        let name = parts
            .into_iter()
            .next()
            .ok_or_else(|| PlanError::invalid("empty lambda variable name"))?;
        let col = datafusion_expr::Expr::Column(Column::new_unqualified(name.clone()));
        Ok(NamedExpr::new(vec![name], col))
    }

    pub(super) async fn resolve_higher_order_function(
        &self,
        function_name: &str,
        arguments: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        match function_name {
            "filter" => self.resolve_hof_filter(arguments, schema, state).await,
            "transform" => self.resolve_hof_transform(arguments, schema, state).await,
            "exists" => self.resolve_hof_exists(arguments, schema, state).await,
            "forall" => self.resolve_hof_forall(arguments, schema, state).await,
            "aggregate" | "reduce" => self.resolve_hof_aggregate(arguments, schema, state).await,
            "zip_with" => self.resolve_hof_zip_with(arguments, schema, state).await,
            "transform_keys" => {
                self.resolve_hof_transform_keys(arguments, schema, state)
                    .await
            }
            "transform_values" => {
                self.resolve_hof_transform_values(arguments, schema, state)
                    .await
            }
            "map_filter" => self.resolve_hof_map_filter(arguments, schema, state).await,
            "map_zip_with" => {
                self.resolve_hof_map_zip_with(arguments, schema, state)
                    .await
            }
            "array_sort" => self.resolve_hof_array_sort(arguments, schema, state).await,
            other => Err(PlanError::unsupported(format!(
                "higher-order function: {other}"
            ))),
        }
    }

    async fn resolve_hof_filter(
        &self,
        arguments: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let mut args = check_arg_count(arguments, 2)?;
        let array_arg = args.remove(0);
        let lambda_arg = args.remove(0);

        let array_expr = self.resolve_expression(array_arg, schema, state).await?;
        let array_type = array_expr.get_type(schema)?;
        let elem_type = list_element_type(&array_type)?;

        let (params, body) = extract_lambda(lambda_arg)?;
        if params.is_empty() || params.len() > 2 {
            return Err(PlanError::invalid(
                "filter lambda must have 1 or 2 parameters",
            ));
        }
        let mut schema_params = vec![(params[0].clone(), elem_type)];
        if params.len() == 2 {
            schema_params.push((params[1].clone(), DataType::Int64));
        }
        let lambda_schema = make_lambda_schema(&schema_params, state)?;
        let body_expr = self.resolve_expression(body, &lambda_schema, state).await?;
        cleanup_lambda_params(&schema_params, state);
        let phys_expr = self.create_simplified_physical_expr(body_expr, &lambda_schema)?;

        let udf = SailArrayFilter::new(phys_expr, params, array_type.clone());
        let func_expr = make_scalar_udf_expr(udf, vec![array_expr]);
        Ok(NamedExpr::new(vec!["filter".to_string()], func_expr))
    }

    async fn resolve_hof_transform(
        &self,
        arguments: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let mut args = check_arg_count(arguments, 2)?;
        let array_arg = args.remove(0);
        let lambda_arg = args.remove(0);

        let array_expr = self.resolve_expression(array_arg, schema, state).await?;
        let array_type = array_expr.get_type(schema)?;
        let elem_type = list_element_type(&array_type)?;

        let (params, body) = extract_lambda(lambda_arg)?;
        if params.is_empty() || params.len() > 2 {
            return Err(PlanError::invalid(
                "transform lambda must have 1 or 2 parameters",
            ));
        }
        let mut schema_params = vec![(params[0].clone(), elem_type)];
        if params.len() == 2 {
            schema_params.push((params[1].clone(), DataType::Int64));
        }
        let lambda_schema = make_lambda_schema(&schema_params, state)?;
        let body_expr = self.resolve_expression(body, &lambda_schema, state).await?;
        cleanup_lambda_params(&schema_params, state);
        let result_elem_type = body_expr.get_type(&lambda_schema)?;
        let phys_expr = self.create_simplified_physical_expr(body_expr, &lambda_schema)?;

        let result_field = Arc::new(Field::new("item", result_elem_type, true));
        let return_type = if is_large_list(&array_type) {
            DataType::LargeList(result_field)
        } else {
            DataType::List(result_field)
        };
        let udf = SailArrayTransform::new(phys_expr, params, return_type.clone());
        let func_expr = make_scalar_udf_expr(udf, vec![array_expr]);
        Ok(NamedExpr::new(vec!["transform".to_string()], func_expr))
    }

    async fn resolve_hof_exists(
        &self,
        arguments: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let mut args = check_arg_count(arguments, 2)?;
        let array_arg = args.remove(0);
        let lambda_arg = args.remove(0);

        let array_expr = self.resolve_expression(array_arg, schema, state).await?;
        let array_type = array_expr.get_type(schema)?;
        let elem_type = list_element_type(&array_type)?;

        let (params, body) = extract_lambda(lambda_arg)?;
        let param = check_lambda_params(params, 1)?.remove(0);

        let schema_params = [(param.clone(), elem_type)];
        let lambda_schema = make_lambda_schema(&schema_params, state)?;
        let body_expr = self.resolve_expression(body, &lambda_schema, state).await?;
        cleanup_lambda_params(&schema_params, state);
        let phys_expr = self.create_simplified_physical_expr(body_expr, &lambda_schema)?;

        let udf = SailArrayExists::new(phys_expr, param);
        let func_expr = make_scalar_udf_expr(udf, vec![array_expr]);
        Ok(NamedExpr::new(vec!["exists".to_string()], func_expr))
    }

    async fn resolve_hof_forall(
        &self,
        arguments: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let mut args = check_arg_count(arguments, 2)?;
        let array_arg = args.remove(0);
        let lambda_arg = args.remove(0);

        let array_expr = self.resolve_expression(array_arg, schema, state).await?;
        let array_type = array_expr.get_type(schema)?;
        let elem_type = list_element_type(&array_type)?;

        let (params, body) = extract_lambda(lambda_arg)?;
        let param = check_lambda_params(params, 1)?.remove(0);

        let schema_params = [(param.clone(), elem_type)];
        let lambda_schema = make_lambda_schema(&schema_params, state)?;
        let body_expr = self.resolve_expression(body, &lambda_schema, state).await?;
        cleanup_lambda_params(&schema_params, state);
        let phys_expr = self.create_simplified_physical_expr(body_expr, &lambda_schema)?;

        let udf = SailArrayForAll::new(phys_expr, param);
        let func_expr = make_scalar_udf_expr(udf, vec![array_expr]);
        Ok(NamedExpr::new(vec!["forall".to_string()], func_expr))
    }

    async fn resolve_hof_aggregate(
        &self,
        arguments: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        if arguments.len() < 3 || arguments.len() > 4 {
            return Err(PlanError::invalid("aggregate requires 3 or 4 arguments"));
        }
        let mut args_iter = arguments.into_iter();
        let array_arg = args_iter
            .next()
            .ok_or_else(|| PlanError::invalid("aggregate: missing array argument"))?;
        let init_arg = args_iter
            .next()
            .ok_or_else(|| PlanError::invalid("aggregate: missing init argument"))?;
        let merge_lambda = args_iter
            .next()
            .ok_or_else(|| PlanError::invalid("aggregate: missing merge lambda"))?;
        let finish_lambda = args_iter.next();

        let array_expr = self.resolve_expression(array_arg, schema, state).await?;
        let array_type = array_expr.get_type(schema)?;
        let elem_type = list_element_type(&array_type)?;

        let init_expr = self.resolve_expression(init_arg, schema, state).await?;
        let acc_type = init_expr.get_type(schema)?;

        let (merge_params, merge_body) = extract_lambda(merge_lambda)?;
        let merge_params = check_lambda_params(merge_params, 2)?;
        let acc_param = merge_params[0].clone();
        let elem_param = merge_params[1].clone();

        let merge_schema_params = [
            (acc_param.clone(), acc_type.clone()),
            (elem_param.clone(), elem_type),
        ];
        let merge_schema = make_lambda_schema(&merge_schema_params, state)?;
        let merge_body_expr = self
            .resolve_expression(merge_body, &merge_schema, state)
            .await?;
        cleanup_lambda_params(&merge_schema_params, state);
        let merge_phys = self.create_simplified_physical_expr(merge_body_expr, &merge_schema)?;

        let (finish_phys, finish_param, result_type) = if let Some(finish_lam) = finish_lambda {
            let (finish_params, finish_body) = extract_lambda(finish_lam)?;
            let finish_params = check_lambda_params(finish_params, 1)?;
            let fp = finish_params[0].clone();
            let finish_schema_params = [(fp.clone(), acc_type.clone())];
            let finish_schema = make_lambda_schema(&finish_schema_params, state)?;
            let finish_body_expr = self
                .resolve_expression(finish_body, &finish_schema, state)
                .await?;
            cleanup_lambda_params(&finish_schema_params, state);
            let result_type = finish_body_expr.get_type(&finish_schema)?;
            let finish_phys =
                self.create_simplified_physical_expr(finish_body_expr, &finish_schema)?;
            (Some(finish_phys), fp, result_type)
        } else {
            (None, String::new(), acc_type)
        };

        let udf = SailArrayAggregate::new(
            merge_phys,
            acc_param,
            elem_param,
            finish_phys,
            finish_param,
            result_type,
        );
        let func_expr = make_scalar_udf_expr(udf, vec![array_expr, init_expr]);
        Ok(NamedExpr::new(vec!["aggregate".to_string()], func_expr))
    }

    async fn resolve_hof_zip_with(
        &self,
        arguments: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let mut args = check_arg_count(arguments, 3)?;
        let arr1_arg = args.remove(0);
        let arr2_arg = args.remove(0);
        let lambda_arg = args.remove(0);

        let arr1_expr = self.resolve_expression(arr1_arg, schema, state).await?;
        let arr2_expr = self.resolve_expression(arr2_arg, schema, state).await?;
        let arr1_type = arr1_expr.get_type(schema)?;
        let arr2_type = arr2_expr.get_type(schema)?;
        let elem1_type = list_element_type(&arr1_type)?;
        let elem2_type = list_element_type(&arr2_type)?;

        let (params, body) = extract_lambda(lambda_arg)?;
        let params = check_lambda_params(params, 2)?;

        let schema_params = [
            (params[0].clone(), elem1_type),
            (params[1].clone(), elem2_type),
        ];
        let lambda_schema = make_lambda_schema(&schema_params, state)?;
        let body_expr = self.resolve_expression(body, &lambda_schema, state).await?;
        cleanup_lambda_params(&schema_params, state);
        let result_elem_type = body_expr.get_type(&lambda_schema)?;
        let phys_expr = self.create_simplified_physical_expr(body_expr, &lambda_schema)?;

        let result_field = Arc::new(Field::new("item", result_elem_type, true));
        let return_type = if is_large_list(&arr1_type) {
            DataType::LargeList(result_field)
        } else {
            DataType::List(result_field)
        };
        let udf = SailArrayZipWith::new(phys_expr, params, return_type);
        let func_expr = make_scalar_udf_expr(udf, vec![arr1_expr, arr2_expr]);
        Ok(NamedExpr::new(vec!["zip_with".to_string()], func_expr))
    }

    async fn resolve_hof_transform_keys(
        &self,
        arguments: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let mut args = check_arg_count(arguments, 2)?;
        let map_arg = args.remove(0);
        let lambda_arg = args.remove(0);

        let map_expr = self.resolve_expression(map_arg, schema, state).await?;
        let map_type = map_expr.get_type(schema)?;
        let (key_type, val_type) = map_key_value_types(&map_type)?;

        let (params, body) = extract_lambda(lambda_arg)?;
        let params = check_lambda_params(params, 2)?;

        let schema_params = [
            (params[0].clone(), key_type),
            (params[1].clone(), val_type.clone()),
        ];
        let lambda_schema = make_lambda_schema(&schema_params, state)?;
        let body_expr = self.resolve_expression(body, &lambda_schema, state).await?;
        cleanup_lambda_params(&schema_params, state);
        let new_key_type = body_expr.get_type(&lambda_schema)?;
        let phys_expr = self.create_simplified_physical_expr(body_expr, &lambda_schema)?;

        let return_type = build_map_type(new_key_type, val_type, false);
        let udf =
            SailMapTransformKeys::new(phys_expr, params[0].clone(), params[1].clone(), return_type);
        let func_expr = make_scalar_udf_expr(udf, vec![map_expr]);
        Ok(NamedExpr::new(
            vec!["transform_keys".to_string()],
            func_expr,
        ))
    }

    async fn resolve_hof_transform_values(
        &self,
        arguments: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let mut args = check_arg_count(arguments, 2)?;
        let map_arg = args.remove(0);
        let lambda_arg = args.remove(0);

        let map_expr = self.resolve_expression(map_arg, schema, state).await?;
        let map_type = map_expr.get_type(schema)?;
        let (key_type, val_type) = map_key_value_types(&map_type)?;

        let (params, body) = extract_lambda(lambda_arg)?;
        let params = check_lambda_params(params, 2)?;

        let schema_params = [
            (params[0].clone(), key_type.clone()),
            (params[1].clone(), val_type),
        ];
        let lambda_schema = make_lambda_schema(&schema_params, state)?;
        let body_expr = self.resolve_expression(body, &lambda_schema, state).await?;
        cleanup_lambda_params(&schema_params, state);
        let new_val_type = body_expr.get_type(&lambda_schema)?;
        let phys_expr = self.create_simplified_physical_expr(body_expr, &lambda_schema)?;

        let return_type = build_map_type(key_type, new_val_type, false);
        let udf = SailMapTransformValues::new(
            phys_expr,
            params[0].clone(),
            params[1].clone(),
            return_type,
        );
        let func_expr = make_scalar_udf_expr(udf, vec![map_expr]);
        Ok(NamedExpr::new(
            vec!["transform_values".to_string()],
            func_expr,
        ))
    }

    async fn resolve_hof_map_filter(
        &self,
        arguments: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let mut args = check_arg_count(arguments, 2)?;
        let map_arg = args.remove(0);
        let lambda_arg = args.remove(0);

        let map_expr = self.resolve_expression(map_arg, schema, state).await?;
        let map_type = map_expr.get_type(schema)?;
        let (key_type, val_type) = map_key_value_types(&map_type)?;

        let (params, body) = extract_lambda(lambda_arg)?;
        let params = check_lambda_params(params, 2)?;

        let schema_params = [(params[0].clone(), key_type), (params[1].clone(), val_type)];
        let lambda_schema = make_lambda_schema(&schema_params, state)?;
        let body_expr = self.resolve_expression(body, &lambda_schema, state).await?;
        cleanup_lambda_params(&schema_params, state);
        let phys_expr = self.create_simplified_physical_expr(body_expr, &lambda_schema)?;

        let return_type = map_type.clone();
        let udf = SailMapFilter::new(phys_expr, params[0].clone(), params[1].clone(), return_type);
        let func_expr = make_scalar_udf_expr(udf, vec![map_expr]);
        Ok(NamedExpr::new(vec!["map_filter".to_string()], func_expr))
    }

    async fn resolve_hof_map_zip_with(
        &self,
        arguments: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let mut args = check_arg_count(arguments, 3)?;
        let map1_arg = args.remove(0);
        let map2_arg = args.remove(0);
        let lambda_arg = args.remove(0);

        let map1_expr = self.resolve_expression(map1_arg, schema, state).await?;
        let map2_expr = self.resolve_expression(map2_arg, schema, state).await?;
        let map1_type = map1_expr.get_type(schema)?;
        let map2_type = map2_expr.get_type(schema)?;
        let (key1_type, val1_type) = map_key_value_types(&map1_type)?;
        let (_, val2_type) = map_key_value_types(&map2_type)?;

        let (params, body) = extract_lambda(lambda_arg)?;
        let params = check_lambda_params(params, 3)?;

        let schema_params = [
            (params[0].clone(), key1_type.clone()),
            (params[1].clone(), val1_type),
            (params[2].clone(), val2_type),
        ];
        let lambda_schema = make_lambda_schema(&schema_params, state)?;
        let body_expr = self.resolve_expression(body, &lambda_schema, state).await?;
        cleanup_lambda_params(&schema_params, state);
        let result_val_type = body_expr.get_type(&lambda_schema)?;
        let phys_expr = self.create_simplified_physical_expr(body_expr, &lambda_schema)?;

        let return_type = build_map_type(key1_type, result_val_type, false);
        let udf = SailMapZipWith::new(
            phys_expr,
            params[0].clone(),
            params[1].clone(),
            params[2].clone(),
            return_type,
        );
        let func_expr = make_scalar_udf_expr(udf, vec![map1_expr, map2_expr]);
        Ok(NamedExpr::new(vec!["map_zip_with".to_string()], func_expr))
    }

    async fn resolve_hof_array_sort(
        &self,
        arguments: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let mut args = check_arg_count(arguments, 2)?;
        let array_arg = args.remove(0);
        let lambda_arg = args.remove(0);

        let array_expr = self.resolve_expression(array_arg, schema, state).await?;
        let array_type = array_expr.get_type(schema)?;
        let elem_type = list_element_type(&array_type)?;

        let (params, body) = extract_lambda(lambda_arg)?;
        let params = check_lambda_params(params, 2)?;

        let schema_params = [
            (params[0].clone(), elem_type.clone()),
            (params[1].clone(), elem_type),
        ];
        let lambda_schema = make_lambda_schema(&schema_params, state)?;
        let body_expr = self.resolve_expression(body, &lambda_schema, state).await?;
        cleanup_lambda_params(&schema_params, state);
        let phys_expr = self.create_simplified_physical_expr(body_expr, &lambda_schema)?;

        let udf = SailArraySort::new(phys_expr, params, array_type.clone());
        let func_expr = make_scalar_udf_expr(udf, vec![array_expr]);
        Ok(NamedExpr::new(vec!["array_sort".to_string()], func_expr))
    }
}

// ----------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------

fn make_lambda_schema(
    params: &[(String, DataType)],
    state: &mut PlanResolverState,
) -> PlanResult<DFSchemaRef> {
    // Register each lambda parameter in state using the param name as the field ID
    // so that attribute resolution can find them during body expression resolution.
    let fields: Vec<Field> = params
        .iter()
        .map(|(name, dtype)| {
            state.register_lambda_param(name);
            Field::new(name.as_str(), dtype.clone(), true)
        })
        .collect();
    let arrow_schema = Schema::new(fields);
    let df_schema = DFSchema::try_from(arrow_schema)?;
    Ok(Arc::new(df_schema))
}

fn cleanup_lambda_params(params: &[(String, DataType)], state: &mut PlanResolverState) {
    for (name, _) in params {
        state.unregister_field(name);
    }
}

fn list_element_type(dt: &DataType) -> PlanResult<DataType> {
    match dt {
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            Ok(f.data_type().clone())
        }
        _ => Err(PlanError::invalid(format!(
            "expected array type, got {:?}",
            dt
        ))),
    }
}

fn map_key_value_types(dt: &DataType) -> PlanResult<(DataType, DataType)> {
    match dt {
        DataType::Map(field, _) => {
            if let DataType::Struct(fields) = field.data_type() {
                let key_type = fields[0].data_type().clone();
                let value_type = fields[1].data_type().clone();
                Ok((key_type, value_type))
            } else {
                Err(PlanError::invalid(format!(
                    "expected map with struct entries, got {:?}",
                    dt
                )))
            }
        }
        _ => Err(PlanError::invalid(format!(
            "expected map type, got {:?}",
            dt
        ))),
    }
}

fn is_large_list(dt: &DataType) -> bool {
    matches!(dt, DataType::LargeList(_))
}

fn build_map_type(key_type: DataType, val_type: DataType, sorted: bool) -> DataType {
    let entries_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", key_type, false),
            Field::new("value", val_type, true),
        ])),
        false,
    ));
    DataType::Map(entries_field, sorted)
}

fn extract_lambda(expr: spec::Expr) -> PlanResult<(Vec<String>, spec::Expr)> {
    match expr {
        spec::Expr::LambdaFunction {
            function,
            arguments,
        } => {
            let params: Vec<String> = arguments
                .into_iter()
                .map(|v| {
                    let parts: Vec<String> = v.name.into();
                    parts.into_iter().next().unwrap_or_default()
                })
                .collect();
            Ok((params, *function))
        }
        other => Err(PlanError::invalid(format!(
            "expected lambda function, got {:?}",
            other
        ))),
    }
}

fn check_arg_count(args: Vec<spec::Expr>, expected: usize) -> PlanResult<Vec<spec::Expr>> {
    if args.len() != expected {
        return Err(PlanError::invalid(format!(
            "expected {} arguments, got {}",
            expected,
            args.len()
        )));
    }
    Ok(args)
}

fn check_lambda_params(params: Vec<String>, expected: usize) -> PlanResult<Vec<String>> {
    if params.len() != expected {
        return Err(PlanError::invalid(format!(
            "expected lambda with {} parameters, got {}",
            expected,
            params.len()
        )));
    }
    Ok(params)
}

fn make_scalar_udf_expr<U: datafusion_expr::ScalarUDFImpl + 'static>(
    udf_impl: U,
    args: Vec<datafusion_expr::Expr>,
) -> datafusion_expr::Expr {
    let udf = ScalarUDF::new_from_impl(udf_impl);
    datafusion_expr::Expr::ScalarFunction(ScalarFunction {
        func: Arc::new(udf),
        args,
    })
}
