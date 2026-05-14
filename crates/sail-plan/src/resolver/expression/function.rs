use datafusion_common::DFSchemaRef;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::utils::{expand_qualified_wildcard, expand_wildcard};
use datafusion_expr::{expr, EmptyRelation, Expr, LogicalPlan};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::multi_expr::MultiExpr;
use sail_python_udf::udf::pyspark_unresolved_udf::PySparkUnresolvedUDF;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{AggFunctionInput, FunctionContextInput, ScalarFunctionInput};
use crate::function::{get_built_in_aggregate_function, get_built_in_function};
use crate::resolver::expression::NamedExpr;
use crate::resolver::function::PythonUdf;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_function(
        &self,
        function: spec::UnresolvedFunction,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let mut scope = state.enter_config_scope();
        let state = scope.state();
        let spec::UnresolvedFunction {
            function_name,
            arguments,
            named_arguments,
            is_distinct,
            is_user_defined_function: _,
            is_internal: _,
            ignore_nulls,
            filter,
            order_by,
        } = function;

        let Ok(function_name) = <Vec<String>>::from(function_name).one() else {
            return Err(PlanError::unsupported("qualified function name"));
        };
        // Extract any NamedArgument entries embedded in arguments (Spark Connect inline path).
        // PySpark encodes kwargs as NamedArgumentExpression inside the arguments[] repeated field,
        // which are converted to spec::Expr::NamedArgument. extract_kwargs peels those out before
        // resolve_expressions_and_names, which would otherwise reject them as standalone expressions.
        let (mut arguments, mut kwarg_names) = Self::extract_kwargs(arguments);
        // Also merge named_arguments from spec::UnresolvedFunction (SQL analyzer path).
        // PySpark sends registered-UDF kwargs here rather than as embedded NamedArgument entries.
        for (key, value) in named_arguments {
            kwarg_names.push(Some(key.into()));
            arguments.push(value);
        }

        // Validate named arguments: reject duplicate kwarg names early so we surface
        // a proper AnalysisException instead of letting it crash in the Python worker.
        {
            let mut seen_kwarg_names = std::collections::HashSet::new();
            for name in kwarg_names.iter().flatten() {
                if !seen_kwarg_names.insert(name.as_str()) {
                    return Err(PlanError::AnalysisError(format!(
                        "[DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT.DOUBLE_NAMED_ARGUMENT_REFERENCE] \
                         Duplicate named argument: '{name}' is assigned more than once."
                    )));
                }
            }
        }

        let canonical_function_name = function_name.to_ascii_lowercase();
        let catalog_manager = self.ctx.extension::<CatalogManager>()?;
        if let Some(udf) = catalog_manager.get_function(&canonical_function_name)? {
            if udf.inner().as_any().is::<PySparkUnresolvedUDF>() {
                state.config_mut().arrow_allow_large_var_types = true;
            }
        }

        // For functions that accept a date-part keyword as the first argument
        // (e.g., DATEDIFF(DAY, start, end)), convert the unresolved attribute
        // to a string literal before resolution.
        let arguments = Self::convert_date_part_argument(&canonical_function_name, arguments);

        let (argument_display_names, arguments, argument_metadatas) =
            if canonical_function_name == "struct" {
                let (names, exprs) = self
                    .resolve_struct_expressions_and_names(arguments, schema, state)
                    .await?;
                // TODO: Surface per-argument metadata for struct and attach it to
                // each inner Field of the resulting Struct Arrow type, so e.g.
                // `struct(INTERVAL '10' YEAR)` keeps its inner field's interval
                // qualifier extension metadata. The propagation rule below cannot
                // express this because the function's output type (Struct{...})
                // never equals a single argument's type — struct metadata flows
                // into *inner* fields, not the outer Field.
                (names, exprs, None)
            } else {
                let named = self
                    .resolve_named_expressions(arguments, schema, state)
                    .await?;
                let mut names = Vec::with_capacity(named.len());
                let mut exprs = Vec::with_capacity(named.len());
                let mut metadatas = Vec::with_capacity(named.len());
                for n in named {
                    names.push(n.name.one()?);
                    exprs.push(n.expr);
                    metadatas.push(n.metadata);
                }
                (names, exprs, Some(metadatas))
            };

        let argument_types: Vec<Option<datafusion::arrow::datatypes::DataType>> = {
            use datafusion_expr::ExprSchemable;
            arguments.iter().map(|a| a.get_type(schema).ok()).collect()
        };

        // FIXME: `is_user_defined_function` is always false,
        //   so we need to check UDFs before built-in functions.
        let func = if let Some(udf) = catalog_manager.get_function(&canonical_function_name)? {
            if ignore_nulls.is_some() || filter.is_some() || order_by.is_some() {
                return Err(PlanError::invalid("invalid scalar function clause"));
            }
            if let Some(f) = udf.inner().as_any().downcast_ref::<PySparkUnresolvedUDF>() {
                if f.eval_type().is_table_function() {
                    return Err(PlanError::AnalysisError(format!(
                        "user-defined table function cannot be used as a scalar function: {function_name}"
                    )));
                }
                let output_type = f.output_type().cloned().ok_or_else(|| {
                    PlanError::internal(format!(
                        "unresolved UDF {function_name} has no scalar return type"
                    ))
                })?;
                let function = PythonUdf {
                    python_version: f.python_version().to_string(),
                    eval_type: f.eval_type(),
                    command: f.command().to_vec(),
                    output_type,
                };
                self.resolve_python_udf_expr(
                    function,
                    &function_name,
                    arguments,
                    &argument_display_names,
                    &kwarg_names, // pass kwargs from named_arguments
                    schema,
                    f.deterministic(),
                    is_distinct,
                    state,
                )?
            } else {
                expr::Expr::ScalarFunction(ScalarFunction {
                    func: std::sync::Arc::new(udf),
                    args: arguments,
                })
            }
        } else if let Ok(func) = get_built_in_function(&canonical_function_name) {
            if ignore_nulls.is_some() || filter.is_some() || order_by.is_some() {
                return Err(PlanError::invalid("invalid scalar function clause"));
            }
            let input = ScalarFunctionInput {
                arguments,
                function_context: FunctionContextInput {
                    argument_display_names: &argument_display_names,
                    plan_config: &self.config,
                    session_context: self.ctx,
                    schema,
                },
            };
            func(input)?
        } else if let Ok(func) = get_built_in_aggregate_function(&canonical_function_name) {
            let filter = match filter {
                Some(x) => Some(Box::new(self.resolve_expression(*x, schema, state).await?)),
                None => None,
            };
            let order_by = match order_by {
                Some(x) => self.resolve_sort_orders(x, true, schema, state).await?,
                None => vec![],
            };
            // For DISTINCT aggregate functions with a wildcard argument (e.g., COUNT(DISTINCT *)),
            // expand the wildcard to visible column references here in the resolver where we have
            // access to `state` for hidden-column filtering. This ensures hidden columns (e.g.,
            // join keys) are excluded from the distinct count.
            #[expect(deprecated)]
            let arguments = if is_distinct
                && matches!(
                    arguments.as_slice(),
                    [expr::Expr::Wildcard {
                        qualifier: None,
                        options: _
                    }]
                ) {
                schema
                    .columns()
                    .into_iter()
                    .filter(|c| {
                        state
                            .get_field_info(&c.name)
                            .is_ok_and(|info| !info.is_hidden())
                    })
                    .map(expr::Expr::Column)
                    .collect()
            } else {
                arguments
            };
            let input = AggFunctionInput {
                arguments,
                distinct: is_distinct,
                ignore_nulls,
                filter,
                order_by,
                function_context: FunctionContextInput {
                    argument_display_names: &argument_display_names,
                    plan_config: &self.config,
                    session_context: self.ctx,
                    schema,
                },
            };
            func(input)?
        } else {
            return Err(PlanError::unsupported(format!(
                "unknown function: {function_name}",
            )));
        };

        // When `COUNT(DISTINCT *)` is used, expand the wildcard display names
        // to individual column names so the output header matches Spark JVM behavior
        // (e.g., `count(DISTINCT a, b, c)` instead of `count(DISTINCT *)`).
        let argument_display_names =
            if is_distinct && argument_display_names.iter().any(|n| n == "*") {
                schema
                    .columns()
                    .iter()
                    .filter_map(|c| {
                        let info = state.get_field_info(&c.name).ok()?;
                        if info.is_hidden() {
                            None
                        } else {
                            Some(info.name().to_string())
                        }
                    })
                    .collect::<Vec<_>>()
            } else {
                argument_display_names
            };
        // Spark canonicalizes `Numeric * Interval` to `Interval * Numeric` in
        // the column header (multiplication is commutative). The expression
        // itself stays unswapped — the SparkTryMult / Duration paths in
        // `spark_multiply` handle either operand order — but the display name
        // must match Spark's normalized form.
        let argument_display_names = if canonical_function_name == "*"
            && argument_types.len() == 2
            && argument_types[0].as_ref().is_some_and(|t| t.is_numeric())
            && argument_types[1].as_ref().is_some_and(|t| {
                matches!(
                    t,
                    datafusion::arrow::datatypes::DataType::Interval(_)
                        | datafusion::arrow::datatypes::DataType::Duration(_)
                )
            }) {
            let mut swapped = argument_display_names;
            swapped.swap(0, 1);
            swapped
        } else {
            argument_display_names
        };
        let service = self.ctx.extension::<PlanService>()?;
        let name = service.plan_formatter().function_to_string(
            &function_name,
            argument_display_names.iter().map(|x| x.as_str()).collect(),
            is_distinct,
        )?;

        // Extract metadata from UDF if it implements return_field_from_args.
        let mut metadata =
            if let expr::Expr::ScalarFunction(ScalarFunction {
                func: udf, args, ..
            }) = &func
            {
                extract_metadata_from_udf(udf, args, schema, argument_metadatas.as_deref())?
            } else {
                vec![]
            };

        // Fall back to propagating a single argument's metadata when the
        // function's output Arrow type matches that argument's type. This is
        // how qualifier extension metadata on interval literals survives
        // type-preserving operations like unary minus (`-INTERVAL '10' YEAR`,
        // which lowers to `Expr::Negative` for year-month and to the
        // `NegateDuration` UDF — neither of which currently sets output
        // metadata on its own) — the inner Cast attaches the metadata to its
        // NamedExpr and without this fallback the call would drop it.
        //
        // FIXME: When multiple arguments share the function's output type
        // (e.g. `INTERVAL '10' YEAR + INTERVAL '5' MONTH` — both args are
        // `Interval(YearMonth)`) but carry different metadata, we
        // conservatively skip propagation. The right behavior depends on the
        // operation (e.g. Spark widens `YEAR + MONTH` to `YEAR TO MONTH`),
        // which this generic rule cannot express. The output column falls
        // back to the default formatter; not incorrect, just lossy.
        if metadata.is_empty() {
            use datafusion_expr::ExprSchemable;
            if let (Some(metas), Ok(output_type)) = (&argument_metadatas, func.get_type(schema)) {
                let matching: Vec<&Vec<(String, String)>> = argument_types
                    .iter()
                    .zip(metas.iter())
                    .filter_map(|(arg_type, meta)| {
                        if !meta.is_empty() && arg_type.as_ref() == Some(&output_type) {
                            Some(meta)
                        } else {
                            None
                        }
                    })
                    .collect();
                match matching.as_slice() {
                    [single] => {
                        // `Interval × Numeric` / `Interval / Numeric` widens the
                        // result to the broadest qualifier in the input's family
                        // (Spark semantics), unlike type-preserving ops like
                        // unary minus which keep the input qualifier as-is.
                        let widens_to_broadest =
                            matches!(canonical_function_name.as_str(), "*" | "/");
                        if widens_to_broadest {
                            if let Some(widened) =
                                broadest_interval_qualifier_metadata(&output_type)
                            {
                                metadata = widened;
                            } else {
                                metadata = (*single).clone();
                            }
                        } else {
                            metadata = (*single).clone();
                        }
                    }
                    multi if multi.len() > 1 => {
                        let widened_result = widen_interval_qualifier_metadata(multi, &output_type);
                        if let Some(widened) = widened_result {
                            metadata = widened;
                        }
                    }
                    _ => {}
                }
            }
        }

        if !metadata.is_empty() {
            Ok(NamedExpr::new(vec![name], func).with_metadata(metadata))
        } else {
            Ok(NamedExpr::new(vec![name], func))
        }
    }

    pub(super) async fn resolve_expression_call_function(
        &self,
        function_name: spec::ObjectName,
        arguments: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let function = spec::UnresolvedFunction {
            function_name,
            arguments,
            named_arguments: vec![],
            is_distinct: false,
            is_user_defined_function: false,
            is_internal: None,
            ignore_nulls: None,
            filter: None,
            order_by: None,
        };
        self.resolve_expression_function(function, schema, state)
            .await
    }

    async fn resolve_struct_expressions_and_names(
        &self,
        expressions: Vec<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<(Vec<String>, Vec<expr::Expr>)> {
        fn column_display_name(
            column: &datafusion_common::Column,
            state: &PlanResolverState,
        ) -> PlanResult<Option<String>> {
            let info = state.get_field_info(column.name())?;
            if info.is_hidden() {
                return Ok(None);
            }
            Ok(Some(info.name().to_string()))
        }

        let mut names: Vec<String> = vec![];
        let mut exprs: Vec<expr::Expr> = vec![];

        for expression in expressions {
            let NamedExpr { name, expr, .. } = self
                .resolve_named_expression(expression, schema, state)
                .await?;

            match expr {
                // Expand wildcard inside `struct(...)` only, to match Spark behavior:
                // - struct(*) expands to all visible columns
                // - struct(alias.*) expands to all visible columns from that qualifier
                #[expect(deprecated)]
                Expr::Wildcard { qualifier, options } => {
                    let plan = LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    });

                    let expanded = match qualifier {
                        Some(q) => expand_qualified_wildcard(&q, schema, Some(&options))?,
                        None => expand_wildcard(schema, &plan, Some(&options))?,
                    };

                    for e in expanded {
                        let Expr::Column(column) = e else {
                            return Err(PlanError::internal(format!(
                                "column expected for expanded wildcard expression in struct, got: {e:?}"
                            )));
                        };
                        if let Some(display_name) = column_display_name(&column, state)? {
                            names.push(display_name);
                            exprs.push(Expr::Column(column));
                        }
                    }
                }

                // Nested-field wildcard expansion can produce a MultiExpr.
                // Flatten it so `struct(a.*)` can become `struct(a.x, a.y, ...)`.
                Expr::ScalarFunction(ScalarFunction { func, args })
                    if func.inner().as_any().is::<MultiExpr>() =>
                {
                    if name.len() == args.len() {
                        for (n, arg) in name.into_iter().zip(args) {
                            let field_name = n;
                            names.push(field_name.clone());
                            exprs.push(Expr::Alias(expr::Alias::new(
                                arg,
                                None::<datafusion_common::TableReference>,
                                field_name,
                            )));
                        }
                    } else {
                        return Err(PlanError::internal(format!(
                            "MultiExpr in struct argument must provide one name per expression, got {} names for {} expressions",
                            name.len(),
                            args.len()
                        )));
                    }
                }

                other => {
                    names.push(name.one()?);
                    exprs.push(other);
                }
            }
        }

        Ok((names, exprs))
    }

    /// For functions that accept a date-part keyword as their first argument
    /// (e.g., `DATEDIFF(DAY, start, end)`, `TIMESTAMPDIFF(HOUR, start, end)`),
    /// convert the first argument from an unresolved attribute to a string literal.
    fn convert_date_part_argument(
        function_name: &str,
        mut arguments: Vec<spec::Expr>,
    ) -> Vec<spec::Expr> {
        const DATE_PART_FUNCTIONS: &[&str] = &["datediff", "date_diff", "timestampdiff"];
        if arguments.len() >= 3 && DATE_PART_FUNCTIONS.contains(&function_name) {
            if let spec::Expr::UnresolvedAttribute { ref name, .. } = arguments[0] {
                let parts: Vec<String> = name.clone().into();
                if parts.len() == 1 {
                    arguments[0] = spec::Expr::Literal(spec::Literal::Utf8 {
                        value: Some(parts.into_iter().next().unwrap_or_default()),
                    });
                }
            }
        }
        arguments
    }
}

/// Extract metadata from a UDF by calling `return_field_from_args` with real
/// argument information. `schema` is the schema in scope so column references
/// resolve to their actual types (rather than defaulting to `Null` under an
/// empty schema), and `arg_metadatas` carries each argument's `NamedExpr`
/// metadata — without it, a UDF that wants to propagate input field metadata
/// (e.g. interval qualifier extension metadata through `NegateDuration`) sees
/// empty input fields and has nothing to forward.
fn extract_metadata_from_udf(
    udf: &std::sync::Arc<datafusion_expr::ScalarUDF>,
    args: &[expr::Expr],
    schema: &datafusion_common::DFSchemaRef,
    arg_metadatas: Option<&[Vec<(String, String)>]>,
) -> PlanResult<Vec<(String, String)>> {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ExprSchemable, ReturnFieldArgs};

    let arg_fields: Vec<Arc<Field>> = args
        .iter()
        .enumerate()
        .map(|(i, arg)| {
            let data_type = arg.get_type(schema.as_ref()).unwrap_or(DataType::Null);
            let mut field = Field::new(format!("arg_{}", i), data_type, true);
            if let Some(meta) = arg_metadatas.and_then(|m| m.get(i)) {
                if !meta.is_empty() {
                    let map: HashMap<String, String> = meta.iter().cloned().collect();
                    field = field.with_metadata(map);
                }
            }
            Arc::new(field)
        })
        .collect();

    // Extract literal scalar values from expressions
    let mut scalar_values = Vec::new();

    for arg in args {
        if let expr::Expr::Literal(scalar_value, _) = arg {
            scalar_values.push(scalar_value.clone());
        } else {
            scalar_values.push(ScalarValue::Null);
        }
    }

    let scalar_refs: Vec<Option<&ScalarValue>> = scalar_values
        .iter()
        .map(|v| if v.is_null() { None } else { Some(v) })
        .collect();

    let return_field_args = ReturnFieldArgs {
        arg_fields: &arg_fields,
        scalar_arguments: &scalar_refs,
    };

    // Try to extract metadata, but don't fail if it doesn't work
    if let Ok(field) = udf.return_field_from_args(return_field_args) {
        Ok(field
            .metadata()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    } else {
        Ok(vec![])
    }
}

/// Widen multiple `spark.interval` qualifier metadatas to the broadest
/// covering `(start_field, end_field)` pair within a single qualifier family
/// (year-month or day-time). Used when a type-preserving function call has
/// more than one input argument that shares the output Arrow type *and*
/// carries interval qualifier metadata — e.g. `INTERVAL '10' YEAR + INTERVAL
/// '5' MONTH` should widen to `YEAR TO MONTH`. The family is determined by
/// `output_type` so the numeric `(start, end)` encoding is interpreted in the
/// right namespace (the same `(0, 0)` pair means `YEAR` for year-month and
/// `DAY` for day-time). Returns `None` if any input isn't a valid Spark
/// interval qualifier in the chosen family, in which case the caller leaves
/// output metadata empty and the default formatter takes over.
fn widen_interval_qualifier_metadata(
    metas: &[&Vec<(String, String)>],
    output_type: &datafusion::arrow::datatypes::DataType,
) -> Option<Vec<(String, String)>> {
    use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
    use sail_common::interval::{IntervalQualifierMetadata, SparkIntervalKind};
    use sail_common::spec::{
        EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY, SAIL_INTERVAL_EXTENSION_NAME,
    };

    let from_fields: fn(i32, i32) -> Option<SparkIntervalKind> = match output_type {
        DataType::Interval(IntervalUnit::YearMonth) => SparkIntervalKind::from_year_month_fields,
        DataType::Duration(TimeUnit::Microsecond) => SparkIntervalKind::from_day_time_fields,
        _ => return None,
    };

    let mut min_start: Option<i32> = None;
    let mut max_end: Option<i32> = None;
    for meta in metas {
        let name = meta
            .iter()
            .find(|(k, _)| k == EXTENSION_TYPE_NAME_KEY)
            .map(|(_, v)| v.as_str());
        if name != Some(SAIL_INTERVAL_EXTENSION_NAME) {
            return None;
        }
        let json = meta
            .iter()
            .find(|(k, _)| k == EXTENSION_TYPE_METADATA_KEY)
            .map(|(_, v)| v.as_str())?;
        let parsed: IntervalQualifierMetadata = serde_json::from_str(json).ok()?;
        let (start, end) = (parsed.start_field?, parsed.end_field?);
        // Reject inputs that don't form a valid kind in the chosen family —
        // arithmetic typing should already reject mixed kinds upstream, but
        // we validate so an upstream bug can't produce an invalid qualifier.
        from_fields(start, end)?;
        min_start = Some(min_start.map_or(start, |s| s.min(start)));
        max_end = Some(max_end.map_or(end, |e| e.max(end)));
    }
    let start = min_start?;
    let end = max_end?;
    // Year-month and day-time qualifier sets are both closed under widening,
    // so this can only fail if an earlier `from_fields` accepted something
    // malformed.
    from_fields(start, end)?;
    let widened = IntervalQualifierMetadata {
        start_field: Some(start),
        end_field: Some(end),
    };
    let json = serde_json::to_string(&widened).ok()?;
    Some(vec![
        (
            EXTENSION_TYPE_NAME_KEY.to_string(),
            SAIL_INTERVAL_EXTENSION_NAME.to_string(),
        ),
        (EXTENSION_TYPE_METADATA_KEY.to_string(), json),
    ])
}

/// The broadest `spark.interval` qualifier metadata for the given output Arrow
/// type — `YEAR TO MONTH` for `Interval(YearMonth)`, `DAY TO SECOND` for
/// `Duration(Microsecond)`. Used for operations like `Interval × Numeric` and
/// `Interval / Numeric`, where Spark always widens the result regardless of
/// the input qualifier (e.g. `INTERVAL '3' DAY * 4` → `DAY TO SECOND`).
fn broadest_interval_qualifier_metadata(
    output_type: &datafusion::arrow::datatypes::DataType,
) -> Option<Vec<(String, String)>> {
    use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
    use sail_common::interval::IntervalQualifierMetadata;
    use sail_common::spec::{
        EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY, SAIL_INTERVAL_EXTENSION_NAME,
    };

    let (start, end) = match output_type {
        DataType::Interval(IntervalUnit::YearMonth) => (0, 1),
        DataType::Duration(TimeUnit::Microsecond) => (0, 3),
        _ => return None,
    };
    let widened = IntervalQualifierMetadata {
        start_field: Some(start),
        end_field: Some(end),
    };
    let json = serde_json::to_string(&widened).ok()?;
    Some(vec![
        (
            EXTENSION_TYPE_NAME_KEY.to_string(),
            SAIL_INTERVAL_EXTENSION_NAME.to_string(),
        ),
        (EXTENSION_TYPE_METADATA_KEY.to_string(), json),
    ])
}
