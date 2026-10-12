use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Column, DFSchemaRef, ScalarValue, plan_datafusion_err};
use datafusion_expr::expr::FieldMetadata;
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr, lit, when};
use datafusion_functions::core::expr_ext::FieldAccessor;
use datafusion_functions::expr_fn as datafusion_fn;
use datafusion_functions_nested::expr_fn::{array_element, array_length, map_extract};
use sail_common::spec::{self, DEFAULT_COLUMN_VALUE_PLACEHOLDER_ID};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::drop_struct_field::DropStructField;
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_function::scalar::table_input::TableInput;
use sail_function::scalar::update_struct_field::UpdateStructField;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_alias(
        &self,
        expr: spec::Expr,
        name: Vec<spec::Identifier>,
        metadata: Option<Vec<(String, String)>>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let name = name.into_iter().map(|x| x.into()).collect::<Vec<String>>();
        let named_expr = self.resolve_named_expression(expr, schema, state).await?;
        let NamedExpr {
            name: inner_name,
            expr,
            metadata: inner_metadata,
        } = named_expr;
        if name.is_empty() {
            return Ok(
                NamedExpr::new(inner_name, expr).with_metadata(metadata.unwrap_or(inner_metadata))
            );
        }
        let metadata = metadata.unwrap_or(inner_metadata);
        let expr = if let [n] = name.as_slice() {
            if !metadata.is_empty() {
                let metadata_map: HashMap<String, String> = metadata.into_iter().collect();
                let field_metadata = Some(FieldMetadata::from(metadata_map));
                expr.alias_with_metadata(n, field_metadata)
            } else {
                expr.alias(n)
            }
        } else {
            expr
        };
        Ok(NamedExpr::new(name, expr))
    }

    pub(super) async fn resolve_expression_placeholder(
        &self,
        placeholder: String,
        state: &PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let name = placeholder.clone();
        let field = placeholder.get(1..).and_then(|key| {
            state
                .get_param_value(key)
                .or_else(|| {
                    key.parse::<usize>()
                        .ok()
                        .and_then(|index| index.checked_sub(1))
                        .and_then(|index| state.get_positional_param_value(index))
                })
                .map(|value| Arc::new(Field::new("", value.data_type(), true)))
        });
        let expr = expr::Expr::Placeholder(expr::Placeholder::new_with_field(placeholder, field));
        Ok(NamedExpr::new(vec![name], expr))
    }

    pub(super) fn resolve_expression_default_column_value(&self) -> PlanResult<NamedExpr> {
        let expr = expr::Expr::Placeholder(expr::Placeholder::new_with_field(
            DEFAULT_COLUMN_VALUE_PLACEHOLDER_ID.to_string(),
            None,
        ));
        Ok(NamedExpr::new(vec!["DEFAULT".to_string()], expr))
    }

    pub(super) async fn resolve_expression_identifier_clause(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let resolved = self.resolve_expression(expr, schema, state).await?;
        let name = self.evaluate_identifier_expr(resolved, state)?;
        let object_name = sail_sql_analyzer::expression::from_ast_object_name(
            sail_sql_analyzer::parser::parse_object_name(&name)?,
        )?;
        self.resolve_expression_attribute(object_name, None, false, schema, state)
    }

    /// Evaluates a resolved DataFusion expression as an identifier string.
    ///
    /// Named parameter placeholders (e.g. `:col`) are substituted from the
    /// current parameter scope in `state` before constant-folding, which
    /// allows expressions like `IDENTIFIER(:col)` or
    /// `IDENTIFIER(:tab || '.' || :col)` to work inside parameterized SQL.
    pub(in super::super) fn evaluate_identifier_expr(
        &self,
        expr: expr::Expr,
        state: &PlanResolverState,
    ) -> PlanResult<String> {
        use datafusion_common::tree_node::{Transformed, TreeNode};
        let expr = expr
            .transform(|e| {
                if let expr::Expr::Placeholder(expr::Placeholder { id, .. }) = &e {
                    if id.is_empty() {
                        return Ok(Transformed::no(e));
                    }
                    // Strip the leading prefix character (e.g. ':' or '$') from the
                    // placeholder id to get the param key, mirroring DataFusion's own
                    // `get_placeholders_with_values` which does `id[1..]`.
                    let key = &id[1..];
                    // Try named parameter.
                    if let Some(scalar) = state.get_param_value(key) {
                        return Ok(Transformed::yes(expr::Expr::Literal(scalar.clone(), None)));
                    }
                    // Try positional parameter (key is a 1-based integer index).
                    if let Ok(index) = key.parse::<usize>()
                        && index > 0
                        && let Some(scalar) = state.get_positional_param_value(index - 1)
                    {
                        return Ok(Transformed::yes(expr::Expr::Literal(scalar.clone(), None)));
                    }
                }
                Ok(Transformed::no(e))
            })
            .map_err(|e| {
                PlanError::invalid(format!("IDENTIFIER placeholder substitution failed: {e}"))
            })?
            .data;
        let evaluator = LiteralEvaluator::new();
        // Any placeholder that was not substituted above (e.g. because it had no
        // matching parameter) will cause the evaluation to fail here, since the
        // LiteralEvaluator cannot constant-fold an unresolved placeholder expression.
        let scalar = evaluator.evaluate(&expr).map_err(|e| {
            PlanError::invalid(format!("IDENTIFIER expression must be a constant: {e}"))
        })?;
        match scalar {
            ScalarValue::Utf8(Some(s))
            | ScalarValue::LargeUtf8(Some(s))
            | ScalarValue::Utf8View(Some(s)) => Ok(s),
            _ => Err(PlanError::invalid(
                "IDENTIFIER expression must evaluate to a string",
            )),
        }
    }

    pub(super) async fn resolve_expression_table(
        &self,
        expr: spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let query = match expr {
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
                    "expected a query or a table reference for table input",
                ));
            }
        };
        let plan = self.resolve_query_plan(query, state).await?;
        Ok(NamedExpr::new(
            vec!["table".to_string()],
            ScalarUDF::from(TableInput::new(Arc::new(plan))).call(vec![]),
        ))
    }

    pub(super) async fn resolve_expression_regex(
        &self,
        col_name: String,
        plan_id: Option<i64>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        use regex::Regex;
        use sail_function::scalar::multi_expr::MultiExpr;

        let schema = &Self::local_schema(schema, state);

        // Spark expands a quoted regex over the current output without the
        // originating DataFrame's plan ID (SparkConnectPlanner.transformUnresolvedRegex).
        let quoted_pattern = col_name
            .strip_prefix('`')
            .and_then(|name| name.strip_suffix('`'))
            .filter(|name| !name.is_empty());
        let pattern_str = quoted_pattern.unwrap_or_else(|| col_name.trim_matches('`'));
        let plan_id = if quoted_pattern.is_some() {
            None
        } else {
            plan_id
        };

        // `String.matches` matches the whole name, so the pattern is grouped before it is
        // anchored: an alternation would otherwise bind tighter than the anchors. It is printed
        // back from its syntax tree first, since in extended mode a trailing comment would
        // otherwise run over the parenthesis that closes the group.
        let normalized_pattern = regex_syntax::ast::parse::Parser::new()
            .parse(pattern_str)
            .map_err(|e| {
                PlanError::invalid(format!("invalid regex pattern '{}': {}", pattern_str, e))
            })?
            .to_string();
        let anchored_pattern = format!("^(?:{normalized_pattern})$");
        // This one does not fold: Rust folds all of Unicode where Java folds only ASCII, so
        // folding here would match `Ä` with `ä`, which Spark does not. Measured on the oracle:
        // the pattern `ä` matches only `ä`, while `col_ä` does match `COL_ä`.
        let pattern = Regex::new(&anchored_pattern).map_err(|e| {
            PlanError::invalid(format!("invalid regex pattern '{}': {}", pattern_str, e))
        })?;

        // Spark compiles the pattern case-insensitively unless the analysis is case sensitive, and
        // Java's `(?i)` folds ASCII alone
        // (`org.apache.spark.sql.catalyst.analysis.UnresolvedRegex#expandStar`), where Rust's folds
        // all of Unicode. Turning the Unicode mode off gives the same folding, at the price of
        // matching bytes rather than characters, so it is only used for a name that is ASCII.
        //
        // TODO: fold ASCII in a name that is not, and in a pattern the byte builder refuses.
        // Both land on the pattern above, which does not fold at all. Rewriting each ASCII letter
        // of the parsed pattern into a two-element class would be Java's rule exactly and would
        // remove the need for the byte pattern. See `test_col_regex_folds_only_ascii`.
        let ascii_pattern = if self.config.case_sensitive {
            None
        } else {
            regex::bytes::RegexBuilder::new(&anchored_pattern)
                .case_insensitive(true)
                .unicode(false)
                .build()
                .ok()
        };

        // Collect all matching columns
        let mut matching_columns = Vec::new();
        let mut matching_names = Vec::new();

        for (qualifier, field) in schema.iter() {
            // Get field info
            let Ok(info) = state.get_field_info(field.name()) else {
                continue;
            };

            // Skip hidden fields
            if info.is_hidden() {
                continue;
            }

            // Check if the field name matches the pattern and plan_id
            let field_name = info.name();
            let matched = match &ascii_pattern {
                Some(ascii) if field_name.is_ascii() => ascii.is_match(field_name.as_bytes()),
                _ => pattern.is_match(field_name),
            };
            if matched && info.has_plan_id(plan_id) {
                matching_columns.push(expr::Expr::Column(Column::new(
                    qualifier.cloned(),
                    field.name(),
                )));
                matching_names.push(field_name.to_string());
            }
        }

        // If no columns match, return empty MultiExpr (like Spark does)
        if matching_columns.is_empty() {
            let multi_expr = ScalarUDF::from(MultiExpr::new()).call(matching_columns);
            return Ok(NamedExpr::new(matching_names, multi_expr));
        }

        // If only one column matches, return it directly
        if matching_columns.len() == 1 {
            return Ok(NamedExpr::new(matching_names, matching_columns.one()?));
        }

        // If multiple columns match, wrap them in a MultiExpr
        let multi_expr = ScalarUDF::from(MultiExpr::new()).call(matching_columns);
        Ok(NamedExpr::new(matching_names, multi_expr))
    }

    pub(super) async fn resolve_expression_extract_value(
        &self,
        child: spec::Expr,
        extraction: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        fn is_attribute_path(expr: &spec::Expr) -> bool {
            match expr {
                spec::Expr::UnresolvedAttribute { .. } => true,
                spec::Expr::UnresolvedExtractValue { child, .. } => is_attribute_path(child),
                spec::Expr::Alias { expr, .. } => is_attribute_path(expr),
                _ => false,
            }
        }

        fn discard_failed_missing_input(
            expr: &expr::Expr,
            child_is_attribute_path: bool,
            schema: &DFSchemaRef,
            state: &mut PlanResolverState,
        ) {
            let Some(input) = state.missing_input_mut(schema) else {
                return;
            };
            let Some(index) = expr
                .column_refs()
                .into_iter()
                .filter_map(|column| {
                    input
                        .schemas()
                        .iter()
                        .position(|schema| schema.has_column(column))
                })
                .max()
            else {
                return;
            };
            if child_is_attribute_path {
                // Spark discards tentative descendant bindings when extracting a
                // struct field fails, then retries deeper outputs and outer references.
                input.discard(index);
            } else {
                // Spark binds the arguments of a function before resolving the function, so it
                // keeps those bindings and fails. Retry only earlier outputs and outer references,
                // which cannot bind a deeper column in place of the failed one.
                // TODO: Keep the bindings as Spark does once name resolution is staged. Spark
                //   discards the bindings of SQL `CASE`, which Sail cannot tell from `when`.
                // TODO: Preserve analyzer staging for native `UpdateFields`: unlike an
                //   unresolved function, it can fail and discard bindings in this pass.
                input.discard_from(index);
            }
        }

        let child_is_attribute_path = is_attribute_path(&child);
        let NamedExpr { name, expr, .. } =
            self.resolve_named_expression(child, schema, state).await?;
        let data_type = expr.get_type(schema)?;

        // For Maps, we support non-literal expressions as keys
        if matches!(data_type, DataType::Map(_, _)) {
            let NamedExpr {
                name: extraction_name,
                expr: extraction_expr,
                ..
            } = self
                .resolve_named_expression(extraction, schema, state)
                .await?;

            let result_name = format!("{}[{}]", name.one()?, extraction_name.one()?);
            // Use map_extract which supports dynamic keys, then extract first element
            let result_expr = array_element(map_extract(expr, extraction_expr), lit(1));
            return Ok(NamedExpr::new(vec![result_name], result_expr));
        }

        // For other types (List, Struct), extraction must be a literal.
        // SQL dot selectors are represented as literals; an attribute selector
        // is a column expression and cannot select a struct field.
        let extraction = match extraction {
            spec::Expr::Literal(lit) => lit,
            spec::Expr::UnresolvedAttribute { name, .. } => {
                if matches!(data_type, DataType::Struct(_)) {
                    // A column cannot select a struct field, even if it is named like one.
                    discard_failed_missing_input(&expr, child_is_attribute_path, schema, state);
                    return Err(PlanError::AnalysisError(
                        "extraction must be a literal".to_string(),
                    ));
                }
                let name: Vec<String> = name.into();
                spec::Literal::Utf8 {
                    value: Some(name.one()?),
                }
            }
            _ => {
                // Array-index validation preserves the resolved array binding in Spark.
                if !matches!(
                    data_type,
                    DataType::List(_)
                        | DataType::LargeList(_)
                        | DataType::FixedSizeList(_, _)
                        | DataType::ListView(_)
                        | DataType::LargeListView(_)
                ) {
                    discard_failed_missing_input(&expr, child_is_attribute_path, schema, state);
                }
                return Err(PlanError::invalid("extraction must be a literal"));
            }
        };
        let extraction = self.resolve_literal(extraction, state)?;
        let service = self.ctx.extension::<PlanService>()?;
        let extraction_name = service
            .plan_formatter()
            .literal_to_string(&extraction, &self.config.session_timezone)?;
        let name = match data_type {
            DataType::Struct(_) => {
                format!("{}.{}", name.one()?, extraction_name)
            }
            _ => {
                format!("{}[{}]", name.one()?, extraction_name)
            }
        };
        let expr = match data_type {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _)
            | DataType::ListView(field)
            | DataType::LargeListView(field) => {
                let ScalarValue::Int64(index) = extraction.cast_to(&DataType::Int64)? else {
                    return Err(PlanError::AnalysisError(format!(
                        "invalid extraction value for array: {extraction}"
                    )));
                };
                let index_expr = lit(ScalarValue::Int64(index));
                let element = array_element(
                    expr.clone(),
                    lit(ScalarValue::Int64(index.map(|x| x.saturating_add(1)))),
                );
                if self.config.ansi_mode {
                    let length = cast(array_length(expr.clone()), DataType::Int64);
                    let message = datafusion_fn::concat(vec![
                        lit("[INVALID_ARRAY_INDEX] The index "),
                        cast(index_expr, DataType::Utf8),
                        lit(" is out of bounds. The array has "),
                        cast(length.clone(), DataType::Utf8),
                        lit(
                            " elements. Use the SQL function `get()` to tolerate accessing element at invalid index and return NULL instead.",
                        ),
                    ]);
                    let error = cast(
                        ScalarUDF::from(RaiseError::new()).call(vec![message]),
                        field.data_type().clone(),
                    );
                    let out_of_bounds = match index {
                        Some(index) if index < 0 => expr.is_not_null(),
                        Some(index) => expr.is_not_null().and(length.lt_eq(lit(index))),
                        None => lit(false),
                    };
                    when(out_of_bounds, error).otherwise(element)?
                } else {
                    element
                }
            }
            DataType::Struct(fields) => {
                let ScalarValue::Utf8(Some(name)) = extraction else {
                    discard_failed_missing_input(&expr, child_is_attribute_path, schema, state);
                    return Err(PlanError::AnalysisError(format!(
                        "invalid extraction value for struct: {extraction}"
                    )));
                };
                // Ambiguous (matches more than one field) or missing, either discards the
                // tentative binding the same way, so a retry can recover an older struct.
                let field = match self.resolve_struct_field(&fields, &name) {
                    Ok(Some(field)) => field,
                    Ok(None) => {
                        discard_failed_missing_input(&expr, child_is_attribute_path, schema, state);
                        let names = fields
                            .iter()
                            .map(|x| x.name().to_string())
                            .collect::<Vec<_>>();
                        return Err(Self::field_not_found_error(&name, &names));
                    }
                    Err(e) => {
                        discard_failed_missing_input(&expr, child_is_attribute_path, schema, state);
                        return Err(e);
                    }
                };
                expr.field(field.name().clone())
            }
            _ => {
                discard_failed_missing_input(&expr, child_is_attribute_path, schema, state);
                return Err(PlanError::AnalysisError(format!(
                    "cannot extract value from data type: {data_type}"
                )));
            }
        };
        Ok(NamedExpr::new(vec![name], expr))
    }

    pub(super) async fn resolve_expression_update_fields(
        &self,
        struct_expression: spec::Expr,
        field_name: spec::ObjectName,
        value_expression: Option<spec::Expr>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let field_name: Vec<String> = field_name.into();
        let NamedExpr { name, expr, .. } = self
            .resolve_named_expression(struct_expression, schema, state)
            .await?;
        let name = if name.len() == 1 {
            name.one()?
        } else {
            let names = format!("({})", name.join(", "));
            return Err(PlanError::invalid(format!(
                "one name expected for expression, got: {names}"
            )));
        };

        // Spark prints a lambda parameter as `namedlambdavariable()` rather than by its name
        // (`NamedLambdaVariable.toString`), and Sail's name for one is generated per call, so
        // echoing it would put a name in the message that changes between identical runs.
        let name = if matches!(expr, expr::Expr::LambdaVariable(_)) {
            "namedlambdavariable()".to_string()
        } else {
            name
        };

        // The type is read before the expression is consumed, so that the levels of the path can
        // be checked against it below.
        let data_type = expr.get_type(schema)?;

        // Spark names the column after the `UpdateFields` expression tree, where
        // each operation is rendered as `WithField(<value name>)` (the value
        // expression's display name, not the target field name) or `dropfield()`.
        let levels = field_name.clone();
        let is_drop = value_expression.is_none();
        let (op, new_expr) = if let Some(value_expression) = value_expression {
            let NamedExpr {
                name: value_name,
                expr: value_expr,
                ..
            } = self
                .resolve_named_expression(value_expression, schema, state)
                .await?;
            (
                format!("WithField({})", value_name.one()?),
                ScalarUDF::from(UpdateStructField::new(
                    field_name,
                    self.config.case_sensitive,
                ))
                .call(vec![expr, value_expr]),
            )
        } else {
            (
                "dropfield()".to_string(),
                ScalarUDF::from(DropStructField::new(field_name, self.config.case_sensitive))
                    .call(vec![expr]),
            )
        };
        // Checks the input of `update_fields` before rebuilding, so a level that is not a struct
        // is refused here, with the message naming the reading expression and its SQL type.
        let target = self.check_update_fields_input(&data_type, &name, &levels, &op)?;

        // Dropping every field of the struct is refused by the same `checkInputDataTypes`, so it
        // is reported from here as well, naming the `update_fields` whose input the fields belong
        // to rather than the outermost one: for `dropFields("t.a")` Spark names `s.t`, which is
        // the base the walk above ends on.
        if let (Some((target_name, fields)), Some(level)) = (target, levels.last())
            && is_drop
        {
            let kept = fields
                .iter()
                .filter(|x| !self.match_identifier(x.name(), level))
                .count();
            if kept == 0 && !fields.is_empty() {
                return Err(PlanError::AnalysisError(format!(
                    "[DATATYPE_MISMATCH.CANNOT_DROP_ALL_FIELDS] Cannot resolve \"{}\" due to \
                     data type mismatch: Cannot drop all fields in struct.",
                    Self::update_fields_name(&target_name, &op)
                )));
            }
        }

        Ok(NamedExpr::new(
            vec![Self::update_fields_name(&name, &op)],
            new_expr,
        ))
    }

    /// Checks the input of every `update_fields` a path builds. Spark rewrites `withField("a.b")`
    /// into one `UpdateFields` per level, and each of them refuses an input that is not a struct
    /// (`UpdateFields.checkInputDataTypes`), naming the expression that reads it. The last name
    /// is only ever written or dropped, so it is not read and not checked.
    fn check_update_fields_input(
        &self,
        data_type: &DataType,
        base: &str,
        levels: &[String],
        op: &str,
    ) -> PlanResult<Option<(String, Fields)>> {
        let mut base = base.to_string();
        let mut data_type = data_type.clone();
        for level in levels.iter().take(levels.len().saturating_sub(1)) {
            let DataType::Struct(fields) = &data_type else {
                // A level before the last one is READ before it is rebuilt, and the read is the
                // `ExtractValue` that `updateFieldsHelper` builds. It is built while the plan is,
                // so it refuses a base that is not a complex type before the `update_fields`
                // above it is type checked, and with the class that reading the same name on its
                // own raises. An array or a map IS complex, so the read succeeds there and the
                // refusal is the one below; a NULL base is read as NULL
                // (`ExtractValue.applyOrNull`) and is likewise refused below.
                //
                // TODO: Spark keeps extracting through an array or a map, so the level it names
                // is deeper than the one named here. See
                // `test_a_level_that_is_complex_but_not_a_struct_names_the_level_spark_names`.
                if matches!(
                    data_type,
                    DataType::Null
                        | DataType::List(_)
                        | DataType::LargeList(_)
                        | DataType::FixedSizeList(_, _)
                        | DataType::ListView(_)
                        | DataType::LargeListView(_)
                        | DataType::Map(_, _)
                ) {
                    return Err(self.update_fields_input_type_error(&base, op, &data_type)?);
                }
                return Err(self.invalid_extract_base_error(&base, &data_type)?);
            };
            // A level that matches nothing, or matches twice, is reported by the function, which
            // walks the same path with the same resolver.
            let Ok(Some(field)) = self.resolve_struct_field(fields, level) else {
                return Ok(None);
            };
            base = format!("{base}.{level}");
            data_type = field.data_type().clone();
        }
        let DataType::Struct(fields) = &data_type else {
            return Err(self.update_fields_input_type_error(&base, op, &data_type)?);
        };
        Ok(Some((base, fields.clone())))
    }

    /// Splices an operation into the name of the `update_fields` the path builds. Spark collapses
    /// chained `withField`/`dropFields` into a single `update_fields(x, op1, op2, ...)`, so the
    /// operation joins an existing one rather than nesting inside it.
    fn update_fields_name(base: &str, op: &str) -> String {
        match base.strip_suffix(')') {
            Some(prefix) if prefix.starts_with("update_fields(") => format!("{prefix}, {op})"),
            _ => format!("update_fields({base}, {op})"),
        }
    }

    /// The error `UpdateFields` raises for an input that is not a struct
    /// (`UpdateFields.checkInputDataTypes`), naming the expression that reads it.
    fn update_fields_input_type_error(
        &self,
        base: &str,
        op: &str,
        data_type: &DataType,
    ) -> PlanResult<PlanError> {
        Ok(PlanError::AnalysisError(format!(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \
             \"update_fields({base}, {op})\" due to data type mismatch: The first parameter \
             requires the \"STRUCT\" type, however \"{base}\" has the type \"{}\".",
            self.spark_type_name(data_type)?
        )))
    }

    /// Rewrites the resolved expression to refer to columns in an external schema.
    /// The external schema has user-facing field names instead of internal names
    /// derived from field IDs in the resolver state.
    pub(in super::super) fn rewrite_expression_for_external_schema(
        &self,
        expr: expr::Expr,
        state: &PlanResolverState,
    ) -> PlanResult<expr::Expr> {
        let rewrite = |e: expr::Expr| -> datafusion_common::Result<Transformed<expr::Expr>> {
            if let expr::Expr::Column(Column {
                name,
                relation,
                spans,
            }) = e
            {
                let info = state
                    .get_field_info(&name)
                    .map_err(|_| plan_datafusion_err!("column {name} not found"))?;
                Ok(Transformed::yes(expr::Expr::Column(Column {
                    name: info.name().to_string(),
                    relation,
                    spans,
                })))
            } else {
                Ok(Transformed::no(e))
            }
        };
        Ok(expr.transform(rewrite).data()?)
    }
}
