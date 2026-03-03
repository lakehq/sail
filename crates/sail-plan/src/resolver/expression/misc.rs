use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{plan_datafusion_err, Column, DFSchemaRef, ScalarValue};
use datafusion_expr::expr::FieldMetadata;
use datafusion_expr::{expr, lit, BinaryExpr, ExprSchemable, ScalarUDF};
use datafusion_expr_common::operator::Operator;
use datafusion_functions::core::expr_ext::FieldAccessor;
use datafusion_functions_nested::expr_fn::{array_element, map_extract};
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::drop_struct_field::DropStructField;
use sail_function::scalar::table_input::TableInput;
use sail_function::scalar::update_struct_field::UpdateStructField;

use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_alias(
        &self,
        expr: spec::Expr,
        name: Vec<spec::Identifier>,
        metadata: Option<Vec<(String, String)>>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        let name = name.into_iter().map(|x| x.into()).collect::<Vec<String>>();
        let expr = if let [n] = name.as_slice() {
            if let Some(metadata) = metadata {
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
    ) -> PlanResult<NamedExpr> {
        let name = placeholder.clone();
        let expr = expr::Expr::Placeholder(expr::Placeholder::new_with_field(placeholder, None));
        Ok(NamedExpr::new(vec![name], expr))
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
                    if let Ok(index) = key.parse::<usize>() {
                        if index > 0 {
                            if let Some(scalar) = state.get_positional_param_value(index - 1) {
                                return Ok(Transformed::yes(expr::Expr::Literal(
                                    scalar.clone(),
                                    None,
                                )));
                            }
                        }
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

        // Remove backticks from the pattern if present
        let pattern_str = col_name.trim_matches('`');

        // Add anchors to match the entire column name (like Spark does)
        let anchored_pattern = format!("^{}$", pattern_str);

        // Compile the regex pattern
        let pattern = Regex::new(&anchored_pattern).map_err(|e| {
            PlanError::invalid(format!("invalid regex pattern '{}': {}", pattern_str, e))
        })?;

        // Collect all matching columns
        let mut matching_columns = Vec::new();
        let mut matching_names = Vec::new();

        for (qualifier, field) in schema.iter() {
            // Skip qualified columns if no qualifier is expected
            if qualifier.is_some() {
                continue;
            }

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
            if pattern.is_match(field_name) && info.matches(field_name, plan_id) {
                matching_columns.push(expr::Expr::Column(Column::new_unqualified(field.name())));
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
        // An UnresolvedAttribute from dot notation (e.g. `a.b`) is treated as a
        // literal field name so that the spec can keep the attribute unresolved.
        let extraction = match extraction {
            spec::Expr::Literal(lit) => lit,
            spec::Expr::UnresolvedAttribute { name, .. } => {
                let name: Vec<String> = name.into();
                spec::Literal::Utf8 {
                    value: Some(name.one()?),
                }
            }
            _ => return Err(PlanError::invalid("extraction must be a literal")),
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
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::ListView(_)
            | DataType::LargeListView(_) => array_element(
                expr,
                expr::Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(expr::Expr::Literal(extraction, None)),
                    Operator::Plus,
                    Box::new(lit(1i64)),
                )),
            ),
            DataType::Struct(fields) => {
                let ScalarValue::Utf8(Some(name)) = extraction else {
                    return Err(PlanError::AnalysisError(format!(
                        "invalid extraction value for struct: {extraction}"
                    )));
                };
                let Ok(name) = fields
                    .iter()
                    .filter(|x| x.name().eq_ignore_ascii_case(&name))
                    .map(|x| x.name().to_string())
                    .collect::<Vec<_>>()
                    .one()
                else {
                    return Err(PlanError::AnalysisError(format!(
                        "missing or ambiguous field: {name}"
                    )));
                };
                expr.field(name)
            }
            _ => {
                return Err(PlanError::AnalysisError(format!(
                    "cannot extract value from data type: {data_type}"
                )))
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

        let new_expr = if let Some(value_expression) = value_expression {
            let value_expr = self
                .resolve_expression(value_expression, schema, state)
                .await?;
            ScalarUDF::from(UpdateStructField::new(field_name)).call(vec![expr, value_expr])
        } else {
            ScalarUDF::from(DropStructField::new(field_name)).call(vec![expr])
        };
        Ok(NamedExpr::new(vec![name], new_expr))
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
