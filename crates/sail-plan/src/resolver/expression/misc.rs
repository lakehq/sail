use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{plan_datafusion_err, Column, DFSchemaRef, ScalarValue};
use datafusion_expr::expr::FieldMetadata;
use datafusion_expr::{expr, lit, BinaryExpr, ExprSchemable, ScalarUDF};
use datafusion_expr_common::operator::Operator;
use datafusion_functions::core::expr_ext::FieldAccessor;
use datafusion_functions_nested::expr_fn::array_element;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::extension::function::drop_struct_field::DropStructField;
use crate::extension::function::table_input::TableInput;
use crate::extension::function::update_struct_field::UpdateStructField;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;
use crate::utils::ItemTaker;

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
        let expr = expr::Expr::Placeholder(expr::Placeholder::new(placeholder, None));
        Ok(NamedExpr::new(vec![name], expr))
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
                read_type: spec::ReadType::NamedTable(spec::ReadNamedTable {
                    name,
                    temporal: None,
                    sample: None,
                    options: vec![],
                }),
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
        _col_name: String,
        _plan_id: Option<i64>,
        _schema: &DFSchemaRef,
        _state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        Err(PlanError::todo("unresolved regex"))
    }

    pub(super) async fn resolve_expression_extract_value(
        &self,
        child: spec::Expr,
        extraction: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let spec::Expr::Literal(extraction) = extraction else {
            return Err(PlanError::invalid("extraction must be a literal"));
        };
        let extraction = self.resolve_literal(extraction, state)?;
        let extraction_name = self
            .config
            .plan_formatter
            .literal_to_string(&extraction, &self.config)?;
        let NamedExpr { name, expr, .. } =
            self.resolve_named_expression(child, schema, state).await?;
        let data_type = expr.get_type(schema)?;
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
            // TODO: support non-string map keys
            DataType::Map(_, _) => expr.field(extraction),
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
