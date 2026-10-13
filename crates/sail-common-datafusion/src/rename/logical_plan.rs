use std::borrow::Cow;
use std::sync::Arc;

use datafusion_common::arrow::datatypes::Schema;
use datafusion_common::{Column, DFSchema, DFSchemaRef, HashSet, exec_err};
use datafusion_expr::expr::Alias;
use datafusion_expr::{Expr, LogicalPlan, Projection, SubqueryAlias};

/// Aliases each output column to a new name through an identity projection.
pub fn rename_logical_plan(
    plan: LogicalPlan,
    names: &[String],
) -> datafusion_common::Result<LogicalPlan> {
    rename_logical_plan_impl(plan, names, false)
}

/// Renames a fresh relation instance without stacking another projection.
pub fn rename_logical_plan_reusing_projection(
    plan: LogicalPlan,
    names: &[String],
) -> datafusion_common::Result<LogicalPlan> {
    rename_logical_plan_impl(plan, names, true)
}

fn rename_logical_plan_impl(
    plan: LogicalPlan,
    names: &[String],
    reuse_projection: bool,
) -> datafusion_common::Result<LogicalPlan> {
    if plan.schema().fields().len() != names.len() {
        return exec_err!(
            "cannot rename fields for logical plan with {} fields using {} names",
            plan.schema().fields().len(),
            names.len()
        );
    }
    let unique_names = names.iter().collect::<HashSet<_>>().len() == names.len();
    if !reuse_projection {
        return rename_projection(
            Arc::clone(plan.schema()),
            Arc::new(plan),
            std::iter::empty(),
            names,
            unique_names,
        );
    }
    // A reference alias does not evaluate expressions. Rename its projection
    // in place so renewed CTE attributes do not add another projection layer.
    let plan = match plan {
        LogicalPlan::SubqueryAlias(alias) => {
            return rename_subquery_alias(alias, names, unique_names);
        }
        plan => plan,
    };
    let original_schema = Arc::clone(plan.schema());
    match plan {
        LogicalPlan::Projection(projection) => rename_projection(
            original_schema,
            projection.input,
            projection.expr.into_iter().map(Cow::Owned),
            names,
            unique_names,
        ),
        plan => rename_projection(
            original_schema,
            Arc::new(plan),
            std::iter::empty(),
            names,
            unique_names,
        ),
    }
}

/// Renames the projection below a subquery alias, looking through nested aliases
/// (e.g. a SQL CTE aliased by both its definition and its reference name).
fn rename_subquery_alias(
    mut alias: SubqueryAlias,
    names: &[String],
    unique_names: bool,
) -> datafusion_common::Result<LogicalPlan> {
    let input = match alias.input.as_ref() {
        // CTE references share this projection. Borrow its expressions so
        // renaming does not clone the expression vector and obsolete aliases.
        LogicalPlan::Projection(projection) => rename_projection(
            Arc::clone(&projection.schema),
            Arc::clone(&projection.input),
            projection.expr.iter().map(Cow::Borrowed),
            names,
            unique_names,
        )?,
        LogicalPlan::SubqueryAlias(inner) => {
            rename_subquery_alias(inner.clone(), names, unique_names)?
        }
        _ => {
            return rename_projection(
                Arc::clone(&alias.schema),
                Arc::new(LogicalPlan::SubqueryAlias(alias)),
                std::iter::empty(),
                names,
                unique_names,
            );
        }
    };
    // Fresh internal names are unique. Requalification changes no field
    // types or dependencies, so reuse the validated schema. Preserve the
    // constructor's duplicate-name handling for other rename callers.
    if unique_names {
        alias.schema = Arc::new(
            input
                .schema()
                .as_ref()
                .clone()
                .replace_qualifier(alias.alias.clone()),
        );
        alias.input = Arc::new(input);
        return Ok(LogicalPlan::SubqueryAlias(alias));
    }
    Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
        Arc::new(input),
        alias.alias,
    )?))
}

fn rename_projection<'a>(
    original_schema: DFSchemaRef,
    input: Arc<LogicalPlan>,
    mut expressions: impl Iterator<Item = Cow<'a, Expr>>,
    names: &[String],
    unique_names: bool,
) -> datafusion_common::Result<LogicalPlan> {
    let (expr, fields): (Vec<_>, Vec<_>) = original_schema
        .iter()
        .zip(names)
        .map(|((qualifier, field), name)| {
            // Each original output is used exactly once. An existing projection
            // can supply its expression directly, including alias metadata.
            let expr = expressions
                .next()
                .unwrap_or_else(|| Cow::Owned(Expr::Column(Column::from((qualifier, field)))));
            let expr = match expr {
                Cow::Owned(Expr::Alias(mut alias)) => {
                    alias.name = name.clone();
                    alias.relation = qualifier.cloned();
                    Expr::Alias(alias)
                }
                Cow::Borrowed(Expr::Alias(alias)) => {
                    let mut renamed =
                        Alias::new(alias.expr.as_ref().clone(), qualifier.cloned(), name);
                    renamed.metadata = alias.metadata.clone();
                    Expr::Alias(renamed)
                }
                expr => expr.into_owned().alias_qualified(qualifier.cloned(), name),
            };
            (
                expr,
                (
                    qualifier.cloned(),
                    Arc::new(field.as_ref().clone().with_name(name)),
                ),
            )
        })
        .unzip();
    // Identity renames preserve types, nullability, metadata, and dependency
    // positions. Reuse them instead of looking up every column's type again.
    let metadata = original_schema.metadata().clone();
    let schema = if unique_names {
        // Unique names cannot introduce qualified-name ambiguity. Avoid the
        // constructor's ordered-set validation for fresh internal field IDs.
        let (qualifiers, fields): (Vec<_>, Vec<_>) = fields.into_iter().unzip();
        DFSchema::try_from(Schema::new_with_metadata(fields, metadata))?
            .with_field_specific_qualified_schema(qualifiers)?
    } else {
        DFSchema::new_with_metadata(fields, metadata)?
    };
    let schema = Arc::new(
        schema.with_functional_dependencies(original_schema.functional_dependencies().clone())?,
    );
    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
        expr, input, schema,
    )?))
}
