use std::collections::HashSet;
use std::sync::Arc;

use datafusion_common::{DFSchema, Result};
use datafusion_expr::logical_plan::{LogicalPlan, Projection};
use datafusion_expr::{Expr, ExprSchemable};

/// Project every input column, followed by `keys`.
pub(super) fn project_with_keys(input: Arc<LogicalPlan>, keys: Vec<Expr>) -> Result<Projection> {
    let mut expressions = input
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    expressions.extend(keys);
    match key_schema(&input, &expressions)? {
        Some(schema) => Projection::try_new_with_schema(expressions, input, Arc::new(schema)),
        None => Projection::try_new(expressions, input),
    }
}

/// Derive the native projection schema without looking up each pass-through field;
/// return `None` if names could collide.
///
/// `Projection::try_new` looks up each column's field by name, and the field each output
/// inherits functional dependencies from by display name, which is quadratic in the width.
/// When no two fields share a name and a table, or a display name, each column resolves to
/// its own field, so only the keys need deriving.
// The key schema follows DataFusion's `projection_schema` and `calc_func_dependencies_for_project`.
// [CREDIT]: https://github.com/apache/datafusion/blob/55.2.0/datafusion/expr/src/logical_plan/plan.rs
fn key_schema(input: &LogicalPlan, expressions: &[Expr]) -> Result<Option<DFSchema>> {
    let schema = input.schema();
    let names = schema.field_names();
    let mut columns = HashSet::new();
    let mut displays = HashSet::new();
    if !schema.iter().zip(&names).all(|((qualifier, field), name)| {
        columns.insert((field.name(), qualifier.map(|qualifier| qualifier.table())))
            && displays.insert(name)
    }) {
        return Ok(None);
    }
    let mut fields = schema
        .iter()
        .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
        .collect::<Vec<_>>();
    // Like `calc_func_dependencies_for_project`, a key inherits the dependencies of the
    // input field named like its expression, or like its alias's expression.
    let mut indices = (0..names.len()).collect::<Vec<_>>();
    for key in &expressions[names.len()..] {
        fields.push(key.to_field(schema)?);
        let display = match key {
            Expr::Alias(alias) => alias.expr.to_string(),
            key => key.to_string(),
        };
        indices.push(
            names
                .iter()
                .position(|name| *name == display)
                .unwrap_or(usize::MAX),
        );
    }
    let projected = DFSchema::new_with_metadata(fields, schema.metadata().clone())?
        .with_functional_dependencies(
            schema
                .functional_dependencies()
                .project_functional_dependencies(&indices, expressions.len()),
        )?;
    #[cfg(debug_assertions)]
    assert_eq!(
        projected,
        *datafusion_expr::logical_plan::projection_schema(input, expressions)?,
        "a key projection must match DataFusion's projection schema"
    );
    Ok(Some(projected))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::provider_as_source;
    use datafusion::functions_aggregate::expr_fn::max;
    use datafusion_common::TableReference;
    use datafusion_expr::{JoinType, LogicalPlanBuilder, cast, col, lit};

    use super::*;

    fn scan(name: impl Into<TableReference>, fields: Vec<Field>) -> Result<LogicalPlanBuilder> {
        let table = EmptyTable::new(Arc::new(Schema::new(fields)));
        LogicalPlanBuilder::scan(name, provider_as_source(Arc::new(table)), None)
    }

    fn cross_join(left: LogicalPlan, right: LogicalPlan) -> Result<LogicalPlan> {
        LogicalPlanBuilder::from(left)
            .join_on(right, JoinType::Inner, vec![lit(true)])?
            .build()
    }

    /// Compare the schema to `Projection::try_new` and check which construction path ran.
    fn assert_matches(input: LogicalPlan, keys: Vec<Expr>, fast_path: bool) -> Result<()> {
        let input = Arc::new(input);
        let mut expressions = input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect::<Vec<_>>();
        expressions.extend(keys.clone());
        assert_eq!(key_schema(&input, &expressions)?.is_some(), fast_path);
        let expected = Projection::try_new(expressions, Arc::clone(&input))?;
        let actual = project_with_keys(input, keys)?;
        assert_eq!(actual.expr, expected.expr);
        assert_eq!(actual.schema, expected.schema);
        Ok(())
    }

    #[test]
    fn raw_and_cast_keys_match_projection_schema() -> Result<()> {
        let comment = HashMap::from([("comment".to_string(), "key".to_string())]);
        let extension =
            HashMap::from([("ARROW:extension:name".to_string(), "arrow.uuid".to_string())]);
        let input = scan(
            "t",
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, true).with_metadata(comment),
                Field::new("c", DataType::FixedSizeBinary(16), true).with_metadata(extension),
            ],
        )?
        .build()?;
        let keys = vec![
            col("t.a").alias("__sail_correlation_key_1"),
            cast(col("t.b"), DataType::Int64).alias("__sail_correlation_key_2"),
            cast(col("t.c"), DataType::Binary).alias("__sail_correlation_key_3"),
        ];
        assert_matches(input, keys, true)
    }

    #[test]
    fn schema_and_nested_metadata_survive_without_extra_keys() -> Result<()> {
        let nested = Field::new("value", DataType::Int32, false)
            .with_metadata(HashMap::from([("comment".to_owned(), "nested".to_owned())]));
        let input = scan(
            "t",
            vec![Field::new(
                "s",
                DataType::Struct(vec![Arc::new(nested)].into()),
                true,
            )],
        )?
        .build()?;
        let schema = DFSchema::new_with_metadata(
            input
                .schema()
                .iter()
                .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
                .collect(),
            HashMap::from([("source".to_owned(), "fixture".to_owned())]),
        )?;
        let input = LogicalPlan::Projection(Projection::new_from_schema(
            Arc::new(input),
            Arc::new(schema),
        ));
        assert_matches(input, vec![], true)
    }

    #[test]
    fn keys_inherit_functional_dependencies() -> Result<()> {
        let input = scan(
            "t",
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int64, true),
            ],
        )?
        .aggregate(vec![col("t.a")], vec![max(col("t.b"))])?
        .build()?;
        assert!(!input.schema().functional_dependencies().is_empty());
        let keys = vec![
            col("t.a").alias("__sail_correlation_key_1"),
            cast(col("t.a"), DataType::Utf8).alias("__sail_correlation_key_2"),
        ];
        assert_matches(input, keys, true)
    }

    #[test]
    fn same_name_in_different_tables_uses_cached_fields() -> Result<()> {
        let input = cross_join(
            scan("t", vec![Field::new("id", DataType::Int32, false)])?.build()?,
            scan("u", vec![Field::new("id", DataType::Int64, true)])?.build()?,
        )?;
        assert_matches(
            input,
            vec![col("u.id").alias("__sail_correlation_key_1")],
            true,
        )
    }

    #[test]
    fn colliding_names_fall_back() -> Result<()> {
        // `t.a` resolves to the field of `s.t.a`, whose qualifier also names table `t`.
        let input = cross_join(
            scan(
                TableReference::partial("s", "t"),
                vec![Field::new("a", DataType::Int32, false)],
            )?
            .build()?,
            scan("t", vec![Field::new("a", DataType::Int64, true)])?.build()?,
        )?;
        assert_matches(
            input,
            vec![col("t.a").alias("__sail_correlation_key_1")],
            false,
        )?;

        // An unqualified field named `t.a` shares the display name of column `a` of `t`.
        let input = cross_join(
            scan("u", vec![Field::new("x", DataType::Int64, true)])?
                .project(vec![col("u.x").alias("t.a")])?
                .build()?,
            scan("t", vec![Field::new("a", DataType::Int32, false)])?.build()?,
        )?;
        assert_matches(
            input,
            vec![col("t.a").alias("__sail_correlation_key_1")],
            false,
        )
    }
}
