use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, plan_datafusion_err};
use datafusion_expr::type_coercion::binary::comparison_coercion;
use datafusion_expr::{Expr, ExprSchemable, cast};
use sail_common::spec::{
    SAIL_SPARK_INTERVAL_METADATA_KEY, SPARK_METADATA_JSON_KEY, SparkIntervalMetadata,
};

/// Computed expressions retain logical types, but do not inherit Spark column annotations.
pub fn without_spark_user_metadata(mut field: Field) -> Field {
    field.metadata_mut().remove(SPARK_METADATA_JSON_KEY);
    field
}

/// Merge nested annotations after DataFusion resolves the physical common type.
pub fn common_type_with_metadata(left: &DataType, right: &DataType) -> Result<Option<DataType>> {
    if left == right || right.is_null() {
        return Ok(Some(left.clone()));
    }
    if left.is_null() {
        return Ok(Some(right.clone()));
    }
    // DataFusion does not coerce view children with different nullability, even
    // when their physical types match. Merge only those field differences here.
    let common = comparison_coercion(left, right);
    let merge_nullability = common.is_none();
    let Some(common) = common.or_else(|| same_physical_type(left, right).then(|| left.clone()))
    else {
        return Ok(None);
    };
    let merged = match (&common, left, right) {
        (DataType::Struct(target), DataType::Struct(left), DataType::Struct(right))
            if target.len() == left.len() && left.len() == right.len() =>
        {
            DataType::Struct(
                target
                    .iter()
                    .zip(left)
                    .zip(right)
                    .map(|((target, left), right)| {
                        merge_field(target, left, right, merge_nullability)
                    })
                    .collect::<Result<_>>()?,
            )
        }
        (DataType::List(target), DataType::List(left), DataType::List(right)) => {
            DataType::List(merge_field(target, left, right, merge_nullability)?)
        }
        (DataType::LargeList(target), DataType::LargeList(left), DataType::LargeList(right)) => {
            DataType::LargeList(merge_field(target, left, right, merge_nullability)?)
        }
        (DataType::ListView(target), DataType::ListView(left), DataType::ListView(right)) => {
            DataType::ListView(merge_field(target, left, right, merge_nullability)?)
        }
        (
            DataType::LargeListView(target),
            DataType::LargeListView(left),
            DataType::LargeListView(right),
        ) => DataType::LargeListView(merge_field(target, left, right, merge_nullability)?),
        (
            DataType::FixedSizeList(target, size),
            DataType::FixedSizeList(left, _),
            DataType::FixedSizeList(right, _),
        ) => DataType::FixedSizeList(merge_field(target, left, right, merge_nullability)?, *size),
        (DataType::Map(target, sorted), DataType::Map(left, _), DataType::Map(right, _)) => {
            DataType::Map(
                merge_field(target, left, right, merge_nullability)?,
                *sorted,
            )
        }
        _ => common,
    };
    Ok(Some(merged))
}

fn merge_field(
    target: &FieldRef,
    left: &FieldRef,
    right: &FieldRef,
    merge_nullability: bool,
) -> Result<FieldRef> {
    let data_type = common_type_with_metadata(left.data_type(), right.data_type())?
        .unwrap_or_else(|| target.data_type().clone());
    let mut field = target.as_ref().clone().with_data_type(data_type);
    if merge_nullability {
        field = field.with_nullable(left.is_nullable() || right.is_nullable());
    }
    // Spark creates new StructFields when resolving different complex types.
    field.metadata_mut().remove(SPARK_METADATA_JSON_KEY);
    field.metadata_mut().remove("comment");
    let interval = |field: &Field| {
        field
            .metadata()
            .get(SAIL_SPARK_INTERVAL_METADATA_KEY)
            .map(|value| {
                SparkIntervalMetadata::from_json(value)
                    .map_err(|error| plan_datafusion_err!("{error}"))
            })
            .transpose()
    };
    // Missing qualifiers denote the full physical interval range, not the other branch's qualifier.
    let qualifier = match (interval(left)?, interval(right)?) {
        (None, right) if left.data_type().is_null() => right,
        (left, None) if right.data_type().is_null() => left,
        (Some(left), Some(right)) => left.wider(right),
        _ => None,
    };
    match qualifier {
        Some(common) => {
            field.metadata_mut().insert(
                SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
                common
                    .to_json()
                    .map_err(|error| plan_datafusion_err!("{error}"))?,
            );
        }
        _ => {
            field
                .metadata_mut()
                .remove(SAIL_SPARK_INTERVAL_METADATA_KEY);
        }
    }
    Ok(Arc::new(field))
}

/// Compare nested value types without their field annotations or nullability.
pub fn same_physical_type(left: &DataType, right: &DataType) -> bool {
    match (left, right) {
        (DataType::Struct(left), DataType::Struct(right)) => {
            left.len() == right.len()
                && left.iter().zip(right).all(|(left, right)| {
                    left.name() == right.name()
                        && same_physical_type(left.data_type(), right.data_type())
                })
        }
        (DataType::List(left), DataType::List(right))
        | (DataType::LargeList(left), DataType::LargeList(right))
        | (DataType::ListView(left), DataType::ListView(right))
        | (DataType::LargeListView(left), DataType::LargeListView(right)) => {
            same_physical_type(left.data_type(), right.data_type())
        }
        (DataType::FixedSizeList(left, l), DataType::FixedSizeList(right, r)) => {
            l == r && same_physical_type(left.data_type(), right.data_type())
        }
        (DataType::Map(left, l), DataType::Map(right, r)) => {
            l == r && same_physical_type(left.data_type(), right.data_type())
        }
        _ => left == right,
    }
}

/// Apply merged annotations without changing the caller's value coercion rules.
fn with_common_metadata(source: &DataType, common: &DataType) -> Option<DataType> {
    if source.is_null() {
        return Some(source.clone());
    }
    let field = |source: &FieldRef, common: &FieldRef| {
        let data_type = with_common_metadata(source.data_type(), common.data_type())?;
        let mut field = source.as_ref().clone().with_data_type(data_type);
        for key in [
            SPARK_METADATA_JSON_KEY,
            "comment",
            SAIL_SPARK_INTERVAL_METADATA_KEY,
        ] {
            let value = common.metadata().get(key).filter(|_| {
                key != SAIL_SPARK_INTERVAL_METADATA_KEY || source.data_type() == common.data_type()
            });
            match value {
                Some(value) => {
                    field.metadata_mut().insert(key.to_string(), value.clone());
                }
                None => {
                    field.metadata_mut().remove(key);
                }
            }
        }
        Some(Arc::new(field))
    };
    Some(match (source, common) {
        (DataType::Struct(source), DataType::Struct(common))
            if source.len() == common.len()
                && source.iter().zip(common).all(|(s, c)| s.name() == c.name()) =>
        {
            DataType::Struct(
                source
                    .iter()
                    .zip(common)
                    .map(|(s, c)| field(s, c))
                    .collect::<Option<Vec<_>>>()?
                    .into(),
            )
        }
        (DataType::List(source), DataType::List(common)) => DataType::List(field(source, common)?),
        (DataType::LargeList(source), DataType::LargeList(common)) => {
            DataType::LargeList(field(source, common)?)
        }
        (DataType::ListView(source), DataType::ListView(common)) => {
            DataType::ListView(field(source, common)?)
        }
        (DataType::LargeListView(source), DataType::LargeListView(common)) => {
            DataType::LargeListView(field(source, common)?)
        }
        (DataType::FixedSizeList(source, size), DataType::FixedSizeList(common, common_size))
            if size == common_size =>
        {
            DataType::FixedSizeList(field(source, common)?, *size)
        }
        (DataType::Map(source, sorted), DataType::Map(common, common_sorted))
            if sorted == common_sorted =>
        {
            DataType::Map(field(source, common)?, *sorted)
        }
        _ if !source.is_nested() && !common.is_nested() => source.clone(),
        _ => return None,
    })
}

/// Coerce field metadata/nullability differences; ordinary value coercion remains with the caller.
pub fn coerce_common_metadata(
    arguments: Vec<Expr>,
    schema: &dyn datafusion_common::ExprSchema,
) -> Result<Vec<Expr>> {
    let types = arguments
        .iter()
        .map(|arg| arg.get_type(schema))
        .collect::<Result<Vec<_>>>()?;
    let Some(first) = types.iter().find(|data_type| !data_type.is_null()) else {
        return Ok(arguments);
    };
    let same_physical = types
        .iter()
        .all(|data_type| data_type.is_null() || same_physical_type(data_type, first));
    let mut common = first.clone();
    for data_type in &types {
        common = match common_type_with_metadata(&common, data_type)? {
            Some(common) => common,
            None if same_physical => {
                return Err(plan_datafusion_err!("incompatible metadata types"));
            }
            None => return Ok(arguments),
        };
    }
    // Mixed physical types still follow the function's own coercion rules. In
    // particular, comparison coercion must not choose a conditional's value type.
    let targets = if same_physical {
        vec![common; types.len()]
    } else {
        let Some(targets) = types
            .iter()
            .map(|data_type| with_common_metadata(data_type, &common))
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(arguments);
        };
        targets
    };
    Ok(arguments
        .into_iter()
        .zip(types)
        .zip(targets)
        .map(|((arg, data_type), target)| {
            if data_type == target {
                arg
            } else {
                cast(arg, target)
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::{IntervalUnit, Schema};
    use datafusion_common::DFSchema;
    use datafusion_expr::col;

    use super::*;

    fn interval_field(qualifier: &str, nullable: bool) -> FieldRef {
        Arc::new(
            Field::new(
                "item",
                DataType::Interval(IntervalUnit::YearMonth),
                nullable,
            )
            .with_metadata(HashMap::from([
                (SAIL_SPARK_INTERVAL_METADATA_KEY.into(), qualifier.into()),
                (
                    SPARK_METADATA_JSON_KEY.into(),
                    r#"{"comment":"source"}"#.into(),
                ),
                ("comment".into(), "source".into()),
            ])),
        )
    }

    #[test]
    fn view_metadata_merges_qualifiers_and_nullability() -> Result<()> {
        let year = r#"{"unit":"yearMonth","startField":"year","endField":"year"}"#;
        let month = r#"{"unit":"yearMonth","startField":"month","endField":"month"}"#;
        let year_month = r#"{"unit":"yearMonth","startField":"year","endField":"month"}"#;
        for view in [DataType::ListView, DataType::LargeListView] {
            for nullable in [false, true] {
                let left = view(interval_field(year, false));
                let right = view(interval_field(month, nullable));
                let expected = view(Arc::new(
                    Field::new(
                        "item",
                        DataType::Interval(IntervalUnit::YearMonth),
                        nullable,
                    )
                    .with_metadata(HashMap::from([(
                        SAIL_SPARK_INTERVAL_METADATA_KEY.into(),
                        year_month.into(),
                    )])),
                ));
                assert!(same_physical_type(&left, &right));
                assert_eq!(
                    common_type_with_metadata(&left, &right)?,
                    Some(expected.clone())
                );
                assert_eq!(
                    common_type_with_metadata(&right, &left)?,
                    Some(expected.clone())
                );
                // Equal types and NULL inputs keep their existing field metadata.
                assert_eq!(common_type_with_metadata(&left, &left)?, Some(left.clone()));
                assert_eq!(
                    common_type_with_metadata(&left, &DataType::Null)?,
                    Some(left.clone())
                );
                let schema = DFSchema::try_from(Schema::new(vec![
                    Field::new("a", left, false),
                    Field::new("b", right, false),
                ]))?;
                for expr in coerce_common_metadata(vec![col("a"), col("b")], &schema)? {
                    assert_eq!(expr.get_type(&schema)?, expected);
                }
            }
        }
        Ok(())
    }

    #[test]
    fn nested_views_merge_nullability_without_changing_physical_types() -> Result<()> {
        let wrap = |view| {
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(vec![Arc::new(Field::new("values", view, false))].into()),
                false,
            )))
        };
        for view in [DataType::ListView, DataType::LargeListView] {
            let non_null = Arc::new(Field::new("item", DataType::Int32, false));
            let nullable = Arc::new(non_null.as_ref().clone().with_nullable(true));
            let left = wrap(view(non_null));
            let right = wrap(view(nullable));
            assert!(same_physical_type(&left, &right));
            assert_eq!(
                common_type_with_metadata(&left, &right)?,
                Some(right.clone())
            );
            assert_eq!(
                common_type_with_metadata(&right, &left)?,
                Some(right.clone())
            );
            let incompatible = wrap(view(Arc::new(Field::new("item", DataType::Boolean, true))));
            assert!(!same_physical_type(&right, &incompatible));
            assert_eq!(common_type_with_metadata(&right, &incompatible)?, None);
        }
        let field = Arc::new(Field::new("item", DataType::Int32, false));
        let view = DataType::ListView(Arc::clone(&field));
        for other in [
            DataType::List(Arc::clone(&field)),
            DataType::LargeListView(field),
        ] {
            assert!(!same_physical_type(&view, &other));
            assert_eq!(common_type_with_metadata(&view, &other)?, None);
        }
        Ok(())
    }
}
