use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{DFSchema, Result, plan_err};
use datafusion_expr::{Expr, ExprSchemable, ScalarUDF, cast};
use sail_function::scalar::spark_struct_rename::SparkStructRename;

/// Validate struct names with Spark's resolver and align them positionally before
/// Arrow casts can match/reorder them by name. Keep leaf types and nullability.
pub(crate) fn align_struct_names(
    expressions: Vec<Expr>,
    schema: &DFSchema,
    case_sensitive: bool,
) -> Result<Vec<Expr>> {
    align_names(expressions, schema, case_sensitive, false)
}

/// Comparisons accept structurally equal operands regardless of names/nullability.
/// Finding a wider complex type still requires Spark's configured name resolver.
pub(crate) fn align_comparison_struct_names(
    expressions: Vec<Expr>,
    schema: &DFSchema,
    case_sensitive: bool,
) -> Result<Vec<Expr>> {
    align_names(expressions, schema, case_sensitive, true)
}

fn align_names(
    expressions: Vec<Expr>,
    schema: &DFSchema,
    case_sensitive: bool,
    comparison: bool,
) -> Result<Vec<Expr>> {
    let types = expressions
        .iter()
        .map(|expr| expr.get_type(schema))
        .collect::<Result<Vec<_>>>()?;
    let Some(reference) = types.iter().find(|data_type| !data_type.is_null()) else {
        return Ok(expressions);
    };
    let require_names = !comparison
        || !types
            .iter()
            .all(|data_type| structurally_equal(data_type, reference));
    expressions
        .into_iter()
        .zip(&types)
        .map(|(expr, source)| {
            let target = aligned_type(source, reference, case_sensitive, require_names)?;
            if &target == source {
                Ok(expr)
            } else {
                Ok(ScalarUDF::from(SparkStructRename::new(target)).call(vec![expr]))
            }
        })
        .collect()
}

/// DataFusion's nested CAST maps by name even when ordered names match. Stage
/// duplicate fields through unique names, keeping all leaf conversion in CAST.
pub(crate) fn cast_comparison_operand(expression: Expr, from: &DataType, to: &DataType) -> Expr {
    if from == to {
        return expression;
    }
    if !has_duplicate_struct_names(from) && !has_duplicate_struct_names(to) {
        return cast(expression, to.clone());
    }
    let renamed_from = unique_struct_names(from);
    let renamed_to = unique_struct_names(to);
    let expression = if &renamed_from == from {
        expression
    } else {
        ScalarUDF::from(SparkStructRename::new(renamed_from)).call(vec![expression])
    };
    let expression = cast(expression, renamed_to.clone());
    if &renamed_to == to {
        expression
    } else {
        ScalarUDF::from(SparkStructRename::new(to.clone())).call(vec![expression])
    }
}

fn has_duplicate_struct_names(data_type: &DataType) -> bool {
    match data_type {
        DataType::Struct(fields) => {
            let mut names = HashSet::new();
            fields.iter().any(|field| {
                !names.insert(field.name()) || has_duplicate_struct_names(field.data_type())
            })
        }
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => has_duplicate_struct_names(field.data_type()),
        _ => false,
    }
}

fn unique_struct_names(data_type: &DataType) -> DataType {
    let rename_field = |field: &FieldRef| {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(unique_struct_names(field.data_type())),
        )
    };
    match data_type {
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    Arc::new(
                        rename_field(field)
                            .as_ref()
                            .clone()
                            .with_name(format!("__sail_comparison_field_{index}")),
                    )
                })
                .collect(),
        ),
        DataType::List(field) => DataType::List(rename_field(field)),
        DataType::LargeList(field) => DataType::LargeList(rename_field(field)),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(rename_field(field), *size),
        DataType::Map(field, sorted) => DataType::Map(rename_field(field), *sorted),
        _ => data_type.clone(),
    }
}

fn structurally_equal(left: &DataType, right: &DataType) -> bool {
    use DataType::*;
    match (left, right) {
        (Struct(left), Struct(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| structurally_equal(left.data_type(), right.data_type()))
        }
        (List(left), List(right)) | (LargeList(left), LargeList(right)) => {
            structurally_equal(left.data_type(), right.data_type())
        }
        (FixedSizeList(left, left_size), FixedSizeList(right, right_size)) => {
            left_size == right_size && structurally_equal(left.data_type(), right.data_type())
        }
        (Map(left, left_sorted), Map(right, right_sorted)) => {
            left_sorted == right_sorted && structurally_equal(left.data_type(), right.data_type())
        }
        _ => left == right,
    }
}

fn aligned_type(
    source: &DataType,
    reference: &DataType,
    case_sensitive: bool,
    require_names: bool,
) -> Result<DataType> {
    use DataType::*;
    let align_field = |source: &FieldRef, reference: &FieldRef| -> Result<FieldRef> {
        Ok(Arc::new(source.as_ref().clone().with_data_type(
            aligned_type(
                source.data_type(),
                reference.data_type(),
                case_sensitive,
                require_names,
            )?,
        )))
    };
    Ok(match (source, reference) {
        (Struct(source), Struct(reference)) => {
            if source.len() != reference.len() {
                return plan_err!("Cannot coerce structs with different field counts");
            }
            Struct(
                source
                    .iter()
                    .zip(reference)
                    .map(|(source, reference)| {
                        if require_names
                            && source.name() != reference.name()
                            && (case_sensitive
                                || !source.name().eq_ignore_ascii_case(reference.name()))
                        {
                            return plan_err!(
                                "Cannot coerce struct fields {} and {} at the same position",
                                reference.name(),
                                source.name()
                            );
                        }
                        Ok(Arc::new(
                            source
                                .as_ref()
                                .clone()
                                .with_name(reference.name())
                                .with_data_type(aligned_type(
                                    source.data_type(),
                                    reference.data_type(),
                                    case_sensitive,
                                    require_names,
                                )?),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into(),
            )
        }
        (List(source), List(reference) | LargeList(reference) | FixedSizeList(reference, _)) => {
            List(align_field(source, reference)?)
        }
        (
            LargeList(source),
            List(reference) | LargeList(reference) | FixedSizeList(reference, _),
        ) => LargeList(align_field(source, reference)?),
        (
            FixedSizeList(source, size),
            List(reference) | LargeList(reference) | FixedSizeList(reference, _),
        ) => FixedSizeList(align_field(source, reference)?, *size),
        (Map(source, sorted), Map(reference, _)) => Map(align_field(source, reference)?, *sorted),
        _ => source.clone(),
    })
}
