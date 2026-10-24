//! Common field rules for explicit casts inserted during query analysis.

use std::sync::Arc;

use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use arrow_schema::{DataType, FieldRef};
use datafusion_common::{Result, plan_err};
use datafusion_expr::Operator;
use datafusion_expr::type_coercion::binary::{BinaryTypeCoercer, type_union_resolution};

/// A cast preserves source nullability and non-extension metadata. A field's
/// extension describes its physical representation and cannot survive a change
/// of that representation without a policy explicitly reattaching it.
pub fn cast_field(source: &FieldRef, target: &DataType) -> FieldRef {
    if source.data_type() == target {
        return Arc::clone(source);
    }
    let mut metadata = source.metadata().clone();
    metadata.remove(EXTENSION_TYPE_NAME_KEY);
    metadata.remove(EXTENSION_TYPE_METADATA_KEY);
    Arc::new(
        source
            .as_ref()
            .clone()
            .with_data_type(target.clone())
            .with_metadata(metadata),
    )
}

pub fn common_type(types: &[DataType]) -> Result<DataType> {
    let non_null = types
        .iter()
        .filter(|t| !t.is_null())
        .cloned()
        .collect::<Vec<_>>();
    let Some(first) = non_null.first() else {
        return Ok(DataType::Null);
    };
    if non_null.iter().all(|t| t == first) {
        return Ok(first.clone());
    }
    if let DataType::Struct(first_fields) = first {
        let structs = non_null
            .iter()
            .map(|t| match t {
                DataType::Struct(fields)
                    if fields.len() == first_fields.len()
                        && fields
                            .iter()
                            .zip(first_fields)
                            .all(|(a, b)| a.name() == b.name()) =>
                {
                    Some(fields)
                }
                _ => None,
            })
            .collect::<Option<Vec<_>>>();
        if let Some(structs) = structs {
            let fields = first_fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    let target = common_type(
                        &structs
                            .iter()
                            .map(|fields| fields[index].data_type().clone())
                            .collect::<Vec<_>>(),
                    )?;
                    Ok(Arc::new(
                        field.as_ref().clone().with_data_type(target).with_nullable(
                            structs.iter().any(|fields| fields[index].is_nullable()),
                        ),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(DataType::Struct(fields.into()));
        }
    }
    if matches!(first, DataType::List(_) | DataType::LargeList(_)) {
        let fields = non_null
            .iter()
            .map(|t| match t {
                DataType::List(f) | DataType::LargeList(f) => Some(f),
                _ => None,
            })
            .collect::<Option<Vec<_>>>();
        if let Some(fields) = fields {
            let target = common_type(
                &fields
                    .iter()
                    .map(|f| f.data_type().clone())
                    .collect::<Vec<_>>(),
            )?;
            let field = Arc::new(
                fields[0]
                    .as_ref()
                    .clone()
                    .with_data_type(target)
                    .with_nullable(fields.iter().any(|f| f.is_nullable())),
            );
            return Ok(
                if non_null.iter().any(|t| matches!(t, DataType::LargeList(_))) {
                    DataType::LargeList(field)
                } else {
                    DataType::List(field)
                },
            );
        }
    }
    type_union_resolution(&non_null)
        .ok_or_else(|| datafusion_common::plan_datafusion_err!("no common type for {types:?}"))
}

/// Comparison operands admit conversions (such as a string to a date) that
/// are not valid when determining the output type of a UNION or CASE.
pub fn comparison_type(types: &[DataType]) -> Result<DataType> {
    if let Ok(data_type) = common_type(types) {
        return Ok(data_type);
    }
    types.iter().try_fold(DataType::Null, |common, next| {
        datafusion_expr::type_coercion::binary::comparison_coercion(&common, next).ok_or_else(
            || datafusion_common::plan_datafusion_err!("no common comparison type for {types:?}"),
        )
    })
}

pub struct BinaryTyping {
    pub left: DataType,
    pub right: DataType,
    pub result: DataType,
    pub nullable: bool,
}

pub fn binary(left: &FieldRef, operator: Operator, right: &FieldRef) -> Result<BinaryTyping> {
    let coercer = BinaryTypeCoercer::new(left.data_type(), &operator, right.data_type());
    let (left_type, right_type) = coercer.get_input_types()?;
    let result = BinaryTypeCoercer::new(&left_type, &operator, &right_type).get_result_type()?;
    Ok(BinaryTyping {
        left: left_type,
        right: right_type,
        result,
        nullable: !matches!(
            operator,
            Operator::IsDistinctFrom | Operator::IsNotDistinctFrom
        ) && (left.is_nullable() || right.is_nullable()),
    })
}

pub fn validate_arity(name: &str, actual: usize, expected: usize) -> Result<()> {
    if actual != expected {
        return plan_err!("{name} expects {expected} arguments, got {actual}");
    }
    Ok(())
}

/// INT zero contributes ten integer digits while the decimal remains bounded
/// by Spark's maximum precision of 38 digits.
pub fn zeroifnull_decimal_type(precision: u8, scale: i8) -> DataType {
    let integer_digits = (i16::from(precision) - i16::from(scale)).max(10);
    let scale = i16::from(scale).max(0);
    let precision = (integer_digits + scale).min(38) as u8;
    let scale = scale.min((38 - integer_digits).max(0)) as i8;
    DataType::Decimal128(precision, scale)
}
