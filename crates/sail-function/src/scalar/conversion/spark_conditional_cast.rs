use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, OffsetSizeTrait, StructArray, UInt64Array, make_array,
    new_empty_array,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::{CastOptions, cast_with_options, nullif, take};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::invalid_arg_count_exec_err;

// Strict casts introduced by conditional branch and numeric argument coercion. Keeping the cast
// in a UDF lets DataFusion defer invalid literals in unselected CASE branches.
// TODO: Preserve Spark's eager constant-cast errors outside conditional branches,
//  including UNION inputs consumed by a conditional filter. Shared constant folding
//  currently defers those errors too, so filtering can discard an invalid input.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkConditionalCast {
    signature: Signature,
    target_type: DataType,
}

impl SparkConditionalCast {
    pub fn new(target_type: DataType) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            target_type,
        }
    }

    pub fn target_type(&self) -> &DataType {
        &self.target_type
    }
}

impl ScalarUDFImpl for SparkConditionalCast {
    fn name(&self) -> &str {
        "spark_conditional_cast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.target_type.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [source] = args.arg_fields else {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (1, 1),
                args.arg_fields.len(),
            ));
        };
        if source.data_type() == &self.target_type {
            return Ok(Arc::new(source.as_ref().clone().with_name(self.name())));
        }
        let nullable = source.is_nullable()
            || (source.data_type().is_string() && self.target_type.is_numeric())
            // Spark Cast.forceNullable includes fractional-to-integral casts,
            // including in ANSI mode where an invalid value raises an error.
            || ((source.data_type().is_floating() || source.data_type().is_decimal())
            && self.target_type.is_integer());
        Ok(Arc::new(Field::new(
            self.name(),
            self.target_type.clone(),
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (1, 1),
                args.args.len(),
            ));
        };
        // IN-list planning probes expressions with an empty batch. A row-dependent
        // CASE can return a scalar for that batch, so prevent it from looking constant.
        if args.number_rows == 0 {
            return Ok(ColumnarValue::Array(new_empty_array(&self.target_type)));
        }
        if arg.data_type() == self.target_type {
            return Ok(arg.clone());
        }
        // TODO: Match Spark's numeric STRING grammar when shared cast support is available:
        // control-character trimming, floating-point suffixes/hex literals, and DECIMAL exponents.
        // Arrow's parser currently rejects these forms, as it does for ordinary CAST expressions.
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        match arg {
            ColumnarValue::Scalar(value) if value.data_type().is_nested() => {
                let array = cast_visible_values(&value.to_array()?, &self.target_type, &options)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &array, 0,
                )?))
            }
            ColumnarValue::Scalar(value) => Ok(ColumnarValue::Scalar(
                value.cast_to_with_options(&self.target_type, &options)?,
            )),
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(cast_visible_values(
                array,
                &self.target_type,
                &options,
            )?)),
        }
    }
}

fn cast_visible_values(
    array: &ArrayRef,
    target: &DataType,
    options: &CastOptions<'_>,
) -> Result<ArrayRef> {
    if array.data_type() == target {
        return Ok(Arc::clone(array));
    }
    Ok(cast_with_options(
        &visible_nested_values(array, Some(target))?,
        target,
        options,
    )?)
}

fn has_hidden_values<O: OffsetSizeTrait>(offsets: &[O], nulls: Option<&NullBuffer>) -> bool {
    nulls.is_some_and(|nulls| {
        nulls.null_count() > 0
            && nulls
                .iter()
                .zip(offsets.windows(2))
                .any(|(valid, range)| !valid && range[0] != range[1])
    })
}

// Spark casts only present nested values. Arrow casts every child slot, including
// values masked by a NULL parent or outside the offsets of a sliced list/map.
fn visible_nested_values(array: &ArrayRef, target: Option<&DataType>) -> Result<ArrayRef> {
    if target == Some(array.data_type()) || !array.data_type().is_nested() {
        return Ok(Arc::clone(array));
    }
    let sliced_values = match array.data_type() {
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            list.value_offsets()[0] != 0
                || list.value_offsets()[list.len()] as usize != list.values().len()
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            list.value_offsets()[0] != 0
                || list.value_offsets()[list.len()] as usize != list.values().len()
        }
        DataType::Map(_, _) => {
            let map = array.as_map();
            map.value_offsets()[0] != 0
                || map.value_offsets()[map.len()] as usize != map.entries().len()
        }
        _ => false,
    };
    let mask_struct = matches!(array.data_type(), DataType::Struct(_)) && array.null_count() > 0;
    // A NULL list/map entry hides child values only when its offsets span child slots.
    // Conditional outputs usually encode NULL entries as empty ranges.
    let hidden_values = match array.data_type() {
        DataType::List(_) => {
            has_hidden_values(array.as_list::<i32>().value_offsets(), array.nulls())
        }
        DataType::LargeList(_) => {
            has_hidden_values(array.as_list::<i64>().value_offsets(), array.nulls())
        }
        DataType::Map(_, _) => has_hidden_values(array.as_map().value_offsets(), array.nulls()),
        _ => array.null_count() > 0 && !mask_struct,
    };
    let array = if hidden_values || sliced_values {
        // List/map take drops entries belonging to null parents and unused
        // prefixes/suffixes without introducing NULLs into non-nullable entries.
        let indices =
            UInt64Array::from_iter((0..array.len()).map(|i| array.is_valid(i).then_some(i as u64)));
        take(array, &indices, None)?
    } else {
        Arc::clone(array)
    };
    let data = array.to_data();
    let parent_mask = if mask_struct {
        array
            .nulls()
            .map(|nulls| BooleanArray::new(!nulls.inner(), None))
    } else {
        None
    };
    let mut changed = mask_struct;
    // Unchanged siblings are not cast, so their hidden values need no masking or
    // compaction. In particular, avoid copying large lists beside a cast scalar.
    // For reordered structs, leave matching to Arrow and keep the existing full
    // masking path rather than assuming that source and target positions agree.
    let target_fields = match (array.data_type(), target) {
        (DataType::Struct(source), Some(DataType::Struct(target)))
            if source.len() == target.len()
                && source.iter().zip(target).all(|(a, b)| a.name() == b.name()) =>
        {
            Some(target)
        }
        _ => None,
    };
    let children = data
        .child_data()
        .iter()
        .enumerate()
        .map(|(index, child)| {
            let child = make_array(child.clone());
            let child_target = match (array.data_type(), target) {
                (
                    DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _),
                    Some(
                        DataType::List(field)
                        | DataType::LargeList(field)
                        | DataType::FixedSizeList(field, _),
                    ),
                )
                | (DataType::Map(_, _), Some(DataType::Map(field, _))) => Some(field.data_type()),
                _ => target_fields.map(|fields| fields[index].data_type()),
            };
            if child_target == Some(child.data_type()) {
                return Ok(child);
            }
            // The kernel changes only validity, without copying or revalidating
            // value buffers (including potentially large STRING children).
            let child = match &parent_mask {
                Some(mask) => nullif(child.as_ref(), mask)?,
                None => child,
            };
            let visible = visible_nested_values(&child, child_target)?;
            changed |= !Arc::ptr_eq(&child, &visible);
            Ok(visible)
        })
        .collect::<Result<Vec<_>>>()?;
    if changed {
        if let DataType::Struct(fields) = array.data_type() {
            return Ok(Arc::new(StructArray::try_new(
                fields.clone(),
                children,
                array.nulls().cloned(),
            )?));
        }
        Ok(make_array(
            data.into_builder()
                .child_data(children.iter().map(|child| child.to_data()).collect())
                .build()?,
        ))
    } else {
        Ok(array)
    }
}
