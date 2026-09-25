use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float32Array, Float64Array, LargeListArray, ListArray, MapArray,
    StructArray, UInt64Array, new_empty_array, new_null_array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::{concat, take, take_arrays};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, plan_err};
use datafusion_expr::type_coercion::binary::comparison_coercion;
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};
use sail_common::spec::{SAIL_MAP_FIELD_NAME, SAIL_MAP_KEY_FIELD_NAME, SAIL_MAP_VALUE_FIELD_NAME};

use crate::functions_nested_utils::{evaluate_lambdas_until_null, scatter_active_rows};
use crate::scalar::array::lambda_utils::coerce_single_list_arg;

// Spark's ZipWith and MapZipWith in higherOrderFunctions.scala. Both evaluate
// their inputs left-to-right, then invoke a lambda over aligned element pairs.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkZipWith {
    signature: HigherOrderSignature,
    map: bool,
    ansi_mode: bool,
    case_sensitive: bool,
}

impl SparkZipWith {
    pub fn new(map: bool, ansi_mode: bool, case_sensitive: bool) -> Self {
        Self {
            // The planner wraps the two collections in hidden lambdas so a null
            // left collection can skip evaluation of the right collection.
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Lambda(()); 3],
                Volatility::Immutable,
            ),
            map,
            ansi_mode,
            case_sensitive,
        }
    }

    pub fn is_map(&self) -> bool {
        self.map
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }

    pub fn case_sensitive(&self) -> bool {
        self.case_sensitive
    }

    pub fn coerce_collection_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        let [left, right] = types else {
            return plan_err!("{} requires two collections", self.name());
        };
        if !self.map {
            return types
                .iter()
                .map(|data_type| {
                    Ok(
                        coerce_single_list_arg(self.name(), std::slice::from_ref(data_type))?
                            .remove(0),
                    )
                })
                .collect();
        }
        let left_fields = map_fields(left)?;
        let right_fields = map_fields(right)?;
        let left_key = left_fields[0].data_type();
        let right_key = right_fields[0].data_type();
        let key = common_key_type(left_key, right_key, self.ansi_mode, self.case_sensitive)
            // Spark checks Cast.forceNullable on the outer key. The same cast is
            // allowed inside array/struct keys, whose nested fields become nullable.
            .filter(|key| !key_cast_nullable(left_key, key) && !key_cast_nullable(right_key, key))
            .ok_or_else(|| {
                datafusion_common::plan_datafusion_err!(
                    "map_zip_with requires compatible map key types, got {} and {}",
                    left_fields[0].data_type(),
                    right_fields[0].data_type()
                )
            })?;
        Ok([left_fields, right_fields]
            .into_iter()
            .map(|fields| {
                map_type(
                    key.clone(),
                    fields[1].data_type().clone(),
                    fields[1].is_nullable(),
                )
            })
            .collect())
    }

    // TODO: Preserve supplied interval qualifiers after shared nested-lambda
    // rebinding retains field metadata through optimization; see the Sail-only
    // cases in zip_with_interval_qualifiers.feature.
    fn parameters(&self, left: &FieldRef, right: &FieldRef) -> Result<Vec<FieldRef>> {
        let types =
            self.coerce_collection_types(&[left.data_type().clone(), right.data_type().clone()])?;
        if self.map {
            let left = map_fields(&types[0])?;
            let right = map_fields(&types[1])?;
            Ok(vec![
                Arc::new(Field::new("", left[0].data_type().clone(), false)),
                Arc::new(Field::new("", left[1].data_type().clone(), true)),
                Arc::new(Field::new("", right[1].data_type().clone(), true)),
            ])
        } else {
            types
                .iter()
                .map(|data_type| match data_type {
                    DataType::List(field) | DataType::LargeList(field) => {
                        Ok(Arc::new(Field::new("", field.data_type().clone(), true)))
                    }
                    _ => plan_err!("zip_with requires arrays"),
                })
                .collect()
        }
    }
}

impl HigherOrderUDFImpl for SparkZipWith {
    fn name(&self) -> &str {
        if self.map { "map_zip_with" } else { "zip_with" }
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            Ok(vec![])
        } else {
            plan_err!("{} expects lambda arguments", self.name())
        }
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        // Before resolving user lambdas Sail asks with the original value
        // arguments; after planning those values are hidden lambdas themselves.
        match fields {
            [
                ValueOrLambda::Value(left),
                ValueOrLambda::Value(right),
                ValueOrLambda::Lambda(_),
            ] => Ok(LambdaParametersProgress::Complete(vec![
                self.parameters(left, right)?,
            ])),
            [
                ValueOrLambda::Lambda(left),
                ValueOrLambda::Lambda(right),
                ValueOrLambda::Lambda(_),
            ] => {
                let dummy = vec![Arc::new(Field::new("", DataType::Null, true))];
                match (left, right) {
                    (Some(left), Some(right)) => Ok(LambdaParametersProgress::Complete(vec![
                        dummy.clone(),
                        dummy,
                        self.parameters(left, right)?,
                    ])),
                    _ => Ok(LambdaParametersProgress::Partial(vec![
                        Some(dummy.clone()),
                        Some(dummy),
                        None,
                    ])),
                }
            }
            _ => plan_err!("{} expects two collections and a lambda", self.name()),
        }
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        let [
            ValueOrLambda::Lambda(left),
            ValueOrLambda::Lambda(right),
            ValueOrLambda::Lambda(function),
        ] = args.arg_fields
        else {
            return plan_err!("{} expects three lambda arguments", self.name());
        };
        let types =
            self.coerce_collection_types(&[left.data_type().clone(), right.data_type().clone()])?;
        // TODO: Preserve precise literal-struct key fields once named_struct's
        // existing nullable-field inference is fixed (see the Sail-only xfail).
        let data_type = if self.map {
            map_type(
                map_fields(&types[0])?[0].data_type().clone(),
                function.data_type().clone(),
                function.is_nullable(),
            )
        } else {
            let field = Arc::new(Field::new_list_field(
                function.data_type().clone(),
                function.is_nullable(),
            ));
            if types
                .iter()
                .any(|data_type| matches!(data_type, DataType::LargeList(_)))
            {
                DataType::LargeList(field)
            } else {
                DataType::List(field)
            }
        };
        Ok(Arc::new(Field::new(
            "",
            data_type,
            left.is_nullable() || right.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let [
            ValueOrLambda::Lambda(left),
            ValueOrLambda::Lambda(right),
            ValueOrLambda::Lambda(function),
        ] = args.args.as_slice()
        else {
            return plan_err!("{} expects three lambda arguments", self.name());
        };
        let (collections, rows) = evaluate_lambdas_until_null(&[left, right], args.number_rows)?;
        if rows.is_empty() {
            return Ok(ColumnarValue::Array(new_null_array(
                args.return_type(),
                args.number_rows,
            )));
        }
        let left = &collections[0];
        let right = &collections[1];
        let mut row_indices = Vec::new();
        let mut lengths = Vec::with_capacity(rows.len());
        let mut left_indices = Vec::new();
        let mut right_indices = Vec::new();
        let mut key_indices = Vec::new();
        let (left_values, right_values, keys) = if self.map {
            let left = left.as_map();
            let right = right.as_map();
            let keys = concat(&[left.keys().as_ref(), right.keys().as_ref()])?;
            // TODO: Support Spark's legacy mapZipWithUsesJavaCollections=false
            // setting when it is exposed through PlanConfig. That compatibility
            // mode deliberately keeps separate NaN keys (see the sail-bug test).
            let normalized = normalize_keys(&keys)?;
            let mut positions = HashMap::new();
            for (row, original_row) in rows.iter().copied().enumerate() {
                positions.clear();
                let start = key_indices.len();
                for (side, map) in [left, right].into_iter().enumerate() {
                    let offsets = map.value_offsets();
                    for index in offsets[row] as usize..offsets[row + 1] as usize {
                        let key_index = index + if side == 0 { 0 } else { left.keys().len() };
                        let key = ScalarValue::try_from_array(&normalized, key_index)?.compacted();
                        let position = *positions.entry(key).or_insert_with(|| {
                            let position = key_indices.len();
                            key_indices.push(key_index as u64);
                            left_indices.push(None);
                            right_indices.push(None);
                            row_indices.push(original_row);
                            position
                        });
                        let indices = if side == 0 {
                            &mut left_indices
                        } else {
                            &mut right_indices
                        };
                        // Spark passes only the first value for a duplicate key.
                        indices[position].get_or_insert(index as u64);
                    }
                }
                lengths.push(key_indices.len() - start);
            }
            // Spark normalizes top-level floating map keys as well as comparing
            // them canonically; nested keys preserve the first original value.
            let keys = if keys.data_type().is_floating() {
                normalized
            } else {
                keys
            };
            (
                Arc::clone(left.values()),
                Arc::clone(right.values()),
                Some(keys),
            )
        } else {
            let (left_values, left_offsets) = list_parts(left)?;
            let (right_values, right_offsets) = list_parts(right)?;
            for (row, original_row) in rows.iter().copied().enumerate() {
                let left_length = left_offsets[row + 1] - left_offsets[row];
                let right_length = right_offsets[row + 1] - right_offsets[row];
                let length = left_length.max(right_length);
                for index in 0..length {
                    left_indices
                        .push((index < left_length).then_some((left_offsets[row] + index) as u64));
                    right_indices.push(
                        (index < right_length).then_some((right_offsets[row] + index) as u64),
                    );
                    row_indices.push(original_row);
                }
                lengths.push(length);
            }
            (left_values, right_values, None)
        };
        let left_indices = UInt64Array::from(left_indices);
        let right_indices = UInt64Array::from(right_indices);
        let row_indices = UInt64Array::from(row_indices);
        let key_indices = UInt64Array::from(key_indices);
        let left_param = || Ok(take(&left_values, &left_indices, None)?);
        let right_param = || Ok(take(&right_values, &right_indices, None)?);
        let keys = keys
            .map(|keys| take(&keys, &key_indices, None))
            .transpose()?;
        let key_param = || {
            keys.clone()
                .ok_or_else(|| exec_datafusion_err!("map_zip_with missing keys"))
        };
        let params: Vec<&dyn Fn() -> Result<ArrayRef>> = if self.map {
            vec![&key_param, &left_param, &right_param]
        } else {
            vec![&left_param, &right_param]
        };
        let element_type = match args.return_type() {
            DataType::Map(_, _) => map_fields(args.return_type())?[1].data_type(),
            DataType::List(field) | DataType::LargeList(field) => field.data_type(),
            _ => return plan_err!("{} has invalid return type", self.name()),
        };
        let values = if row_indices.is_empty() {
            // A constant/erroring lambda must never run on empty collections.
            new_empty_array(element_type)
        } else {
            function
                .evaluate(&params, |captures| {
                    Ok(take_arrays(captures, &row_indices, None)?)
                })?
                .into_array(row_indices.len())?
        };
        let output: ArrayRef = match args.return_type() {
            DataType::Map(field, _) => {
                let fields = map_fields(args.return_type())?;
                let keys = keys.ok_or_else(|| exec_datafusion_err!("map_zip_with missing keys"))?;
                Arc::new(MapArray::try_new(
                    Arc::clone(field),
                    checked_offsets(&lengths)?,
                    StructArray::try_new(fields.clone(), vec![keys, values], None)?,
                    None,
                    false,
                )?)
            }
            DataType::List(field) => Arc::new(ListArray::try_new(
                Arc::clone(field),
                checked_offsets(&lengths)?,
                values,
                None,
            )?),
            DataType::LargeList(field) => Arc::new(LargeListArray::try_new(
                Arc::clone(field),
                OffsetBuffer::from_lengths(lengths),
                values,
                None,
            )?),
            _ => return plan_err!("{} has invalid return type", self.name()),
        };
        Ok(ColumnarValue::Array(scatter_active_rows(
            output,
            &rows,
            args.number_rows,
        )?))
    }
}

fn checked_offsets(lengths: &[usize]) -> Result<OffsetBuffer<i32>> {
    let mut offsets = vec![0i32];
    let mut total = 0i32;
    for &length in lengths {
        total = i32::try_from(length)
            .ok()
            .and_then(|length| total.checked_add(length))
            .ok_or_else(|| exec_datafusion_err!("zip output exceeds Arrow collection capacity"))?;
        offsets.push(total);
    }
    Ok(OffsetBuffer::new(offsets.into()))
}

fn list_parts(array: &ArrayRef) -> Result<(ArrayRef, Vec<usize>)> {
    match array.data_type() {
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            Ok((
                Arc::clone(list.values()),
                list.value_offsets()
                    .iter()
                    .map(|&offset| offset as usize)
                    .collect(),
            ))
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            Ok((
                Arc::clone(list.values()),
                list.value_offsets()
                    .iter()
                    .map(|&offset| offset as usize)
                    .collect(),
            ))
        }
        _ => plan_err!("zip_with requires arrays"),
    }
}

fn map_fields(data_type: &DataType) -> Result<&Fields> {
    if let DataType::Map(field, _) = data_type
        && let DataType::Struct(fields) = field.data_type()
        && fields.len() == 2
    {
        return Ok(fields);
    }
    plan_err!("map_zip_with requires maps, got {data_type}")
}

fn map_type(key: DataType, value: DataType, nullable: bool) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            SAIL_MAP_FIELD_NAME,
            DataType::Struct(
                vec![
                    Field::new(SAIL_MAP_KEY_FIELD_NAME, key, false),
                    Field::new(SAIL_MAP_VALUE_FIELD_NAME, value, nullable),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

// Find Spark's wider key type before applying the outer map-key nullability
// restriction. Nested array elements and struct fields can become nullable.
fn common_key_type(
    left: &DataType,
    right: &DataType,
    ansi_mode: bool,
    case_sensitive: bool,
) -> Option<DataType> {
    match (left, right) {
        (DataType::Map(..), _) | (_, DataType::Map(..)) => None,
        // Keep Spark's microsecond timestamp precision instead of DataFusion's
        // date/timestamp comparison coercion, which produces nanoseconds.
        (DataType::Date32, timestamp @ DataType::Timestamp(..))
        | (timestamp @ DataType::Timestamp(..), DataType::Date32) => Some(timestamp.clone()),
        (DataType::List(left), DataType::List(right)) => {
            let key = common_key_type(
                left.data_type(),
                right.data_type(),
                ansi_mode,
                case_sensitive,
            )?;
            let nullable = left.is_nullable()
                || right.is_nullable()
                || key_cast_nullable(left.data_type(), &key)
                || key_cast_nullable(right.data_type(), &key);
            Some(DataType::List(Arc::new(Field::new_list_field(
                key, nullable,
            ))))
        }
        (DataType::Struct(left), DataType::Struct(right)) if left.len() == right.len() => {
            let fields = left
                .iter()
                .zip(right)
                .map(|(left, right)| {
                    if if case_sensitive {
                        left.name() != right.name()
                    } else {
                        !left.name().eq_ignore_ascii_case(right.name())
                    } {
                        return None;
                    }
                    let key = common_key_type(
                        left.data_type(),
                        right.data_type(),
                        ansi_mode,
                        case_sensitive,
                    )?;
                    let nullable = left.is_nullable()
                        || right.is_nullable()
                        || key_cast_nullable(left.data_type(), &key)
                        || key_cast_nullable(right.data_type(), &key);
                    Some(Field::new(left.name(), key, nullable))
                })
                .collect::<Option<Vec<_>>>()?;
            Some(DataType::Struct(fields.into()))
        }
        _ if left == right || right.is_null() => Some(left.clone()),
        _ if left.is_null() => Some(right.clone()),
        _ if left.is_string() != right.is_string() => {
            let other = if left.is_string() { right } else { left };
            if ansi_mode {
                match other {
                    _ if other.is_integer() => Some(DataType::Int64),
                    _ if other.is_floating() || other.is_decimal() => Some(DataType::Float64),
                    _ if other.is_binary() => Some(other.clone()),
                    DataType::Boolean | DataType::Date32 | DataType::Timestamp(..) => {
                        Some(other.clone())
                    }
                    _ => None,
                }
            } else if other.is_numeric()
                || matches!(other, DataType::Date32 | DataType::Timestamp(..))
            {
                Some(DataType::Utf8)
            } else {
                None
            }
        }
        _ if left.is_numeric() && right.is_numeric() => {
            if (left.is_decimal() && right.is_floating())
                || (right.is_decimal() && left.is_floating())
                || (ansi_mode
                    && ((left == &DataType::Float32 && right.is_integer())
                        || (right == &DataType::Float32 && left.is_integer())))
            {
                Some(DataType::Float64)
            } else if left.is_decimal() || right.is_decimal() {
                let (left_precision, left_scale) = decimal_key_parts(left)?;
                let (right_precision, right_scale) = decimal_key_parts(right)?;
                let scale = left_scale.max(right_scale);
                let integral = (left_precision - left_scale).max(right_precision - right_scale);
                // Spark DecimalPrecisionTypeCoercion.bounded retains integral
                // digits first when the common precision would exceed 38.
                let requested_precision = integral + scale;
                let precision = requested_precision.min(38);
                let scale = if requested_precision > 38 {
                    (scale - (requested_precision - 38)).max(0)
                } else {
                    scale
                };
                Some(DataType::Decimal128(precision as u8, scale as i8))
            } else {
                comparison_coercion(left, right)
            }
        }
        // DataFusion comparison coercion accepts date/integer and mixed
        // interval families, which Spark's wider-type coercion rejects.
        _ if left.is_temporal() != right.is_temporal() => None,
        (DataType::Interval(_), DataType::Duration(_))
        | (DataType::Duration(_), DataType::Interval(_)) => None,
        _ => comparison_coercion(left, right),
    }
}

// Spark Cast.forceNullable for the widening casts admitted by common_key_type.
fn key_cast_nullable(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        _ if from == to || from.is_null() => false,
        _ if from.is_string() => !to.is_string() && !to.is_binary(),
        (DataType::Date32, DataType::Timestamp(_, Some(_))) => false,
        (DataType::Date32, _) => !to.is_string(),
        _ => false,
    }
}

fn decimal_key_parts(data_type: &DataType) -> Option<(i16, i16)> {
    match data_type {
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            Some((i16::from(*precision), i16::from(*scale)))
        }
        DataType::Int8 => Some((3, 0)),
        DataType::Int16 => Some((5, 0)),
        DataType::Int32 => Some((10, 0)),
        DataType::Int64 => Some((20, 0)),
        _ => None,
    }
}

// ScalarValue hashes float bits, whereas Spark equates all NaNs and both
// signed zeros, including inside array/struct keys. Normalize only for lookup.
fn normalize_keys(array: &ArrayRef) -> Result<ArrayRef> {
    Ok(match array.data_type() {
        DataType::Float32 => Arc::new(Float32Array::from_iter(
            array
                .as_primitive::<datafusion::arrow::datatypes::Float32Type>()
                .iter()
                .map(|value| {
                    value.map(|value| {
                        if value.is_nan() {
                            f32::NAN
                        } else if value == 0.0 {
                            0.0
                        } else {
                            value
                        }
                    })
                }),
        )),
        DataType::Float64 => Arc::new(Float64Array::from_iter(
            array
                .as_primitive::<datafusion::arrow::datatypes::Float64Type>()
                .iter()
                .map(|value| {
                    value.map(|value| {
                        if value.is_nan() {
                            f64::NAN
                        } else if value == 0.0 {
                            0.0
                        } else {
                            value
                        }
                    })
                }),
        )),
        DataType::List(field) => {
            let list = array.as_list::<i32>();
            Arc::new(ListArray::try_new(
                Arc::clone(field),
                list.offsets().clone(),
                normalize_keys(list.values())?,
                list.nulls().cloned(),
            )?)
        }
        DataType::LargeList(field) => {
            let list = array.as_list::<i64>();
            Arc::new(LargeListArray::try_new(
                Arc::clone(field),
                list.offsets().clone(),
                normalize_keys(list.values())?,
                list.nulls().cloned(),
            )?)
        }
        DataType::Struct(fields) => {
            let structure = array.as_struct();
            Arc::new(StructArray::try_new(
                fields.clone(),
                structure
                    .columns()
                    .iter()
                    .map(normalize_keys)
                    .collect::<Result<Vec<_>>>()?,
                structure.nulls().cloned(),
            )?)
        }
        _ => Arc::clone(array),
    })
}
