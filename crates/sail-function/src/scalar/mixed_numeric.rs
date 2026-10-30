use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, PrimitiveArray, new_null_array};
use arrow::compute::binary;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::{
    ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

/// Execute an already-coerced DOUBLE operation without intermediate cast arrays.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MixedNumeric {
    operator: Operator,
    signature: Signature,
}

impl MixedNumeric {
    pub fn operator(&self) -> Operator {
        self.operator
    }

    pub fn new(operator: Operator) -> Option<Self> {
        matches!(
            operator,
            Operator::Plus | Operator::Minus | Operator::Multiply
        )
        .then(|| Self {
            operator,
            signature: Signature::any(2, Volatility::Immutable),
        })
    }

    pub fn from_name(name: &str) -> Option<Self> {
        Self::new(match name {
            "mixed_numeric_add" => Operator::Plus,
            "mixed_numeric_subtract" => Operator::Minus,
            "mixed_numeric_multiply" => Operator::Multiply,
            _ => return None,
        })
    }

    fn evaluate(&self, left: &ColumnarValue, right: &ColumnarValue) -> Result<ColumnarValue> {
        match self.operator {
            Operator::Plus => evaluate::<0>(left, right),
            Operator::Minus => evaluate::<1>(left, right),
            Operator::Multiply => evaluate::<2>(left, right),
            _ => internal_err!("unsupported mixed numeric operation"),
        }
    }
}

/// Execute mixed DOUBLE arithmetic followed by Arrow's comparison to a scalar literal.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MixedNumericPredicate {
    arithmetic: Operator,
    comparison: Operator,
    name: String,
    signature: Signature,
}

impl MixedNumericPredicate {
    pub fn new(arithmetic: Operator, comparison: Operator) -> Option<Self> {
        let arithmetic_name = match arithmetic {
            Operator::Plus => "add",
            Operator::Minus => "subtract",
            Operator::Multiply => "multiply",
            _ => return None,
        };
        let comparison_name = match comparison {
            Operator::Eq => "eq",
            Operator::NotEq => "ne",
            Operator::Lt => "lt",
            Operator::LtEq => "le",
            Operator::Gt => "gt",
            Operator::GtEq => "ge",
            _ => return None,
        };
        Some(Self {
            arithmetic,
            comparison,
            name: format!("mixed_numeric_{arithmetic_name}_{comparison_name}"),
            signature: Signature::any(3, Volatility::Immutable),
        })
    }

    pub fn from_name(name: &str) -> Option<Self> {
        let (arithmetic, comparison) = name.strip_prefix("mixed_numeric_")?.split_once('_')?;
        let arithmetic = match arithmetic {
            "add" => Operator::Plus,
            "subtract" => Operator::Minus,
            "multiply" => Operator::Multiply,
            _ => return None,
        };
        let comparison = match comparison {
            "eq" => Operator::Eq,
            "ne" => Operator::NotEq,
            "lt" => Operator::Lt,
            "le" => Operator::LtEq,
            "gt" => Operator::Gt,
            "ge" => Operator::GtEq,
            _ => return None,
        };
        Self::new(arithmetic, comparison)
    }

    fn evaluate(
        &self,
        left: &ColumnarValue,
        right: &ColumnarValue,
        threshold: Option<f64>,
    ) -> Result<ColumnarValue> {
        let value = match self.arithmetic {
            Operator::Plus => evaluate::<0>(left, right)?,
            Operator::Minus => evaluate::<1>(left, right)?,
            Operator::Multiply => evaluate::<2>(left, right)?,
            _ => return internal_err!("unsupported mixed numeric predicate arithmetic"),
        };
        let threshold = ColumnarValue::Scalar(ScalarValue::Float64(threshold));
        // Reuse DataFusion's zero normalization and Arrow's specialized comparisons.
        datafusion::physical_expr_common::datum::apply_cmp(self.comparison, &value, &threshold)
    }
}

impl ScalarUDFImpl for MixedNumericPredicate {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn evaluate_bounds(&self, _input: &[&Interval]) -> Result<Interval> {
        Interval::make_unbounded(&DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [
            left,
            right,
            ColumnarValue::Scalar(ScalarValue::Float64(threshold)),
        ] = args.args.as_slice()
        else {
            return internal_err!(
                "mixed numeric predicate expects two operands and a DOUBLE scalar"
            );
        };
        if args.return_type() != &DataType::Boolean {
            return internal_err!("mixed numeric predicate must return BOOLEAN");
        }
        self.evaluate(left, right, *threshold)
    }
}

impl ScalarUDFImpl for MixedNumeric {
    fn name(&self) -> &str {
        match self.operator {
            Operator::Plus => "mixed_numeric_add",
            Operator::Minus => "mixed_numeric_subtract",
            Operator::Multiply => "mixed_numeric_multiply",
            _ => unreachable!("constructor checks mixed numeric operator"),
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn evaluate_bounds(&self, _input: &[&Interval]) -> Result<Interval> {
        Interval::make_unbounded(&DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left, right] = args.args.as_slice() else {
            return internal_err!("mixed numeric operation expects two arguments");
        };
        if args.return_type() != &DataType::Float64 {
            return internal_err!("mixed numeric operation must retain its DOUBLE result type");
        }
        self.evaluate(left, right)
    }
}

trait Widen {
    fn to_double(self) -> f64;
}

macro_rules! widen {
    ($($ty:ty),+) => {
        $(impl Widen for $ty {
            #[inline]
            fn to_double(self) -> f64 { self as f64 }
        })+
    };
}
widen!(i8, i16, i32, i64, u8, u16, u32, u64, f32, f64);

#[inline]
fn operation<const OP: u8>(left: f64, right: f64) -> f64 {
    match OP {
        0 => left + right,
        1 => left - right,
        2 => left * right,
        _ => unreachable!(),
    }
}

fn scalar_double(value: &ScalarValue) -> Result<Option<f64>> {
    match value {
        ScalarValue::Int8(value) => Ok(value.map(Widen::to_double)),
        ScalarValue::Int16(value) => Ok(value.map(Widen::to_double)),
        ScalarValue::Int32(value) => Ok(value.map(Widen::to_double)),
        ScalarValue::Int64(value) => Ok(value.map(Widen::to_double)),
        ScalarValue::UInt8(value) => Ok(value.map(Widen::to_double)),
        ScalarValue::UInt16(value) => Ok(value.map(Widen::to_double)),
        ScalarValue::UInt32(value) => Ok(value.map(Widen::to_double)),
        ScalarValue::UInt64(value) => Ok(value.map(Widen::to_double)),
        ScalarValue::Float32(value) => Ok(value.map(Widen::to_double)),
        ScalarValue::Float64(value) => Ok(*value),
        _ => internal_err!(
            "unsupported mixed numeric scalar type: {}",
            value.data_type()
        ),
    }
}

fn evaluate<const OP: u8>(left: &ColumnarValue, right: &ColumnarValue) -> Result<ColumnarValue> {
    match (left, right) {
        (ColumnarValue::Array(left), ColumnarValue::Array(right)) => {
            macro_rules! apply {
                ($ty:ty) => {
                    array_right::<$ty, OP>(left.as_primitive::<$ty>(), right)
                };
            }
            Ok(ColumnarValue::Array(match left.data_type() {
                DataType::Float32 => apply!(Float32Type)?,
                DataType::Float64 => apply!(Float64Type)?,
                DataType::Int8 => apply!(Int8Type)?,
                DataType::Int16 => apply!(Int16Type)?,
                DataType::Int32 => apply!(Int32Type)?,
                DataType::Int64 => apply!(Int64Type)?,
                DataType::UInt8 => apply!(UInt8Type)?,
                DataType::UInt16 => apply!(UInt16Type)?,
                DataType::UInt32 => apply!(UInt32Type)?,
                DataType::UInt64 => apply!(UInt64Type)?,
                data_type => {
                    return internal_err!("unsupported mixed numeric array type: {data_type}");
                }
            }))
        }
        (ColumnarValue::Array(array), ColumnarValue::Scalar(scalar)) => Ok(ColumnarValue::Array(
            array_scalar::<OP>(array, scalar, false)?,
        )),
        (ColumnarValue::Scalar(scalar), ColumnarValue::Array(array)) => Ok(ColumnarValue::Array(
            array_scalar::<OP>(array, scalar, true)?,
        )),
        (ColumnarValue::Scalar(left), ColumnarValue::Scalar(right)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(
                scalar_double(left)?
                    .zip(scalar_double(right)?)
                    .map(|(left, right)| operation::<OP>(left, right)),
            )))
        }
    }
}

fn array_right<L, const OP: u8>(left: &PrimitiveArray<L>, right: &ArrayRef) -> Result<ArrayRef>
where
    L: ArrowPrimitiveType,
    L::Native: Widen,
{
    macro_rules! apply {
        ($ty:ty) => {
            Arc::new(binary::<L, $ty, _, Float64Type>(
                left,
                right.as_primitive::<$ty>(),
                |left, right| operation::<OP>(left.to_double(), right.to_double()),
            )?) as ArrayRef
        };
    }
    Ok(match right.data_type() {
        DataType::Float32 => apply!(Float32Type),
        DataType::Float64 => apply!(Float64Type),
        DataType::Int8 => apply!(Int8Type),
        DataType::Int16 => apply!(Int16Type),
        DataType::Int32 => apply!(Int32Type),
        DataType::Int64 => apply!(Int64Type),
        DataType::UInt8 => apply!(UInt8Type),
        DataType::UInt16 => apply!(UInt16Type),
        DataType::UInt32 => apply!(UInt32Type),
        DataType::UInt64 => apply!(UInt64Type),
        data_type => return internal_err!("unsupported mixed numeric array type: {data_type}"),
    })
}

fn array_scalar<const OP: u8>(
    array: &ArrayRef,
    scalar: &ScalarValue,
    scalar_left: bool,
) -> Result<ArrayRef> {
    let Some(scalar) = scalar_double(scalar)? else {
        return Ok(new_null_array(&DataType::Float64, array.len()));
    };
    macro_rules! apply {
        ($ty:ty) => {{
            let array = array.as_primitive::<$ty>();
            Arc::new(if scalar_left {
                array.unary::<_, Float64Type>(|value| operation::<OP>(scalar, value.to_double()))
            } else {
                array.unary::<_, Float64Type>(|value| operation::<OP>(value.to_double(), scalar))
            }) as ArrayRef
        }};
    }
    Ok(match array.data_type() {
        DataType::Float32 => apply!(Float32Type),
        DataType::Float64 => apply!(Float64Type),
        DataType::Int8 => apply!(Int8Type),
        DataType::Int16 => apply!(Int16Type),
        DataType::Int32 => apply!(Int32Type),
        DataType::Int64 => apply!(Int64Type),
        DataType::UInt8 => apply!(UInt8Type),
        DataType::UInt16 => apply!(UInt16Type),
        DataType::UInt32 => apply!(UInt32Type),
        DataType::UInt64 => apply!(UInt64Type),
        data_type => return internal_err!("unsupported mixed numeric array type: {data_type}"),
    })
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, AsArray, Float32Array, Float64Array, Int32Array, Int64Array,
    };
    use arrow::compute::kernels::numeric::{add, mul, sub};
    use arrow::compute::{CastOptions, cast_with_options};
    use arrow::datatypes::{DataType, Float64Type};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, Operator};

    use super::MixedNumeric;

    fn native(left: &ColumnarValue, right: &ColumnarValue, op: Operator) -> Result<ArrayRef> {
        let len = match (left, right) {
            (ColumnarValue::Array(array), _) | (_, ColumnarValue::Array(array)) => array.len(),
            _ => 1,
        };
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        let left = cast_with_options(&left.to_array(len)?, &DataType::Float64, &options)?;
        let right = cast_with_options(&right.to_array(len)?, &DataType::Float64, &options)?;
        Ok(match op {
            Operator::Plus => add(&left.as_ref(), &right.as_ref())?,
            Operator::Minus => sub(&left.as_ref(), &right.as_ref())?,
            Operator::Multiply => mul(&left.as_ref(), &right.as_ref())?,
            _ => unreachable!(),
        })
    }

    fn exact(left: &ColumnarValue, right: &ColumnarValue, op: Operator) -> Result<()> {
        let expected = native(left, right, op)?;
        let actual = MixedNumeric::new(op).unwrap().evaluate(left, right)?;
        assert_eq!(
            matches!(actual, ColumnarValue::Scalar(_)),
            matches!(
                (left, right),
                (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_))
            )
        );
        let actual = actual.to_array(expected.len())?;
        assert_eq!(actual.data_type(), expected.data_type());
        let actual = actual.as_primitive::<Float64Type>();
        let expected = expected.as_primitive::<Float64Type>();
        for i in 0..expected.len() {
            assert_eq!(actual.is_valid(i), expected.is_valid(i));
            if expected.is_valid(i) {
                assert_eq!(
                    actual.value(i).to_bits(),
                    expected.value(i).to_bits(),
                    "{op:?} at {i}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn mixed_arithmetic_uses_double_precision_and_operand_order() -> Result<()> {
        let left = ColumnarValue::Array(Arc::new(Float32Array::from(vec![16_777_216.0, 3.0])));
        let right = ColumnarValue::Array(Arc::new(Int32Array::from(vec![16_777_217, 16_777_217])));
        for (op, expected) in [
            (Operator::Plus, vec![33_554_433.0, 16_777_220.0]),
            (Operator::Minus, vec![-1.0, -16_777_214.0]),
            (
                Operator::Multiply,
                vec![281_474_993_487_872.0, 50_331_651.0],
            ),
        ] {
            let actual = MixedNumeric::new(op)
                .unwrap()
                .evaluate(&left, &right)?
                .to_array(2)?;
            assert_eq!(
                actual.as_primitive::<Float64Type>(),
                &Float64Array::from(expected)
            );
            exact(&left, &right, op)?;
            exact(&right, &left, op)?;
        }
        Ok(())
    }

    #[test]
    fn mixed_arithmetic_matches_cast_then_operator_for_boundaries_and_slices() -> Result<()> {
        let floats: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(0.0),
            None,
            Some(-0.0),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
            Some(f32::from_bits(0x7f80_0001)),
            Some(f32::from_bits(0xffc0_1234)),
            Some(f32::MAX),
            Some(f32::MIN_POSITIVE),
            Some(f32::from_bits(1)),
            Some(1.25),
        ]));
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(0),
            Some(1),
            None,
            Some(i64::MIN),
            Some(i64::MAX),
            Some(0),
            Some(-1),
            Some((1_i64 << 53) + 1),
            Some(i64::MAX),
            Some(i64::MIN),
            Some(17),
        ]));
        for (offset, len) in [(0, 11), (1, 9), (3, 0)] {
            let left = ColumnarValue::Array(floats.slice(offset, len));
            let right = ColumnarValue::Array(ints.slice(offset, len));
            for op in [Operator::Plus, Operator::Minus, Operator::Multiply] {
                exact(&left, &right, op)?;
                exact(&right, &left, op)?;
                for scalar in [
                    ScalarValue::Float64(Some(1.0)),
                    ScalarValue::Int64(Some(i64::MAX)),
                    ScalarValue::Float64(None),
                ] {
                    let scalar = ColumnarValue::Scalar(scalar);
                    exact(&left, &scalar, op)?;
                    exact(&scalar, &left, op)?;
                }
            }
        }
        Ok(())
    }

    #[test]
    fn mixed_arithmetic_keeps_scalar_shape_and_nulls() -> Result<()> {
        for left in [
            ScalarValue::Float32(Some(-0.0)),
            ScalarValue::Float32(Some(f32::from_bits(0x7f80_0001))),
            ScalarValue::Float32(None),
        ] {
            for right in [
                ScalarValue::Int64(Some(i64::MIN)),
                ScalarValue::Int32(Some(0)),
                ScalarValue::Int32(None),
            ] {
                for op in [Operator::Plus, Operator::Minus, Operator::Multiply] {
                    exact(
                        &ColumnarValue::Scalar(left.clone()),
                        &ColumnarValue::Scalar(right.clone()),
                        op,
                    )?;
                    exact(
                        &ColumnarValue::Scalar(right.clone()),
                        &ColumnarValue::Scalar(left.clone()),
                        op,
                    )?;
                }
            }
        }
        Ok(())
    }

    #[test]
    fn unsigned_arithmetic_matches_native_widening_in_both_orders() -> Result<()> {
        use arrow::array::{UInt8Array, UInt16Array, UInt32Array, UInt64Array};

        let arrays: [ArrayRef; 4] = [
            Arc::new(UInt8Array::from(vec![
                Some(0),
                Some(1),
                Some(u8::MAX - 1),
                Some(u8::MAX),
                None,
            ])),
            Arc::new(UInt16Array::from(vec![
                Some(0),
                Some(1),
                Some(u16::MAX - 1),
                Some(u16::MAX),
                None,
            ])),
            Arc::new(UInt32Array::from(vec![
                Some(0),
                Some(1),
                Some(u32::MAX - 1),
                Some(u32::MAX),
                None,
            ])),
            Arc::new(UInt64Array::from(vec![
                Some(0),
                Some((1_u64 << 53) + 1),
                Some((1_u64 << 63) + 1),
                Some(u64::MAX),
                None,
            ])),
        ];
        let floats: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(-0.0),
            Some(16_777_216.0),
            Some(-1.25),
            Some(f32::from_bits(1)),
            None,
        ]));
        for array in arrays {
            for (offset, len) in [(0, 5), (1, 3), (2, 0)] {
                let left = ColumnarValue::Array(floats.slice(offset, len));
                let right = ColumnarValue::Array(array.slice(offset, len));
                for op in [Operator::Plus, Operator::Minus, Operator::Multiply] {
                    exact(&left, &right, op)?;
                    exact(&right, &left, op)?;
                }
            }
        }
        let float = ColumnarValue::Array(floats);
        for scalar in [
            ScalarValue::UInt8(Some(u8::MAX)),
            ScalarValue::UInt16(Some(u16::MAX)),
            ScalarValue::UInt32(Some(u32::MAX)),
            ScalarValue::UInt64(Some(u64::MAX)),
            ScalarValue::UInt64(None),
        ] {
            let scalar = ColumnarValue::Scalar(scalar);
            for op in [Operator::Plus, Operator::Minus, Operator::Multiply] {
                exact(&float, &scalar, op)?;
                exact(&scalar, &float, op)?;
                let scalar_float = ColumnarValue::Scalar(ScalarValue::Float32(Some(1.25)));
                exact(&scalar_float, &scalar, op)?;
                exact(&scalar, &scalar_float, op)?;
            }
        }
        Ok(())
    }

    fn predicate_exact(
        left: &ColumnarValue,
        right: &ColumnarValue,
        arithmetic: Operator,
        comparison: Operator,
        threshold: Option<f64>,
    ) -> Result<()> {
        use arrow::array::{BooleanArray, Scalar};
        use arrow::compute::kernels::cmp;

        // DataFusion normalizes both zero signs before Arrow's total-order comparison.
        // Keep every NaN sign/payload: Spark's all-NaNs-equal rule is not our native oracle.
        let normalize = |value: f64| if value == 0.0 { 0.0 } else { value };
        let value = native(left, right, arithmetic)?;
        let value = value
            .as_primitive::<Float64Type>()
            .unary::<_, Float64Type>(normalize);
        let bound = Scalar::new(Float64Array::from(vec![threshold.map(normalize)]));
        let expected = match comparison {
            Operator::Eq => cmp::eq(&value, &bound)?,
            Operator::NotEq => cmp::neq(&value, &bound)?,
            Operator::Lt => cmp::lt(&value, &bound)?,
            Operator::LtEq => cmp::lt_eq(&value, &bound)?,
            Operator::Gt => cmp::gt(&value, &bound)?,
            Operator::GtEq => cmp::gt_eq(&value, &bound)?,
            _ => unreachable!(),
        };
        let actual = super::MixedNumericPredicate::new(arithmetic, comparison)
            .unwrap()
            .evaluate(left, right, threshold)?;
        assert_eq!(
            matches!(actual, ColumnarValue::Scalar(_)),
            matches!(
                (left, right),
                (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_))
            )
        );
        let actual = actual.to_array(expected.len())?;
        assert_eq!(
            actual.as_any().downcast_ref::<BooleanArray>().unwrap(),
            &expected,
            "{arithmetic:?} then {comparison:?}, threshold={threshold:?}"
        );
        Ok(())
    }

    fn predicate_matrix(left: &ColumnarValue, right: &ColumnarValue) -> Result<()> {
        for arithmetic in [Operator::Plus, Operator::Minus, Operator::Multiply] {
            for comparison in [
                Operator::Eq,
                Operator::NotEq,
                Operator::Lt,
                Operator::LtEq,
                Operator::Gt,
                Operator::GtEq,
            ] {
                for threshold in [
                    None,
                    Some(-0.0),
                    Some(0.0),
                    Some(100.0),
                    Some(9_007_199_254_740_992.0),
                    Some(f64::INFINITY),
                    Some(f64::NEG_INFINITY),
                    Some(f64::from_bits(0x7ff0_0000_0000_0001)),
                    Some(f64::from_bits(0x7ff8_0000_2000_0000)),
                    Some(f64::from_bits(0xfff8_0000_0000_1234)),
                ] {
                    predicate_exact(left, right, arithmetic, comparison, threshold)?;
                    predicate_exact(right, left, arithmetic, comparison, threshold)?;
                }
            }
        }
        Ok(())
    }

    #[test]
    fn mixed_predicates_match_native_nan_zero_null_and_sliced_arrays() -> Result<()> {
        use arrow::array::UInt64Array;

        let floats: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(0.0),
            Some(-0.0),
            None,
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
            Some(f32::from_bits(0x7f80_0001)),
            Some(f32::from_bits(0xffc0_1234)),
            Some(f32::MAX),
            Some(f32::from_bits(1)),
            Some(16_777_216.0),
            Some(1.25),
        ]));
        let integers: [ArrayRef; 2] = [
            Arc::new(Int64Array::from(vec![
                Some(0),
                Some(0),
                Some(1),
                Some(i64::MIN),
                Some(i64::MAX),
                Some(0),
                Some(-1),
                Some((1_i64 << 53) + 1),
                Some(i64::MAX),
                Some(16_777_217),
                None,
            ])),
            Arc::new(UInt64Array::from(vec![
                Some(0),
                Some(0),
                Some(1),
                Some(u64::MAX),
                Some(1_u64 << 63),
                Some(0),
                Some(1),
                Some((1_u64 << 53) + 1),
                Some(u64::MAX),
                Some(16_777_217),
                None,
            ])),
        ];
        for integers in integers {
            for (offset, len) in [(0, 11), (1, 9), (3, 0)] {
                predicate_matrix(
                    &ColumnarValue::Array(floats.slice(offset, len)),
                    &ColumnarValue::Array(integers.slice(offset, len)),
                )?;
            }
        }
        Ok(())
    }

    #[test]
    fn mixed_predicates_keep_scalar_shape_and_integer_dispatch() -> Result<()> {
        let array = ColumnarValue::Array(Arc::new(Float64Array::from(vec![
            Some(-0.0),
            Some(0.0),
            Some(f64::from_bits(0x7ff0_0000_0000_0001)),
            Some(f64::from_bits(0xfff8_0000_0000_1234)),
            None,
        ])));
        for scalar in [
            ScalarValue::Int8(Some(i8::MIN)),
            ScalarValue::Int16(Some(i16::MAX)),
            ScalarValue::Int32(Some(16_777_217)),
            ScalarValue::Int64(Some((1_i64 << 53) + 1)),
            ScalarValue::UInt8(Some(u8::MAX)),
            ScalarValue::UInt16(Some(u16::MAX)),
            ScalarValue::UInt32(Some(u32::MAX)),
            ScalarValue::UInt64(Some(u64::MAX)),
            ScalarValue::Int64(None),
            ScalarValue::UInt64(None),
            ScalarValue::Float64(None),
        ] {
            let scalar = ColumnarValue::Scalar(scalar);
            predicate_matrix(&array, &scalar)?;
            // Array dispatch must preserve each integer width, not just scalar widening.
            predicate_matrix(&array, &ColumnarValue::Array(scalar.to_array(5)?))?;
            for value in [Some(-0.0), Some(f32::from_bits(0x7f80_0001)), None] {
                predicate_matrix(&ColumnarValue::Scalar(ScalarValue::Float32(value)), &scalar)?;
            }
        }
        Ok(())
    }
}
