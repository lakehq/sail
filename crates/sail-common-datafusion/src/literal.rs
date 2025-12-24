use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::physical_expr::create_physical_expr;
use datafusion_common::{exec_datafusion_err, exec_err, DFSchema, Result, ScalarValue};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{ColumnarValue, Expr};

pub struct LiteralEvaluator {
    schema: DFSchema,
    input: RecordBatch,
    props: ExecutionProps,
}

impl Default for LiteralEvaluator {
    fn default() -> Self {
        Self::new()
    }
}

impl LiteralEvaluator {
    pub fn new() -> Self {
        let schema = DFSchema::empty();
        #[allow(clippy::expect_used)]
        let input = RecordBatch::try_new_with_options(
            schema.inner().clone(),
            vec![],
            &RecordBatchOptions::default().with_row_count(Some(1)),
        )
        .expect("placeholder input record batch for literal evaluator");
        Self {
            schema,
            input,
            props: ExecutionProps::new(),
        }
    }

    pub fn evaluate(&self, expr: &Expr) -> Result<ScalarValue> {
        let expr = create_physical_expr(expr, &self.schema, &self.props)?;
        match expr.evaluate(&self.input)? {
            ColumnarValue::Array(array) => {
                if array.len() != 1 {
                    exec_err!("expected a single value, but got {}", array.len())
                } else {
                    Ok(ScalarValue::try_from_array(array.as_ref(), 0)?)
                }
            }
            ColumnarValue::Scalar(scalar) => Ok(scalar),
        }
    }
}

/// A wrapper that provides methods to cast a scalar value to Rust data types.
/// The conversion logic is different from the [TryFrom] trait implementations for [ScalarValue].
/// For example, this wrapper does not allow converting date/time types to integers.
pub struct LiteralValue<'a>(pub &'a ScalarValue);

macro_rules! try_cast_primitive {
    { $name:ident, $type:ty => [ $($variant:ident),* ] } => {
        pub fn $name(self) -> Result<$type> {
            match self.0 {
                $(
                    ScalarValue::$variant(Some(x)) => {
                        <$type>::try_from(*x).map_err(|e| exec_datafusion_err!("{e}"))
                    }
                )*
                _ => exec_err!(concat!("cannot convert {:?} to ", stringify!($type)), self.0),
            }
        }
    };
}

macro_rules! try_cast_primitive_integral {
    { $name:ident, $type:ty } => {
        try_cast_primitive!{
            $name,
            $type => [Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64]
        }
    };
}

impl<'a> LiteralValue<'a> {
    try_cast_primitive_integral! { try_to_i8, i8}
    try_cast_primitive_integral! { try_to_i16, i16 }
    try_cast_primitive_integral! { try_to_i32, i32 }
    try_cast_primitive_integral! { try_to_i64, i64 }
    try_cast_primitive_integral! { try_to_u8, u8 }
    try_cast_primitive_integral! { try_to_u16, u16 }
    try_cast_primitive_integral! { try_to_u32, u32 }
    try_cast_primitive_integral! { try_to_u64, u64 }
    try_cast_primitive_integral! { try_to_usize, usize }
    try_cast_primitive! { try_to_bool, bool => [Boolean] }
    try_cast_primitive! { try_to_f32, f32 => [Float16, Float32] }
    try_cast_primitive! { try_to_f64, f64 => [Float16, Float32, Float64] }

    pub fn try_to_string(self) -> Result<&'a str> {
        match self.0 {
            ScalarValue::Utf8(Some(x))
            | ScalarValue::LargeUtf8(Some(x))
            | ScalarValue::Utf8View(Some(x)) => Ok(x.as_str()),
            _ => exec_err!("cannot convert {:?} to string", self.0),
        }
    }

    pub fn try_to_binary(self) -> Result<&'a [u8]> {
        match self.0 {
            ScalarValue::Binary(Some(x))
            | ScalarValue::LargeBinary(Some(x))
            | ScalarValue::FixedSizeBinary(_, Some(x)) => Ok(x),
            _ => exec_err!("cannot convert {:?} to binary", self.0),
        }
    }
}
