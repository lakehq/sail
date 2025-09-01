use arrow::array::{ArrayRef, Int64Array, Int64Builder, StringArray, StringBuilder, Array, Int32Array, Int32Builder};
use arrow::datatypes::{DataType};
use arrow::datatypes::DataType::{Utf8,Int32,Int64};
use datafusion_common::{exec_err, DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use datafusion_expr_common::signature::Volatility::Immutable;
use std::any::Any;
use std::sync::Arc;

use arrow::compute::cast;
use arrow::datatypes::DataType::{ Utf8View };
use crate::extension::function::functions_nested_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkElt {
    signature: Signature,
}

impl Default for SparkElt {
    fn default() -> Self { SparkElt::new() }
}
impl SparkElt {
    pub fn new() -> Self {
        Self { signature: Signature::variadic_any(Immutable) }
    }
}

impl ScalarUDFImpl for SparkElt {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "elt" }
    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() < 2 {
            return exec_err!("elt expects at least 2 arguments: index, value1");
        }
        Ok(match &arg_types[1] {
          Utf8 => Utf8View,
            other => other.clone(),
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(elt)(&args.args)
    }
}

fn elt(args: &[ArrayRef]) -> Result<ArrayRef, DataFusionError> {
    if args.len() < 2 {
        return exec_err!("elt expects at least 2 arguments: index, value1");
    }

    let idx = args[0]
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| DataFusionError::Plan("elt: first argument must be Int64".into()))?;

    let num_rows = args[0].len();
    let k = args.len() - 1;

    let val_dt = args[1].data_type().clone();
    for (j, a) in args.iter().enumerate().skip(1) {
        if a.len() != num_rows {
            return exec_err!(
                "elt: all arguments must have the same length (arg {} has {}, expected {})",
                j, a.len(), num_rows
            );
        }
        if a.data_type() != &val_dt {
            return exec_err!(
                "elt: all value arguments must share the same type (arg 1 is {:?}, arg {} is {:?})",
                val_dt, j, a.data_type()
            );
        }
    }

    match val_dt {
        // do macro
        Utf8 => {
            let mut vals: Vec<&StringArray> = Vec::with_capacity(k);
            for a in &args[1..] {
                vals.push(
                    a.as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| DataFusionError::Internal("downcast Utf8 failed".into()))?,
                );
            }

            let mut b= StringBuilder::new();
            for row in 0..num_rows {
                if idx.is_null(row) { b.append_null(); continue; }
                let n = idx.value(row);
                if n < 1 || (n as usize) > k { b.append_null(); continue; }
                let j = (n as usize) - 1;
                let col = vals[j];
                if col.is_null(row) { b.append_null(); } else { b.append_value(col.value(row)); }
            }
            let out_utf8 = Arc::new(b.finish()) as ArrayRef;
            let out_view = cast(&out_utf8, &Utf8View)?;
            Ok(out_view)
        }

        Int64 => {
            let mut vals: Vec<&Int64Array> = Vec::with_capacity(k);
            for a in &args[1..] {
                vals.push(
                    a.as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| DataFusionError::Internal("downcast Int64 failed".into()))?,
                );
            }

            let mut b= Int64Builder::new();
            for row in 0..num_rows {
                if idx.is_null(row) { b.append_null(); continue; }
                let n = idx.value(row);
                if n < 1 || (n as usize) > k { b.append_null(); continue; }
                let j = (n as usize) - 1;
                let col = vals[j];
                if col.is_null(row) { b.append_null(); } else { b.append_value(col.value(row)); }
            }
            Ok(Arc::new(b.finish()))
        }
        Int32 => {
            let mut vals: Vec<&Int32Array> = Vec::with_capacity(k);
            for a in &args[1..] {
                vals.push(
                    a.as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| DataFusionError::Internal("downcast Int64 failed".into()))?,
                );
            }

            let mut b= Int32Builder::new();
            for row in 0..num_rows {
                if idx.is_null(row) { b.append_null(); continue; }
                let n = idx.value(row);
                if n < 1 || (n as usize) > k { b.append_null(); continue; }
                let j = (n as usize) - 1;
                let col = vals[j];
                if col.is_null(row) { b.append_null(); } else { b.append_value(col.value(row)); }
            }
            Ok(Arc::new(b.finish()))
        }

        other => exec_err!("elt: unsupported value type for now: {:?}", other),
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, Int64Array, StringArray, StringViewArray};
    use datafusion_common::Result;
    fn run_elt_arrays(arrs: Vec<ArrayRef>) -> Result<ArrayRef> {
        elt(&arrs)
    }

    #[test]
    fn elt_utf8_basic() {
        let idx = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(0),
            None,
        ]));
        let v1 = Arc::new(StringArray::from(vec![
            Some("a1"),
            Some("a2"),
            Some("a3"),
            Some("a4"),
            Some("a5"),
            Some("a6"),
        ]));
        let v2 = Arc::new(StringArray::from(vec![
            Some("b1"),
            Some("b2"),
            None,
            Some("b4"),
            Some("b5"),
            Some("b6"),
        ]));
        let v3 = Arc::new(StringArray::from(vec![
            Some("c1"),
            Some("c2"),
            Some("c3"),
            None,
            Some("c5"),
            Some("c6"),
        ]));

        let out = run_elt_arrays(vec![idx, v1, v2, v3]).unwrap();

        let out = out.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(out.len(), 6);
        assert_eq!(out.value(0), "a1");
        assert_eq!(out.value(1), "b2");
        assert_eq!(out.value(2), "c3");
        assert!(out.is_null(3));
        assert!(out.is_null(4));
        assert!(out.is_null(5));
    }

    #[test]
    fn elt_int64_basic() {
        let idx = Arc::new(Int32Array::from(vec![Some(2), Some(1), Some(2)]));
        let v1 = Arc::new(Int64Array::from(vec![Some(10), Some(20), Some(30)]));
        let v2 = Arc::new(Int64Array::from(vec![Some(100), None, Some(300)]));

        let out = run_elt_arrays(vec![idx, v1, v2]).unwrap();

        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out.value(0), 100);
        assert_eq!(out.value(1), 20);
        assert_eq!(out.value(2), 300);
    }

    #[test]
    fn elt_out_of_range_all_null() {
        let idx = Arc::new(Int32Array::from(vec![Some(5), Some(-1), Some(0)]));
        let v1 = Arc::new(StringArray::from(vec![Some("x"), Some("y"), Some("z")]));
        let v2 = Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")]));

        let out = run_elt_arrays(vec![idx, v1, v2]).unwrap();

        let out = out.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert!(out.is_null(0));
        assert!(out.is_null(1));
        assert!(out.is_null(2));
    }

    #[test]
    fn elt_len_mismatch_error() {
        let idx = Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(1)]));
        let v1 = Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")]));
        let v2 = Arc::new(StringArray::from(vec![Some("x"), Some("y")]));

        let err = run_elt_arrays(vec![idx, v1, v2]).unwrap_err();

        let msg = format!("{err}");
        assert!(msg.contains("all arguments must have the same length"));
    }

    #[test]
    fn elt_type_mismatch_error() {
        let idx = Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(1)]));
        let v1 = Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")]));
        let v2 = Arc::new(Int64Array::from(vec![Some(10), Some(20), Some(30)]));

        let err = run_elt_arrays(vec![idx, v1, v2]).unwrap_err();

        let msg = format!("{err}");
        assert!(msg.contains("all value arguments must share the same type"));
    }

    #[test]
    fn elt_return_type_equals_first_value_type() {
        let f = SparkElt::new();
        // (index:Int64, value1:Utf8, value2:Utf8) => retorno Utf8/Utf8View depende de tu return_type
        let ty = f.return_type(&[Int64, Utf8, Utf8]).unwrap();
        // según tu return_type actual, si pusiste Utf8View para strings:
        assert!(matches!(ty, Utf8View | Utf8));
        // y con enteros:
        let ty2 = f.return_type(&[Int64, Int64, Int64]).unwrap();
        assert_eq!(ty2, Int64);
    }
}
