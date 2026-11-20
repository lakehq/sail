use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::as_string_array;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::spec::utils::path_comparison_key;

/// URI comparison for replay keys; never applied to stored action payloads.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DeltaPathKey {
    signature: Signature,
}

impl Default for DeltaPathKey {
    fn default() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DeltaPathKey {
    fn name(&self) -> &str {
        "delta_path_key"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [path] = args.args.as_slice() else {
            return exec_err!("delta_path_key requires one argument");
        };
        match path {
            ColumnarValue::Scalar(ScalarValue::Utf8(path)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(path.as_deref().map(path_comparison_key)),
            )),
            ColumnarValue::Array(paths) => {
                let keys = as_string_array(paths)?
                    .iter()
                    .map(|path| path.map(path_comparison_key))
                    .collect::<StringArray>();
                Ok(ColumnarValue::Array(Arc::new(keys)))
            }
            _ => exec_err!("delta_path_key requires a Utf8 argument"),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::RecordBatch;
    use datafusion::common::config::ConfigOptions;
    use datafusion::physical_expr::expressions::{Column, Literal};
    use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
    use datafusion_expr::ScalarUDF;

    use super::*;

    #[test]
    fn path_keys_match_scalar_and_array_without_decoding() -> Result<()> {
        let paths = Arc::new(StringArray::from(vec![
            Some("caf%c3%a9.parquet"),
            Some("café.parquet"),
            Some("p=a%252fb"),
            None,
        ]));
        let batch = RecordBatch::try_from_iter(vec![("path", paths as _)])?;
        let function = Arc::new(ScalarUDF::from(DeltaPathKey::default()));
        let array = ScalarFunctionExpr::try_new(
            function.clone(),
            vec![Arc::new(Column::new("path", 0))],
            batch.schema().as_ref(),
            Arc::new(ConfigOptions::default()),
        )?;
        let actual = array.evaluate(&batch)?.into_array(batch.num_rows())?;
        assert_eq!(
            as_string_array(&actual)?,
            &StringArray::from(vec![
                Some("caf%C3%A9.parquet"),
                Some("café.parquet"),
                Some("p=a%252fb"),
                None,
            ])
        );
        let scalar = ScalarFunctionExpr::try_new(
            function,
            vec![Arc::new(Literal::new(ScalarValue::Utf8(Some(
                "caf%c3%a9.parquet".to_string(),
            ))))],
            batch.schema().as_ref(),
            Arc::new(ConfigOptions::default()),
        )?;
        assert_eq!(
            scalar.evaluate(&batch)?.into_array(1)?,
            Arc::new(StringArray::from(vec!["caf%C3%A9.parquet"])) as _,
        );
        Ok(())
    }
}
