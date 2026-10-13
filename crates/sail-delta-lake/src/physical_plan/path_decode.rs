use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion_common::cast::as_string_array;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::spec::utils::decode_path;

/// Decodes a Delta log URI into the file path carried by data scans.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DeltaDecodePath {
    signature: Signature,
}

impl Default for DeltaDecodePath {
    fn default() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl DeltaDecodePath {
    pub(crate) fn expression(column: &str, schema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(ScalarFunctionExpr::try_new(
            Arc::new(ScalarUDF::from(Self::default())),
            vec![Arc::new(Column::new(column, schema.index_of(column)?))],
            schema,
            Arc::new(ConfigOptions::default()),
        )?))
    }
}

impl ScalarUDFImpl for DeltaDecodePath {
    fn name(&self) -> &str {
        "delta_decode_path"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [path] = args.args.as_slice() else {
            return exec_err!("delta_decode_path requires one argument");
        };
        let decode = |path: &str| {
            decode_path(path).map_err(|error| DataFusionError::External(Box::new(error)))
        };
        match path {
            ColumnarValue::Scalar(ScalarValue::Utf8(path)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(path.as_deref().map(decode).transpose()?),
            )),
            ColumnarValue::Array(paths) => {
                let decoded = as_string_array(paths)?
                    .iter()
                    .map(|path| path.map(decode).transpose())
                    .collect::<Result<StringArray>>()?;
                Ok(ColumnarValue::Array(Arc::new(decoded)))
            }
            _ => exec_err!("delta_decode_path requires a Utf8 argument"),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::common::config::ConfigOptions;
    use datafusion::physical_expr::expressions::{Column, Literal};
    use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
    use datafusion_expr::ScalarUDF;

    use super::*;

    #[test]
    fn delta_paths_decode_once_and_preserve_literal_plus() -> Result<()> {
        let paths = Arc::new(StringArray::from(vec![
            Some("p=2026-09-10%252012%253A34%253A56/file.parquet"),
            Some("p=a+b%2520c/file.parquet"),
            Some("p=%E4%B8%AD%2F%25/file.parquet"),
            None,
        ]));
        let batch = RecordBatch::try_from_iter(vec![("path", paths as _)])?;
        let expression = ScalarFunctionExpr::try_new(
            Arc::new(ScalarUDF::from(DeltaDecodePath::default())),
            vec![Arc::new(Column::new("path", 0))],
            batch.schema().as_ref(),
            Arc::new(ConfigOptions::default()),
        )?;
        let actual = expression.evaluate(&batch)?.into_array(batch.num_rows())?;
        assert_eq!(
            as_string_array(&actual)?,
            &StringArray::from(vec![
                Some("p=2026-09-10%2012%3A34%3A56/file.parquet"),
                Some("p=a+b%20c/file.parquet"),
                Some("p=中/%/file.parquet"),
                None,
            ])
        );
        Ok(())
    }

    #[test]
    fn delta_path_scalar_rejects_invalid_utf8() -> Result<()> {
        let schema = Schema::new(vec![Field::new("path", DataType::Utf8, true)]);
        let expression = ScalarFunctionExpr::try_new(
            Arc::new(ScalarUDF::from(DeltaDecodePath::default())),
            vec![Arc::new(Literal::new(ScalarValue::Utf8(Some(
                "%FF".to_string(),
            ))))],
            &schema,
            Arc::new(ConfigOptions::default()),
        )?;
        assert!(
            expression
                .evaluate(&RecordBatch::new_empty(Arc::new(schema)))
                .is_err()
        );
        Ok(())
    }
}
