use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};
use datafusion_functions_nested::extract::ArrayElement;
use datafusion_functions_nested::map_extract::MapExtract;
use sail_common_datafusion::utils::scalar::scalar_from_array;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CollectionElement {
    map: bool,
    signature: Signature,
}

impl CollectionElement {
    pub fn array() -> Self {
        Self {
            map: false,
            signature: ArrayElement::new().signature().clone(),
        }
    }

    pub fn map() -> Self {
        Self {
            map: true,
            signature: MapExtract::new().signature().clone(),
        }
    }
}

pub fn array_element(array: Expr, index: Expr) -> Expr {
    ScalarUDF::from(CollectionElement::array()).call(vec![array, index])
}

pub fn map_element(map: Expr, key: Expr) -> Expr {
    ScalarUDF::from(CollectionElement::map()).call(vec![map, key])
}

impl ScalarUDFImpl for CollectionElement {
    fn name(&self) -> &str {
        if self.map {
            "spark_map_element"
        } else {
            "spark_array_element"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if self.map {
            MapExtract::new().coerce_types(arg_types)
        } else {
            ArrayElement::new().coerce_types(arg_types)
        }
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if self.map {
            match MapExtract::new().return_type(arg_types)? {
                DataType::List(field) => Ok(field.data_type().clone()),
                other => Ok(other),
            }
        } else {
            ArrayElement::new().return_type(arg_types)
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [collection, _] = args.arg_fields else {
            return exec_err!("{} expects two arguments", self.name());
        };
        let field = match collection.data_type() {
            DataType::List(field) | DataType::LargeList(field) if !self.map => field,
            DataType::Map(entries, _) if self.map => {
                let DataType::Struct(fields) = entries.data_type() else {
                    return exec_err!("expected map entries struct");
                };
                &fields[1]
            }
            DataType::Null => return Ok(Arc::new(Field::new(self.name(), DataType::Null, true))),
            other => return exec_err!("{} does not support {other}", self.name()),
        };
        Ok(Arc::new(
            field
                .as_ref()
                .clone()
                .with_name(self.name())
                .with_nullable(true),
        ))
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        // Invoke the existing kernels with arrays: their scalar adapters rebuild list fields.
        args.args = ColumnarValue::values_to_arrays(&args.args)?
            .into_iter()
            .map(ColumnarValue::Array)
            .collect();
        if self.map {
            let mut extract_args = args.clone();
            let arg_types = extract_args
                .args
                .iter()
                .map(ColumnarValue::data_type)
                .collect::<Vec<_>>();
            let extract_field = Arc::new(Field::new(
                "map_extract",
                MapExtract::new().return_type(&arg_types)?,
                true,
            ));
            extract_args.return_field = Arc::clone(&extract_field);
            let extracted = MapExtract::new().invoke_with_args(extract_args)?;
            let rows = match &extracted {
                ColumnarValue::Array(array) => array.len(),
                ColumnarValue::Scalar(_) => 1,
            };
            args.args = vec![
                extracted,
                ColumnarValue::Array(ScalarValue::Int64(Some(1)).to_array_of_size(rows)?),
            ];
            args.arg_fields = vec![
                extract_field,
                Arc::new(Field::new("index", DataType::Int64, false)),
            ];
        }
        let result = ArrayElement::new().invoke_with_args(args)?;
        match result {
            ColumnarValue::Array(array) if scalar => {
                Ok(ColumnarValue::Scalar(scalar_from_array(&array, 0)?))
            }
            other => Ok(other),
        }
    }
}
