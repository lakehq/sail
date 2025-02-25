use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ListArray, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::cast::as_map_array;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct MapToArray {
    signature: Signature,
    nullable: bool,
}

impl MapToArray {
    pub fn new(nullable: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            nullable,
        }
    }

    fn nullable_entry_fields(&self, field: &FieldRef) -> Result<Fields> {
        match field.data_type() {
            DataType::Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|f| f.as_ref().clone().with_nullable(self.nullable))
                    .collect::<Vec<_>>();
                Ok(Fields::from(fields))
            }
            _ => Err(DataFusionError::Internal(format!(
                "map entry should be a struct, found: {:?}",
                field.data_type(),
            ))),
        }
    }

    fn nullable_map_field(&self, field: &FieldRef) -> Result<FieldRef> {
        let data_type = DataType::Struct(self.nullable_entry_fields(field)?);
        Ok(Arc::new(Field::new(field.name(), data_type, true)))
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }
}

impl ScalarUDFImpl for MapToArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "map_to_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types {
            &[DataType::Map(field, _)] => Ok(DataType::List(self.nullable_map_field(field)?)),
            _ => Err(DataFusionError::Internal(format!(
                "{} should only be called with a map",
                self.name(),
            ))),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        if args.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "{} should only be called with a single argument",
                self.name()
            )));
        }
        let arg = &args[0];
        let out = match arg.data_type() {
            DataType::Map(field, _) => {
                let array = as_map_array(arg)?;
                let fields = self.nullable_entry_fields(field)?;
                let inner = StructArray::new(
                    fields,
                    array.entries().columns().to_vec(),
                    array.entries().nulls().cloned(),
                );
                ListArray::try_new(
                    self.nullable_map_field(field)?,
                    array.offsets().clone(),
                    Arc::new(inner),
                    array.nulls().cloned(),
                )?
            }
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "{} should only be called with a map",
                    self.name(),
                )));
            }
        };
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}
