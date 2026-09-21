use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef, Fields};
use datafusion::common::{Result, ScalarValue};
use datafusion::functions::core::named_struct::NamedStructFunc;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    StructFieldMapping,
};

/// DataFusion's `named_struct`, except that each struct field keeps the metadata of the value it
/// is built from, as Sail's `struct` does.
///
/// Sail keeps logical types such as GEOMETRY and GEOGRAPHY in field metadata, and DataFusion
/// rebuilds each field from the value's `DataType` alone, so `named_struct('g', geometry)` would
/// otherwise turn the field into plain BINARY.
#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub struct SparkNamedStruct {
    inner: NamedStructFunc,
}

impl SparkNamedStruct {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ScalarUDFImpl for SparkNamedStruct {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let field = self.inner.return_field_from_args(ReturnFieldArgs {
            arg_fields: args.arg_fields,
            scalar_arguments: args.scalar_arguments,
        })?;
        let DataType::Struct(fields) = field.data_type() else {
            return Ok(field);
        };
        let values = args.arg_fields.iter().skip(1).step_by(2);
        let fields = fields
            .iter()
            .zip(values)
            .map(|(field, value)| {
                field
                    .as_ref()
                    .clone()
                    .with_metadata(value.metadata().clone())
            })
            .collect::<Fields>();
        Ok(Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(DataType::Struct(fields)),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.inner.invoke_with_args(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }

    fn struct_field_mapping(
        &self,
        literal_args: &[Option<ScalarValue>],
    ) -> Option<StructFieldMapping> {
        self.inner.struct_field_mapping(literal_args)
    }
}
