use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, ListArray, RecordBatch, StructArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::logical_expr::{Accumulator, Signature, Volatility};
use datafusion_common::Result;
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::AggregateUDFImpl;

use crate::accumulator::{BatchAggregateAccumulator, BatchAggregator};
use crate::error::{PyUdfError, PyUdfResult};

/// An aggregation function that collects batches into a list of structs.
/// This function is for internal use.
#[derive(Debug)]
pub struct PySparkBatchCollectorUDF {
    signature: Signature,
    input_types: Vec<DataType>,
    output_type: DataType,
}

impl PySparkBatchCollectorUDF {
    pub fn new(input_types: Vec<DataType>, output_type: DataType) -> Self {
        let signature = Signature::exact(input_types.clone(), Volatility::Immutable);
        Self {
            signature,
            input_types,
            output_type,
        }
    }

    pub fn input_types(&self) -> &[DataType] {
        &self.input_types
    }

    pub fn output_type(&self) -> &DataType {
        &self.output_type
    }

    fn inner_schema(&self) -> PyUdfResult<SchemaRef> {
        let schema = match &self.output_type {
            DataType::List(field) => match field.data_type() {
                DataType::Struct(fields) => Arc::new(Schema::new(fields.clone())),
                _ => return Err(PyUdfError::invalid("batch collector output type")),
            },
            _ => return Err(PyUdfError::invalid("batch collector output type")),
        };
        Ok(schema)
    }
}

impl AggregateUDFImpl for PySparkBatchCollectorUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "collect_batch"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.output_type.clone())
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let aggregator = Box::new(PySparkBatchCollector {
            schema: self.inner_schema()?,
        });
        Ok(Box::new(BatchAggregateAccumulator::new(
            self.input_types.clone(),
            self.output_type.clone(),
            aggregator,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        BatchAggregateAccumulator::state_fields(args)
    }
}

struct PySparkBatchCollector {
    schema: SchemaRef,
}

impl BatchAggregator for PySparkBatchCollector {
    fn call(&self, args: &[ArrayRef]) -> PyUdfResult<ArrayRef> {
        let batch = RecordBatch::try_new(self.schema.clone(), args.to_vec())
            .map_err(|e| PyUdfError::invalid(e.to_string()))?;
        let array = StructArray::from(batch);
        let array = ListArray::new(
            Arc::new(Field::new_list_field(array.data_type().clone(), false)),
            OffsetBuffer::from_lengths(vec![array.len()]),
            Arc::new(array),
            None,
        );
        Ok(Arc::new(array))
    }
}
