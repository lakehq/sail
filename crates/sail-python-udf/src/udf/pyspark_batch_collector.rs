use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::logical_expr::{Accumulator, Signature, Volatility};
use datafusion_common::Result;
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::AggregateUDFImpl;

use crate::accumulator::{BatchAggregateAccumulator, BatchAggregator};
use crate::array::{build_singleton_list_array, get_fields, get_struct_array_type};

/// An aggregation function that collects batches into a list of structs.
/// This function is for internal use.
#[derive(Debug)]
pub struct PySparkBatchCollectorUDF {
    signature: Signature,
    input_types: Vec<DataType>,
    input_names: Vec<String>,
}

impl PySparkBatchCollectorUDF {
    pub fn new(input_types: Vec<DataType>, input_names: Vec<String>) -> Self {
        let signature = Signature::exact(input_types.clone(), Volatility::Immutable);
        Self {
            signature,
            input_types,
            input_names,
        }
    }

    pub fn input_types(&self) -> &[DataType] {
        &self.input_types
    }

    pub fn input_names(&self) -> &[String] {
        &self.input_names
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
        get_struct_array_type(&self.input_types, &self.input_names)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let aggregator = Box::new(PySparkBatchCollector {
            fields: get_fields(&self.input_types, &self.input_names)?,
        });
        Ok(Box::new(BatchAggregateAccumulator::new(
            self.input_types.clone(),
            get_struct_array_type(&self.input_types, &self.input_names)?,
            aggregator,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        BatchAggregateAccumulator::state_fields(args)
    }
}

struct PySparkBatchCollector {
    fields: Vec<Field>,
}

impl BatchAggregator for PySparkBatchCollector {
    fn call(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        let array = StructArray::try_new(self.fields.clone().into(), args.to_vec(), None)?;
        Ok(build_singleton_list_array(Arc::new(array)))
    }
}
