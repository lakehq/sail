// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, StringArray};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Float16Type, Float32Type, Float64Type,
};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::functions::utils::make_scalar_function;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err};

/// Preserve directory names written by the native Hive float formatter.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HivePartitionFloat {
    signature: Signature,
}

impl Default for HivePartitionFloat {
    fn default() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Float16, DataType::Float32, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for HivePartitionFloat {
    fn name(&self) -> &str {
        "sail_hive_partition_float"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(
            |args| {
                let [array] = take_function_args(self.name(), args)?;
                fn format<T: ArrowPrimitiveType>(array: &ArrayRef) -> ArrayRef
                where
                    T::Native: std::fmt::Display,
                {
                    Arc::new(StringArray::from_iter(
                        array
                            .as_primitive::<T>()
                            .iter()
                            .map(|value| value.map(|value| value.to_string())),
                    ))
                }
                Ok(match array.data_type() {
                    DataType::Float16 => format::<Float16Type>(array),
                    DataType::Float32 => format::<Float32Type>(array),
                    DataType::Float64 => format::<Float64Type>(array),
                    _ => return internal_err!("expected a floating partition column"),
                })
            },
            vec![],
        )(&args.args)
    }
}
use datafusion_common::tree_node::TreeNodeRecursion;

/// Lets listing sinks write the Spark Hive null marker as a partition value.
#[derive(Debug)]
pub struct HivePartitionWriteExec {
    input: Arc<dyn ExecutionPlan>,
}

impl HivePartitionWriteExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self { input }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for HivePartitionWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl ExecutionPlan for HivePartitionWriteExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.try_into().map_err(|_| {
            datafusion_common::internal_datafusion_err!("HivePartitionWriteExec expects one child")
        })?;
        Ok(Arc::new(Self::new(input)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut config = context.session_config().clone();
        config
            .options_mut()
            .execution
            .listing_table_hive_default_partition_name
            .clear();
        let context = Arc::new(TaskContext::new(
            context.task_id(),
            context.session_id(),
            config,
            context.scalar_functions().clone(),
            context.higher_order_functions().clone(),
            context.aggregate_functions().clone(),
            context.window_functions().clone(),
            context.runtime_env(),
        ));
        self.input.execute(partition, context)
    }
}
