use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::config::MapKeyDedupPolicy;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_spark::function::map::map_from_arrays::MapFromArrays;
use datafusion_spark::function::map::map_from_entries::MapFromEntries;

fn apply_map_key_dedup_policy(
    mut args: ScalarFunctionArgs,
    last_value_wins: bool,
) -> ScalarFunctionArgs {
    let mut config_options = args.config_options.as_ref().clone();
    config_options.spark.map_key_dedup_policy = if last_value_wins {
        MapKeyDedupPolicy::LastWin
    } else {
        MapKeyDedupPolicy::Exception
    };
    args.config_options = Arc::new(config_options);
    args
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMapFromArrays {
    delegate: MapFromArrays,
    last_value_wins: bool,
}

impl SparkMapFromArrays {
    pub fn new(last_value_wins: bool) -> Self {
        Self {
            delegate: MapFromArrays::new(),
            last_value_wins,
        }
    }

    pub fn last_value_wins(&self) -> bool {
        self.last_value_wins
    }
}

impl ScalarUDFImpl for SparkMapFromArrays {
    fn name(&self) -> &str {
        self.delegate.name()
    }

    fn signature(&self) -> &Signature {
        self.delegate.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.delegate.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.delegate.return_field_from_args(args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.delegate
            .invoke_with_args(apply_map_key_dedup_policy(args, self.last_value_wins))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMapFromEntries {
    delegate: MapFromEntries,
    last_value_wins: bool,
}

impl SparkMapFromEntries {
    pub fn new(last_value_wins: bool) -> Self {
        Self {
            delegate: MapFromEntries::new(),
            last_value_wins,
        }
    }

    pub fn last_value_wins(&self) -> bool {
        self.last_value_wins
    }
}

impl ScalarUDFImpl for SparkMapFromEntries {
    fn name(&self) -> &str {
        self.delegate.name()
    }

    fn signature(&self) -> &Signature {
        self.delegate.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.delegate.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.delegate.return_field_from_args(args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.delegate
            .invoke_with_args(apply_map_key_dedup_policy(args, self.last_value_wins))
    }
}
