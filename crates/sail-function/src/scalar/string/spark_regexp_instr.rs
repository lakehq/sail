use std::sync::Arc;

use datafusion::arrow::array::{NullArray, UInt64Array, make_array, new_null_array};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::{CastOptions, cast, cast_with_options, take_arrays};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_datafusion_err, exec_err};
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};
use datafusion_functions::regex::regexpinstr::regexp_instr_func;

use crate::functions_nested_utils::{evaluate_lambda_rows, scatter_active_rows};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkRegexpInstr {
    signature: HigherOrderSignature,
    ansi_mode: bool,
}

impl SparkRegexpInstr {
    pub fn new(ansi_mode: bool) -> Self {
        Self {
            signature: HigherOrderSignature::variadic_any(Volatility::Immutable),
            ansi_mode,
        }
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }
}

impl HigherOrderUDFImpl for SparkRegexpInstr {
    fn name(&self) -> &str {
        "spark_regexp_instr"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        regexp_instr_arguments(fields)?;
        Ok(LambdaParametersProgress::Complete(vec![vec![Arc::new(
            Field::new("", DataType::Null, true),
        )]]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        let (string, pattern, index) = regexp_instr_arguments(args.arg_fields)?;
        let nullable = string.is_nullable()
            || pattern.is_nullable()
            || index.is_nullable()
            || index.data_type().is_string();
        Ok(Arc::new(Field::new(self.name(), DataType::Int32, nullable)))
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let (string, pattern, index) = regexp_instr_arguments(&args.args)?;
        let string = string.to_array(args.number_rows)?;
        let pattern = pattern.to_array(args.number_rows)?;
        let nulls = NullBuffer::union(string.nulls(), pattern.nulls());
        let active_rows = if nulls.as_ref().is_none_or(|nulls| nulls.null_count() == 0) {
            None
        } else {
            Some(
                (0..args.number_rows)
                    .filter(|row| nulls.as_ref().is_none_or(|nulls| nulls.is_valid(*row)))
                    .map(|row| {
                        u64::try_from(row).map_err(|_| {
                            exec_datafusion_err!("regexp_instr row index does not fit in u64")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
        };
        if args.number_rows == 0 || active_rows.as_ref().is_some_and(Vec::is_empty) {
            return Ok(ColumnarValue::Array(new_null_array(
                args.return_type(),
                args.number_rows,
            )));
        }

        // Convert the index only where both evaluated search arguments are non-null.
        let index = if let Some(rows) = &active_rows {
            evaluate_lambda_rows(index, rows)?
        } else {
            let dummy = || Ok(Arc::new(NullArray::new(args.number_rows)) as _);
            index
                .evaluate(&[&dummy], |arrays| Ok(arrays.to_vec()))?
                .into_array(args.number_rows)?
        };
        let index = if self.ansi_mode {
            cast_with_options(
                index.as_ref(),
                &DataType::Int32,
                &CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )?
        } else {
            index
        };
        let search = if let Some(rows) = &active_rows {
            take_arrays(&[string, pattern], &UInt64Array::from(rows.clone()), None)?
        } else {
            vec![string, pattern]
        };
        let [string, pattern] = search.as_slice() else {
            return exec_err!("regexp_instr requires two search arguments");
        };
        let string = make_array(
            string
                .to_data()
                .into_builder()
                .nulls(index.logical_nulls())
                .build()?,
        );
        let result = regexp_instr_func(&[string, Arc::clone(pattern)])?;
        let result = cast(result.as_ref(), &DataType::Int32)?;
        let result = match active_rows {
            Some(rows) => scatter_active_rows(result, &rows, args.number_rows)?,
            None => result,
        };
        Ok(ColumnarValue::Array(result))
    }
}

fn regexp_instr_arguments<V, L>(args: &[ValueOrLambda<V, L>]) -> Result<(&V, &V, &L)> {
    match args {
        [
            ValueOrLambda::Value(string),
            ValueOrLambda::Value(pattern),
            ValueOrLambda::Lambda(index),
        ] => Ok((string, pattern, index)),
        _ => exec_err!("regexp_instr requires two search arguments and an index lambda"),
    }
}
