use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, StructArray, UInt64Array, new_null_array};
use datafusion::arrow::compute::{take, take_arrays};
use datafusion::arrow::datatypes::{DataType, FieldRef, Fields};
use datafusion_common::{Result, exec_datafusion_err, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, Expr, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};

use crate::functions_nested_utils::scatter_active_rows;

/// Binds each ordinal child of one evaluated struct to its conversion lambda.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkStructCast {
    signature: HigherOrderSignature,
    target_fields: Fields,
}

impl SparkStructCast {
    pub fn new(target_fields: Fields) -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                std::iter::once(ValueOrLambda::Value(()))
                    .chain(std::iter::repeat_n(
                        ValueOrLambda::Lambda(()),
                        target_fields.len(),
                    ))
                    .collect(),
                Volatility::Immutable,
            ),
            target_fields,
        }
    }

    pub fn target_fields(&self) -> &Fields {
        &self.target_fields
    }
}

impl HigherOrderUDFImpl for SparkStructCast {
    fn name(&self) -> &str {
        "spark_struct_cast"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        let (source, lambdas) = args.split_first()?;
        Some((vec![source], lambdas.iter().collect()))
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let [ValueOrLambda::Value(source), lambdas @ ..] = fields else {
            return plan_err!("spark_struct_cast expects a struct followed by field lambdas");
        };
        let DataType::Struct(source_fields) = source.data_type() else {
            return plan_err!("spark_struct_cast expects a struct");
        };
        if source_fields.len() != self.target_fields.len()
            || lambdas.len() != source_fields.len()
            || lambdas
                .iter()
                .any(|arg| !matches!(arg, ValueOrLambda::Lambda(_)))
        {
            return plan_err!("spark_struct_cast expects one lambda per struct field");
        }
        Ok(LambdaParametersProgress::Complete(
            source_fields
                .iter()
                .map(|field| vec![Arc::clone(field)])
                .collect(),
        ))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        let [ValueOrLambda::Value(source), lambdas @ ..] = args.arg_fields else {
            return plan_err!("spark_struct_cast expects a struct followed by field lambdas");
        };
        self.coerce_value_types(&[source.data_type().clone()])?;
        if lambdas.len() != self.target_fields.len() {
            return plan_err!("spark_struct_cast expects one lambda per struct field");
        }
        for (lambda, target) in lambdas.iter().zip(&self.target_fields) {
            if !matches!(lambda, ValueOrLambda::Lambda(result) if result.data_type() == target.data_type())
            {
                return plan_err!(
                    "spark_struct_cast lambda must return {}",
                    target.data_type()
                );
            }
        }
        Ok(Arc::new(source.as_ref().clone().with_data_type(
            DataType::Struct(self.target_fields.clone()),
        )))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let [ValueOrLambda::Value(source), lambdas @ ..] = args.args.as_slice() else {
            return exec_err!("spark_struct_cast expects a struct followed by field lambdas");
        };
        let source = source.to_array(args.number_rows)?;
        let DataType::Struct(source_fields) = source.data_type() else {
            return exec_err!("spark_struct_cast expects a struct");
        };
        let target_type = DataType::Struct(self.target_fields.clone());
        if source_fields.len() != self.target_fields.len()
            || lambdas.len() != source_fields.len()
            || args.return_type() != &target_type
        {
            return exec_err!("spark_struct_cast field count or return type mismatch");
        }
        let lambdas = lambdas
            .iter()
            .map(|arg| match arg {
                ValueOrLambda::Lambda(lambda) => Ok(lambda),
                _ => exec_err!("spark_struct_cast expects a lambda for each field"),
            })
            .collect::<Result<Vec<_>>>()?;
        let source = source.as_struct();
        // A NULL parent must not expose hidden child values to the conversions.
        let active_rows = (0..source.len())
            .filter(|&index| source.is_valid(index))
            .map(|index| {
                u64::try_from(index).map_err(|_| {
                    exec_datafusion_err!("spark_struct_cast row index does not fit in u64")
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if active_rows.is_empty() {
            return Ok(ColumnarValue::Array(new_null_array(
                &target_type,
                args.number_rows,
            )));
        }
        let indices = UInt64Array::from(active_rows.clone());
        let columns = lambdas
            .iter()
            .zip(source.columns())
            .map(|(lambda, column)| {
                let parameter = || Ok(take(column.as_ref(), &indices, None)?);
                lambda
                    .evaluate(&[&parameter], |captures| {
                        Ok(take_arrays(captures, &indices, None)?)
                    })?
                    .into_array(active_rows.len())
            })
            .collect::<Result<Vec<_>>>()?;
        let result = Arc::new(StructArray::try_new_with_length(
            self.target_fields.clone(),
            columns,
            None,
            active_rows.len(),
        )?);
        Ok(ColumnarValue::Array(scatter_active_rows(
            result,
            &active_rows,
            args.number_rows,
        )?))
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [source @ DataType::Struct(fields)] if fields.len() == self.target_fields.len() => {
                Ok(vec![source.clone()])
            }
            _ => plan_err!("spark_struct_cast expects a struct matching the target field count"),
        }
    }
}
