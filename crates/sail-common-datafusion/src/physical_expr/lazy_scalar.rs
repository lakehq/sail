use std::fmt::{self, Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, new_empty_array, new_null_array};
use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::{DataType, FieldRef, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion_common::config::{ConfigEntry, ConfigOptions};
use datafusion_common::{Result, internal_err};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::ExprProperties;
use datafusion_expr::type_coercion::functions::fields_with_udf;
use datafusion_expr::{
    ColumnarValue, ExpressionPlacement, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature,
};

/// Marks a regular scalar UDF for left-to-right, NULL-short-circuiting evaluation.
///
/// DataFusion uses this wrapper during logical and initial physical planning. Sail replaces the
/// resulting [`ScalarFunctionExpr`] with [`LazyScalarExpr`] before execution.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LazyScalarUDF {
    function: Arc<ScalarUDF>,
}

impl LazyScalarUDF {
    pub fn new(function: Arc<ScalarUDF>) -> Self {
        Self { function }
    }

    pub fn function(&self) -> &Arc<ScalarUDF> {
        &self.function
    }
}

impl ScalarUDFImpl for LazyScalarUDF {
    fn name(&self) -> &str {
        self.function.name()
    }

    fn aliases(&self) -> &[String] {
        self.function.aliases()
    }

    fn signature(&self) -> &Signature {
        self.function.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.function.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arguments_nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        let return_field = self.function.return_field_from_args(args)?;
        let nullable = return_field.is_nullable() || arguments_nullable;
        Ok(Arc::new(
            return_field.as_ref().clone().with_nullable(nullable),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.function.invoke_with_args(args)
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.function.coerce_types(arg_types)
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        self.function.placement(args)
    }
}

/// A scalar physical expression whose children are evaluated per row from left to right.
///
/// A NULL child makes the current row NULL and prevents evaluation of later children. The
/// row-at-a-time execution deliberately preserves Spark's error order and evaluates volatile or
/// stateful child expressions at most once for each row.
pub struct LazyScalarExpr {
    function: Arc<ScalarUDF>,
    name: String,
    arguments: Vec<Arc<dyn PhysicalExpr>>,
    return_field: FieldRef,
    config_options: Arc<ConfigOptions>,
}

impl LazyScalarExpr {
    pub fn try_new(
        function: Arc<ScalarUDF>,
        arguments: Vec<Arc<dyn PhysicalExpr>>,
        input_schema: &Schema,
        config_options: Arc<ConfigOptions>,
    ) -> Result<Self> {
        let argument_fields = arguments
            .iter()
            .map(|argument| argument.return_field(input_schema))
            .collect::<Result<Vec<_>>>()?;
        let expected_fields = fields_with_udf(&argument_fields, function.as_ref())?;
        if let Some((index, (actual, expected))) = argument_fields
            .iter()
            .zip(&expected_fields)
            .enumerate()
            .find(|(_, (actual, expected))| actual.data_type() != expected.data_type())
        {
            return internal_err!(
                "Lazy scalar function {} argument {index} was not coerced: expected {}, got {}",
                function.name(),
                expected.data_type(),
                actual.data_type()
            );
        }

        let scalar_arguments = arguments
            .iter()
            .map(|argument| {
                argument
                    .downcast_ref::<Literal>()
                    .map(|literal| literal.value())
            })
            .collect::<Vec<_>>();
        let return_field = function.return_field_from_args(ReturnFieldArgs {
            arg_fields: &argument_fields,
            scalar_arguments: &scalar_arguments,
        })?;
        let nullable =
            return_field.is_nullable() || argument_fields.iter().any(|field| field.is_nullable());
        let return_field = Arc::new(return_field.as_ref().clone().with_nullable(nullable));

        Ok(Self {
            name: function.name().to_string(),
            function,
            arguments,
            return_field,
            config_options,
        })
    }

    pub fn try_from_scalar_function(
        scalar: &ScalarFunctionExpr,
        input_schema: &Schema,
    ) -> Result<Option<Self>> {
        let Some(marker) = scalar.fun().inner().downcast_ref::<LazyScalarUDF>() else {
            return Ok(None);
        };
        Self::try_new(
            Arc::clone(marker.function()),
            scalar.args().to_vec(),
            input_schema,
            Arc::new(scalar.config_options().clone()),
        )
        .map(Some)
    }

    pub fn function(&self) -> &Arc<ScalarUDF> {
        &self.function
    }

    pub fn arguments(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.arguments
    }

    pub fn config_options(&self) -> &Arc<ConfigOptions> {
        &self.config_options
    }

    fn invoke_row(
        &self,
        values: Vec<ColumnarValue>,
        argument_fields: &[FieldRef],
    ) -> Result<ArrayRef> {
        let result = self.function.invoke_with_args(ScalarFunctionArgs {
            args: values,
            arg_fields: argument_fields.to_vec(),
            number_rows: 1,
            return_field: Arc::clone(&self.return_field),
            config_options: Arc::clone(&self.config_options),
        })?;
        let result = result.into_array(1)?;
        if result.len() != 1 {
            return internal_err!(
                "Lazy scalar function {} returned {} rows for a one-row input",
                self.name,
                result.len()
            );
        }
        Ok(result)
    }
}

impl Debug for LazyScalarExpr {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LazyScalarExpr")
            .field("function", &self.name)
            .field("arguments", &self.arguments)
            .field("return_field", &self.return_field)
            .finish()
    }
}

impl fmt::Display for LazyScalarExpr {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}(", self.name)?;
        for (index, argument) in self.arguments.iter().enumerate() {
            if index > 0 {
                write!(formatter, ", ")?;
            }
            write!(formatter, "{argument}")?;
        }
        write!(formatter, ")")
    }
}

impl PartialEq for LazyScalarExpr {
    fn eq(&self, other: &Self) -> bool {
        if std::ptr::eq(self, other) {
            return true;
        }
        self.function == other.function
            && self.name == other.name
            && self.arguments == other.arguments
            && self.return_field == other.return_field
            && (Arc::ptr_eq(&self.config_options, &other.config_options)
                || sorted_config_entries(&self.config_options)
                    == sorted_config_entries(&other.config_options))
    }
}

impl Eq for LazyScalarExpr {}

impl Hash for LazyScalarExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.function.hash(state);
        self.name.hash(state);
        self.arguments.hash(state);
        self.return_field.hash(state);
    }
}

fn sorted_config_entries(config_options: &ConfigOptions) -> Vec<ConfigEntry> {
    let mut entries = config_options.entries();
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    entries
}

impl PhysicalExpr for LazyScalarExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.return_field.data_type().clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.return_field.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let row_count = batch.num_rows();
        if row_count == 0 {
            return Ok(ColumnarValue::Array(new_empty_array(
                self.return_field.data_type(),
            )));
        }

        let argument_fields = self
            .arguments
            .iter()
            .map(|argument| argument.return_field(batch.schema_ref()))
            .collect::<Result<Vec<_>>>()?;
        let mut row_results = Vec::with_capacity(row_count);
        let selection = BooleanArray::from(vec![true]);

        for row in 0..row_count {
            let row_batch = batch.slice(row, 1);
            let mut values = Vec::with_capacity(self.arguments.len());
            let mut row_is_null = false;

            for argument in &self.arguments {
                let value = argument.evaluate_selection(&row_batch, &selection)?;
                let value = value.into_array(1)?;
                if matches!(value.data_type(), DataType::Null) || value.is_null(0) {
                    row_is_null = true;
                    break;
                }
                values.push(ColumnarValue::Array(value));
            }

            let result = if row_is_null {
                new_null_array(self.return_field.data_type(), 1)
            } else {
                self.invoke_row(values, &argument_fields)?
            };
            row_results.push(result);
        }

        let row_result_refs = row_results
            .iter()
            .map(|array| array.as_ref())
            .collect::<Vec<_>>();
        Ok(ColumnarValue::Array(concat(&row_result_refs)?))
    }

    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::clone(&self.return_field))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.arguments.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            function: Arc::clone(&self.function),
            name: self.name.clone(),
            arguments: children,
            return_field: Arc::clone(&self.return_field),
            config_options: Arc::clone(&self.config_options),
        }))
    }

    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        self.function.evaluate_bounds(children)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        self.function.propagate_constraints(interval, children)
    }

    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        let sort_properties = self.function.output_ordering(children)?;
        let preserves_lex_ordering = self.function.preserves_lex_ordering(children)?;
        let child_ranges = children
            .iter()
            .map(|properties| &properties.range)
            .collect::<Vec<_>>();
        let range = self.function.evaluate_bounds(&child_ranges)?;
        Ok(ExprProperties {
            sort_properties,
            range,
            preserves_lex_ordering,
        })
    }

    fn fmt_sql(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}(", self.name)?;
        for (index, argument) in self.arguments.iter().enumerate() {
            if index > 0 {
                write!(formatter, ", ")?;
            }
            argument.fmt_sql(formatter)?;
        }
        write!(formatter, ")")
    }
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::hash::{Hash, Hasher};
    use std::sync::{Arc, Mutex, MutexGuard};

    use datafusion::arrow::array::{Array, Int32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{Result, ScalarValue, exec_err, internal_datafusion_err, internal_err};
    use datafusion_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    use super::LazyScalarExpr;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct FirstArgument {
        signature: Signature,
    }

    impl FirstArgument {
        fn new() -> Self {
            Self {
                signature: Signature::any(2, Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for FirstArgument {
        fn name(&self) -> &str {
            "first_argument"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int32)
        }

        fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            let [first, _second] = args.args.as_slice() else {
                return exec_err!("first_argument expects two arguments");
            };
            Ok(first.clone())
        }
    }

    #[derive(Debug)]
    struct ObservedColumn {
        name: &'static str,
        error_on: Option<i32>,
        observations: Arc<Mutex<Vec<Option<i32>>>>,
    }

    impl PartialEq for ObservedColumn {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name && self.error_on == other.error_on
        }
    }

    impl Eq for ObservedColumn {}

    impl Hash for ObservedColumn {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.name.hash(state);
            self.error_on.hash(state);
        }
    }

    impl fmt::Display for ObservedColumn {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.name)
        }
    }

    impl PhysicalExpr for ObservedColumn {
        fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
            Ok(DataType::Int32)
        }

        fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
            Ok(true)
        }

        fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
            let array = batch.column(0);
            let values = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| datafusion_common::exec_datafusion_err!("expected Int32 input"))?;
            for index in 0..values.len() {
                let value = (!values.is_null(index)).then(|| values.value(index));
                lock_observations(&self.observations)?.push(value);
                if let Some(error_on) = self.error_on
                    && value == Some(error_on)
                {
                    return exec_err!("{} rejected {error_on}", self.name);
                }
            }
            Ok(ColumnarValue::Array(Arc::clone(array)))
        }

        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn PhysicalExpr>>,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            if !children.is_empty() {
                return internal_err!("ObservedColumn does not accept children");
            }
            Ok(self)
        }

        fn fmt_sql(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.name)
        }
    }

    fn observed_column(
        name: &'static str,
        error_on: Option<i32>,
        observations: &Arc<Mutex<Vec<Option<i32>>>>,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(ObservedColumn {
            name,
            error_on,
            observations: Arc::clone(observations),
        })
    }

    fn lock_observations(
        observations: &Mutex<Vec<Option<i32>>>,
    ) -> Result<MutexGuard<'_, Vec<Option<i32>>>> {
        observations
            .lock()
            .map_err(|_| internal_datafusion_err!("observation mutex is poisoned"))
    }

    fn test_batch(values: Vec<Option<i32>>) -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));
        Ok(RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(values))],
        )?)
    }

    #[test]
    fn preserves_row_major_error_order() -> Result<()> {
        let batch = test_batch(vec![Some(0), Some(1)])?;
        let first_observations = Arc::new(Mutex::new(Vec::new()));
        let second_observations = Arc::new(Mutex::new(Vec::new()));
        let expression = LazyScalarExpr::try_new(
            Arc::new(ScalarUDF::from(FirstArgument::new())),
            vec![
                observed_column("first", Some(1), &first_observations),
                observed_column("second", Some(0), &second_observations),
            ],
            batch.schema_ref(),
            Arc::new(ConfigOptions::default()),
        )?;

        let error = match expression.evaluate(&batch) {
            Ok(_) => return internal_err!("expected lazy scalar evaluation to fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("second rejected 0"), "{error}");
        assert_eq!(*lock_observations(&first_observations)?, vec![Some(0)]);
        assert_eq!(*lock_observations(&second_observations)?, vec![Some(0)]);
        Ok(())
    }

    #[test]
    fn skips_later_arguments_after_null() -> Result<()> {
        let batch = test_batch(vec![None, Some(1)])?;
        let first_observations = Arc::new(Mutex::new(Vec::new()));
        let second_observations = Arc::new(Mutex::new(Vec::new()));
        let expression = LazyScalarExpr::try_new(
            Arc::new(ScalarUDF::from(FirstArgument::new())),
            vec![
                observed_column("first", None, &first_observations),
                observed_column("second", None, &second_observations),
            ],
            batch.schema_ref(),
            Arc::new(ConfigOptions::default()),
        )?;

        let result = expression.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result = result
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| internal_datafusion_err!("expected Int32 result"))?;
        assert!(result.is_null(0));
        assert_eq!(result.value(1), 1);
        assert_eq!(
            *lock_observations(&first_observations)?,
            vec![None, Some(1)]
        );
        assert_eq!(*lock_observations(&second_observations)?, vec![Some(1)]);
        Ok(())
    }

    #[test]
    fn skips_later_arguments_after_untyped_null() -> Result<()> {
        let batch = test_batch(vec![Some(1)])?;
        let second_observations = Arc::new(Mutex::new(Vec::new()));
        let expression = LazyScalarExpr::try_new(
            Arc::new(ScalarUDF::from(FirstArgument::new())),
            vec![
                Arc::new(Literal::new(ScalarValue::Null)),
                observed_column("second", Some(1), &second_observations),
            ],
            batch.schema_ref(),
            Arc::new(ConfigOptions::default()),
        )?;

        let result = expression.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result = result
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| internal_datafusion_err!("expected Int32 result"))?;
        assert!(result.is_null(0));
        assert!(lock_observations(&second_observations)?.is_empty());
        Ok(())
    }
}
