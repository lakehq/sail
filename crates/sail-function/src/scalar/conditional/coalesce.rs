// [CREDIT]: https://github.com/apache/datafusion/blob/90b55e38b5de8e74a17e24bf7ea7259cbcdab022/datafusion/functions/src/core/coalesce.rs

use std::sync::Arc;

use arrow::array::{BooleanArray, new_null_array};
use arrow::compute::kernels::zip::zip;
use arrow::compute::{and, is_not_null, is_null};
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow_schema::extension::EXTENSION_TYPE_METADATA_KEY;
use datafusion_common::metadata::FieldMetadata;
use datafusion_common::{Result, ScalarValue, exec_err, internal_err, plan_err};
use datafusion_expr::binary::try_type_union_resolution;
use datafusion_expr::conditional_expressions::CaseBuilder;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use itertools::Itertools;
use sail_common::spec::SAIL_SPARK_INTERVAL_METADATA_KEY;
use sail_common_datafusion::metadata::{
    coerced_fields_metadata, type_metadata, widest_interval_metadata,
};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_common_datafusion::uuid::{UUID_EXTENSION_NAME, UUID_METADATA_KEY, is_uuid_field};

use crate::scalar::conditional::coalesce_lazy::CoalesceLazy;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CoalesceFunc {
    pub(super) signature: Signature,
}

impl Default for CoalesceFunc {
    fn default() -> Self {
        CoalesceFunc::new()
    }
}

impl CoalesceFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CoalesceFunc {
    fn name(&self) -> &str {
        // IMPORTANT: Do not change the name. DataFusion optimizer rules find these functions by name.
        "coalesce"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // If any the arguments in coalesce is non-null, the result is non-null
        let nullable = args.arg_fields.iter().all(|f| f.is_nullable());
        let Some(return_type) = args
            .arg_fields
            .iter()
            .map(|f| f.data_type())
            .find_or_first(|d| !d.is_null())
            .cloned()
        else {
            return plan_err!("coalesce must have at least one argument");
        };
        // Propagate field metadata (e.g. Arrow extension types) from the arguments that can
        // supply a value; see `coerced_fields_metadata` for the merge semantics.
        let metadata = coerced_fields_metadata(args.arg_fields.iter().map(|f| f.as_ref()))?;
        // Keep only the type metadata of the arguments; see `type_metadata`. The interval
        // range of the result is the widest range of the arguments, not that of the first one.
        let mut metadata = type_metadata(&metadata).inner().clone();
        // Untyped NULL is neutral, but typed fixed[16] (even NULL) must agree
        // on UUID identity. Other extension types retain their merge semantics.
        if metadata.get(UUID_METADATA_KEY).map(String::as_str) == Some(UUID_EXTENSION_NAME)
            && (return_type != DataType::FixedSizeBinary(16)
                || !args
                    .arg_fields
                    .iter()
                    .filter(|field| !field.data_type().is_null())
                    .all(|field| is_uuid_field(field)))
        {
            metadata.remove(UUID_METADATA_KEY);
            metadata.remove(EXTENSION_TYPE_METADATA_KEY);
        }
        let fields = args
            .arg_fields
            .iter()
            .map(|f| f.as_ref())
            .zip(args.scalar_arguments.iter().copied());
        match widest_interval_metadata(fields)? {
            Some(interval) => {
                metadata.insert(SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(), interval)
            }
            None => metadata.remove(SAIL_SPARK_INTERVAL_METADATA_KEY),
        };
        let metadata = FieldMetadata::from(metadata);
        Ok(metadata
            .add_to_field(Field::new(self.name(), return_type, nullable))
            .into())
    }

    /// coalesce evaluates to the first value which is not NULL
    ///
    /// Only reached when [`Self::simplify`] keeps the call because an argument
    /// is volatile; evaluates each argument exactly once.
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let return_type = args.return_field.data_type().clone();
        let args = args.args;
        // do not accept 0 arguments.
        if args.is_empty() {
            return exec_err!(
                "coalesce was called with {} arguments. It requires at least 1.",
                args.len()
            );
        }

        let mut return_array = args.iter().filter_map(|x| match x {
            ColumnarValue::Array(array) => Some(array.len()),
            _ => None,
        });

        if let Some(size) = return_array.next() {
            // start with nulls as default output
            let mut current_value = new_null_array(&return_type, size);
            let mut remainder = BooleanArray::from(vec![true; size]);

            for arg in args {
                // Fixed[16] coercion preserves untyped NULL for identity consensus.
                if arg.data_type().is_null() {
                    continue;
                }
                match arg {
                    ColumnarValue::Array(ref array) => {
                        let to_apply = and(&remainder, &is_not_null(array.as_ref())?)?;
                        current_value = zip(&to_apply, array, &current_value)?;
                        remainder = and(&remainder, &is_null(array)?)?;
                    }
                    ColumnarValue::Scalar(value) => {
                        if value.is_null() {
                            continue;
                        } else {
                            let last_value = value.to_scalar()?;
                            current_value = zip(&remainder, &last_value, &current_value)?;
                            break;
                        }
                    }
                }
                if remainder.true_count() == 0 {
                    break;
                }
            }
            Ok(ColumnarValue::Array(current_value))
        } else {
            match args
                .iter()
                .find(|x| matches!(x, ColumnarValue::Scalar(s) if !s.is_null()))
            {
                Some(value) => Ok(value.clone()),
                None => Ok(ColumnarValue::Scalar(ScalarValue::try_from(&return_type)?)),
            }
        }
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if args.is_empty() {
            return plan_err!("coalesce must have at least one argument");
        }

        if args.len() == 1 {
            return Ok(ExprSimplifyResult::Simplified(args.one()?));
        }

        // `coalesce(a, b)` is rewritten to the (lazy, fast)
        // `CASE WHEN a IS NOT NULL THEN a ELSE b END`, which names every argument
        // but the last one twice. A volatile argument named twice is evaluated
        // twice, so the rewrite stops at the first volatile non-final argument
        // and the rest is left to the `invoke_with_args` kernel, which evaluates
        // each argument once. See https://github.com/apache/datafusion/issues/25477
        let (init, last) = args.split_at(args.len() - 1);
        let (prefix, rest) = match init.iter().position(|arg| arg.is_volatile()) {
            None => (init, last[0].clone()),
            // The kernel evaluates every argument eagerly, so an argument
            // after a non-NULL volatile one would still be evaluated (and could
            // fail), unlike Spark's lazy `Coalesce`. Use `CoalesceLazy` for the
            // rest unless every later argument is a column or a literal, which
            // is cheap and cannot fail (this also keeps the plan shape of
            // `coalesce(input_file_name(), '')`).
            Some(i) if !args[i + 1..].iter().all(is_column_or_literal) => {
                if i == 0 {
                    return Ok(ExprSimplifyResult::Simplified(CoalesceLazy::call(args)?));
                }
                (&init[..i], CoalesceLazy::call(args[i..].to_vec())?)
            }
            Some(0) => return Ok(ExprSimplifyResult::Original(args)),
            Some(i) => (
                &init[..i],
                Expr::ScalarFunction(ScalarFunction::new_udf(
                    Arc::new(ScalarUDF::new_from_impl(CoalesceFunc::new())),
                    args[i..].to_vec(),
                )),
            ),
        };

        let whens = prefix.iter().map(|x| x.clone().is_not_null()).collect();
        Ok(ExprSimplifyResult::Simplified(
            CaseBuilder::new(None, whens, prefix.to_vec(), Some(Box::new(rest))).end()?,
        ))
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        let eager = vec![&args[0]];
        let lazy = args[1..].iter().collect();
        Some((eager, lazy))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            return exec_err!("coalesce must have at least one argument");
        }

        // DataFusion inserts casts before deriving the return field. Preserve
        // untyped NULL here so it cannot become a typed fixed[16] UUID veto.
        if arg_types.contains(&DataType::FixedSizeBinary(16))
            && arg_types
                .iter()
                .all(|t| matches!(t, DataType::Null | DataType::FixedSizeBinary(16)))
        {
            return Ok(arg_types.to_vec());
        }
        try_type_union_resolution(arg_types)
    }
}

// See `CoalesceFunc::simplify`.
fn is_column_or_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(_) | Expr::Literal(..))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray,
    };
    use datafusion::arrow::datatypes::DataType;
    use datafusion::dataframe::DataFrame;
    use datafusion::prelude::SessionContext;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{Expr, ScalarUDF, cast, col, lit, when};

    use super::CoalesceFunc;
    use crate::scalar::math::random::Random;
    use crate::scalar::misc::raise_error::RaiseError;

    // The result keeps only the type metadata of the arguments; see `type_metadata`.
    #[test]
    fn coalesce_return_field_keeps_only_type_metadata() -> Result<()> {
        use datafusion::arrow::datatypes::Field;
        use datafusion_expr::{ReturnFieldArgs, ScalarUDFImpl};

        let a = Arc::new(Field::new("a", DataType::Int32, true).with_metadata(
            std::collections::HashMap::from([
                ("ARROW:extension:name".to_string(), "ext.type".to_string()),
                ("SPARK::metadata::json".to_string(), "{}".to_string()),
            ]),
        ));
        let b = Arc::new(Field::new("b", DataType::Int32, true).with_metadata(
            std::collections::HashMap::from([("PARQUET:field_id".to_string(), "2".to_string())]),
        ));
        let field = CoalesceFunc::new().return_field_from_args(ReturnFieldArgs {
            arg_fields: &[a, b],
            scalar_arguments: &[None, None],
        })?;
        let keys = field
            .metadata()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>();
        assert_eq!(keys, vec!["ARROW:extension:name"]);
        Ok(())
    }

    #[test]
    fn coalesce_uuid_requires_typed_input_consensus() -> Result<()> {
        use arrow::datatypes::Field;
        use datafusion_expr::{
            HigherOrderReturnFieldArgs, HigherOrderUDFImpl, ReturnFieldArgs, ScalarUDFImpl,
            ValueOrLambda,
        };
        use sail_common_datafusion::uuid::{UUID_EXTENSION_NAME, UUID_METADATA_KEY, is_uuid_field};

        use crate::scalar::conditional::coalesce_lazy::CoalesceLazy;

        let uuid = Arc::new(
            Field::new("u", DataType::FixedSizeBinary(16), true).with_metadata(
                std::collections::HashMap::from([(
                    UUID_METADATA_KEY.to_string(),
                    UUID_EXTENSION_NAME.to_string(),
                )]),
            ),
        );
        let fixed = Arc::new(Field::new("f", DataType::FixedSizeBinary(16), true));
        let null = Arc::new(Field::new("null", DataType::Null, true));
        let typed_null = ScalarValue::FixedSizeBinary(16, None);
        for (other, scalar, expected) in [
            (Arc::clone(&uuid), None, true),
            (Arc::clone(&uuid), Some(&typed_null), true),
            (null, Some(&ScalarValue::Null), true),
            (Arc::clone(&fixed), None, false),
            (fixed, Some(&typed_null), false),
        ] {
            for reverse in [false, true] {
                let mut fields = [Arc::clone(&uuid), Arc::clone(&other)];
                let mut scalars = [None, scalar];
                if reverse {
                    fields.reverse();
                    scalars.reverse();
                }
                let field = CoalesceFunc::new().return_field_from_args(ReturnFieldArgs {
                    arg_fields: &fields,
                    scalar_arguments: &scalars,
                })?;
                assert_eq!(field.data_type(), &DataType::FixedSizeBinary(16));
                assert!(field.is_nullable());
                assert_eq!(is_uuid_field(&field), expected);
                assert_eq!(field.metadata().len(), usize::from(expected));
                let lazy =
                    CoalesceLazy::new().return_field_from_args(HigherOrderReturnFieldArgs {
                        arg_fields: &[
                            ValueOrLambda::Value(Arc::clone(&fields[0])),
                            ValueOrLambda::Lambda(Arc::clone(&fields[1])),
                        ],
                        scalar_arguments: &scalars,
                    })?;
                assert_eq!(field, lazy);
            }
        }
        // A stale marker on a different physical type must never survive.
        let binary = Arc::new(uuid.as_ref().clone().with_data_type(DataType::Binary));
        let field = CoalesceFunc::new().return_field_from_args(ReturnFieldArgs {
            arg_fields: &[binary],
            scalar_arguments: &[None],
        })?;
        assert!(!field.metadata().contains_key(UUID_METADATA_KEY));
        Ok(())
    }

    #[test]
    fn coalesce_only_removes_uuid_extension_parameters() -> Result<()> {
        use arrow::datatypes::Field;
        use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
        use datafusion_expr::{ReturnFieldArgs, ScalarUDFImpl};
        use sail_common_datafusion::uuid::UUID_EXTENSION_NAME;

        for name in [UUID_EXTENSION_NAME, "other.extension", "parquet.variant"] {
            let typed = Arc::new(
                Field::new("typed", DataType::FixedSizeBinary(16), true).with_metadata(
                    [
                        (EXTENSION_TYPE_NAME_KEY.into(), name.into()),
                        (EXTENSION_TYPE_METADATA_KEY.into(), String::new()),
                    ]
                    .into(),
                ),
            );
            let fixed = Arc::new(Field::new("fixed", DataType::FixedSizeBinary(16), true));
            for fields in [
                [typed.clone(), fixed.clone()],
                [fixed.clone(), typed.clone()],
            ] {
                let result = CoalesceFunc::new().return_field_from_args(ReturnFieldArgs {
                    arg_fields: &fields,
                    scalar_arguments: &[None, None],
                })?;
                if name == UUID_EXTENSION_NAME {
                    assert!(result.metadata().is_empty());
                } else {
                    assert_eq!(result.metadata(), typed.metadata());
                }
            }
        }
        Ok(())
    }

    #[test]
    fn coalesce_fixed_null_coercion_preserves_untyped_null() -> Result<()> {
        use datafusion_expr::ScalarUDFImpl;

        let function = CoalesceFunc::new();
        for types in [
            vec![DataType::Null, DataType::FixedSizeBinary(16)],
            vec![DataType::FixedSizeBinary(16), DataType::Null],
            vec![DataType::FixedSizeBinary(16)],
            vec![DataType::FixedSizeBinary(16); 2],
            vec![
                DataType::Null,
                DataType::FixedSizeBinary(16),
                DataType::Null,
                DataType::FixedSizeBinary(16),
            ],
        ] {
            assert_eq!(function.coerce_types(&types)?, types);
        }
        // Mixed binary inputs still delegate to upstream union resolution.
        // Its category check currently rejects them before binary widening.
        for types in [
            vec![DataType::FixedSizeBinary(16), DataType::Binary],
            vec![
                DataType::Null,
                DataType::FixedSizeBinary(16),
                DataType::Binary,
            ],
            vec![
                DataType::Binary,
                DataType::FixedSizeBinary(16),
                DataType::Null,
            ],
        ] {
            assert_eq!(
                function.coerce_types(&types).map_err(|e| e.to_string()),
                datafusion_expr::binary::try_type_union_resolution(&types)
                    .map_err(|e| e.to_string())
            );
        }
        assert_eq!(
            function.coerce_types(&[DataType::Null, DataType::Null])?,
            vec![DataType::Null; 2]
        );
        assert_eq!(
            function.coerce_types(&[DataType::Null, DataType::Int64])?,
            vec![DataType::Int64; 2]
        );
        Ok(())
    }

    #[test]
    fn coalesce_fixed_null_kernel_uses_return_type() -> Result<()> {
        use arrow::array::{FixedSizeBinaryArray, NullArray};
        use arrow::datatypes::Field;
        use datafusion_common::config::ConfigOptions;
        use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

        let fixed = Arc::new(Field::new("v", DataType::FixedSizeBinary(16), true));
        let value = [0x5a; 16];
        let array = Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            [Some(value.as_slice()), None].into_iter(),
            16,
        )?) as ArrayRef;
        for reverse in [false, true] {
            for null in [
                ColumnarValue::Scalar(ScalarValue::Null),
                ColumnarValue::Array(Arc::new(NullArray::new(2))),
            ] {
                let mut args = vec![null, ColumnarValue::Array(Arc::clone(&array))];
                if reverse {
                    args.reverse();
                }
                let arg_fields = args
                    .iter()
                    .map(|arg| Arc::new(Field::new("arg", arg.data_type(), true)))
                    .collect();
                let result = CoalesceFunc::new()
                    .invoke_with_args(ScalarFunctionArgs {
                        args,
                        arg_fields,
                        number_rows: 2,
                        return_field: Arc::clone(&fixed),
                        config_options: Arc::new(ConfigOptions::default()),
                    })?
                    .into_array(2)?;
                assert_eq!(result.as_ref(), array.as_ref());
            }
            let mut args = vec![
                ColumnarValue::Scalar(ScalarValue::Null),
                ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(16, None)),
            ];
            if reverse {
                args.reverse();
            }
            let arg_fields = args
                .iter()
                .map(|arg| Arc::new(Field::new("arg", arg.data_type(), true)))
                .collect();
            let result = CoalesceFunc::new().invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: 1,
                return_field: Arc::clone(&fixed),
                config_options: Arc::new(ConfigOptions::default()),
            })?;
            assert!(matches!(
                result,
                ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(16, None))
            ));
        }
        Ok(())
    }

    #[test]
    fn coalesce_lazy_fixed_untyped_null_arguments() -> Result<()> {
        use arrow::array::{FixedSizeBinaryArray, NullArray};
        use arrow::datatypes::{Field, FieldRef};
        use datafusion::physical_expr::expressions::{Column, Literal};
        use datafusion_common::config::ConfigOptions;
        use datafusion_expr::{
            ColumnarValue, HigherOrderFunctionArgs, HigherOrderUDFImpl, LambdaArgument,
            ValueOrLambda,
        };

        use crate::scalar::conditional::coalesce_lazy::CoalesceLazy;

        let fixed_array = |values: [Option<[u8; 16]>; 4]| -> Result<ArrayRef> {
            Ok(Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 16)?,
            ))
        };
        let captured = |array: ArrayRef| -> Result<(LambdaArgument, FieldRef)> {
            let field = Arc::new(Field::new("capture", array.data_type().clone(), true));
            let captures = RecordBatch::try_from_iter([("capture", array)])?;
            Ok((
                LambdaArgument::new(
                    vec![Arc::new(Field::new("unused", DataType::Null, true))],
                    Arc::new(Column::new("capture", 0)),
                    Some(captures),
                    &[],
                ),
                field,
            ))
        };
        let invoke = |first: ArrayRef,
                      lambdas: Vec<(LambdaArgument, FieldRef)>|
         -> Result<ArrayRef> {
            let mut fields = vec![ValueOrLambda::Value(Arc::new(Field::new(
                "first",
                first.data_type().clone(),
                true,
            )))];
            let mut args = vec![ValueOrLambda::Value(ColumnarValue::Array(first))];
            for (lambda, field) in lambdas {
                args.push(ValueOrLambda::Lambda(lambda));
                fields.push(ValueOrLambda::Lambda(field));
            }
            CoalesceLazy::new()
                .invoke_with_args(HigherOrderFunctionArgs {
                    args,
                    arg_fields: fields,
                    number_rows: 4,
                    return_field: Arc::new(Field::new("v", DataType::FixedSizeBinary(16), true)),
                    config_options: Arc::new(ConfigOptions::default()),
                })?
                .into_array(4)
        };
        let null = Arc::new(NullArray::new(4)) as ArrayRef;
        let fallback = fixed_array([Some([1; 16]), Some([2; 16]), None, Some([4; 16])])?;
        // An untyped NullArray first argument must be cast before interleaving.
        let result = invoke(Arc::clone(&null), vec![captured(Arc::clone(&fallback))?])?;
        assert_eq!(result.as_ref(), fallback.as_ref());

        for null_lambda in [
            captured(Arc::clone(&null))?,
            (
                LambdaArgument::new(
                    vec![Arc::new(Field::new("unused", DataType::Null, true))],
                    Arc::new(Literal::new(ScalarValue::Null)),
                    None,
                    &[],
                ),
                Arc::new(Field::new("null", DataType::Null, true)),
            ),
        ] {
            // Both captured NullArray and scalar NULL bodies leave all rows pending.
            let result = invoke(
                Arc::clone(&null),
                vec![null_lambda.clone(), captured(Arc::clone(&fallback))?],
            )?;
            assert_eq!(result.as_ref(), fallback.as_ref());

            // The same NULL bodies run only for rows 1 and 2. The fixed fallback
            // must gather those captured rows, preserving row 2's final NULL.
            let first = fixed_array([Some([9; 16]), None, None, Some([8; 16])])?;
            let result = invoke(first, vec![null_lambda, captured(Arc::clone(&fallback))?])?;
            let expected = fixed_array([Some([9; 16]), Some([2; 16]), None, Some([8; 16])])?;
            assert_eq!(result.as_ref(), expected.as_ref());
        }
        Ok(())
    }

    #[test]
    fn coalesce_interval_metadata_still_uses_scalar_nulls()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        use arrow::datatypes::{Field, IntervalUnit};
        use datafusion_expr::{ReturnFieldArgs, ScalarUDFImpl};
        use sail_common::spec::{
            IntervalFieldType, IntervalUnit as SparkIntervalUnit, SAIL_SPARK_INTERVAL_METADATA_KEY,
            SparkIntervalMetadata,
        };

        let year = SparkIntervalMetadata::try_new(
            SparkIntervalUnit::YearMonth,
            Some(IntervalFieldType::Year),
            Some(IntervalFieldType::Year),
        )?
        .ok_or_else(|| datafusion_common::internal_datafusion_err!("expected interval metadata"))?
        .to_json()?;
        let plain = Field::new("i", DataType::Interval(IntervalUnit::YearMonth), true);
        let qualified = Arc::new(
            plain
                .clone()
                .with_metadata(std::collections::HashMap::from([(
                    SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
                    year.clone(),
                )])),
        );
        let null = ScalarValue::IntervalYearMonth(None);
        for (scalar, expected) in [(Some(&null), Some(&year)), (None, None)] {
            let field = CoalesceFunc::new().return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&qualified), Arc::new(plain.clone())],
                scalar_arguments: &[None, scalar],
            })?;
            assert_eq!(
                field.metadata().get(SAIL_SPARK_INTERVAL_METADATA_KEY),
                expected
            );
        }
        Ok(())
    }

    fn coalesce(args: Vec<Expr>) -> Expr {
        ScalarUDF::from(CoalesceFunc::new()).call(args)
    }

    fn rand() -> Expr {
        ScalarUDF::from(Random::new()).call(vec![])
    }

    fn raise_error(message: Expr) -> Expr {
        cast(
            ScalarUDF::from(RaiseError::new()).call(vec![message]),
            DataType::Float64,
        )
    }

    fn table(columns: Vec<(&str, ArrayRef)>) -> Result<DataFrame> {
        let batch = RecordBatch::try_from_iter(columns)?;
        SessionContext::new().read_batch(batch)
    }

    fn nullable_doubles() -> Result<DataFrame> {
        table(vec![(
            "a",
            Arc::new(Float64Array::from(vec![None, Some(1.0)])) as ArrayRef,
        )])
    }

    /// Each probe: (name, dataframe with a single output column `v`).
    fn probes() -> Result<Vec<(&'static str, DataFrame)>> {
        let three = table(vec![(
            "m",
            Arc::new(StringArray::from(vec!["boom"; 3])) as ArrayRef,
        )])?;
        let many = table(vec![(
            "x",
            Arc::new(Int64Array::from_iter_values(0..10_000)) as ArrayRef,
        )])?;
        let sometimes_null =
            when(rand().lt(lit(0.5)), lit(ScalarValue::Int64(None))).otherwise(lit(1_i64))?;
        Ok(vec![
            // Volatile first argument that is never null: later arguments
            // must not be evaluated, so the error is never raised.
            (
                "volatile_first_literal_error",
                three.clone().select(vec![
                    coalesce(vec![rand(), raise_error(lit("boom"))]).alias("v"),
                ])?,
            ),
            // The error message comes from a column.
            (
                "volatile_first_column_error",
                three.select(vec![
                    coalesce(vec![rand(), raise_error(col("m"))]).alias("v"),
                ])?,
            ),
            // Volatile argument in the middle: only the rest after the
            // non-volatile prefix stays a `coalesce` call.
            (
                "volatile_middle_error",
                nullable_doubles()?.select(vec![
                    coalesce(vec![col("a"), rand(), raise_error(lit("boom"))]).alias("v"),
                ])?,
            ),
            // Volatile last argument: only evaluated where `a` is null.
            (
                "volatile_last",
                nullable_doubles()?.select(vec![coalesce(vec![col("a"), rand()]).alias("v")])?,
            ),
            // A volatile, sometimes-null first argument must be evaluated once:
            // the result is never null when the last argument is non-null.
            (
                "volatile_sometimes_null",
                many.select(vec![coalesce(vec![sometimes_null, lit(2_i64)]).alias("v")])?,
            ),
        ])
    }

    /// Spark semantics: every probe succeeds and `v` is never null.
    async fn null_count(df: DataFrame) -> Result<usize> {
        let batches = df.collect().await?;
        Ok(batches.iter().map(|b| b.column(0).null_count()).sum())
    }

    /// Later arguments of the lazy path are evaluated on the rows that are
    /// still NULL, and each row takes the first non-NULL value.
    #[tokio::test]
    async fn coalesce_lazy_takes_the_first_non_null_value() -> Result<()> {
        let x = || col("x");
        let df = table(vec![(
            "x",
            Arc::new(Int64Array::from_iter_values(0..10)) as ArrayRef,
        )])?
        .select(vec![
            coalesce(vec![
                // Volatile, and NULL where x % 3 = 0.
                when(
                    rand().lt(lit(2.0)).and((x() % lit(3_i64)).eq(lit(0_i64))),
                    lit(ScalarValue::Int64(None)),
                )
                .otherwise(x())?,
                // NULL where x % 2 = 0.
                when(
                    (x() % lit(2_i64)).eq(lit(0_i64)),
                    lit(ScalarValue::Int64(None)),
                )
                .otherwise(x() * lit(10_i64))?,
                x() * lit(100_i64),
            ])
            .alias("v"),
        ])?;
        let plan = df
            .clone()
            .into_optimized_plan()?
            .display_indent()
            .to_string();
        assert!(plan.contains("coalesce_lazy"), "{plan}");
        let batches = df.collect().await?;
        let values = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|array| array.iter().collect::<Vec<_>>())
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        let expected = (0..10_i64)
            .map(|x| match (x % 3, x % 2) {
                (0, 0) => Some(x * 100),
                (0, _) => Some(x * 10),
                _ => Some(x),
            })
            .collect::<Vec<_>>();
        assert_eq!(values, expected);
        Ok(())
    }

    /// When the first argument is NULL on every row, the next argument is
    /// evaluated on the whole batch.
    #[tokio::test]
    async fn coalesce_lazy_first_argument_null_on_every_row() -> Result<()> {
        let df = table(vec![(
            "x",
            Arc::new(Int64Array::from_iter_values(0..10)) as ArrayRef,
        )])?
        .select(vec![
            coalesce(vec![
                when(rand().lt(lit(2.0)), lit(ScalarValue::Int64(None))).end()?,
                col("x") * lit(10_i64),
            ])
            .alias("v"),
        ])?;
        let plan = df
            .clone()
            .into_optimized_plan()?
            .display_indent()
            .to_string();
        assert!(plan.contains("coalesce_lazy"), "{plan}");
        let batches = df.collect().await?;
        let values = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|array| array.iter().collect::<Vec<_>>())
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            (0..10_i64).map(|x| Some(x * 10)).collect::<Vec<_>>()
        );
        Ok(())
    }

    async fn collect_int64(df: DataFrame) -> Result<Vec<Option<i64>>> {
        let plan = df
            .clone()
            .into_optimized_plan()?
            .display_indent()
            .to_string();
        assert!(plan.contains("coalesce_lazy"), "{plan}");
        let batches = df.collect().await?;
        Ok(batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|array| array.iter().collect::<Vec<_>>())
                    .unwrap_or_default()
            })
            .collect())
    }

    /// Covers a lazy argument evaluated on every row whose body uses no
    /// columns, an all-rows argument followed by a partial one, and rows that
    /// stay NULL after every argument.
    #[tokio::test]
    async fn coalesce_lazy_mixed_pending_rows_and_trailing_nulls() -> Result<()> {
        let x = || col("x");
        let null = || lit(ScalarValue::Int64(None));
        let df = table(vec![(
            "x",
            Arc::new(Int64Array::from_iter_values(0..10)) as ArrayRef,
        )])?
        .select(vec![
            coalesce(vec![
                // NULL on every row.
                when(rand().lt(lit(2.0)), null()).end()?,
                // Evaluated on every row, uses no columns, NULL on every row.
                when(rand().lt(lit(2.0)), null()).end()?,
                // Evaluated on every row; NULL where x is even.
                when((x() % lit(2_i64)).eq(lit(0_i64)), null()).otherwise(x() * lit(10_i64))?,
                // Evaluated on the even rows; NULL where x % 3 = 0.
                when((x() % lit(3_i64)).eq(lit(0_i64)), null()).otherwise(x() * lit(100_i64))?,
            ])
            .alias("v"),
        ])?;
        let expected = (0..10_i64)
            .map(|x| match (x % 2, x % 3) {
                (1, _) => Some(x * 10),
                (_, 0) => None,
                _ => Some(x * 100),
            })
            .collect::<Vec<_>>();
        assert_eq!(collect_int64(df).await?, expected);
        Ok(())
    }

    /// The lazy path with a struct return type.
    #[tokio::test]
    async fn coalesce_lazy_struct_values() -> Result<()> {
        use datafusion::functions::core::expr_fn::{get_field, named_struct};

        let x = || col("x");
        let df = table(vec![(
            "x",
            Arc::new(Int64Array::from_iter_values(0..10)) as ArrayRef,
        )])?
        .select(vec![
            get_field(
                coalesce(vec![
                    when(
                        rand().lt(lit(2.0)).and((x() % lit(2_i64)).eq(lit(0_i64))),
                        lit(ScalarValue::Null),
                    )
                    .otherwise(named_struct(vec![lit("a"), x()]))?,
                    named_struct(vec![lit("a"), x() * lit(10_i64)]),
                ]),
                "a",
            )
            .alias("v"),
        ])?;
        let expected = (0..10_i64)
            .map(|x| Some(if x % 2 == 0 { x * 10 } else { x }))
            .collect::<Vec<_>>();
        assert_eq!(collect_int64(df).await?, expected);
        Ok(())
    }

    /// Like Spark's `Coalesce`, arguments are evaluated lazily and at most
    /// once per row, also when an argument is volatile.
    #[tokio::test]
    async fn coalesce_volatile_arguments_follow_spark_semantics() -> Result<()> {
        let mut failures = vec![];
        for (name, df) in probes()? {
            match null_count(df).await {
                Ok(0) => {}
                Ok(n) => failures.push(format!("{name}: {n} null results")),
                Err(e) => failures.push(format!("{name}: {e}")),
            }
        }
        assert!(failures.is_empty(), "{failures:#?}");
        Ok(())
    }
}
