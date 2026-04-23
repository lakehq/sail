use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, ArrowNativeTypeOp, AsArray, Float32Array, Float64Array};
use datafusion::arrow::compute::CastOptions;
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Field, FieldRef, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE,
};
use datafusion::arrow::util::display::FormatOptions;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::expr_fn::cast;
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::preimage::PreimageResult;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use num::integer::{div_ceil, div_floor};
use num::traits::CheckedAdd;

use crate::error::{
    generic_exec_err, generic_internal_err, invalid_arg_count_exec_err,
    unsupported_data_type_exec_err, unsupported_data_types_exec_err,
};
use crate::scalar::math::utils::decimal::round_decimal_base;

/// Extract the first argument and optional `target_scale`.
///
/// Arity is typically enforced by `coerce_types` (1 or 2 args), but this helper still
/// validates defensively to avoid a panic on `args[0]` if the UDF is invoked through a
/// code path that bypasses `coerce_types` (e.g. API misuse, error paths).
fn extract_call_args<'a>(
    name: &str,
    args: &'a [ColumnarValue],
) -> Result<(&'a ColumnarValue, &'a Option<i32>)> {
    match args.len() {
        1 => Ok((&args[0], &None)),
        2 => match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Int32(value)) => Ok((&args[0], value)),
            other => Err(unsupported_data_type_exec_err(
                name,
                "Target scale must be Integer literal",
                &other.data_type(),
            )),
        },
        n => Err(invalid_arg_count_exec_err(name, (1, 2), n)),
    }
}

fn ceil_floor_coerce_types(name: &str, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() == 1 {
        Ok(vec![ceil_floor_coerce_first_arg(name, &arg_types[0])?])
    } else if arg_types.len() == 2 {
        let first = ceil_floor_coerce_first_arg(name, &arg_types[0])?;
        if arg_types[1].is_integer() {
            Ok(vec![first, DataType::Int32])
        } else {
            Err(unsupported_data_types_exec_err(
                name,
                "Numeric Type for expr and Integer Type for target scale",
                arg_types,
            ))
        }
    } else {
        Err(invalid_arg_count_exec_err(name, (1, 2), arg_types.len()))
    }
}

fn ceil_floor_return_type_from_args(name: &str, args: ReturnFieldArgs) -> Result<FieldRef> {
    // Arity is enforced by `coerce_types` (1 or 2 args).
    let arg_fields = args.arg_fields;
    let scalar_arguments = args.scalar_arguments;
    let return_type = if arg_fields.len() == 1 {
        match &arg_fields[0].data_type() {
            DataType::Decimal128(precision, scale) => {
                let (precision, scale) =
                    round_decimal_base(*precision as i32, *scale as i32, 0, true);
                Ok(DataType::Decimal128(precision, scale))
            }
            DataType::Decimal256(precision, scale) => {
                if *precision <= DECIMAL128_MAX_PRECISION && *scale <= DECIMAL128_MAX_SCALE {
                    let (precision, scale) =
                        round_decimal_base(*precision as i32, *scale as i32, 0, false);
                    Ok(DataType::Decimal128(precision, scale))
                } else {
                    Err(unsupported_data_type_exec_err(
                        name,
                        format!("Decimal Type must have precision <= {DECIMAL128_MAX_PRECISION} and scale <= {DECIMAL128_MAX_SCALE}").as_str(),
                        arg_fields[0].data_type(),
                    ))
                }
            }
            _ => Ok(DataType::Int64),
        }
    } else if arg_fields.len() == 2 {
        if let Some(target_scale) = scalar_arguments[1] {
            let expr = &arg_fields[0].data_type();
            let target_scale: i32 = match target_scale {
                ScalarValue::Int8(Some(v)) => Ok(*v as i32),
                ScalarValue::Int16(Some(v)) => Ok(*v as i32),
                ScalarValue::Int32(Some(v)) => Ok(*v),
                ScalarValue::Int64(Some(v)) => Ok(*v as i32),
                ScalarValue::UInt8(Some(v)) => Ok(*v as i32),
                ScalarValue::UInt16(Some(v)) => Ok(*v as i32),
                ScalarValue::UInt32(Some(v)) => Ok(*v as i32),
                ScalarValue::UInt64(Some(v)) => Ok(*v as i32),
                _ => Err(unsupported_data_type_exec_err(
                    name,
                    "Target scale must be Integer literal",
                    &target_scale.data_type(),
                )),
            }?;
            // Decimal128 can hold 10^37 (38 digits: 1 followed by 37 zeros) but not 10^38.
            // target_scale = -n produces a result that's a multiple of 10^n, so n ≤ 37.
            if target_scale < -37 {
                return Err(generic_exec_err(
                    name,
                    "Target scale must be greater than or equal to -37",
                ));
            }
            let (precision, scale) = match expr {
                DataType::Int8 => Ok((3, 0)),
                DataType::UInt8 | DataType::Int16 => Ok((5, 0)),
                DataType::UInt16 | DataType::Int32 => Ok((10, 0)),
                DataType::UInt32 | DataType::UInt64 | DataType::Int64 => Ok((20, 0)),
                DataType::Float32 => Ok((14, 7)),
                DataType::Float64 => Ok((30, 15)),
                DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                    if *precision <= DECIMAL128_MAX_PRECISION && *scale <= DECIMAL128_MAX_SCALE {
                        Ok((*precision as i32, *scale as i32))
                    } else {
                        Err(unsupported_data_type_exec_err(
                            name,
                            format!("Decimal Type must have precision <= {DECIMAL128_MAX_PRECISION} and scale <= {DECIMAL128_MAX_SCALE}").as_str(),
                            arg_fields[0].data_type(),
                        ))
                    }
                }
                _ => Err(unsupported_data_type_exec_err(
                    name,
                    "Numeric Type for expr",
                    expr,
                )),
            }?;
            let (precision, scale) = round_decimal_base(precision, scale, target_scale, true);
            Ok(DataType::Decimal128(precision, scale))
        } else {
            Err(generic_exec_err(
                name,
                "Target scale must be Integer literal, received: None",
            ))
        }
    } else {
        // Defensive: DataFusion may call `return_field_from_args` in paths
        // that bypass `coerce_types`. Surface a precise arity error.
        Err(invalid_arg_count_exec_err(name, (1, 2), arg_fields.len()))
    }?;
    Ok(Arc::new(Field::new(name.to_string(), return_type, true)))
}

fn ceil_floor_coerce_first_arg(name: &str, arg_type: &DataType) -> Result<DataType> {
    match arg_type {
        DataType::Null => Ok(DataType::Float64),
        DataType::UInt8 => Ok(DataType::Int16),
        DataType::UInt16 => Ok(DataType::Int32),
        DataType::UInt32 | DataType::UInt64 => Ok(DataType::Int64),
        DataType::Decimal256(precision, scale) => {
            if *precision <= DECIMAL128_MAX_PRECISION && *scale <= DECIMAL128_MAX_SCALE {
                Ok(DataType::Decimal128(*precision, *scale))
            } else {
                Err(unsupported_data_type_exec_err(
                    name,
                    format!("Decimal Type must have precision <= {DECIMAL128_MAX_PRECISION} and scale <= {DECIMAL128_MAX_SCALE}").as_str(),
                    arg_type,
                ))
            }
        }
        other if other.is_numeric() => Ok(other.clone()),
        other => Err(unsupported_data_type_exec_err(
            name,
            "First arg must be Numeric Type",
            other,
        )),
    }
}

#[inline]
fn get_return_type_precision_scale(return_type: &DataType) -> Result<(u8, i8)> {
    match return_type {
        DataType::Decimal128(precision, scale) => Ok((*precision, *scale)),
        other => Err(generic_internal_err(
            "ceil",
            format!("Expected return type to be Decimal128, got: {other}").as_str(),
        )),
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCeil {
    signature: Signature,
    /// Bound at planning time from `PlanConfig::ansi_mode` (which maps
    /// `spark.sql.ansi.enabled`). Controls overflow handling in the
    /// Float→Decimal cast path: `true` errors on overflow, `false`
    /// returns NULL. Serialized in `codec.rs` for distributed execution.
    ansi_mode: bool,
}

impl Default for SparkCeil {
    fn default() -> Self {
        Self::new(false)
    }
}

impl SparkCeil {
    pub fn new(ansi_mode: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            ansi_mode,
        }
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }
}

impl ScalarUDFImpl for SparkCeil {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "spark_ceil"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(generic_internal_err(
            "ceil",
            "`return_type` should not be called, call `return_type_from_args` instead",
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        ceil_floor_return_type_from_args("ceil", args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let (arg, target_scale) = extract_call_args("ceil", &args.args)?;
        let return_type = args.return_field.data_type();
        spark_ceil_floor("ceil", arg, target_scale, return_type, self.ansi_mode)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        ceil_floor_coerce_types("ceil", arg_types)
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        Ok(input
            .first()
            .map(|expr| expr.sort_properties)
            .unwrap_or(SortProperties::Unordered))
    }

    fn preserves_lex_ordering(&self, _inputs: &[ExprProperties]) -> Result<bool> {
        // ceil is monotonic non-decreasing → preserves lex order of its input.
        // Default is false; opting in unlocks equivalence-based ordering
        // inference (see equivalence/properties/mod.rs:469).
        Ok(true)
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        simplify_ceil_floor("spark_ceil", args, info)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        propagate_ceil_floor(CeilFloor::Ceil, interval, inputs)
    }

    fn evaluate_bounds(&self, input: &[&Interval]) -> Result<Interval> {
        evaluate_bounds_ceil_floor(CeilFloor::Ceil, input)
    }

    // NOTE: no `preimage` hook for `ceil`. The preimage of `ceil(x) = N` is
    // `(N - 1, N]` (right-closed, left-open), but `PreimageResult::Range` is
    // `[lower, upper)` (left-closed, right-open). There is no [lower, upper)
    // that exactly represents (N - 1, N]: including N - 1 is wrong
    // (`ceil(N - 1) = N - 1`, not N), excluding N is wrong (`ceil(N) = N`).
    // Any rewrite would be unsound. Upstream DF v53 skips this on `CeilFunc`
    // for the same reason. Filter pushdown for `ceil` would require extending
    // `PreimageResult` to support right-closed intervals.
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkFloor {
    signature: Signature,
    /// See [`SparkCeil::ansi_mode`].
    ansi_mode: bool,
}

impl Default for SparkFloor {
    fn default() -> Self {
        Self::new(false)
    }
}

impl SparkFloor {
    pub fn new(ansi_mode: bool) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            ansi_mode,
        }
    }

    pub fn ansi_mode(&self) -> bool {
        self.ansi_mode
    }
}

impl ScalarUDFImpl for SparkFloor {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "spark_floor"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(generic_internal_err(
            "floor",
            "`return_type` should not be called, call `return_type_from_args` instead",
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        ceil_floor_return_type_from_args("floor", args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let (arg, target_scale) = extract_call_args("floor", &args.args)?;
        let return_type = args.return_field.data_type();
        spark_ceil_floor("floor", arg, target_scale, return_type, self.ansi_mode)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        ceil_floor_coerce_types("floor", arg_types)
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        Ok(input
            .first()
            .map(|expr| expr.sort_properties)
            .unwrap_or(SortProperties::Unordered))
    }

    fn preserves_lex_ordering(&self, _inputs: &[ExprProperties]) -> Result<bool> {
        // floor is monotonic non-decreasing → preserves lex order of its input.
        // See `SparkCeil::preserves_lex_ordering` for details.
        Ok(true)
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        simplify_ceil_floor("spark_floor", args, info)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        propagate_ceil_floor(CeilFloor::Floor, interval, inputs)
    }

    fn evaluate_bounds(&self, input: &[&Interval]) -> Result<Interval> {
        evaluate_bounds_ceil_floor(CeilFloor::Floor, input)
    }

    fn preimage(
        &self,
        args: &[Expr],
        lit_expr: &Expr,
        info: &SimplifyContext,
    ) -> Result<PreimageResult> {
        preimage_floor(args, lit_expr, info)
    }
}

#[derive(Clone, Copy)]
enum CeilFloor {
    Ceil,
    Floor,
}

const PROPAGATE_CAST_OPTIONS: CastOptions<'static> = CastOptions {
    safe: true,
    format_options: FormatOptions::new(),
};

/// Constraint propagation for `ceil` / `floor` — given a known output interval
/// from a filter (e.g. `WHERE ceil(col) > 5`), derive the widest safe input
/// interval that can still produce that output.
///
/// ## Math
///
/// Both functions are monotonic non-decreasing, so the preimage of `[a, b]` is:
/// - `ceil(x)  ∈ [a, b]`  ⇔  `x ∈ (a - 1, b]`   → over-approximate as `[a - 1, b]`
/// - `floor(x) ∈ [a, b]`  ⇔  `x ∈ [a, b + 1)`   → over-approximate as `[a, b + 1]`
///
/// ## Implementation
///
/// Over-approximation is sound: the outer `FilterExec` re-evaluates
/// `ceil/floor(x)` so spurious rows drop out; **missing** rows would be a
/// correctness bug. The widened interval is intersected with the existing
/// child interval to preserve any tighter pre-existing bounds.
///
/// ## Why this hook AND `preimage` (for floor) / why this hook ALONE (for ceil)
///
/// `floor` exposes `preimage()` below; the logical simplifier uses it to
/// rewrite `WHERE floor(col) OP lit` into a pure column predicate *before*
/// physical planning. For floor, `propagate_constraints` is the fallback for
/// shapes the simplifier can't reach (non-literal RHS, multi-arg, volatile
/// contexts).
///
/// `ceil`'s preimage `(N - 1, N]` cannot be expressed as `[lower, upper)`, so
/// `preimage` is unsafe for ceil — `propagate_constraints` is the only
/// forward-compat hook. Keeping it is cheap and becomes load-bearing the
/// moment upstream threads ScalarUDF children through `ExprIntervalGraph`.
///
/// ## Current DF v53 visibility
///
/// The only consumer is `FilterExec::statistics_by_expr()` via
/// `physical_expr::analysis::analyze`. `ExprIntervalGraph` stops at the UDF
/// node, so the refined interval never reaches `FilterExec.statistics` and
/// `row_groups_pruned_statistics` stays 0 today.
fn propagate_ceil_floor(
    kind: CeilFloor,
    interval: &Interval,
    inputs: &[&Interval],
) -> Result<Option<Vec<Interval>>> {
    let Some(child) = inputs.first() else {
        return Ok(Some(vec![]));
    };
    let child_type = child.data_type();
    let cast_output = interval.cast_to(&child_type, &PROPAGATE_CAST_OPTIONS)?;
    let one = ScalarValue::new_one(&child_type)?;
    let (lower, upper) = match kind {
        CeilFloor::Ceil => (cast_output.lower().sub(&one)?, cast_output.upper().clone()),
        CeilFloor::Floor => (cast_output.lower().clone(), cast_output.upper().add(&one)?),
    };
    let widened = Interval::try_new(lower, upper)?;
    // DataFusion contract: returned Vec MUST have `len() == inputs.len()` — one
    // interval per child in order. The consumer in `cp_solver.rs` uses `zip`
    // which silently truncates on mismatch, so a shorter Vec doesn't panic but
    // leaves config args unrefined (silent bug).
    //
    // For the 2-arg form `ceil(v, scale)`:
    //   - `inputs[0] = v` (data arg)    → refined via preimage
    //   - `inputs[1] = scale` (literal) → passed through unchanged; its interval
    //                                     is a singleton `[k, k]` that can't be
    //                                     narrowed further by constraints.
    Ok(child.intersect(widened)?.map(|refined| {
        let mut out = Vec::with_capacity(inputs.len());
        out.push(refined);
        out.extend(inputs.iter().skip(1).map(|i| (*i).clone()));
        out
    }))
}

/// Forward interval evaluation for `ceil` / `floor` (Rule 5, paired with
/// `propagate_constraints`): given an input interval `[lo, hi]`, return a
/// safe over-approximation of the output interval.
///
/// Math (both functions are monotonic non-decreasing, so `f([a, b]) ⊆ [f(a), f(b)]`):
/// - `ceil(x)  ∈ [ceil(lo),  ceil(hi)]  ⊆ [lo,     hi + 1]`
/// - `floor(x) ∈ [floor(lo), floor(hi)] ⊆ [lo - 1, hi]`
///
/// We return the simple over-approximation (`[lo, hi + 1]` / `[lo - 1, hi]`)
/// rather than calling the kernel per scalar. This keeps the hook cheap and
/// type-independent at the cost of one unit of precision. Over-approximation
/// is sound: downstream `FilterExec` re-evaluates the UDF and prunes the
/// spurious rows; missing rows would be a correctness bug.
///
/// Without this hook, `cp_solver.rs` returns `Interval::make_unbounded()` for
/// the output, which breaks interval chaining through nested UDFs. For example,
/// `ceil(floor(x))` without this hook on `floor` would feed Unbounded into
/// `ceil.propagate_constraints`, preventing any refinement on `x`.
fn evaluate_bounds_ceil_floor(kind: CeilFloor, input: &[&Interval]) -> Result<Interval> {
    let Some(child) = input.first() else {
        return Err(generic_internal_err(
            "ceil/floor",
            "evaluate_bounds called with zero inputs",
        ));
    };
    let child_type = child.data_type();
    let one = ScalarValue::new_one(&child_type)?;
    let (lower, upper) = match kind {
        CeilFloor::Ceil => (child.lower().clone(), child.upper().add(&one)?),
        CeilFloor::Floor => (child.lower().sub(&one)?, child.upper().clone()),
    };
    Interval::try_new(lower, upper)
}

/// Algebraic simplifications for `ceil` and `floor`:
/// - `f(f(x)) = f(x)` — idempotent (same-function nesting).
/// - `f(int_expr) = cast(int_expr AS BIGINT)` (1-arg only) — no rounding on integers.
fn simplify_ceil_floor(
    self_name: &str,
    args: Vec<Expr>,
    info: &SimplifyContext,
) -> Result<ExprSimplifyResult> {
    if args.len() != 1 {
        return Ok(ExprSimplifyResult::Original(args));
    }
    let arg = &args[0];
    if let Expr::ScalarFunction(inner) = arg {
        if inner.func.name() == self_name {
            return Ok(ExprSimplifyResult::Simplified(arg.clone()));
        }
    }
    // Int-identity fold: ceil(int) = int, so rewrite to a cast (to get the
    // BIGINT return type Spark expects). `coerce_types` runs before simplify
    // and narrows UInt64 → Int64 (and the other unsigned → signed widenings),
    // so `is_integer()` here only ever matches {Int8, Int16, Int32, Int64}.
    // All four widen cleanly to Int64 — no overflow risk.
    if info.get_data_type(arg)?.is_integer() {
        return Ok(ExprSimplifyResult::Simplified(cast(
            arg.clone(),
            DataType::Int64,
        )));
    }
    Ok(ExprSimplifyResult::Original(args))
}

/// Safe cast options: overflow becomes NULL instead of erroring. Used in
/// the Float→Decimal path when `spark.sql.ansi.enabled = false`.
const SAFE_CAST_OPTIONS: CastOptions<'static> = CastOptions {
    safe: true,
    format_options: FormatOptions::new(),
};

fn spark_ceil_floor(
    name: &str,
    arg: &ColumnarValue,
    target_scale: &Option<i32>,
    return_type: &DataType,
    ansi_mode: bool,
) -> Result<ColumnarValue> {
    // Float→Decimal cast options depend on ANSI mode:
    //   ANSI=true  → None (default: safe=false) → overflow errors, matching Spark ANSI.
    //   ANSI=false → Some(&SAFE_CAST_OPTIONS) → overflow becomes NULL, matching Spark non-ANSI.
    let float_cast_options: Option<&CastOptions> = if ansi_mode {
        None
    } else {
        Some(&SAFE_CAST_OPTIONS)
    };
    if matches!(
        arg.data_type(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    ) {
        if let Some(target_scale) = *target_scale {
            if target_scale >= 0 {
                Ok(arg.cast_to(return_type, None)?)
            } else {
                let (return_type_precision, return_type_scale) =
                    get_return_type_precision_scale(return_type)?;
                match arg.data_type() {
                    DataType::Int8 => match arg {
                        ColumnarValue::Scalar(ScalarValue::Int8(value)) => {
                            let result = value.map(|decimal| {
                                ceil_floor_with_target_scale(name, decimal as i128, 0, target_scale)
                            });
                            Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                                result,
                                return_type_precision,
                                return_type_scale,
                            )))
                        }
                        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                            array
                                .as_primitive::<Int8Type>()
                                .unary::<_, Decimal128Type>(|decimal| {
                                    ceil_floor_with_target_scale(
                                        name,
                                        decimal as i128,
                                        0,
                                        target_scale,
                                    )
                                })
                                .with_data_type(DataType::Decimal128(
                                    return_type_precision,
                                    return_type_scale,
                                )),
                        )
                            as ArrayRef)),
                        _ => Err(unsupported_data_type_exec_err(
                            name,
                            format!("{}", DataType::Int8).as_str(),
                            &arg.data_type(),
                        )),
                    },
                    DataType::Int16 => match arg {
                        ColumnarValue::Scalar(ScalarValue::Int16(value)) => {
                            let result = value.map(|decimal| {
                                ceil_floor_with_target_scale(name, decimal as i128, 0, target_scale)
                            });
                            Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                                result,
                                return_type_precision,
                                return_type_scale,
                            )))
                        }
                        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                            array
                                .as_primitive::<Int16Type>()
                                .unary::<_, Decimal128Type>(|decimal| {
                                    ceil_floor_with_target_scale(
                                        name,
                                        decimal as i128,
                                        0,
                                        target_scale,
                                    )
                                })
                                .with_data_type(DataType::Decimal128(
                                    return_type_precision,
                                    return_type_scale,
                                )),
                        )
                            as ArrayRef)),
                        _ => Err(unsupported_data_type_exec_err(
                            name,
                            format!("{}", DataType::Int16).as_str(),
                            &arg.data_type(),
                        )),
                    },
                    DataType::Int32 => match arg {
                        ColumnarValue::Scalar(ScalarValue::Int32(value)) => {
                            let result = value.map(|decimal| {
                                ceil_floor_with_target_scale(name, decimal as i128, 0, target_scale)
                            });
                            Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                                result,
                                return_type_precision,
                                return_type_scale,
                            )))
                        }
                        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                            array
                                .as_primitive::<Int32Type>()
                                .unary::<_, Decimal128Type>(|decimal| {
                                    ceil_floor_with_target_scale(
                                        name,
                                        decimal as i128,
                                        0,
                                        target_scale,
                                    )
                                })
                                .with_data_type(DataType::Decimal128(
                                    return_type_precision,
                                    return_type_scale,
                                )),
                        )
                            as ArrayRef)),
                        _ => Err(unsupported_data_type_exec_err(
                            name,
                            format!("{}", DataType::Int32).as_str(),
                            &arg.data_type(),
                        )),
                    },
                    DataType::Int64 => match arg {
                        ColumnarValue::Scalar(ScalarValue::Int64(value)) => {
                            let result = value.map(|decimal| {
                                ceil_floor_with_target_scale(name, decimal as i128, 0, target_scale)
                            });
                            Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                                result,
                                return_type_precision,
                                return_type_scale,
                            )))
                        }
                        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                            array
                                .as_primitive::<Int64Type>()
                                .unary::<_, Decimal128Type>(|decimal| {
                                    ceil_floor_with_target_scale(
                                        name,
                                        decimal as i128,
                                        0,
                                        target_scale,
                                    )
                                })
                                .with_data_type(DataType::Decimal128(
                                    return_type_precision,
                                    return_type_scale,
                                )),
                        )
                            as ArrayRef)),
                        _ => Err(unsupported_data_type_exec_err(
                            name,
                            format!("{}", DataType::Int64).as_str(),
                            &arg.data_type(),
                        )),
                    },
                    other => Err(unsupported_data_type_exec_err(
                        name,
                        "Numeric Type for expr",
                        &other,
                    )),
                }
            }
        } else {
            Ok(arg.cast_to(&DataType::Int64, None)?)
        }
    } else {
        match arg.data_type() {
            DataType::Float32 => {
                if let Some(target_scale) = *target_scale {
                    let (return_type_precision, return_type_scale) =
                        get_return_type_precision_scale(return_type)?;
                    let masked = mask_non_finite_f32(arg);
                    let arg = masked.cast_to(
                        &DataType::Decimal128(return_type_precision, return_type_scale),
                        float_cast_options,
                    )?;
                    decimal128_ceil_floor(name, &arg, &Some(target_scale), return_type)
                } else {
                    let func = if matches!(name, "ceil") {
                        f32::ceil
                    } else {
                        f32::floor
                    };
                    match arg {
                        ColumnarValue::Scalar(ScalarValue::Float32(value)) => {
                            Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                                value.map(|x| func(x) as i64),
                            )))
                        }
                        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                            array
                                .as_primitive::<Float32Type>()
                                .unary::<_, Int64Type>(|x| func(x) as i64),
                        )
                            as ArrayRef)),
                        _ => Err(unsupported_data_type_exec_err(
                            name,
                            format!("{}", DataType::Float32).as_str(),
                            &arg.data_type(),
                        )),
                    }
                }
            }
            DataType::Float64 => {
                if let Some(target_scale) = *target_scale {
                    let (return_type_precision, return_type_scale) =
                        get_return_type_precision_scale(return_type)?;
                    let masked = mask_non_finite_f64(arg);
                    let arg = masked.cast_to(
                        &DataType::Decimal128(return_type_precision, return_type_scale),
                        float_cast_options,
                    )?;
                    decimal128_ceil_floor(name, &arg, &Some(target_scale), return_type)
                } else {
                    let func = if matches!(name, "ceil") {
                        f64::ceil
                    } else {
                        f64::floor
                    };
                    match arg {
                        ColumnarValue::Scalar(ScalarValue::Float64(value)) => {
                            Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                                value.map(|x| func(x) as i64),
                            )))
                        }
                        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                            array
                                .as_primitive::<Float64Type>()
                                .unary::<_, Int64Type>(|x| func(x) as i64),
                        )
                            as ArrayRef)),
                        _ => Err(unsupported_data_type_exec_err(
                            name,
                            format!("{}", DataType::Float64).as_str(),
                            &arg.data_type(),
                        )),
                    }
                }
            }
            DataType::Decimal128(_precision, _scale) => {
                decimal128_ceil_floor(name, arg, target_scale, return_type)
            }
            other => Err(unsupported_data_type_exec_err(
                name,
                "Numeric Type for expr",
                &other,
            )),
        }
    }
}

fn decimal128_ceil_floor(
    name: &str,
    arg: &ColumnarValue,
    target_scale: &Option<i32>,
    return_type: &DataType,
) -> Result<ColumnarValue> {
    match arg.data_type() {
        DataType::Decimal128(_precision, scale) => {
            let (return_type_precision, return_type_scale) =
                get_return_type_precision_scale(return_type)?;
            let target_scale = (*target_scale).unwrap_or(0);
            match arg {
                ColumnarValue::Scalar(ScalarValue::Decimal128(value, _precision, _scale)) => {
                    if let Some(value) = value {
                        Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                            Some(ceil_floor_with_target_scale(
                                name,
                                *value,
                                scale,
                                target_scale,
                            )),
                            return_type_precision,
                            return_type_scale,
                        )))
                    } else {
                        Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                            None,
                            return_type_precision,
                            return_type_scale,
                        )))
                    }
                }
                ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
                    array
                        .as_primitive::<Decimal128Type>()
                        .unary::<_, Decimal128Type>(|value| {
                            ceil_floor_with_target_scale(name, value, scale, target_scale)
                        })
                        .with_data_type(DataType::Decimal128(
                            return_type_precision,
                            return_type_scale,
                        )),
                )
                    as ArrayRef)),
                _ => Err(unsupported_data_type_exec_err(
                    name,
                    "Decimal128 Type",
                    &arg.data_type(),
                )),
            }
        }
        other => Err(unsupported_data_type_exec_err(
            name,
            "Decimal128 Type for Decimal128 ceil",
            &other,
        )),
    }
}

/// Replace NaN and +/-Infinity values with NULL so they survive the cast to Decimal128.
/// Spark semantics: `ceil(NaN, s)` and `ceil(+/-Infinity, s)` return NULL even under ANSI,
/// because NaN/Inf have no valid decimal representation. Finite overflow is intentionally
/// left to raise a cast error (matching Spark ANSI=true).
fn mask_non_finite_f32(arg: &ColumnarValue) -> ColumnarValue {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Float32(Some(v))) if !v.is_finite() => {
            ColumnarValue::Scalar(ScalarValue::Float32(None))
        }
        ColumnarValue::Array(array) => {
            let float_arr = array.as_primitive::<Float32Type>();
            let masked: Float32Array = float_arr
                .iter()
                .map(|v| match v {
                    Some(x) if !x.is_finite() => None,
                    other => other,
                })
                .collect();
            ColumnarValue::Array(Arc::new(masked) as ArrayRef)
        }
        other => other.clone(),
    }
}

fn mask_non_finite_f64(arg: &ColumnarValue) -> ColumnarValue {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) if !v.is_finite() => {
            ColumnarValue::Scalar(ScalarValue::Float64(None))
        }
        ColumnarValue::Array(array) => {
            let float_arr = array.as_primitive::<Float64Type>();
            let masked: Float64Array = float_arr
                .iter()
                .map(|v| match v {
                    Some(x) if !x.is_finite() => None,
                    other => other,
                })
                .collect();
            ColumnarValue::Array(Arc::new(masked) as ArrayRef)
        }
        other => other.clone(),
    }
}

/// Preimage for `floor(x) = N`: the set of `x` with `floor(x) = N` is exactly the
/// half-open interval `[N, N + 1)`, which matches the contract of
/// `PreimageResult::Range`. Enables the logical simplifier to rewrite
/// `WHERE floor(col) = N` into `WHERE col >= N AND col < N + 1`, dropping the
/// UDF call in the filter and unlocking downstream pruning (min/max stats,
/// `PruningPredicate`).
///
/// Only the 1-arg form (`floor(x)`) is handled. The 2-arg form
/// `floor(x, target_scale)` returns a Decimal128 whose preimage still fits a
/// half-open interval in principle, but the literal on the RHS would need to
/// match `(precision, scale)` exactly, and the extra complexity isn't justified
/// until we see a real workload.
///
/// Returns `PreimageResult::None` whenever the rewrite would be unsound:
/// - non-integer literal (e.g. `floor(x) = 1.5` has no solution),
/// - NaN / Infinity,
/// - overflow of `N + 1` at the primitive's MAX,
/// - unsupported literal type (strings, bools, etc.).
fn preimage_floor(
    args: &[Expr],
    lit_expr: &Expr,
    info: &SimplifyContext,
) -> Result<PreimageResult> {
    if args.len() != 1 {
        return Ok(PreimageResult::None);
    }
    let Expr::Literal(lit_value, _) = lit_expr else {
        return Ok(PreimageResult::None);
    };
    // Spark's `floor(x)` returns an integer, so a non-integer RHS means the
    // equality has no solution and the rewrite must bail.
    let Some(rhs) = lit_as_integer(lit_value) else {
        return Ok(PreimageResult::None);
    };
    // The returned interval must match `args[0]`'s data type; the rewrite emits
    // `args[0] >= lower AND args[0] < upper` and DataFusion's interval graph
    // rejects mismatched types (e.g. Float64 column vs Int64 literal).
    let input_type = info.get_data_type(&args[0])?;

    let bounds = match &input_type {
        DataType::Float64 => float_bounds(rhs).map(|(lo, hi)| {
            (
                ScalarValue::Float64(Some(lo)),
                ScalarValue::Float64(Some(hi)),
            )
        }),
        DataType::Float32 => float_bounds(rhs).and_then(|(lo, hi)| {
            let lo32 = lo as f32;
            let hi32 = hi as f32;
            // f32 can only represent integers exactly up to 2^24. Reject when
            // the round-trip would collapse the interval.
            if (lo32 as f64) != lo || (hi32 as f64) != hi || hi32 <= lo32 {
                None
            } else {
                Some((
                    ScalarValue::Float32(Some(lo32)),
                    ScalarValue::Float32(Some(hi32)),
                ))
            }
        }),
        DataType::Int8 => int_bounds::<i8>(rhs)
            .map(|(lo, hi)| (ScalarValue::Int8(Some(lo)), ScalarValue::Int8(Some(hi)))),
        DataType::Int16 => int_bounds::<i16>(rhs)
            .map(|(lo, hi)| (ScalarValue::Int16(Some(lo)), ScalarValue::Int16(Some(hi)))),
        DataType::Int32 => int_bounds::<i32>(rhs)
            .map(|(lo, hi)| (ScalarValue::Int32(Some(lo)), ScalarValue::Int32(Some(hi)))),
        DataType::Int64 => int_bounds::<i64>(rhs)
            .map(|(lo, hi)| (ScalarValue::Int64(Some(lo)), ScalarValue::Int64(Some(hi)))),
        DataType::Decimal128(precision, scale) => decimal128_bounds(rhs, *precision, *scale),
        _ => None,
    };

    let Some((lower, upper)) = bounds else {
        return Ok(PreimageResult::None);
    };
    Ok(PreimageResult::Range {
        expr: args[0].clone(),
        interval: Box::new(Interval::try_new(lower, upper)?),
    })
}

/// Extract the literal RHS as an integer. `floor` produces integer-valued
/// results, so a non-integer literal means the equality is unsatisfiable and
/// the rewrite must return `None`.
fn lit_as_integer(v: &ScalarValue) -> Option<i128> {
    match v {
        ScalarValue::Int8(Some(n)) => Some(*n as i128),
        ScalarValue::Int16(Some(n)) => Some(*n as i128),
        ScalarValue::Int32(Some(n)) => Some(*n as i128),
        ScalarValue::Int64(Some(n)) => Some(*n as i128),
        ScalarValue::Float32(Some(n)) if n.is_finite() && n.fract() == 0.0 => Some(*n as i128),
        ScalarValue::Float64(Some(n)) if n.is_finite() && n.fract() == 0.0 => Some(*n as i128),
        ScalarValue::Decimal128(Some(n), _, 0) => Some(*n),
        ScalarValue::Decimal128(Some(n), _, scale) if *scale > 0 => {
            let pow = 10_i128.checked_pow(*scale as u32)?;
            if n % pow == 0 {
                Some(n / pow)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn float_bounds(n: i128) -> Option<(f64, f64)> {
    let lo = n as f64;
    let hi = n.checked_add(1).map(|m| m as f64)?;
    if !lo.is_finite() || !hi.is_finite() || hi <= lo {
        return None;
    }
    Some((lo, hi))
}

fn int_bounds<T>(n: i128) -> Option<(T, T)>
where
    T: TryFrom<i128> + CheckedAdd + num::One,
{
    let lo = T::try_from(n).ok()?;
    let hi = lo.checked_add(&T::one())?;
    Some((lo, hi))
}

fn decimal128_bounds(n: i128, precision: u8, scale: i8) -> Option<(ScalarValue, ScalarValue)> {
    if scale < 0 {
        return None;
    }
    // Convert the integer `n` to Decimal128 stored form at `scale`:
    // value_stored = n * 10^scale. Then width-1 unit at `scale` is also `10^scale`.
    let step = 10_i128.checked_pow(scale as u32)?;
    let lo = n.checked_mul(step)?;
    let hi = lo.checked_add(step)?;
    Some((
        ScalarValue::Decimal128(Some(lo), precision, scale),
        ScalarValue::Decimal128(Some(hi), precision, scale),
    ))
}

/// Scale a Decimal128 value and apply ceil/floor rounding. Uses
/// `pow_wrapping` rather than `pow` because the caller is expected to guard
/// exponents to ≤ 37 (see `return_field_from_args` which rejects
/// `target_scale < -37`, and `DECIMAL128_MAX_SCALE = 38` which bounds input
/// scale). 10^37 fits in i128, so the wrap branch is unreachable under those
/// guards — any actual overflow would indicate a guard bypass (bug).
#[inline]
fn ceil_floor_with_target_scale(name: &str, decimal: i128, scale: i8, target_scale: i32) -> i128 {
    // Round to powers of 10 to the left of decimal point when target_scale < 0
    if target_scale < 0 {
        // Convert to integer with scale 0
        let integer_value = match scale.cmp(&0) {
            std::cmp::Ordering::Greater => {
                let factor = 10_i128.pow_wrapping(scale as u32);
                if matches!(name, "ceil") {
                    div_ceil(decimal, factor)
                } else {
                    div_floor(decimal, factor)
                }
            }
            std::cmp::Ordering::Less => decimal * 10_i128.pow_wrapping((-scale) as u32),
            std::cmp::Ordering::Equal => decimal,
        };
        let pow_factor = 10_i128.pow_wrapping((-target_scale) as u32);
        if matches!(name, "ceil") {
            div_ceil(integer_value, pow_factor) * pow_factor
        } else {
            div_floor(integer_value, pow_factor) * pow_factor
        }
    } else {
        let scale_diff = target_scale - (scale as i32);
        if scale_diff >= 0 {
            // target_scale >= input scale: no rounding needed. `round_decimal_base`
            // keeps the return scale equal to the input scale, so the stored value
            // is unchanged.
            decimal
        } else {
            let abs_diff = (-scale_diff) as u32;
            if matches!(name, "ceil") {
                div_ceil(decimal, 10_i128.pow_wrapping(abs_diff))
            } else {
                div_floor(decimal, 10_i128.pow_wrapping(abs_diff))
            }
        }
    }
}
