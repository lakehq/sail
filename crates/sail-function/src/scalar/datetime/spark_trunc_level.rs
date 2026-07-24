use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::error::invalid_arg_count_exec_err;

/// The truncation unit resolved to the granularity it names, in ONE evaluation of the unit.
///
/// The plan builder dispatches on the result with a `CASE`, and that `CASE` has to tell three
/// outcomes apart: the unit named a granularity, the unit was NULL, or the unit matched nothing.
/// An expression cannot: `CASE x WHEN NULL` never matches, so a NULL selector and an unmatched
/// one both fall to the same `ELSE`, and every way of splitting them in the plan -- `coalesce`,
/// `IS NULL` -- mentions the unit a second time. DataFusion will not share a volatile subtree
/// between those mentions (`common_subexpr_eliminate.rs`, `!node.is_volatile_node()`), so a unit
/// such as `IF(rand() < 0.5, NULL, 'YEAR')` would be drawn once per mention and rows would take a
/// branch neither draw called for. Resolving it here keeps it to a single evaluation.
///
/// The table is `date_trunc`'s. `trunc` accepts a subset, and the plan builder omits the finer
/// granularities from its dispatch: a literal one it filters out while building the plan, and a
/// columnar one lands in the `ELSE` -- which is where a unit `trunc` does not accept belongs.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTruncLevel {
    signature: Signature,
}

/// Returned for a NULL unit, to tell it apart from a unit that matched nothing (which yields
/// NULL). Lower case, which `to_uppercase` can never produce, so no real unit collides with it.
pub const TRUNC_LEVEL_NULL: &str = "null";

/// Each granularity with the aliases Spark accepts for it, upper-cased because Spark folds the
/// unit with `toUpperCase(Locale.ROOT)` before matching (`DateTimeUtils.parseTruncLevel`).
const LEVELS: &[(&str, &[&str])] = &[
    ("year", &["YEAR", "YYYY", "YY"]),
    ("quarter", &["QUARTER"]),
    ("month", &["MONTH", "MON", "MM"]),
    ("week", &["WEEK"]),
    ("day", &["DAY", "DD"]),
    ("hour", &["HOUR"]),
    ("minute", &["MINUTE"]),
    ("second", &["SECOND"]),
    ("millisecond", &["MILLISECOND"]),
    ("microsecond", &["MICROSECOND"]),
];

impl Default for SparkTruncLevel {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTruncLevel {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }

    /// `None` for a unit that names no granularity; Spark yields NULL for it rather than
    /// rejecting it.
    ///
    /// Public because the plan builder resolves a LITERAL unit at plan-build time and has to
    /// reach the same answer this function gives at run time. Two alias tables would drift.
    pub fn level(unit: &str) -> Option<&'static str> {
        let folded = unit.to_uppercase();
        LEVELS
            .iter()
            .find(|(_, aliases)| aliases.contains(&folded.as_str()))
            .map(|(level, _)| *level)
    }

    fn resolve(unit: Option<&str>) -> Option<&'static str> {
        match unit {
            None => Some(TRUNC_LEVEL_NULL),
            Some(unit) => Self::level(unit),
        }
    }
}

impl ScalarUDFImpl for SparkTruncLevel {
    fn name(&self) -> &str {
        "spark_trunc_level"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [unit] = args.args.as_slice() else {
            return Err(invalid_arg_count_exec_err(
                "spark_trunc_level",
                (1, 1),
                args.args.len(),
            ));
        };
        match unit {
            ColumnarValue::Scalar(unit) => {
                let unit = match unit {
                    ScalarValue::Utf8(unit)
                    | ScalarValue::LargeUtf8(unit)
                    | ScalarValue::Utf8View(unit) => unit.as_deref(),
                    ScalarValue::Null => None,
                    other => {
                        return Err(exec_datafusion_err!(
                            "`spark_trunc_level` expected a string unit, got {other:?}"
                        ));
                    }
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    Self::resolve(unit).map(str::to_string),
                )))
            }
            ColumnarValue::Array(array) => {
                let levels: StringArray = match array.data_type() {
                    DataType::Utf8 => array.as_string::<i32>().iter().map(Self::resolve).collect(),
                    DataType::LargeUtf8 => {
                        array.as_string::<i64>().iter().map(Self::resolve).collect()
                    }
                    DataType::Utf8View => {
                        array.as_string_view().iter().map(Self::resolve).collect()
                    }
                    other => {
                        return Err(exec_datafusion_err!(
                            "`spark_trunc_level` expected a string unit, got {other:?}"
                        ));
                    }
                };
                Ok(ColumnarValue::Array(Arc::new(levels) as ArrayRef))
            }
        }
    }
}
