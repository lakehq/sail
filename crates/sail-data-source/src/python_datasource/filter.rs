/// Filter pushdown system for Python datasources.
///
/// Converts DataFusion expressions to Python filter objects that can be
/// pushed down to Python datasource readers.
///
/// Supports:
/// - Comparison: EqualTo, GreaterThan, LessThan, etc.
/// - Null checks: IsNull, IsNotNull
/// - Membership: In
/// - Logical: Not, And, Or
/// - String patterns: StartsWith, EndsWith, Contains
use datafusion::logical_expr::{Expr, Operator};
use datafusion_common::ScalarValue;
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::types::{PyAnyMethods, PyList, PyTuple};
#[cfg(feature = "python")]
use pyo3::IntoPyObject;

/// Represents a filter that can be pushed to Python.
#[derive(Debug, Clone)]
pub enum PythonFilter {
    // Comparison filters
    EqualTo {
        column: ColumnPath,
        value: FilterValue,
    },
    EqualNullSafe {
        column: ColumnPath,
        value: FilterValue,
    },
    GreaterThan {
        column: ColumnPath,
        value: FilterValue,
    },
    GreaterThanOrEqual {
        column: ColumnPath,
        value: FilterValue,
    },
    LessThan {
        column: ColumnPath,
        value: FilterValue,
    },
    LessThanOrEqual {
        column: ColumnPath,
        value: FilterValue,
    },

    // Null checks
    IsNull {
        column: ColumnPath,
    },
    IsNotNull {
        column: ColumnPath,
    },

    // Membership
    In {
        column: ColumnPath,
        values: Vec<FilterValue>,
    },

    // Logical
    Not {
        child: Box<PythonFilter>,
    },
    And {
        left: Box<PythonFilter>,
        right: Box<PythonFilter>,
    },
    Or {
        left: Box<PythonFilter>,
        right: Box<PythonFilter>,
    },

    // String patterns
    StringStartsWith {
        column: ColumnPath,
        value: String,
    },
    StringEndsWith {
        column: ColumnPath,
        value: String,
    },
    StringContains {
        column: ColumnPath,
        value: String,
    },
}

/// Column path as tuple of strings (for nested columns).
pub type ColumnPath = Vec<String>;

/// Filter value that can be compared.
#[derive(Debug, Clone)]
pub enum FilterValue {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Utf8(String),
    Date32(i32),
    TimestampMicros(i64),
}

impl FilterValue {
    /// Convert from DataFusion ScalarValue.
    pub fn from_scalar(scalar: &ScalarValue) -> Option<Self> {
        match scalar {
            ScalarValue::Null => Some(Self::Null),
            ScalarValue::Boolean(Some(v)) => Some(Self::Bool(*v)),
            ScalarValue::Int32(Some(v)) => Some(Self::Int32(*v)),
            ScalarValue::Int64(Some(v)) => Some(Self::Int64(*v)),
            ScalarValue::Float32(Some(v)) => Some(Self::Float32(*v)),
            ScalarValue::Float64(Some(v)) => Some(Self::Float64(*v)),
            ScalarValue::Utf8(Some(v)) => Some(Self::Utf8(v.clone())),
            ScalarValue::Date32(Some(v)) => Some(Self::Date32(*v)),
            ScalarValue::TimestampMicrosecond(Some(v), _) => Some(Self::TimestampMicros(*v)),
            _ => None,
        }
    }

    /// Convert to Python object.
    #[cfg(feature = "python")]
    pub fn to_python(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::Null => Ok(py.None()),
            Self::Bool(v) => Ok(v.into_pyobject(py)?.to_owned().into_any().unbind()),
            Self::Int32(v) => Ok(v.into_pyobject(py)?.to_owned().into_any().unbind()),
            Self::Int64(v) => Ok(v.into_pyobject(py)?.to_owned().into_any().unbind()),
            Self::Float32(v) => Ok(v.into_pyobject(py)?.to_owned().into_any().unbind()),
            Self::Float64(v) => Ok(v.into_pyobject(py)?.to_owned().into_any().unbind()),
            Self::Utf8(v) => Ok(v.as_str().into_pyobject(py)?.to_owned().into_any().unbind()),
            Self::Date32(v) => Ok(v.into_pyobject(py)?.to_owned().into_any().unbind()),
            Self::TimestampMicros(v) => Ok(v.into_pyobject(py)?.to_owned().into_any().unbind()),
        }
    }
}

/// Convert DataFusion expressions to Python filters.
///
/// Returns a list of filters that can be pushed down and a list of
/// expressions that cannot be pushed down.
pub fn exprs_to_python_filters(exprs: &[Expr]) -> (Vec<PythonFilter>, Vec<Expr>) {
    let mut pushed = Vec::new();
    let mut unpushed = Vec::new();

    for expr in exprs {
        match expr_to_filter(expr) {
            Some(filter) => pushed.push(filter),
            None => unpushed.push(expr.clone()),
        }
    }

    (pushed, unpushed)
}

/// Convert a single expression to a filter.
fn expr_to_filter(expr: &Expr) -> Option<PythonFilter> {
    match expr {
        // Binary expressions
        Expr::BinaryExpr(binary) => {
            let column = expr_to_column(&binary.left)?;
            let value = expr_to_value(&binary.right)?;

            match binary.op {
                Operator::Eq => Some(PythonFilter::EqualTo { column, value }),
                Operator::NotEq => Some(PythonFilter::Not {
                    child: Box::new(PythonFilter::EqualTo { column, value }),
                }),
                Operator::Lt => Some(PythonFilter::LessThan { column, value }),
                Operator::LtEq => Some(PythonFilter::LessThanOrEqual { column, value }),
                Operator::Gt => Some(PythonFilter::GreaterThan { column, value }),
                Operator::GtEq => Some(PythonFilter::GreaterThanOrEqual { column, value }),
                Operator::And => {
                    let left = expr_to_filter(&binary.left)?;
                    let right = expr_to_filter(&binary.right)?;
                    Some(PythonFilter::And {
                        left: Box::new(left),
                        right: Box::new(right),
                    })
                }
                Operator::Or => {
                    let left = expr_to_filter(&binary.left)?;
                    let right = expr_to_filter(&binary.right)?;
                    Some(PythonFilter::Or {
                        left: Box::new(left),
                        right: Box::new(right),
                    })
                }
                _ => None,
            }
        }

        // IS NULL / IS NOT NULL
        Expr::IsNull(inner) => {
            let column = expr_to_column(inner)?;
            Some(PythonFilter::IsNull { column })
        }
        Expr::IsNotNull(inner) => {
            let column = expr_to_column(inner)?;
            Some(PythonFilter::IsNotNull { column })
        }

        // NOT
        Expr::Not(inner) => {
            let child = expr_to_filter(inner)?;
            Some(PythonFilter::Not {
                child: Box::new(child),
            })
        }

        // IN list
        Expr::InList(in_list) => {
            let column = expr_to_column(&in_list.expr)?;
            let values: Option<Vec<FilterValue>> = in_list.list.iter().map(expr_to_value).collect();

            let filter = PythonFilter::In {
                column,
                values: values?,
            };

            if in_list.negated {
                Some(PythonFilter::Not {
                    child: Box::new(filter),
                })
            } else {
                Some(filter)
            }
        }

        // String functions (LIKE, etc.)
        Expr::Like(like) => {
            let column = expr_to_column(&like.expr)?;
            if let Expr::Literal(ScalarValue::Utf8(Some(pattern)), _) = like.pattern.as_ref() {
                // Convert LIKE patterns to appropriate filter
                if pattern.starts_with('%') && pattern.ends_with('%') && pattern.len() > 2 {
                    let inner = &pattern[1..pattern.len() - 1];
                    if !inner.contains('%') && !inner.contains('_') {
                        return Some(PythonFilter::StringContains {
                            column,
                            value: inner.to_string(),
                        });
                    }
                } else if pattern.ends_with('%') && !pattern[..pattern.len() - 1].contains('%') {
                    return Some(PythonFilter::StringStartsWith {
                        column,
                        value: pattern[..pattern.len() - 1].to_string(),
                    });
                } else if pattern.starts_with('%') && !pattern[1..].contains('%') {
                    return Some(PythonFilter::StringEndsWith {
                        column,
                        value: pattern[1..].to_string(),
                    });
                }
            }
            None
        }

        _ => None,
    }
}

/// Extract column path from expression.
fn expr_to_column(expr: &Expr) -> Option<ColumnPath> {
    match expr {
        Expr::Column(col) => Some(vec![col.name.clone()]),
        _ => None,
    }
}

/// Extract value from expression.
fn expr_to_value(expr: &Expr) -> Option<FilterValue> {
    match expr {
        Expr::Literal(scalar, _) => FilterValue::from_scalar(scalar),
        _ => None,
    }
}

// TODO(Phase 2): Activate filter pushdown - see RFC "Filter Pushdown Pipeline"
// These functions convert Rust PythonFilter objects to Python filter class instances
// for passing to DataSourceReader.pushFilters(). Currently all filters are marked
// Unsupported and DataFusion applies post-read filtering.
/// Convert Python filters to Python objects.
#[cfg(feature = "python")]
#[allow(dead_code)] // Reserved for Phase 2 filter pushdown
pub fn filters_to_python(py: Python<'_>, filters: &[PythonFilter]) -> PyResult<Py<PyAny>> {
    let datasource_module = py.import("pysail.spark.datasource.base")?;

    let py_filters: Vec<Py<PyAny>> = filters
        .iter()
        .map(|f| filter_to_python(py, &datasource_module, f))
        .collect::<PyResult<_>>()?;

    Ok(PyList::new(py, py_filters)?.into_any().unbind())
}

#[cfg(feature = "python")]
#[allow(dead_code)] // Reserved for Phase 2 filter pushdown
fn filter_to_python(
    py: Python<'_>,
    module: &Bound<'_, PyAny>,
    filter: &PythonFilter,
) -> PyResult<Py<PyAny>> {
    match filter {
        PythonFilter::EqualTo { column, value } => {
            let cls = module.getattr("EqualTo")?;
            let col_tuple = column_to_python(py, column)?;
            let val = value.to_python(py)?;
            cls.call1((col_tuple, val)).map(|o| o.into_any().unbind())
        }
        PythonFilter::GreaterThan { column, value } => {
            let cls = module.getattr("GreaterThan")?;
            let col_tuple = column_to_python(py, column)?;
            let val = value.to_python(py)?;
            cls.call1((col_tuple, val)).map(|o| o.into_any().unbind())
        }
        PythonFilter::LessThan { column, value } => {
            let cls = module.getattr("LessThan")?;
            let col_tuple = column_to_python(py, column)?;
            let val = value.to_python(py)?;
            cls.call1((col_tuple, val)).map(|o| o.into_any().unbind())
        }
        PythonFilter::IsNull { column } => {
            let cls = module.getattr("IsNull")?;
            let col_tuple = column_to_python(py, column)?;
            cls.call1((col_tuple,)).map(|o| o.into_any().unbind())
        }
        PythonFilter::IsNotNull { column } => {
            let cls = module.getattr("IsNotNull")?;
            let col_tuple = column_to_python(py, column)?;
            cls.call1((col_tuple,)).map(|o| o.into_any().unbind())
        }
        PythonFilter::Not { child } => {
            let cls = module.getattr("Not")?;
            let child_obj = filter_to_python(py, module, child)?;
            cls.call1((child_obj,)).map(|o| o.into_any().unbind())
        }
        PythonFilter::EqualNullSafe { column, value } => {
            let cls = module.getattr("EqualNullSafe")?;
            let col_tuple = column_to_python(py, column)?;
            let val = value.to_python(py)?;
            cls.call1((col_tuple, val)).map(|o| o.into_any().unbind())
        }
        PythonFilter::GreaterThanOrEqual { column, value } => {
            let cls = module.getattr("GreaterThanOrEqual")?;
            let col_tuple = column_to_python(py, column)?;
            let val = value.to_python(py)?;
            cls.call1((col_tuple, val)).map(|o| o.into_any().unbind())
        }
        PythonFilter::LessThanOrEqual { column, value } => {
            let cls = module.getattr("LessThanOrEqual")?;
            let col_tuple = column_to_python(py, column)?;
            let val = value.to_python(py)?;
            cls.call1((col_tuple, val)).map(|o| o.into_any().unbind())
        }
        PythonFilter::In { column, values } => {
            let cls = module.getattr("In")?;
            let col_tuple = column_to_python(py, column)?;
            let py_values: Vec<Py<PyAny>> = values
                .iter()
                .map(|v| v.to_python(py))
                .collect::<PyResult<_>>()?;
            let values_tuple = PyTuple::new(py, py_values)?;
            cls.call1((col_tuple, values_tuple))
                .map(|o| o.into_any().unbind())
        }
        PythonFilter::And { left, right } => {
            let cls = module.getattr("And")?;
            let left_obj = filter_to_python(py, module, left)?;
            let right_obj = filter_to_python(py, module, right)?;
            cls.call1((left_obj, right_obj))
                .map(|o| o.into_any().unbind())
        }
        PythonFilter::Or { left, right } => {
            let cls = module.getattr("Or")?;
            let left_obj = filter_to_python(py, module, left)?;
            let right_obj = filter_to_python(py, module, right)?;
            cls.call1((left_obj, right_obj))
                .map(|o| o.into_any().unbind())
        }
        PythonFilter::StringStartsWith { column, value } => {
            let cls = module.getattr("StringStartsWith")?;
            let col_tuple = column_to_python(py, column)?;
            cls.call1((col_tuple, value.as_str()))
                .map(|o| o.into_any().unbind())
        }
        PythonFilter::StringEndsWith { column, value } => {
            let cls = module.getattr("StringEndsWith")?;
            let col_tuple = column_to_python(py, column)?;
            cls.call1((col_tuple, value.as_str()))
                .map(|o| o.into_any().unbind())
        }
        PythonFilter::StringContains { column, value } => {
            let cls = module.getattr("StringContains")?;
            let col_tuple = column_to_python(py, column)?;
            cls.call1((col_tuple, value.as_str()))
                .map(|o| o.into_any().unbind())
        }
    }
}

#[cfg(feature = "python")]
#[allow(dead_code)] // Reserved for Phase 2 filter pushdown
fn column_to_python(py: Python<'_>, column: &ColumnPath) -> PyResult<Py<PyAny>> {
    let parts: Vec<&str> = column.iter().map(|s| s.as_str()).collect();
    Ok(PyTuple::new(py, parts)?.into_any().unbind())
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::*;

    use super::*;

    #[test]
    fn test_simple_equality_filter() {
        let expr = col("id").eq(lit(42i32));
        let filter = expr_to_filter(&expr);

        assert!(filter.is_some());
        if let Some(f) = filter {
            assert!(
                matches!(&f, PythonFilter::EqualTo { column, value }
                    if column == &vec!["id".to_string()] && matches!(value, FilterValue::Int32(42))
                ),
                "Expected EqualTo filter with id=42, got {:?}",
                f
            );
        }
    }

    #[test]
    fn test_comparison_filters() {
        let expr = col("age").gt(lit(18i32));
        let filter = expr_to_filter(&expr);

        assert!(matches!(filter, Some(PythonFilter::GreaterThan { .. })));
    }

    #[test]
    fn test_null_filters() {
        let expr = col("name").is_null();
        let filter = expr_to_filter(&expr);

        assert!(matches!(filter, Some(PythonFilter::IsNull { .. })));
    }

    #[test]
    fn test_unsupported_filter() {
        // Complex expression that can't be pushed down
        let expr = col("a").add(col("b")).eq(lit(10i32));
        let filter = expr_to_filter(&expr);

        assert!(filter.is_none());
    }
}
