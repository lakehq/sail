use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions::math::abs;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion_common::Result;
use datafusion_expr::expr::WindowFunction;
use datafusion_expr::{AggregateUDF, Expr, ScalarUDF, lit};
use sail_common::spec::PySparkUdfType;

use super::expr_contains_python_udf;
use super::pyspark_batch_collector::PySparkBatchCollectorUDF;
use super::pyspark_cogroup_map_udf::PySparkCoGroupMapUDF;
use super::pyspark_group_map_udf::{PySparkGroupMapMode, PySparkGroupMapUDF};
use super::pyspark_udaf::{PySparkAggregateMode, PySparkGroupAggKind, PySparkGroupAggregateUDF};
use super::pyspark_udf::{PySparkUDF, PySparkUdfKind};
use super::pyspark_unresolved_udf::PySparkUnresolvedUDF;
use crate::array::get_struct_array_type;
use crate::config::PySparkUdfConfig;

#[test]
fn detects_all_python_scalar_kinds_and_nested_calls() -> Result<()> {
    let config = Arc::new(PySparkUdfConfig::default());
    let mut functions = Vec::new();
    for kind in [
        PySparkUdfKind::Batch,
        PySparkUdfKind::ArrowBatch,
        PySparkUdfKind::ScalarPandas,
        PySparkUdfKind::ScalarPandasIter,
        PySparkUdfKind::ScalarArrow,
        PySparkUdfKind::ScalarArrowIter,
    ] {
        functions.push(ScalarUDF::from(PySparkUDF::new(
            kind,
            format!("{kind:?}"),
            vec![],
            true,
            vec![],
            DataType::Int64,
            Arc::clone(&config),
        )));
    }
    // Registered scalar, aggregate, and table functions share this placeholder.
    for kind in [
        PySparkUdfType::Batched,
        PySparkUdfType::GroupedAggPandas,
        PySparkUdfType::Table,
    ] {
        functions.push(ScalarUDF::from(PySparkUnresolvedUDF::new(
            format!("{kind:?}"),
            "3.11".to_string(),
            kind,
            vec![],
            Some(DataType::Int64),
            true,
        )));
    }
    for is_pandas in [false, true] {
        functions.push(ScalarUDF::from(PySparkCoGroupMapUDF::try_new(
            format!("cogroup_{is_pandas}"),
            vec![],
            true,
            vec![],
            vec![],
            vec![],
            vec![],
            get_struct_array_type(&[], &[])?,
            is_pandas,
            Arc::clone(&config),
        )?));
    }
    // Inspect expression structure without resolving or invoking the payloads.
    for function in functions {
        let expression = function.call(vec![]);
        assert!(expr_contains_python_udf(&expression)?, "{function:?}");
        let nested = abs().call(vec![expression]).alias("nested");
        assert!(expr_contains_python_udf(&nested)?, "{function:?}");
    }
    assert!(!expr_contains_python_udf(&lit(1i64))?);
    assert!(!expr_contains_python_udf(&abs().call(vec![lit(1i64)]))?);
    Ok(())
}

#[test]
fn detects_python_aggregates_and_their_window_wrappers() -> Result<()> {
    let config = Arc::new(PySparkUdfConfig::default());
    let mut functions = vec![AggregateUDF::from(PySparkBatchCollectorUDF::new(
        vec![],
        vec![],
    ))];
    for kind in [PySparkGroupAggKind::Pandas, PySparkGroupAggKind::Arrow] {
        for mode in [PySparkAggregateMode::Grouped, PySparkAggregateMode::Window] {
            functions.push(AggregateUDF::from(PySparkGroupAggregateUDF::new(
                kind,
                mode,
                format!("{kind:?}_{mode:?}"),
                vec![],
                true,
                vec![],
                vec![],
                DataType::Int64,
                Arc::clone(&config),
                0,
            )));
        }
    }
    for is_pandas in [false, true] {
        for is_iter in [false, true] {
            functions.push(AggregateUDF::from(PySparkGroupMapUDF::new(
                format!("group_map_{is_pandas}_{is_iter}"),
                vec![],
                true,
                vec![],
                vec![],
                get_struct_array_type(&[], &[])?,
                PySparkGroupMapMode { is_pandas, is_iter },
                Arc::clone(&config),
            )));
        }
    }
    for function in functions {
        let expression = function.call(vec![]).alias("aggregate");
        assert!(expr_contains_python_udf(&expression)?, "{function:?}");
        let window =
            Expr::WindowFunction(Box::new(WindowFunction::new(Arc::new(function), vec![])));
        assert!(expr_contains_python_udf(&window)?, "{window:?}");
    }
    assert!(!expr_contains_python_udf(
        &count_udaf().call(vec![lit(1i64)])
    )?);
    let window = Expr::WindowFunction(Box::new(WindowFunction::new(count_udaf(), vec![lit(1i64)])));
    assert!(!expr_contains_python_udf(&window)?);
    let window = Expr::WindowFunction(Box::new(WindowFunction::new(row_number_udwf(), vec![])));
    assert!(!expr_contains_python_udf(&window)?);
    Ok(())
}
