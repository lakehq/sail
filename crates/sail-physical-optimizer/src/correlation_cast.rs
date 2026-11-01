use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::ScalarUDF;
use datafusion::physical_expr::expressions::{CastExpr, Column};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::{ExecutionPlan, Partitioning, PlanProperties};
use sail_common::utils::datetime::is_fixed_zero_offset_timezone;
use sail_common_datafusion::OPTIMIZER_CORRELATION_KEY_PREFIX;
use sail_function::scalar::correlation_cast::CorrelationCast;

/// Specialize correlation casts after native statistics have guided join selection.
#[derive(Debug)]
pub struct OptimizeCorrelationCasts;

impl PhysicalOptimizerRule for OptimizeCorrelationCasts {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = Arc::new(config.clone());
        plan.transform_up(|plan| rewrite_projection(plan, &config))
            .map(|result| result.data)
    }

    fn name(&self) -> &str {
        "optimize_correlation_casts"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Substitute correlation cast kernels after native statistics have guided costing.
fn rewrite_projection(
    plan: Arc<dyn ExecutionPlan>,
    config: &Arc<ConfigOptions>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(projection) = plan.downcast_ref::<ProjectionExec>() else {
        return Ok(Transformed::no(plan));
    };
    if !projection.expr().iter().any(|x| {
        x.alias
            .strip_prefix(OPTIMIZER_CORRELATION_KEY_PREFIX)
            .is_some_and(|suffix| suffix.starts_with('_'))
    }) {
        return Ok(Transformed::no(plan));
    }
    let schema = projection.input().schema();
    let mut changed = false;
    let expressions = projection
        .expr()
        .iter()
        .map(|expression| {
            let unchanged = || Ok(expression.clone());
            if !expression
                .alias
                .strip_prefix(OPTIMIZER_CORRELATION_KEY_PREFIX)
                .is_some_and(|suffix| suffix.starts_with('_'))
            {
                return unchanged();
            }
            let Some(cast) = expression.expr.downcast_ref::<CastExpr>() else {
                return unchanged();
            };
            if cast.cast_options().safe
                || !cast.expr().is::<Column>()
                || cast.cast_options().format_options
                    != datafusion::common::format::DEFAULT_FORMAT_OPTIONS
            {
                return unchanged();
            }
            let from = cast.expr().data_type(&schema)?;
            let utc_unit = match cast.cast_type() {
                DataType::Timestamp(unit, Some(zone))
                    if is_fixed_zero_offset_timezone(zone)
                        && matches!(from, DataType::Date32 | DataType::Timestamp(_, None)) =>
                {
                    Some(*unit)
                }
                _ => None,
            };
            let result: Arc<dyn PhysicalExpr> = if let Some(unit) = utc_unit {
                // Keep the target unit: a microsecond detour would truncate nanoseconds.
                let scaled: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(
                    Arc::clone(cast.expr()),
                    DataType::Timestamp(unit, None),
                    Some(cast.cast_options().clone()),
                ));
                let integer: Arc<dyn PhysicalExpr> =
                    Arc::new(CastExpr::new(scaled, DataType::Int64, None));
                Arc::new(CastExpr::new_with_target_field(
                    integer,
                    cast.return_field(&schema)?,
                    Some(cast.cast_options().clone()),
                ))
            } else if CorrelationCast::supports(&from, cast.cast_type()) {
                let field = cast.return_field(&schema)?;
                // Scalar-function serialization must preserve both native field contracts.
                let wire_field =
                    CastExpr::new(Arc::clone(cast.expr()), cast.cast_type().clone(), None)
                        .return_field(&schema)?;
                if !field.metadata().is_empty()
                    || field.is_nullable() != cast.nullable(&schema)?
                    || field.metadata() != wire_field.metadata()
                    || field.is_nullable() != wire_field.is_nullable()
                {
                    return unchanged();
                }
                let function = Arc::new(ScalarUDF::from(CorrelationCast::new()));
                Arc::new(ScalarFunctionExpr::new(
                    function.name(),
                    Arc::clone(&function),
                    vec![Arc::clone(cast.expr())],
                    field,
                    Arc::clone(config),
                ))
            } else {
                return unchanged();
            };
            changed = true;
            Ok(ProjectionExpr::new(result, expression.alias.clone()))
        })
        .collect::<Result<Vec<_>>>()?;
    if !changed {
        return Ok(Transformed::no(plan));
    }
    let result: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new_with_schema_metadata(
        expressions,
        Arc::clone(projection.input()),
        &projection.schema(),
    )?);
    // The integer bridge can lose ordering facts; keep the native expression when it does.
    if result.schema() != plan.schema() || !same_properties(plan.properties(), result.properties())
    {
        return Ok(Transformed::no(plan));
    }
    Ok(Transformed::yes(result))
}

pub(crate) fn same_properties(before: &PlanProperties, after: &PlanProperties) -> bool {
    let partitioning = match (&before.partitioning, &after.partitioning) {
        (Partitioning::UnknownPartitioning(a), Partitioning::UnknownPartitioning(b)) => a == b,
        (a, b) => a == b,
    };
    let (left, right) = (&before.eq_properties, &after.eq_properties);
    partitioning
        && left.schema() == right.schema()
        && left.eq_group().iter().eq(right.eq_group().iter())
        && left.oeq_class() == right.oeq_class()
        && left.constraints() == right.constraints()
        && before.output_ordering() == after.output_ordering()
        && before.emission_type == after.emission_type
        && before.boundedness == after.boundedness
        && before.evaluation_type == after.evaluation_type
        && before.scheduling_type == after.scheduling_type
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, ArrayRef, Date32Array, Int64Array};
    use datafusion::arrow::compute::{CastOptions, cast_with_options};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::{CastExpr, Column, TryCastExpr};
    use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::sorts::sort::SortExec;

    const KEY: &str = "__sail_correlation_key_1";
    fn mult(unit: &TimeUnit) -> i64 {
        match unit {
            TimeUnit::Second => 1,
            TimeUnit::Millisecond => 1000,
            TimeUnit::Microsecond => 1_000_000,
            TimeUnit::Nanosecond => 1_000_000_000,
        }
    }
    fn cast(array: &ArrayRef, target: &DataType) -> ArrayRef {
        cast_with_options(
            array,
            target,
            &CastOptions {
                safe: false,
                ..Default::default()
            },
        )
        .unwrap()
    }
    fn projection(
        source: DataType,
        target: DataType,
        alias: &str,
        ordered: bool,
        try_cast: bool,
        duplicate: bool,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("raw", source, true)],
            HashMap::from([("schema-user".to_owned(), "kept".to_owned())]),
        ));
        let mut input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("raw", 0));
        if ordered {
            input = Arc::new(SortExec::new(
                LexOrdering::new([PhysicalSortExpr::new_default(Arc::clone(&column))]).unwrap(),
                input,
            ));
        }
        let field = Arc::new(Field::new("target", target.clone(), true).with_metadata(
            HashMap::from([("field-user".to_owned(), "kept".to_owned())]),
        ));
        let expr: Arc<dyn PhysicalExpr> = if try_cast {
            Arc::new(TryCastExpr::new(column, target))
        } else {
            Arc::new(CastExpr::new_with_target_field(column, field, None))
        };
        let mut exprs = vec![(Arc::clone(&expr), alias.to_owned())];
        if duplicate {
            exprs.push((expr, "ordinary_alias".to_owned()));
        }
        let derived = ProjectionExec::try_new(exprs.clone(), Arc::clone(&input)).unwrap();
        let output_schema =
            Schema::new_with_metadata(derived.schema().fields().clone(), schema.metadata().clone());
        Arc::new(
            ProjectionExec::try_new_with_schema_metadata(exprs, input, &output_schema).unwrap(),
        )
    }
    #[test]
    fn preserves_units_overflow_fields_and_physical_properties()
    -> Result<(), Box<dyn std::error::Error>> {
        let units = [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ];
        let zones = ["UTC", "Etc/UTC", "+00:00", "-00:00", "Z", "UT"];
        let mut properties = 0;
        for source in [
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ] {
            for target in [TimeUnit::Microsecond, TimeUnit::Nanosecond] {
                for ordered in [false, true] {
                    let old = projection(
                        source.clone(),
                        DataType::Timestamp(target, Some("UTC".into())),
                        KEY,
                        ordered,
                        false,
                        false,
                    );
                    let result = super::rewrite_projection(Arc::clone(&old), &Arc::default())?;
                    assert_eq!(result.transformed, !ordered);
                    assert_eq!(old.schema(), result.data.schema());
                    properties += 1;
                }
            }
        }
        let base = DataType::Timestamp(TimeUnit::Microsecond, None);
        let utc = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        for (source, target, alias, try_cast, duplicate) in [
            (base.clone(), utc.clone(), "ordinary", false, false),
            (
                base.clone(),
                DataType::Timestamp(TimeUnit::Microsecond, Some("Europe/London".into())),
                KEY,
                false,
                false,
            ),
            (utc.clone(), utc.clone(), KEY, false, false),
            (base.clone(), base.clone(), KEY, false, false),
            (base.clone(), utc.clone(), KEY, true, false),
            (base.clone(), utc.clone(), KEY, false, true),
        ] {
            let old = projection(source, target, alias, false, try_cast, duplicate);
            let result = super::rewrite_projection(Arc::clone(&old), &Arc::default())?;
            assert!(!result.transformed);
            assert!(Arc::ptr_eq(&old, &result.data));
            properties += 1;
        }
        let mut cases = 0;
        let mut same = 0;
        let mut improved = 0;
        let mut overflow = 0;
        for zone in zones {
            for target in units {
                for source in std::iter::once(DataType::Date32)
                    .chain(units.into_iter().map(|u| DataType::Timestamp(u, None)))
                {
                    let old = projection(
                        source.clone(),
                        DataType::Timestamp(target, Some(zone.into())),
                        KEY,
                        false,
                        false,
                        false,
                    );
                    let result = super::rewrite_projection(Arc::clone(&old), &Arc::default())?;
                    assert!(result.transformed);
                    assert_eq!(old.schema(), result.data.schema());
                    let old = old.downcast_ref::<ProjectionExec>().unwrap();
                    let new = result.data.downcast_ref::<ProjectionExec>().unwrap();
                    let values: Vec<i64> = if source == DataType::Date32 {
                        vec![
                            i32::MIN as i64,
                            -106751992,
                            -106751991,
                            -106752,
                            -106751,
                            -1,
                            0,
                            1,
                            106751,
                            106752,
                            106751991,
                            106751992,
                            i32::MAX as i64,
                        ]
                    } else {
                        vec![
                            i64::MIN,
                            i64::MIN + 1,
                            -1_001,
                            -1_000,
                            -999,
                            -1,
                            0,
                            1,
                            999,
                            1000,
                            1001,
                            i64::MAX - 1,
                            i64::MAX,
                        ]
                    };
                    for value in values {
                        let raw: ArrayRef = if source == DataType::Date32 {
                            Arc::new(Date32Array::from(vec![None, Some(value as i32), None]))
                        } else {
                            cast(
                                &(Arc::new(Int64Array::from(vec![None, Some(value), None]))
                                    as ArrayRef),
                                &source,
                            )
                        };
                        for raw in [Arc::clone(&raw), raw.slice(1, 2), raw.slice(0, 2)] {
                            let batch =
                                RecordBatch::try_new(old.input().schema(), vec![Arc::clone(&raw)])?;
                            let before = old.expr()[0]
                                .expr
                                .evaluate(&batch)
                                .and_then(|v| v.into_array(raw.len()));
                            let after = new.expr()[0]
                                .expr
                                .evaluate(&batch)
                                .and_then(|v| v.into_array(raw.len()));
                            let expected = match source {
                                DataType::Date32 => value.checked_mul(86_400 * mult(&target)),
                                DataType::Timestamp(from, None) => {
                                    if mult(&from) > mult(&target) {
                                        Some(value / (mult(&from) / mult(&target)))
                                    } else {
                                        value.checked_mul(mult(&target) / mult(&from))
                                    }
                                }
                                _ => return Err("unexpected test source type".into()),
                            };
                            match (before, after, expected) {
                                (Ok(a), Ok(b), Some(expected)) => {
                                    assert_eq!(a.to_data(), b.to_data());
                                    let bits = cast(&b, &DataType::Int64);
                                    for v in bits
                                        .as_any()
                                        .downcast_ref::<Int64Array>()
                                        .unwrap()
                                        .iter()
                                        .flatten()
                                    {
                                        assert_eq!(v, expected);
                                    }
                                    same += 1;
                                }
                                (Err(e), Ok(b), Some(expected)) => {
                                    assert!(
                                        e.to_string().contains("Cannot cast timezone")
                                            || e.to_string().contains("Invalid timezone")
                                    );
                                    let bits = cast(&b, &DataType::Int64);
                                    for v in bits
                                        .as_any()
                                        .downcast_ref::<Int64Array>()
                                        .unwrap()
                                        .iter()
                                        .flatten()
                                    {
                                        assert_eq!(v, expected);
                                    }
                                    improved += 1;
                                }
                                (Err(a), Err(b), None) => {
                                    assert_eq!(a.to_string(), b.to_string());
                                    overflow += 1;
                                }
                                x => {
                                    return Err(format!(
                                        "unexpected {source:?}->{target:?} {value} {zone}: {x:?}"
                                    )
                                    .into());
                                }
                            }
                            cases += 1;
                        }
                    }
                }
            }
        }
        println!(
            "PASS actual staged helper: property/decline cases={properties}; runtime cases={cases}; exact={same}; valid improvements={improved}; identical overflow errors={overflow}"
        );
        Ok(())
    }
    fn column() -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new("raw", 0))
    }
    fn correlation_projection(
        from: DataType,
        to: DataType,
        case: &str,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let nullable = case != "notnull";
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("raw", from.clone(), nullable)],
            HashMap::from([("schema_note".into(), "kept".into())]),
        ));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let metadata = if case == "metadata" {
            HashMap::from([("user_note".into(), "kept".into())])
        } else {
            HashMap::new()
        };
        let target = Arc::new(
            Field::new("custom_target", to.clone(), nullable && case != "mismatch")
                .with_metadata(metadata),
        );
        let options = datafusion::arrow::compute::CastOptions {
            safe: case == "safe",
            format_options: if case == "custom" {
                datafusion::common::format::DEFAULT_FORMAT_OPTIONS.with_null("NULL")
            } else {
                datafusion::common::format::DEFAULT_FORMAT_OPTIONS
            },
        };
        let child = if case == "indirect" {
            Arc::new(CastExpr::new(column(), from, None)) as Arc<dyn PhysicalExpr>
        } else {
            column()
        };
        let expr: Arc<dyn PhysicalExpr> = if case == "try" {
            Arc::new(TryCastExpr::new(child, to))
        } else {
            Arc::new(CastExpr::new_with_target_field(
                child,
                target,
                Some(options),
            ))
        };
        let key = if case == "ordinary" {
            "ordinary"
        } else {
            "__sail_correlation_key_1"
        };
        let mut exprs = vec![(Arc::clone(&expr), key.into())];
        if case == "duplicate" {
            exprs.push((expr, "ordinary_copy".into()));
        }
        let derived = ProjectionExec::try_new(exprs.clone(), Arc::clone(&input))?;
        let out =
            Schema::new_with_metadata(derived.schema().fields().clone(), schema.metadata().clone());
        Ok(Arc::new(ProjectionExec::try_new_with_schema_metadata(
            exprs, input, &out,
        )?))
    }
    #[test]
    fn correlation_cast_preserves_fields_and_declines_unsupported_contracts()
    -> datafusion::common::Result<()> {
        let mut accepted = 0;
        let mut declined = 0;
        for (from, to) in [
            (DataType::Float32, DataType::Float64),
            (DataType::Decimal128(10, 2), DataType::Float64),
            (DataType::Decimal128(10, 2), DataType::Decimal128(9, 1)),
            (DataType::Decimal128(10, 2), DataType::Decimal128(18, 1)),
            (DataType::Float64, DataType::Decimal128(12, 2)),
            (DataType::Float64, DataType::Decimal128(30, 15)),
            (DataType::Float32, DataType::Decimal128(12, 2)),
            (DataType::Int32, DataType::Decimal128(9, 0)),
            (DataType::Int64, DataType::Decimal128(38, 20)),
            (DataType::UInt8, DataType::Decimal128(9, 0)),
            (DataType::UInt16, DataType::Decimal128(9, 0)),
            (DataType::UInt32, DataType::Decimal128(9, 0)),
            (DataType::UInt64, DataType::Decimal128(38, 20)),
            (DataType::Utf8, DataType::Float64),
            (DataType::LargeUtf8, DataType::Float64),
            (DataType::Utf8View, DataType::Float64),
            (DataType::Utf8, DataType::Decimal128(19, 0)),
            (DataType::LargeUtf8, DataType::Decimal128(19, 5)),
            (DataType::Utf8View, DataType::Decimal128(38, 38)),
            (DataType::Decimal128(19, 0), DataType::Int64),
            (DataType::Decimal128(10, 2), DataType::Int64),
            (DataType::Decimal128(10, 2), DataType::Int32),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Date32,
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                DataType::Date32,
            ),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some("Etc/UTC".into())),
                DataType::Date32,
            ),
        ] {
            for case in [
                "regular",
                "notnull",
                "metadata",
                "mismatch",
                "safe",
                "custom",
                "try",
                "indirect",
                "ordinary",
                "duplicate",
            ] {
                let old = correlation_projection(from.clone(), to.clone(), case)?;
                let new = super::rewrite_projection(Arc::clone(&old), &Arc::default())?.data;
                assert_eq!(old.schema(), new.schema(), "schema {from:?}->{to:?} {case}");
                let eligible = matches!(case, "regular" | "notnull");
                assert_eq!(
                    !Arc::ptr_eq(&old, &new),
                    eligible,
                    "eligibility {from:?}->{to:?} {case}"
                );
                if !eligible {
                    declined += 1;
                    continue;
                }
                accepted += 1;
                let a = old.downcast_ref::<ProjectionExec>().unwrap();
                let b = new.downcast_ref::<ProjectionExec>().unwrap();
                assert!(
                    b.expr()[0]
                        .expr
                        .is::<datafusion::physical_expr::ScalarFunctionExpr>()
                );
                assert_eq!(
                    a.expr()[0].expr.nullable(&a.input().schema())?,
                    b.expr()[0].expr.nullable(&b.input().schema())?
                );
                assert_eq!(
                    a.expr()[0].expr.return_field(&a.input().schema())?,
                    b.expr()[0].expr.return_field(&b.input().schema())?
                );
            }
        }
        assert_eq!((accepted, declined), (50, 200));
        Ok(())
    }
}

#[cfg(test)]
mod codec_contracts {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::common::Result;
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::physical_expr::expressions::{CastExpr, Column};
    use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::prelude::SessionContext;
    use sail_function::scalar::correlation_cast::CorrelationCast;

    use super::OptimizeCorrelationCasts;

    const KEY: &str = "__sail_correlation_key_codec";

    fn context() -> SessionContext {
        let context = SessionContext::new();
        context.register_udf(ScalarUDF::from(CorrelationCast::new()));
        context
    }

    fn roundtrip(
        plan: Arc<dyn ExecutionPlan>,
        context: &SessionContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let bytes = datafusion_proto::bytes::physical_plan_to_bytes(plan)?;
        datafusion_proto::bytes::physical_plan_from_bytes(&bytes, &context.task_ctx())
    }

    #[test]
    fn kernel_and_utc_fields_keep_the_native_codec_contract() -> Result<()> {
        let context = context();
        let mut cases = vec![
            (DataType::Float32, DataType::Float64, false),
            (DataType::Decimal128(10, 2), DataType::Float64, false),
            (DataType::Float64, DataType::Decimal128(30, 15), false),
            (DataType::Float32, DataType::Decimal128(12, 2), false),
            (DataType::Int32, DataType::Decimal128(9, 0), false),
            (DataType::Int64, DataType::Decimal128(38, 20), false),
            (DataType::UInt8, DataType::Decimal128(9, 0), false),
            (DataType::UInt16, DataType::Decimal128(9, 0), false),
            (DataType::UInt32, DataType::Decimal128(9, 0), false),
            (DataType::UInt64, DataType::Decimal128(38, 20), false),
            (DataType::Utf8, DataType::Float64, false),
            (DataType::Utf8, DataType::Decimal128(19, 0), false),
            (
                DataType::Decimal128(10, 2),
                DataType::Decimal128(9, 1),
                false,
            ),
            (DataType::Decimal128(19, 0), DataType::Int64, false),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Date32,
                false,
            ),
        ];
        for from in [
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ] {
            for unit in [
                TimeUnit::Second,
                TimeUnit::Millisecond,
                TimeUnit::Microsecond,
                TimeUnit::Nanosecond,
            ] {
                for zone in ["UTC", "Z"] {
                    cases.push((
                        from.clone(),
                        DataType::Timestamp(unit, Some(zone.into())),
                        true,
                    ));
                }
            }
        }
        let mut checked = 0;
        let mut activated = 0;
        for (from, to, bridge) in cases {
            for source_metadata in 0..3 {
                for target_metadata in 0..3 {
                    for source_nullable in [false, true] {
                        for target_nullable in [false, true] {
                            let mut field = Field::new("raw", from.clone(), source_nullable);
                            if source_metadata == 1 {
                                field = field
                                    .with_metadata(HashMap::from([("source".into(), "S".into())]));
                            } else if source_metadata == 2 {
                                field = field.with_metadata(HashMap::from([
                                    ("ARROW:extension:name".into(), "custom".into()),
                                    ("ARROW:extension:metadata".into(), "payload".into()),
                                ]));
                            }
                            let raw: Arc<dyn ExecutionPlan> =
                                Arc::new(EmptyExec::new(Arc::new(Schema::new_with_metadata(
                                    vec![field],
                                    HashMap::from([("schema".into(), "retained".into())]),
                                ))));
                            let mut field = Field::new(KEY, to.clone(), target_nullable);
                            if target_metadata == 1 {
                                field = field
                                    .with_metadata(HashMap::from([("target".into(), "T".into())]));
                            }
                            let cast: Arc<dyn PhysicalExpr> = if target_metadata == 2 {
                                Arc::new(CastExpr::new(
                                    Arc::new(Column::new("raw", 0)),
                                    to.clone(),
                                    None,
                                ))
                            } else {
                                Arc::new(CastExpr::new_with_target_field(
                                    Arc::new(Column::new("raw", 0)),
                                    Arc::new(field),
                                    None,
                                ))
                            };
                            let original: Arc<dyn ExecutionPlan> =
                                Arc::new(ProjectionExec::try_new(vec![(cast, KEY.into())], raw)?);
                            let candidate = OptimizeCorrelationCasts
                                .optimize(Arc::clone(&original), &ConfigOptions::default())?;
                            assert_eq!(candidate.schema(), original.schema());
                            // Native type-only CAST strips extension tags, retains ordinary metadata,
                            // and inherits source nullability. Explicit incompatible fields must decline.
                            let expected_active = bridge
                                || (source_metadata != 1
                                    && target_metadata != 1
                                    && (target_metadata == 2
                                        || source_nullable == target_nullable));
                            assert_eq!(
                                !Arc::ptr_eq(&original, &candidate),
                                expected_active,
                                "{from:?}->{to:?} metadata={source_metadata}/{target_metadata} nullable={source_nullable}/{target_nullable}"
                            );
                            if expected_active {
                                activated += 1;
                                let Some(projection) = candidate.downcast_ref::<ProjectionExec>()
                                else {
                                    return datafusion::common::internal_err!(
                                        "expected rewritten projection"
                                    );
                                };
                                assert_eq!(
                                    projection.expr()[0].expr.is::<ScalarFunctionExpr>(),
                                    !bridge
                                );
                                if bridge {
                                    assert!(
                                        projection.expr()[0]
                                            .expr
                                            .downcast_ref::<CastExpr>()
                                            .is_some_and(|cast| cast
                                                .expr()
                                                .downcast_ref::<CastExpr>()
                                                .is_some_and(|integer| integer.cast_type()
                                                    == &DataType::Int64))
                                    );
                                }
                            }
                            assert_eq!(
                                roundtrip(original, &context)?.schema(),
                                roundtrip(candidate, &context)?.schema(),
                                "wire {from:?}->{to:?} metadata={source_metadata}/{target_metadata} nullable={source_nullable}/{target_nullable}"
                            );
                            checked += 1;
                        }
                    }
                }
            }
        }
        assert_eq!((checked, activated), (1404, 1044));
        Ok(())
    }

    #[test]
    fn cached_projection_metadata_matches_the_native_roundtrip() -> Result<()> {
        let context = context();
        for (from, to) in [
            (DataType::Float32, DataType::Float64),
            (
                DataType::Date32,
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            ),
        ] {
            for changed in 0..3 {
                let original_metadata = HashMap::from([("contract".into(), "projection".into())]);
                let child_metadata = HashMap::from([("contract".into(), "child".into())]);
                let schema = Arc::new(Schema::new_with_metadata(
                    vec![
                        Field::new("raw", from.clone(), false),
                        Field::new("other", DataType::Int32, true)
                            .with_metadata(child_metadata.clone()),
                    ],
                    child_metadata.clone(),
                ));
                let raw: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
                let expression: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(
                    Arc::new(Column::new("raw", 0)),
                    to.clone(),
                    None,
                ));
                let expressions = vec![
                    (expression, KEY.into()),
                    (
                        Arc::new(Column::new("other", 1)) as Arc<dyn PhysicalExpr>,
                        "other".into(),
                    ),
                ];
                let derived = ProjectionExec::try_new(expressions.clone(), Arc::clone(&raw))?;
                let mut fields = derived.schema().fields().to_vec();
                let metadata = if changed == 2 {
                    original_metadata.clone()
                } else {
                    fields[changed] = Arc::new(
                        fields[changed]
                            .as_ref()
                            .clone()
                            .with_metadata(original_metadata),
                    );
                    child_metadata
                };
                let original: Arc<dyn ExecutionPlan> =
                    Arc::new(ProjectionExec::try_new_with_schema_metadata(
                        expressions,
                        raw,
                        &Schema::new_with_metadata(fields, metadata),
                    )?);
                let candidate = OptimizeCorrelationCasts
                    .optimize(Arc::clone(&original), &ConfigOptions::default())?;
                assert!(!Arc::ptr_eq(&original, &candidate));
                assert_eq!(candidate.schema(), original.schema());
                let native_wire = roundtrip(Arc::clone(&original), &context)?;
                let candidate_wire = roundtrip(candidate, &context)?;
                assert_ne!(
                    native_wire.schema(),
                    original.schema(),
                    "fixture must exercise the native codec's override loss"
                );
                assert_eq!(
                    native_wire.schema(),
                    candidate_wire.schema(),
                    "{from:?}->{to:?} override={changed}"
                );
            }
        }
        Ok(())
    }
}
