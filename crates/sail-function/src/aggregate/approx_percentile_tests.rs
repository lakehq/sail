use super::*;

use arrow::array::Int64Array;
use arrow::datatypes::{TimeUnit, i256};
use datafusion_physical_expr::expressions::{CastExpr, Column, Literal};
use half::f16;

fn accumulator(
    data_type: DataType,
    percentiles: Vec<f64>,
    array: bool,
    accuracy: usize,
) -> ApproxPercentileAccumulator {
    let return_type = if array {
        DataType::List(Arc::new(Field::new_list_field(data_type.clone(), false)))
    } else {
        data_type.clone()
    };
    ApproxPercentileAccumulator {
        digest: QuantileSummary::new(accuracy),
        percentiles,
        return_type,
        value_type: data_type,
    }
}

fn list_values(value: ScalarValue) -> Result<Vec<ScalarValue>> {
    let ScalarValue::List(list) = value else {
        return datafusion_common::internal_err!("expected a list result");
    };
    let values = list.value(0);
    (0..values.len())
        .map(|i| ScalarValue::try_from_array(&values, i))
        .collect()
}

fn merge_state(
    target: &mut ApproxPercentileAccumulator,
    source: &mut ApproxPercentileAccumulator,
) -> Result<()> {
    let arrays = source
        .state()?
        .iter()
        .map(ScalarValue::to_array)
        .collect::<Result<Vec<_>>>()?;
    target.merge_batch(&arrays)
}

#[test]
fn approx_percentile_all_numeric_arrow_types_and_state() -> Result<()> {
    let types = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal32(9, 2),
        DataType::Decimal64(18, 4),
        DataType::Decimal128(38, 10),
        DataType::Decimal256(76, 20),
    ];
    for data_type in types {
        let values = [10, 0, 2, 1]
            .into_iter()
            .map(|v| ScalarValue::Int64(Some(v)).cast_to(&data_type))
            .collect::<Result<Vec<_>>>()?;
        let mut input = values.clone();
        input.push(ScalarValue::try_from(&data_type)?);
        let input = ScalarValue::iter_to_array(input)?;
        let mut partial = accumulator(
            data_type.clone(),
            vec![1.0, 0.5, 0.4, 0.1, 0.0, 0.5],
            true,
            1000000,
        );
        partial.update_batch(&[input])?;
        let expected = vec![
            values[0].clone(),
            values[3].clone(),
            values[3].clone(),
            values[1].clone(),
            values[1].clone(),
            values[3].clone(),
        ];
        assert_eq!(list_values(partial.evaluate()?)?, expected, "{data_type}");
        let mut merged = accumulator(
            data_type.clone(),
            partial.percentiles.clone(),
            true,
            1000000,
        );
        merge_state(&mut merged, &mut partial)?;
        assert_eq!(list_values(merged.evaluate()?)?, expected, "{data_type}");
        let state = merged.state()?;
        let fields = ApproxPercentileCont::new().state_fields(StateFieldsArgs {
            name: "test",
            input_fields: &[Arc::new(Field::new("input", data_type.clone(), true))],
            return_field: Arc::new(Field::new("result", merged.return_type.clone(), true)),
            ordering_fields: &[],
            is_distinct: false,
        })?;
        assert_eq!(fields.len(), state.len());
        for (field, value) in fields.iter().zip(state.iter()) {
            assert_eq!(field.data_type(), &value.data_type());
        }
        let mut scalar = accumulator(data_type, vec![0.5], false, 1000000);
        merge_state(&mut scalar, &mut merged)?;
        assert_eq!(scalar.evaluate()?, values[3]);
    }
    Ok(())
}

#[test]
fn approx_percentile_retains_large_native_values() -> Result<()> {
    let wide = i256::from_i128(1) << 200;
    let cases = [
        vec![
            ScalarValue::UInt64(Some(u64::MAX)),
            ScalarValue::UInt64(Some(9007199254740993)),
            ScalarValue::UInt64(Some(u64::MAX - 1)),
        ],
        vec![
            ScalarValue::Int64(Some(i64::MAX)),
            ScalarValue::Int64(Some(i64::MIN + 1)),
            ScalarValue::Int64(Some(9007199254740993)),
        ],
        vec![
            ScalarValue::Decimal128(Some(12345678901234567890123456789012345680), 38, 18),
            ScalarValue::Decimal128(Some(12345678901234567890123456789012345678), 38, 18),
            ScalarValue::Decimal128(Some(12345678901234567890123456789012345679), 38, 18),
        ],
        vec![
            ScalarValue::Decimal256(Some(wide + i256::from_i128(3)), 76, 18),
            ScalarValue::Decimal256(Some(wide + i256::from_i128(1)), 76, 18),
            ScalarValue::Decimal256(Some(wide + i256::from_i128(2)), 76, 18),
        ],
    ];
    for values in cases {
        let data_type = values[0].data_type();
        let mut first = accumulator(data_type.clone(), vec![0.0, 0.5, 1.0], true, 1000000);
        first.update_batch(&[ScalarValue::iter_to_array(values[..2].iter().cloned())?])?;
        let mut second = accumulator(data_type, first.percentiles.clone(), true, 1000000);
        second.update_batch(&[ScalarValue::iter_to_array(values[2..].iter().cloned())?])?;
        merge_state(&mut second, &mut first)?;
        assert_eq!(
            list_values(second.evaluate()?)?,
            vec![values[1].clone(), values[2].clone(), values[0].clone()]
        );
    }
    Ok(())
}

#[test]
fn approx_percentile_empty_inputs_and_percentage_array() -> Result<()> {
    for array in [false, true] {
        let mut empty = accumulator(DataType::Int64, vec![0.5], array, 10000);
        assert_eq!(
            empty.evaluate()?,
            ScalarValue::try_from(&empty.return_type)?
        );
        empty.update_batch(&[Arc::new(Int64Array::from(vec![None, None]))])?;
        let mut merged = accumulator(DataType::Int64, vec![0.5], array, 10000);
        merge_state(&mut merged, &mut empty)?;
        assert_eq!(
            merged.evaluate()?,
            ScalarValue::try_from(&merged.return_type)?
        );
    }
    let mut empty_percentages = accumulator(DataType::Int64, vec![], true, 10000);
    empty_percentages.update_batch(&[Arc::new(Int64Array::from(vec![0, 1, 2]))])?;
    assert_eq!(
        empty_percentages.evaluate()?,
        ScalarValue::try_from(&empty_percentages.return_type)?
    );
    Ok(())
}

#[test]
fn approx_percentile_numeric_coercion_boundaries() -> Result<()> {
    let udf = ApproxPercentileCont::new();
    assert_eq!(
        udf.coerce_types(&[DataType::Null, DataType::Null])?,
        vec![DataType::Float64, DataType::Float64]
    );
    for key in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ] {
        let dictionary = DataType::Dictionary(Box::new(key), Box::new(DataType::Decimal256(76, 8)));
        assert_eq!(
            udf.coerce_types(&[dictionary, DataType::Float32, DataType::UInt64])?,
            vec![
                DataType::Decimal256(76, 8),
                DataType::Float64,
                DataType::UInt64
            ]
        );
    }
    let field = Arc::new(Field::new_list_field(DataType::Decimal64(8, 2), true));
    for percentage in [
        DataType::List(field.clone()),
        DataType::LargeList(field.clone()),
        DataType::FixedSizeList(field.clone(), 3),
        DataType::ListView(field.clone()),
        DataType::LargeListView(field),
    ] {
        assert_eq!(
            udf.return_type(&[DataType::UInt64, percentage])?,
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, false)))
        );
    }
    for invalid in [
        DataType::Boolean,
        DataType::Utf8,
        DataType::Binary,
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::List(Arc::new(Field::new_list_field(DataType::Int32, false))),
    ] {
        assert!(udf.coerce_types(&[invalid, DataType::Float64]).is_err());
    }
    assert!(udf.coerce_types(&[]).is_err());
    assert!(udf.coerce_types(&[DataType::Int64]).is_err());
    assert!(
        udf.coerce_types(&[DataType::Int64, DataType::Float64, DataType::Float64])
            .is_err()
    );
    Ok(())
}

fn create_with_parameters(
    percentage: ScalarValue,
    accuracy: ScalarValue,
) -> Result<ApproxPercentileAccumulator> {
    let udf = ApproxPercentileCont::new();
    let schema = Schema::new(vec![Field::new("value", DataType::Int64, true)]);
    let coerced = udf.coerce_types(&[
        DataType::Int64,
        percentage.data_type(),
        accuracy.data_type(),
    ])?;
    let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
        Arc::new(Column::new("value", 0)),
        Arc::new(CastExpr::new(
            Arc::new(Literal::new(percentage)),
            coerced[1].clone(),
            None,
        )),
        Arc::new(CastExpr::new(
            Arc::new(Literal::new(accuracy)),
            coerced[2].clone(),
            None,
        )),
    ];
    let fields = vec![
        Arc::new(schema.field(0).clone()),
        Arc::new(Field::new("percentage", coerced[1].clone(), true)),
        Arc::new(Field::new("accuracy", coerced[2].clone(), true)),
    ];
    let return_type = udf.return_type(
        &fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>(),
    )?;
    udf.create_accumulator(&AccumulatorArgs {
        return_field: Arc::new(Field::new("result", return_type, true)),
        schema: &schema,
        ignore_nulls: false,
        order_bys: &[],
        is_reversed: false,
        name: "test",
        is_distinct: false,
        exprs: &exprs,
        expr_fields: &fields,
    })
}

#[test]
fn approx_percentile_parameter_validation() -> Result<()> {
    for percentile in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.1, 1.1] {
        assert!(
            create_with_parameters(
                ScalarValue::Float64(Some(percentile)),
                ScalarValue::Int64(Some(10000))
            )
            .is_err()
        );
    }
    assert!(
        create_with_parameters(ScalarValue::Float64(None), ScalarValue::Int64(Some(10000)))
            .is_err()
    );
    let null_list = ScalarValue::List(ScalarValue::new_list(
        &[ScalarValue::Float64(Some(0.5)), ScalarValue::Float64(None)],
        &DataType::Float64,
        true,
    ));
    assert!(create_with_parameters(null_list, ScalarValue::Int64(Some(10000))).is_err());
    for accuracy in [
        ScalarValue::UInt64(Some(0)),
        ScalarValue::Int64(Some(-1)),
        ScalarValue::UInt64(Some(2147483648)),
        ScalarValue::UInt64(Some(u64::MAX)),
        ScalarValue::Int64(None),
        ScalarValue::Float64(Some(1.5)),
    ] {
        assert!(create_with_parameters(ScalarValue::Float64(Some(0.5)), accuracy).is_err());
    }
    for accuracy in [1, 10000, i32::MAX as i64] {
        let mut accumulator = create_with_parameters(
            ScalarValue::Float64(Some(0.5)),
            ScalarValue::Int64(Some(accuracy)),
        )?;
        accumulator.update_batch(&[Arc::new(Int64Array::from(vec![0, 1, 2, 10]))])?;
        assert_eq!(
            accumulator.evaluate()?,
            ScalarValue::Int64(Some(if accuracy == 1 { 0 } else { 1 }))
        );
    }
    let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("percentage", 0));
    assert!(get_parameter(&column).is_err());
    Ok(())
}

#[test]
fn approx_percentile_nan_and_infinity_order_is_consistent_after_merge() -> Result<()> {
    for data_type in [DataType::Float16, DataType::Float32, DataType::Float64] {
        let values = [
            f64::NAN,
            f64::NEG_INFINITY,
            -1.0,
            -0.0,
            0.0,
            1.0,
            f64::INFINITY,
            f64::NAN,
        ]
        .into_iter()
        .map(|v| ScalarValue::Float64(Some(v)).cast_to(&data_type))
        .collect::<Result<Vec<_>>>()?;
        let mut first = accumulator(
            data_type.clone(),
            vec![0.0, 0.25, 0.625, 0.75, 1.0],
            true,
            1000000,
        );
        first.update_batch(&[ScalarValue::iter_to_array(values[..4].iter().cloned())?])?;
        let mut second = accumulator(data_type.clone(), first.percentiles.clone(), true, 1000000);
        second.update_batch(&[ScalarValue::iter_to_array(values[4..].iter().cloned())?])?;
        merge_state(&mut second, &mut first)?;
        let output = list_values(second.evaluate()?)?;
        assert_eq!(
            &output[..4],
            &[
                values[1].clone(),
                values[2].clone(),
                values[5].clone(),
                values[6].clone()
            ]
        );
        assert_eq!(
            output[4],
            match data_type {
                DataType::Float16 => ScalarValue::Float16(Some(f16::NAN)),
                DataType::Float32 => ScalarValue::Float32(Some(f32::NAN)),
                _ => ScalarValue::Float64(Some(f64::NAN)),
            }
        );
    }
    Ok(())
}

fn assert_rank_bounds(summary: &QuantileSummary, count: usize, accuracy: usize) {
    assert_eq!(summary.count, count as u64);
    assert_eq!(
        summary.sampled.iter().map(|sample| sample.g).sum::<u64>(),
        count as u64
    );
    assert!(
        summary.sampled.len() < count / 4,
        "summary did not compress: {}",
        summary.sampled.len()
    );
    for percentile in 0..=100 {
        let p = percentile as f64 / 100.0;
        let ScalarValue::Int64(Some(value)) = summary.quantile(p) else {
            unreachable!()
        };
        let rank = value + 1;
        let lower = (((p - 1.0 / accuracy as f64) * count as f64).floor() as i64).max(1);
        let upper = (((p + 1.0 / accuracy as f64) * count as f64).ceil() as i64).min(count as i64);
        assert!(
            (lower..=upper).contains(&rank),
            "p={p}, accuracy={accuracy}, rank={rank}, bounds=[{lower},{upper}]"
        );
    }
}

#[test]
fn approx_percentile_compressed_rank_bounds_and_partition_merges() {
    // A fixed permutation crosses the 50,000-observation head-buffer boundary twice.
    // The rank oracle is independent of the sketch: values are exactly 0..100000.
    const COUNT: usize = 100000;
    for accuracy in [10, 100, 1000] {
        for distribution in 0..3 {
            let mut single = QuantileSummary::new(accuracy);
            let mut partitions = (0..13)
                .map(|_| QuantileSummary::new(accuracy))
                .collect::<Vec<_>>();
            for index in 0..COUNT {
                let value = match distribution {
                    0 => index,
                    1 => COUNT - index - 1,
                    _ => index * 7919 % COUNT,
                };
                single.insert(ScalarValue::Int64(Some(value as i64)));
                partitions[index % 13].insert(ScalarValue::Int64(Some(value as i64)));
            }
            single.compress();
            assert_rank_bounds(&single, COUNT, accuracy);
            let mut merged = QuantileSummary::new(accuracy);
            for mut partial in partitions.into_iter().rev() {
                partial.compress();
                merged.merge(partial.sampled, partial.count);
            }
            assert_rank_bounds(&merged, COUNT, accuracy);
        }
    }
}

#[test]
fn approx_percentile_dictionary_input_and_parameters_are_decoded() -> Result<()> {
    for key in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ] {
        let percentage = ScalarValue::Dictionary(
            Box::new(key.clone()),
            Box::new(ScalarValue::Decimal64(Some(50), 10, 2)),
        );
        let accuracy = ScalarValue::Dictionary(
            Box::new(key.clone()),
            Box::new(ScalarValue::UInt32(Some(1000000))),
        );
        let mut accumulator = create_with_parameters(percentage, accuracy)?;
        let data_type = DataType::Dictionary(Box::new(key.clone()), Box::new(DataType::Int64));
        let source: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(0),
            Some(1),
            None,
            Some(2),
            Some(10),
        ]));
        let encoded = arrow::compute::cast(&source, &data_type)?;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                data_type.clone(),
                true,
            )])),
            vec![encoded],
        )?;
        let target = ApproxPercentileCont::new().coerce_types(&[data_type, DataType::Float64])?;
        let expression = CastExpr::new(Arc::new(Column::new("value", 0)), target[0].clone(), None);
        accumulator
            .update_batch(&[expression.evaluate(&batch)?.into_array(batch.num_rows())?])?;
        assert_eq!(
            accumulator.evaluate()?,
            ScalarValue::Int64(Some(1)),
            "{key}"
        );
        let zero = ScalarValue::Dictionary(Box::new(key), Box::new(ScalarValue::UInt32(Some(0))));
        assert!(create_with_parameters(ScalarValue::Float64(Some(0.5)), zero).is_err());
    }
    Ok(())
}

#[test]
fn approx_percentile_list_containers_are_physically_coerced() -> Result<()> {
    for dictionary in [false, true] {
        let mut values = vec![
            ScalarValue::Float32(Some(1.0)),
            ScalarValue::Float32(Some(0.5)),
            ScalarValue::Float32(Some(0.0)),
        ];
        if dictionary {
            values = values
                .into_iter()
                .map(|value| ScalarValue::Dictionary(Box::new(DataType::Int16), Box::new(value)))
                .collect();
        }
        let element_type = values[0].data_type();
        let field = Arc::new(Field::new_list_field(element_type.clone(), false));
        let list = ScalarValue::List(ScalarValue::new_list(&values, &element_type, false));
        for container in [
            DataType::List(field.clone()),
            DataType::LargeList(field.clone()),
            DataType::FixedSizeList(field.clone(), 3),
            DataType::ListView(field.clone()),
            DataType::LargeListView(field.clone()),
        ] {
            let percentage = list.cast_to(&container)?;
            assert_eq!(percentage.data_type(), container);
            let mut accumulator =
                create_with_parameters(percentage, ScalarValue::Int32(Some(1000000)))?;
            accumulator.update_batch(&[Arc::new(Int64Array::from(vec![0, 1, 2, 10]))])?;
            assert_eq!(
                list_values(accumulator.evaluate()?)?,
                vec![
                    ScalarValue::Int64(Some(10)),
                    ScalarValue::Int64(Some(1)),
                    ScalarValue::Int64(Some(0))
                ],
                "{container}"
            );
        }
    }
    Ok(())
}
