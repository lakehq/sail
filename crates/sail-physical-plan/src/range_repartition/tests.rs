use datafusion::arrow::array::{
    DictionaryArray, Float64Array, Int8Array, Int64Array, ListArray, StringArray,
};
use datafusion::arrow::datatypes::{Int8Type, Int64Type};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_plan::collect_partitioned;

use super::*;

#[test]
fn typed_keys_preserve_dictionary_nested_null_and_float_comparisons() -> Result<()> {
    let options = [
        SortOptions {
            descending: true,
            nulls_first: false,
        },
        SortOptions {
            descending: false,
            nulls_first: true,
        },
        SortOptions {
            descending: true,
            nulls_first: true,
        },
    ];
    let a = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(0), Some(1), None]),
        Arc::new(StringArray::from(vec!["a", "b"])),
    )?;
    let b = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(1), Some(0), None]),
        Arc::new(StringArray::from(vec!["b", "a"])),
    )?;
    let lists = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1), None]),
        None,
        Some(vec![]),
    ]);
    let floats = Float64Array::from(vec![
        Some(-0.0),
        Some(f64::from_bits(0x7ff8000000000001)),
        None,
    ]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("dict", a.data_type().clone(), true),
        Field::new("nested", lists.data_type().clone(), true),
        Field::new("float", DataType::Float64, true),
    ]));
    let batch_a = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(a),
            Arc::new(lists.clone()),
            Arc::new(floats.clone()),
        ],
    )?;
    let batch_b =
        RecordBatch::try_new(schema, vec![Arc::new(b), Arc::new(lists), Arc::new(floats)])?;
    let keys = options
        .iter()
        .enumerate()
        .map(|(i, o)| (i, *o))
        .collect::<Vec<_>>();
    for row in 0..3 {
        let first = decode_key(&encode_key(&batch_a, &keys, row)?)?;
        let second = decode_key(&encode_key(&batch_b, &keys, row)?)?;
        assert_eq!(compare_rows(&first, &second, &options)?, Ordering::Equal);
        for other in 0..3 {
            let original = (0..3)
                .map(|i| ScalarValue::try_from_array(batch_a.column(i), row))
                .collect::<Result<Vec<_>>>()?;
            let original_other = (0..3)
                .map(|i| ScalarValue::try_from_array(batch_b.column(i), other))
                .collect::<Result<Vec<_>>>()?;
            let decoded_other = decode_key(&encode_key(&batch_b, &keys, other)?)?;
            assert_eq!(
                compare_rows(&first, &decoded_other, &options)?,
                compare_rows(&original, &original_other, &options)?
            );
        }
    }
    let values = vec![
        f64::NEG_INFINITY,
        -0.0,
        0.0,
        f64::INFINITY,
        f64::NAN,
        f64::from_bits(0xfff8000000000001),
    ];
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, false)])),
        vec![Arc::new(Float64Array::from(values.clone()))],
    )?;
    for (i, value) in values.iter().enumerate() {
        let decoded = decode_key(&encode_key(&batch, &[(0, SortOptions::default())], i)?)?;
        let ScalarValue::Float64(Some(actual)) = decoded[0] else {
            return exec_err!("expected float key");
        };
        assert_eq!(actual.to_bits(), value.to_bits());
    }
    Ok(())
}

#[tokio::test]
async fn sampling_is_deterministic_weighted_and_bounded() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(0..10000))],
    )?;
    let input = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
    let sample = RangeSampleExec::try_new(input, vec![(0, SortOptions::default())])?;
    let context = Arc::new(TaskContext::default());
    let first = sample
        .execute(0, context.clone())?
        .try_collect::<Vec<_>>()
        .await?;
    let second = sample
        .execute(0, context.clone())?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(first, second);
    assert_eq!(first[0].num_rows(), PARTITION_SAMPLES);
    let weights = first[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| datafusion::common::exec_datafusion_err!("expected weights"))?;
    assert_eq!(weights.values().iter().sum::<f64>(), 10000.0);
    let mut candidates = Candidates::new(&context);
    let array = first[0]
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| datafusion::common::exec_datafusion_err!("expected keys"))?;
    for i in 0..REDUCER_CANDIDATES + 100 {
        candidates.insert(array.value(i % PARTITION_SAMPLES), 1.0, 100000.0 + i as f64)?;
    }
    assert_eq!(candidates.heap.len(), REDUCER_CANDIDATES);
    assert!(candidates.threshold > 0.0);
    let boundaries = candidates.bounds(8, &[SortOptions::default()])?;
    let decoded = boundaries
        .iter()
        .map(|b| decode_key(b))
        .collect::<Result<Vec<_>>>()?;
    for pair in decoded.windows(2) {
        assert_eq!(
            compare_rows(&pair[0], &pair[1], &[SortOptions::default()])?,
            Ordering::Less
        );
    }
    drop(candidates);
    assert_eq!(context.memory_pool().reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn small_inputs_have_lower_inclusive_splits_and_empty_surplus_slots() -> Result<()> {
    for values in [
        vec![],
        vec![Some(1)],
        vec![None, None, Some(1), Some(1), Some(2), Some(3)],
    ] {
        for keys in [
            vec![],
            vec![PhysicalSortExpr::new_default(Arc::new(Column::new(
                "id", 0,
            )))],
        ] {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(values.clone()))],
            )?;
            let input = MemorySourceConfig::try_new_exec(
                &[vec![], vec![batch], vec![]],
                schema.clone(),
                None,
            )?;
            let plan = Arc::new(SampledRangeRepartitionExec::try_new(
                input,
                keys.clone(),
                6,
            )?);
            assert_eq!(plan.schema(), schema);
            let partitions = collect_partitioned(plan, Arc::new(TaskContext::default())).await?;
            assert_eq!(partitions.len(), 6);
            let rows = partitions
                .iter()
                .map(|b| b.iter().map(RecordBatch::num_rows).sum::<usize>())
                .collect::<Vec<_>>();
            assert_eq!(rows.iter().sum::<usize>(), values.len());
            if keys.is_empty() {
                assert_eq!(rows[0], values.len());
            } else if values.len() == 6 {
                assert_eq!(rows, vec![2, 2, 1, 1, 0, 0]);
            }
        }
    }
    Ok(())
}

#[test]
fn malformed_keys_are_errors_not_panics() -> Result<()> {
    use datafusion::physical_plan::empty::EmptyExec;
    let input = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
    assert!(RangeSampleExec::try_new(input, vec![(usize::MAX, SortOptions::default())]).is_err());
    assert!(decode_key(&[255]).is_err());
    let context = TaskContext::default();
    let mut candidates = Candidates::new(&context);
    assert!(candidates.insert(&[], f64::NAN, 1.0).is_err());
    Ok(())
}
