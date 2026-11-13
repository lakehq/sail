use std::collections::HashMap;

use arrow::array::{ArrayRef, BinaryArray, DictionaryArray, Int32Array, UInt8Array};
use arrow::datatypes::Int32Type;

use super::tests::{assert_clean, runtime};
use super::*;

/// Lower bound on live allocation sizes, deduplicated by allocation base, not
/// slice address. External Arrow buffers report capacity=0; offset+length still
/// proves that the retained backing allocation extends at least this far.
fn backing_lower_bound(batch: &RecordBatch) -> usize {
    fn visit(data: &ArrayData, allocations: &mut HashMap<usize, usize>) {
        for buffer in data
            .buffers()
            .iter()
            .chain(data.nulls().map(|x| x.buffer()))
        {
            let bytes = buffer.capacity().max(buffer.ptr_offset() + buffer.len());
            allocations
                .entry(buffer.data_ptr().as_ptr() as usize)
                .and_modify(|old| *old = (*old).max(bytes))
                .or_insert(bytes);
        }
        for child in data.child_data() {
            visit(child, allocations);
        }
    }
    let mut allocations = HashMap::new();
    for column in batch.columns() {
        visit(&column.to_data(), &mut allocations);
    }
    allocations.values().sum()
}

#[tokio::test]
async fn replay_dictionary_backings_fit_decoder_budget() -> Result<()> {
    let runtime = runtime(32 * 1024 * 1024)?;
    let dictionary_type =
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::UInt8));
    let schema = Arc::new(arrow::datatypes::Schema::new(
        (0..64)
            .map(|i| arrow::datatypes::Field::new(format!("d{i}"), dictionary_type.clone(), false))
            .chain([arrow::datatypes::Field::new(
                "payload",
                DataType::Binary,
                false,
            )])
            .collect::<Vec<_>>(),
    ));
    let payload = vec![37_u8; 160 * 1024];
    let mut values = [0_u8; 64];
    let mut builder = ReplayPartitionBuilder::try_new(schema.clone(), runtime.clone(), 0)?;
    for step in 0..128 {
        values[step % 64] += 1;
        let mut columns = values
            .iter()
            .map(|&value| {
                Ok(Arc::new(DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![0]),
                    Arc::new(UInt8Array::from(vec![value])),
                )?) as ArrayRef)
            })
            .collect::<Result<Vec<_>>>()?;
        columns.push(Arc::new(BinaryArray::from_vec(vec![payload.as_slice()])));
        builder.append(RecordBatch::try_new(schema.clone(), columns)?)?;
    }
    let data = builder.seal()?;
    let budget = data.export(&runtime)?.decoder_memory_bytes;
    let insufficient = super::tests::runtime(budget - 1)?;
    assert!(data.open_reader(&insufficient).is_err());
    assert_clean(&insufficient)?;
    let mut reader = data.open_reader(&runtime)?;
    let mut peak = 0;
    let mut steps = 0;
    while let Some(batch) = reader.next().await.transpose()? {
        peak = peak.max(backing_lower_bound(&batch));
        assert_eq!(batch.num_rows(), 1);
        steps += 1;
    }
    eprintln!(
        "replay dictionary backing lower_bound_peak={peak} decoder_budget={budget} steps={steps}"
    );
    assert_eq!(steps, 128);
    assert!(
        peak <= budget,
        "actual backing lower bound {peak} exceeds decoder budget {budget}"
    );
    drop(reader);
    drop(data);
    assert_clean(&runtime)
}
