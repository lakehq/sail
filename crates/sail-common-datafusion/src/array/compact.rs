use std::sync::Arc;

use arrow_select::dictionary::garbage_collect_any_dictionary;
use datafusion::arrow::array::{ArrayRef, AsArray, make_array};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_common::scalar::copy_array_data;

/// Compact buffers that `copy_array_data`, `take`, or `interleave` leave shared with the input.
/// Dictionary GC removes unused values without decoding the dictionary, while
/// view GC copies only referenced bytes. Recurse into children so nested payloads
/// cannot retain the source buffers either.
pub fn compact_array(array: ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Utf8View => Ok(Arc::new(array.as_string_view().gc())),
        DataType::BinaryView => Ok(Arc::new(array.as_binary_view().gc())),
        DataType::Dictionary(_, _) => {
            let array = garbage_collect_any_dictionary(array.as_any_dictionary())?;
            let dictionary = array.as_any_dictionary();
            // GC can return the original values if every entry is referenced.
            // Detach offset slices as well as any nested view/dictionary buffers.
            let values = make_array(copy_array_data(&dictionary.values().to_data()));
            Ok(dictionary.with_values(compact_array(values)?))
        }
        data_type if data_type.is_nested() => {
            let data = array.to_data();
            let children = data
                .child_data()
                .iter()
                .map(|child| compact_array(make_array(child.clone())).map(|array| array.to_data()))
                .collect::<Result<Vec<_>>>()?;
            Ok(make_array(
                data.into_builder().child_data(children).build()?,
            ))
        }
        _ => Ok(array),
    }
}
