// SPDX-License-Identifier: Apache-2.0

use arrow::buffer::OffsetBuffer;
use arrow::datatypes::ArrowNativeType;

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/94d178ebe9674669b32ecd7896b5597f49e90791/datafusion/functions-nested/src/utils.rs#L37-L46
macro_rules! downcast_arg {
    ($ARG:expr_2021, $ARRAY_TYPE:ident) => {{
        $ARG.as_any().downcast_ref::<$ARRAY_TYPE>().ok_or_else(|| {
            ::datafusion_common::DataFusionError::Internal(format!(
                "could not cast to {}",
                ::std::any::type_name::<$ARRAY_TYPE>()
            ))
        })?
    }};
}
// SPDX-SnippetEnd

macro_rules! opt_downcast_arg {
    ($ARG:expr_2021, $ARRAY_TYPE:ident) => {{ $ARG.as_any().downcast_ref::<$ARRAY_TYPE>() }};
}

pub(crate) use downcast_arg;
pub(crate) use opt_downcast_arg;

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/90b55e38b5de8e74a17e24bf7ea7259cbcdab022/datafusion/common/src/utils/mod.rs#L1226-L1235
/// Returns the number of values covered by an offset buffer.
///
/// Slicing a variable-length array narrows its offsets but retains its entire
/// values buffer. Use this span to size output buffers: it counts child elements
/// for lists and bytes for strings/binary arrays, including values in null rows.
/// An empty array still has one offset and therefore a span of zero.
#[inline]
pub(crate) fn offset_span_len<O: ArrowNativeType>(offsets: &OffsetBuffer<O>) -> usize {
    offset_span(offsets).1
}
// SPDX-SnippetEnd

// SPDX-SnippetBegin
// SPDX-License-Identifier: Apache-2.0
// SPDX-SnippetComment: Credit: https://github.com/apache/datafusion/blob/90b55e38b5de8e74a17e24bf7ea7259cbcdab022/datafusion/common/src/utils/mod.rs#L1237-L1254
/// Returns the start and length of the values covered by an offset buffer.
///
/// The returned pair can be passed to [`arrow::array::Array::slice`] to select the visible
/// child values of a sliced list or map. For strings/binary arrays, the start
/// and length are measured in bytes. Values in null rows are included.
/// An empty array has a length of zero but may have a nonzero start.
#[inline]
pub(crate) fn offset_span<O: ArrowNativeType>(offsets: &OffsetBuffer<O>) -> (usize, usize) {
    let start = offsets.first().as_usize();
    (start, offsets.last().as_usize() - start)
}
// SPDX-SnippetEnd
